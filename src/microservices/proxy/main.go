package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixNano())

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}

	monolithURLStr := os.Getenv("MONOLITH_URL")
	if monolithURLStr == "" {
		log.Fatal("MONOLITH_URL is required")
	}
	monolithURL, err := url.Parse(monolithURLStr)
	if err != nil {
		log.Fatalf("Invalid MONOLITH_URL: %v", err)
	}

	moviesServiceURLStr := os.Getenv("MOVIES_SERVICE_URL")
	if moviesServiceURLStr == "" {
		log.Fatal("MOVIES_SERVICE_URL is required")
	}
	moviesServiceURL, err := url.Parse(moviesServiceURLStr)
	if err != nil {
		log.Fatalf("Invalid MOVIES_SERVICE_URL: %v", err)
	}

	gradualMigrationStr := os.Getenv("GRADUAL_MIGRATION")
	gradualMigration := gradualMigrationStr == "true"

	var migrationPercent int
	if gradualMigration {
		migrationPercentStr := os.Getenv("MOVIES_MIGRATION_PERCENT")
		if migrationPercentStr == "" {
			migrationPercent = 100
		} else {
			p, err := strconv.Atoi(migrationPercentStr)
			if err != nil || p < 0 || p > 100 {
				log.Fatalf("MOVIES_MIGRATION_PERCENT must be an integer between 0 and 100, got: %s", migrationPercentStr)
			}
			migrationPercent = p
		}
	} else {
		migrationPercent = 0 // always use monolith
	}

	// Создаём reverse proxy для монолита (fallback)
	monolithProxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = monolithURL.Scheme
			req.URL.Host = monolithURL.Host
			req.URL.Path = req.URL.Path // сохраняем путь
			// Убираем заголовки, которые могут мешать
			req.Header.Del("X-Forwarded-For")
			req.Header.Del("X-Real-IP")
		},
	}

	moviesProxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = moviesServiceURL.Scheme
			req.URL.Host = moviesServiceURL.Host
			req.URL.Path = req.URL.Path
			req.Header.Del("X-Forwarded-For")
			req.Header.Del("X-Real-IP")
		},
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Логируем запрос (опционально)
		log.Printf("Received %s %s", r.Method, r.URL.Path)

		// Только GET /api/movies подвержен миграции
		if r.Method == http.MethodGet && r.URL.Path == "/api/movies" {
			useMoviesService := false
			if gradualMigration {
				// Простой рандом: не детерминированный, но допустим для MVP
				if rand.Intn(100)+1 <= migrationPercent {
					useMoviesService = true
				}
			}

			if useMoviesService {
				log.Println("Routing to movies-service")
				moviesProxy.ServeHTTP(w, r)
				return
			} else {
				log.Println("Routing to monolith")
				monolithProxy.ServeHTTP(w, r)
				return
			}
		}

		// Все остальные запросы — в монолит
		log.Printf("Routing other path to monolith: %s", r.URL.Path)
		monolithProxy.ServeHTTP(w, r)
	})

	log.Printf("Starting proxy on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, handler))
}