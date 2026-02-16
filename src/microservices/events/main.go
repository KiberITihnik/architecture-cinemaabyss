package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	brokers := []string{"kafka:9092"}
	if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
		brokers = []string{envBrokers}
	}

	topics := []string{"movie-events", "user-events", "payment-events"}

	// Создаём writers после получения brokers
	writers := make(map[string]*kafka.Writer)
	for _, topic := range topics {
		writers[topic] = kafka.NewWriter(kafka.WriterConfig{
			Brokers: brokers,
			Topic:   topic,
			// Убираем RequiredAcks, чтобы избежать проблемы с типами
			// BatchTimeout можно оставить
			BatchTimeout: 1 * time.Second,
		})
	}

	// Запуск consumer'ов (опционально; можно убрать, если только публикуете)
	startConsumers(brokers, topics)

	// Регистрация маршрутов
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", publishHandler(writers["movie-events"], "movie-events"))
	http.HandleFunc("/api/events/user", publishHandler(writers["user-events"], "user-events"))
	http.HandleFunc("/api/events/payment", publishHandler(writers["payment-events"], "payment-events"))

	log.Printf("🚀 Events service listening on :%s, publishing to Kafka at %v", port, brokers)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// Передаём writer напрямую — чище и безопаснее
func publishHandler(writer *kafka.Writer, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		eventBytes, _ := json.Marshal(payload)
		key := []byte(r.RemoteAddr)

		err := writer.WriteMessages(context.Background(), kafka.Message{
			Key:   key,
			Value: eventBytes,
		})
		if err != nil {
			log.Printf("❌ Failed to publish to %s: %v", topic, err)
			http.Error(w, "Publish failed", http.StatusInternalServerError)
			return
		}

		log.Printf("📤 Published to %s: %s", topic, string(eventBytes))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "success",
			"topic":  topic,
		})
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // ← 200, не 201!
	w.Write([]byte(`{"status":true}`))
}

func startConsumers(brokers []string, topics []string) {
	for _, topic := range topics {
		go func(t string) {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers: brokers,
				Topic:   t,
				GroupID: "events-service-group",
			})
			log.Printf("✅ Consumer started for topic: %s", t)
			for {
				msg, err := reader.ReadMessage(context.Background())
				if err != nil {
					log.Printf("❌ Error reading from %s: %v", t, err)
					time.Sleep(time.Second)
					continue
				}
				log.Printf("📥 [CONSUMED] Topic=%s | Key=%s | Value=%s",
					t, string(msg.Key), string(msg.Value))
			}
		}(topic)
	}
}