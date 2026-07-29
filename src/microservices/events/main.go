package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	movieTopic    = "movie-events"
	userTopic     = "user-events"
	paymentTopic  = "payment-events"
	consumerGroup = "events-service"
)

var topics = []string{movieTopic, userTopic, paymentTopic}

type event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type eventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     event  `json:"event"`
}

type movieEvent struct {
	MovieID     int64    `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      int64    `json:"user_id,omitempty"`
	Rating      float64  `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description string   `json:"description,omitempty"`
}

func (e movieEvent) validate() error {
	if e.MovieID == 0 || strings.TrimSpace(e.Title) == "" || strings.TrimSpace(e.Action) == "" {
		return errors.New("movie_id, title and action are required")
	}
	return nil
}

type userEvent struct {
	UserID    int64  `json:"user_id"`
	Username  string `json:"username,omitempty"`
	Email     string `json:"email,omitempty"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

func (e userEvent) validate() error {
	if e.UserID == 0 || strings.TrimSpace(e.Action) == "" || strings.TrimSpace(e.Timestamp) == "" {
		return errors.New("user_id, action and timestamp are required")
	}
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return errors.New("timestamp must be RFC3339")
	}
	return nil
}

type paymentEvent struct {
	PaymentID  int64   `json:"payment_id"`
	UserID     int64   `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type,omitempty"`
}

func (e paymentEvent) validate() error {
	if e.PaymentID == 0 || e.UserID == 0 || strings.TrimSpace(e.Status) == "" || strings.TrimSpace(e.Timestamp) == "" {
		return errors.New("payment_id, user_id, amount, status and timestamp are required")
	}
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return errors.New("timestamp must be RFC3339")
	}
	return nil
}

type publisher interface {
	Publish(ctx context.Context, topic, key string, value []byte) (int32, int64, error)
}

type kafkaPublisher struct {
	producer sarama.SyncProducer
}

func (p kafkaPublisher) Publish(_ context.Context, topic, key string, value []byte) (int32, int64, error) {
	return p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	})
}

type api struct {
	publisher publisher
}

func (a api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", a.health)
	mux.HandleFunc("/api/events/movie", a.publishMovie)
	mux.HandleFunc("/api/events/user", a.publishUser)
	mux.HandleFunc("/api/events/payment", a.publishPayment)
	return mux
}

func (a api) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"status": true})
}

func (a api) publishMovie(w http.ResponseWriter, r *http.Request) {
	var payload movieEvent
	a.publish(w, r, movieTopic, "movie", func() (any, string, error) {
		if err := decodeJSON(r, &payload); err != nil {
			return nil, "", err
		}
		return payload, strconv.FormatInt(payload.MovieID, 10), payload.validate()
	})
}

func (a api) publishUser(w http.ResponseWriter, r *http.Request) {
	var payload userEvent
	a.publish(w, r, userTopic, "user", func() (any, string, error) {
		if err := decodeJSON(r, &payload); err != nil {
			return nil, "", err
		}
		return payload, strconv.FormatInt(payload.UserID, 10), payload.validate()
	})
}

func (a api) publishPayment(w http.ResponseWriter, r *http.Request) {
	var payload paymentEvent
	a.publish(w, r, paymentTopic, "payment", func() (any, string, error) {
		if err := decodeJSON(r, &payload); err != nil {
			return nil, "", err
		}
		return payload, strconv.FormatInt(payload.PaymentID, 10), payload.validate()
	})
}

func (a api) publish(
	w http.ResponseWriter,
	r *http.Request,
	topic string,
	eventType string,
	parse func() (payload any, key string, err error),
) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	payload, key, err := parse()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode event")
		return
	}

	message := event{
		ID:        newEventID(eventType),
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Payload:   rawPayload,
	}
	value, err := json.Marshal(message)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode event")
		return
	}

	partition, offset, err := a.publisher.Publish(r.Context(), topic, key, value)
	if err != nil {
		log.Printf("level=error operation=publish topic=%s error=%q", topic, err)
		writeError(w, http.StatusInternalServerError, "failed to publish event")
		return
	}

	log.Printf("level=info operation=published topic=%s partition=%d offset=%d event_id=%s", topic, partition, offset, message.ID)
	writeJSON(w, http.StatusCreated, eventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     message,
	})
}

func decodeJSON(r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain a single JSON object")
	}
	return nil
}

func newEventID(eventType string) string {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("%s-%d", eventType, time.Now().UnixNano())
	}
	return eventType + "-" + hex.EncodeToString(random)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

type consumerHandler struct{}

func (consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var consumed event
		if err := json.Unmarshal(message.Value, &consumed); err != nil {
			log.Printf(
				"level=error operation=consume topic=%s partition=%d offset=%d error=%q",
				message.Topic, message.Partition, message.Offset, err,
			)
		} else {
			log.Printf(
				"level=info operation=consumed topic=%s partition=%d offset=%d event_id=%s event_type=%s payload=%s",
				message.Topic, message.Partition, message.Offset, consumed.ID, consumed.Type, string(consumed.Payload),
			)
		}
		session.MarkMessage(message, "")
	}
	return nil
}

func consume(ctx context.Context, group sarama.ConsumerGroup) {
	handler := consumerHandler{}
	for ctx.Err() == nil {
		if err := group.Consume(ctx, topics, handler); err != nil {
			log.Printf("level=error operation=consume error=%q", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

func kafkaConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_7_0_0
	cfg.ClientID = "events-service"
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 10
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	return cfg
}

func connectKafka(brokers []string) (sarama.SyncProducer, sarama.ConsumerGroup, error) {
	var lastErr error
	for attempt := 1; attempt <= 60; attempt++ {
		producer, err := sarama.NewSyncProducer(brokers, kafkaConfig())
		if err == nil {
			group, groupErr := sarama.NewConsumerGroup(brokers, consumerGroup, kafkaConfig())
			if groupErr == nil {
				return producer, group, nil
			}
			_ = producer.Close()
			err = groupErr
		}
		lastErr = err
		log.Printf("waiting for Kafka (attempt %d/60): %v", attempt, err)
		time.Sleep(time.Second)
	}
	return nil, nil, fmt.Errorf("connect to Kafka: %w", lastErr)
}

func main() {
	brokerList := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokerList, ",")
	producer, group, err := connectKafka(brokers)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	defer group.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go consume(ctx, group)

	port := envOrDefault("PORT", "8082")
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           api{publisher: kafkaPublisher{producer: producer}}.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("starting events service on port %s; brokers=%s; topics=%s", port, brokerList, strings.Join(topics, ","))
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
