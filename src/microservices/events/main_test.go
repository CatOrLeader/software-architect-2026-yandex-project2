package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type publishedMessage struct {
	topic string
	key   string
	value []byte
}

type fakePublisher struct {
	messages []publishedMessage
	err      error
}

func (p *fakePublisher) Publish(_ context.Context, topic, key string, value []byte) (int32, int64, error) {
	p.messages = append(p.messages, publishedMessage{topic: topic, key: key, value: value})
	return 2, 42, p.err
}

func TestHealth(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/events/health", nil)
	response := httptest.NewRecorder()

	api{publisher: &fakePublisher{}}.routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if !strings.Contains(response.Body.String(), `"status":true`) {
		t.Fatalf("unexpected body: %s", response.Body.String())
	}
}

func TestPublishEvents(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		topic string
		key   string
		body  string
	}{
		{
			name: "movie",
			path: "/api/events/movie", topic: movieTopic, key: "7",
			body: `{"movie_id":7,"title":"Arrival","action":"viewed","user_id":11}`,
		},
		{
			name: "user",
			path: "/api/events/user", topic: userTopic, key: "11",
			body: `{"user_id":11,"username":"test","action":"logged_in","timestamp":"2026-07-27T12:00:00Z"}`,
		},
		{
			name: "payment",
			path: "/api/events/payment", topic: paymentTopic, key: "23",
			body: `{"payment_id":23,"user_id":11,"amount":9.99,"status":"completed","timestamp":"2026-07-27T12:00:00Z"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			publisher := &fakePublisher{}
			request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()

			api{publisher: publisher}.routes().ServeHTTP(response, request)

			if response.Code != http.StatusCreated {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusCreated, response.Body.String())
			}
			if len(publisher.messages) != 1 {
				t.Fatalf("published messages = %d, want 1", len(publisher.messages))
			}
			message := publisher.messages[0]
			if message.topic != test.topic || message.key != test.key {
				t.Fatalf("published to %s with key %s, want %s/%s", message.topic, message.key, test.topic, test.key)
			}
			var envelope event
			if err := json.Unmarshal(message.value, &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Type != test.name || envelope.ID == "" || len(envelope.Payload) == 0 {
				t.Fatalf("unexpected event: %+v", envelope)
			}

			var result eventResponse
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Status != "success" || result.Partition != 2 || result.Offset != 42 {
				t.Fatalf("unexpected response: %+v", result)
			}
		})
	}
}

func TestPublishRejectsInvalidRequest(t *testing.T) {
	publisher := &fakePublisher{}
	request := httptest.NewRequest(http.MethodPost, "/api/events/movie", strings.NewReader(`{"movie_id":7}`))
	response := httptest.NewRecorder()

	api{publisher: publisher}.routes().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if len(publisher.messages) != 0 {
		t.Fatal("invalid event was published")
	}
}

func TestPublishReportsKafkaError(t *testing.T) {
	publisher := &fakePublisher{err: errors.New("Kafka unavailable")}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/events/movie",
		strings.NewReader(`{"movie_id":7,"title":"Arrival","action":"viewed"}`),
	)
	response := httptest.NewRecorder()

	api{publisher: publisher}.routes().ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
}
