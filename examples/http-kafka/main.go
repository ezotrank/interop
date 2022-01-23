package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"

	"github.com/ezotrank/interop"
)

type user struct {
	ID         string `json:"id"`
	Email      string `json:"email"`
	CreatedAt  time.Time
	ApprovedAt time.Time
}

func newStore() *store {
	return &store{
		users: make(map[string]user),
	}
}

type store struct {
	users map[string]user
	mu    sync.RWMutex
}

func (s *store) AddUser(u user) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u.CreatedAt = time.Now()
	s.users[u.ID] = u
}

func (s *store) GetUsers() []user {
	s.mu.RLock()
	defer s.mu.RUnlock()
	users := make([]user, 0, len(s.users))
	for _, u := range s.users {
		users = append(users, u)
	}
	return users
}

func (s *store) SetUserApprovedAt(uid string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[uid]
	if !ok {
		panic("user not found")
	}
	u.ApprovedAt = now
	s.users[uid] = u
}

const (
	broker   = "localhost:9094"
	topic    = "users"
	topicdlq = "users.dlq"
)

//nolint:funlen
func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	st := newStore()

	kwr := &kafka.Writer{
		Addr:  kafka.TCP(broker),
		Topic: topic,
	}

	srv := http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodGet:
				users := st.GetUsers()
				w.Header().Add("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(users); err != nil {
					log.Printf("failed to encode users: %s", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				return
			case http.MethodPost:
				var u user
				if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				st.AddUser(u)

				body, err := json.Marshal(u)
				if err != nil {
					log.Printf("failed to marshal user: %s", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				if err := kwr.WriteMessages(r.Context(),
					kafka.Message{
						Key:   []byte(u.ID),
						Value: body,
					},
				); err != nil {
					log.Printf("failed to write a message: %s", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				return
			default:
				http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
				return
			}
		}),
	}

	inrpt, err := interop.NewInterop(
		[]string{broker},
		"http-kafka",
		interop.Rule{
			Topic:    topic,
			Attempts: 100,
			DLQ:      topicdlq,
			Handler: func(ctx context.Context, msg kafka.Message) error {
				var u user
				if err := json.Unmarshal(msg.Value, &u); err != nil {
					return err
				}

				if n := rand.Intn(100); n != 99 { //nolint:gosec
					return fmt.Errorf("failed to create user in external system, reason: %d", n)
				}

				st.SetUserApprovedAt(u.ID, time.Now())

				return nil
			},
		},
	)
	if err != nil {
		log.Fatalf("failed to create interop: %s", err)
	}

	if err := createTopics([]string{broker}, topic, topicdlq); err != nil {
		log.Fatalf("failed to create topics: %s", err)
	}

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		log.Println("starting interop")
		return inrpt.Start(ctx)
	})
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal("ListenAndServe: ", err)
		}
	}()
	if err := g.Wait(); err != nil {
		log.Panicln(err)
	}
}

func createTopics(brokers []string, topics ...string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers provided")
	}
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka broker: %w", err)
	}

	// Hack to not get a "Not Available: the cluster is in the middle" error in WriteMessages.
	// Create or check if topics exist.
	wg := errgroup.Group{}
	for _, topic := range topics {
		topic := topic
		wg.Go(func() error {
			return conn.CreateTopics(
				kafka.TopicConfig{
					Topic:             topic,
					NumPartitions:     1,
					ReplicationFactor: 1,
				},
			)
		})
	}
	return wg.Wait()
}
