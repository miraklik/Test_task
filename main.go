package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v4"
)

type Message struct {
	ID        int    `json:"id"`
	Content   string `json:"content"`
	Processed bool   `json:"processed"`
}

var conn *pgx.Conn

func main() {
	ctx := context.Background()
	var err error
	conn, err = pgx.Connect(ctx, "postgres://username:password@localhost:5432/dbname")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)

	r := mux.NewRouter()
	r.HandleFunc("/", createMessage).Methods("POST")
	r.HandleFunc("/", getMsgStatic).Methods("GET")

	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))

}

func createMessage(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := conn.QueryRow(ctx, "INSERT INTO messages (content) VALUES ($1) RETURNING id", msg.Content).Scan(&msg.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	go sendTokafka(msg)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(msg)
}

func getMsgStatic(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	var count int
	err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM messages WHERE processed = TRUE").Scan(&count)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	start := map[string]int{"processed_message": count}
	json.NewEncoder(w).Encode(start)
}
