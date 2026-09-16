package main

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

type Broker struct {
	// channels for registering and unregistering clients
	newClients     chan chan string
	closingClients chan chan string
	clients        map[chan string]struct{}
	broadcast      chan string
}

func NewBroker() *Broker {
	return &Broker{
		newClients:     make(chan chan string),
		closingClients: make(chan chan string),
		clients:        make(map[chan string]struct{}),
		broadcast:      make(chan string),
	}
}

func (b *Broker) Start() {
	for {
		select {
		case s := <-b.newClients:
			b.clients[s] = struct{}{}
			log.Printf("New Client Connected. Total clients: %d", len(b.clients))
		case s := <-b.closingClients:
			delete(b.clients, s)
			close(s)
			log.Printf("Client removed. Total clients: %d", len(b.clients))
		case msg := <-b.broadcast:
			for clientChan := range b.clients {
				select {
				case clientChan <- msg:
				default:
					// skip slow clients to avoid blocking the broker loop
					log.Println("Skipping slow client channel")
				}
			}
		}
	}
}

func (b *Broker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	clientChan := make(chan string, 10)
	b.newClients <- clientChan

	defer func() {
		b.closingClients <- clientChan
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-clientChan:
			if !ok {
				return
			}
			_, err := fmt.Fprintf(w, "data: %s\n\n", msg)
			if err != nil {
				log.Printf("Write error: %v", err)
				return
			}

			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
	}
}

func main() {
	broker := NewBroker()

	go broker.Start()
	//
	http.Handle("/events", broker)

	go func() {
		ticker := time.NewTicker(2 * time.Second)

		defer ticker.Stop()

		for t := range ticker.C {
			eventMsg := fmt.Sprintf("The current time is %s", t.Format(time.RFC1123))
			broker.broadcast <- eventMsg
		}
	}()

	log.Println("Server starting on :8080...")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}

}
