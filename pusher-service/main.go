package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/shunsukw/microservice_sample/event"
	"github.com/tinrab/retry"
)

type Config struct {
	NatsAddress string `envconfig:"NATS_ADDRESS"`
}

func main() {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		log.Fatal(err)
	}

	hub := newHub()
	retry.ForeverSleep(2*time.Second, func(attemp int) error {
		addr := fmt.Sprintf("nats://%s", cfg.NatsAddress)
		es, err := event.NewNats(addr)
		if err != nil {
			log.Println(err)
			return err
		}

		// messageをclientにPushする
		err = es.OnMeowCreated(func(m event.MeowCreatedMessage) {
			log.Printf("Meow received: %v\n", m)
			hub.broadcast(newMeowCreatedMessage(m.ID, m.Body, m.CreatedAt), nil)
		})
		if err != nil {
			log.Println(err)
			return err
		}
		event.SetEventStore(es)
		return nil
	})
	defer event.Close()

	go hub.run()
	http.HandleFunc("/pusher", hub.handleWebSocket)
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}
}
