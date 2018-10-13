package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/kelseyhightower/envconfig"
	"github.com/shunsukw/microservice_sample/db"
	"github.com/shunsukw/microservice_sample/event"
	"github.com/shunsukw/microservice_sample/search"
	"github.com/tinrab/retry"
)

type Config struct {
	PostgresDB           string `envconfig:"POSTGRES_DB"`
	PostgresUser         string `envconfig:"POSTGRES_USER"`
	PostgresPassword     string `envconfig:"POSTGRES_PASSWORD"`
	NatsAddress          string `envconfig:"NATS_ADDRESS"`
	ElasticsearchAddress string `envconfig:"ELASTICSEARCH_ADDRESS"`
}

func newRouter() (router *mux.Router) {
	router = mux.NewRouter()
	router.HandleFunc("/meows", listMeowsHandler).
		Methods("GET")
	router.HandleFunc("/search", searchMeowsHandler).
		Methods("GET")
	return
}

func main() {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Postgresに接続
	retry.ForeverSleep(2*time.Second, func(attempt int) error {
		addr := fmt.Sprintf("postgres://%s:%s@postgres/%s?sslmode=disable", cfg.PostgresUser, cfg.PostgresPassword, cfg.PostgresDB)
		repo, err := db.NewPostgres(addr)
		if err != nil {
			log.Println(err)
			return err
		}
		db.SetRepository(repo)
		return nil
	})
	defer db.Close()

	// Natsに接続
	retry.ForeverSleep(2*time.Second, func(attempt int) error {
		addr := fmt.Sprintf("nats://%s", cfg.NatsAddress)
		es, err := event.NewNats(addr)
		if err != nil {
			log.Println(err)
			return err
		}
		err = es.OnMeowCreated(onMeowCreated)
		if err != nil {
			log.Println(err)
			return err
		}
		event.SetEventStore(es)
		return nil
	})
	defer event.Close()

	// ElasticSearchに接続
	retry.ForeverSleep(2*time.Second, func(attempt int) error {
		addr := fmt.Sprintf("http://%s", cfg.ElasticsearchAddress)
		es, err := search.NewElastic(addr)
		if err != nil {
			log.Println(err)
			return err
		}
		search.SetRepository(es)
		return nil
	})
	defer search.Close()

	router := newRouter()
	if err := http.ListenAndServe(":8080", router); err != nil {
		log.Fatal(err)
	}
}
