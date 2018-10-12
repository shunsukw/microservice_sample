package main

import (
	"html/template"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/ksuid"
	"github.com/shunsukw/microservice_sample/db"
	"github.com/shunsukw/microservice_sample/event"
	"github.com/shunsukw/microservice_sample/schema"
	"github.com/shunsukw/microservice_sample/util"
)

func createMeowHandler(w http.ResponseWriter, r *http.Request) {
	type response struct {
		ID string `json:"id"`
	}

	ctx := r.Context()

	// パラメータを取得　ここでは投稿の内容
	body := template.HTMLEscapeString(r.FormValue("body"))
	if len(body) < 1 || len(body) > 140 {
		util.ResponseError(w, http.StatusBadRequest, "Invalid body")
		return
	}

	// 投稿を作成
	createdAt := time.Now().UTC()
	id, err := ksuid.NewRandomWithTime(createdAt)
	if err != nil {
		util.ResponseError(w, http.StatusInternalServerError, "failed to create meow")
		return
	}

	meow := schema.Meow{
		ID:        id.String(),
		Body:      body,
		CreatedAt: createdAt,
	}

	// DBに投稿を保存
	if err := db.InsertMeow(ctx, meow); err != nil {
		log.Println(err)
		util.ResponseError(w, http.StatusInternalServerError, "Failed to create meow")
		return
	}

	// NatsにイベントをPublish
	if err := event.PublishMeowCreated(meow); err != nil {
		log.Println(err)
	}

	util.ResponseOk(w, response{ID: meow.ID})
}
