package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"durable-engine/internal/api"
	"durable-engine/internal/storage"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println(
			"No .env file found, using environment variables",
		)
	}

	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s sslmode=%s",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
		os.Getenv("DB_SSLMODE"),
	)

	db, err := storage.NewPostgresDB(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store := storage.NewWorkflowStorage(db)

	handler := api.NewWorkflowHandler(store)

	http.HandleFunc(
		"/api/v1/workflows",
		handler.CreateWorkflow,
	)

	http.HandleFunc(
		"/api/v1/workflows/",
		func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/execute") {
				handler.ExecuteWorkflow(w, r)
				return
			}

			handler.GetWorkflow(w, r)
		},
	)

	log.Println("Server listening on :8080")

	log.Fatal(http.ListenAndServe(":8080", nil))
}
