package main

import (
	"durable-engine/internal/storage"
	"log"
)

func main() {
	db, err := storage.NewPostgresDB("user=postgres password='hitesh#72' host=localhost port=5432 dbname=taskforge sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	log.Println("Connected to database successfully")
}
