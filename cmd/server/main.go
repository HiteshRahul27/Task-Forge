package main

import (
	"fmt"
	"log"
	"os"

	"durable-engine/internal/storage"

	"github.com/joho/godotenv"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, falling back to system environment variables")
	}

	dsn := fmt.Sprintf("user=%s password='%s' host=%s port=%s dbname=%s sslmode=%s",
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

	log.Println("Connected to database successfully")
}
