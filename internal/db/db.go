package db

import (
	"database/sql"
	"fmt"
	"os"
	"time"
)

func ConnectPGDB() (*sql.DB, error) {
	dsn := os.Getenv("DB_URL")
	var db *sql.DB
	var err error

	// Retry up to 10 times, waiting 3 seconds between attempts
	for i := 1; i <= 10; i++ {
		db, err = sql.Open("pgx", dsn)
		if err != nil {
			fmt.Printf("Attempt %d: failed to open DB: %v\n", i, err)
		} else {
			err = db.Ping()
			if err == nil {
				fmt.Println("Connected to Database!")
				return db, nil
			}
			fmt.Printf("Attempt %d: DB not ready: %v\n", i, err)
		}

		time.Sleep(3 * time.Second)
	}

	// All retries failed
	return nil, fmt.Errorf("could not connect to database after multiple attempts: %w", err)
}
