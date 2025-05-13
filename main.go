package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq" // PostgreSQL driver
)

func main() {
	// Try a simpler connection string
	db_connection := "host=localhost port=5433 user=postgres password=root dbname=chicago_business_intelligence sslmode=disable"

	fmt.Println("Connecting to:", db_connection)

	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	//test connection
	err = db.Ping()
	if err != nil {
		fmt.Printf("Error pinging database: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Successfully connected to database")
}
