package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// type TaxiTripsRecord []struct {
// 	Trip_id                    string `json:"trip_id"`
// 	Trip_start_timestamp       string `json:"trip_start_timestamp"`
// 	Trip_end_timestamp         string `json:"trip_end_timestamp"`
// 	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
// 	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
// 	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
// 	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
// }

func main() {
	// Try a simpler connection string
	db_connection := "host=localhost port=5432 user=postgres password=root dbname=chicago_business_intelligence sslmode=disable"

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
