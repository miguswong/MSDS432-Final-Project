package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq" // PostgreSQL driver
)

type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentJsonRecords []struct {
	Community_area                             string `json:"community_area"`
	Community_area_name                        string `json:"community_area_name"`
	Birth_rate                                 string `json:"birth_rate"`
	General_fertility_rate                     string `json:"general_fertility_rate"`
	Low_birth_weight                           string `json:"low_birth_weight"`
	Prenatal_care_beginning_in_first_trimester string `json:"prenatal_care_beginning_in_first_trimester"`
	Preterm_births                             string `json:"preterm_births"`
	Teen_birth_rate                            string `json:"teen_birth_rate"`
	Assault_homicide                           string `json:"assault_homicide"`
	Breast_cancer_in_females                   string `json:"breast_cancer_in_females"`
	Cancer_all_sites                           string `json:"cancer_all_sites"`
	Colorectal_cancer                          string `json:"colorectal_cancer"`
	Diabetes_related                           string `json:"diabetes_related"`
	Firearm_related                            string `json:"firearm_related"`
	Infant_mortality_rate                      string `json:"infant_mortality_rate"`
	Lung_cancer                                string `json:"lung_cancer"`
	Prostate_cancer_in_males                   string `json:"prostate_cancer_in_males"`
	Stroke_cerebrovascular_disease             string `json:"stroke_cerebrovascular_disease"`
	Childhood_blood_lead_level_screening       string `json:"childhood_blood_lead_level_screening"`
	Childhood_lead_poisoning                   string `json:"childhood_lead_poisoning"`
	Gonorrhea_in_females                       string `json:"gonorrhea_in_females"`
	Gonorrhea_in_males                         string `json:"gonorrhea_in_males"`
	Tuberculosis                               string `json:"tuberculosis"`
	Below_poverty_level                        string `json:"below_poverty_level"`
	Crowded_housing                            string `json:"crowded_housing"`
	Dependency                                 string `json:"dependency"`
	No_high_school_diploma                     string `json:"no_high_school_diploma"`
	Per_capita_income                          string `json:"per_capita_income"`
	Unemployment                               string `json:"unemployment"`
}

type BuildingPermitsJsonRecords []struct {
	Id                     string `json:"id"`
	Permit_Code            string `json:"permit_"`
	Permit_type            string `json:"permit_type"`
	Review_type            string `json:"review_type"`
	Application_start_date string `json:"application_start_date"`
	Issue_date             string `json:"issue_date"`
	Processing_time        string `json:"processing_time"`
	Street_number          string `json:"street_number"`
	Street_direction       string `json:"street_direction"`
	Street_name            string `json:"street_name"`
	Suffix                 string `json:"suffix"`
	Work_description       string `json:"work_description"`
	Building_fee_paid      string `json:"building_fee_paid"`
	Zoning_fee_paid        string `json:"zoning_fee_paid"`
	Other_fee_paid         string `json:"other_fee_paid"`
	Subtotal_paid          string `json:"subtotal_paid"`
	Building_fee_unpaid    string `json:"building_fee_unpaid"`
	Zoning_fee_unpaid      string `json:"zoning_fee_unpaid"`
	Other_fee_unpaid       string `json:"other_fee_unpaid"`
	Subtotal_unpaid        string `json:"subtotal_unpaid"`
	Building_fee_waived    string `json:"building_fee_waived"`
	Zoning_fee_waived      string `json:"zoning_fee_waived"`
	Other_fee_waived       string `json:"other_fee_waived"`
	Subtotal_waived        string `json:"subtotal_waived"`
	Total_fee              string `json:"total_fee"`
	Contact_1_type         string `json:"contact_1_type"`
	Contact_1_name         string `json:"contact_1_name"`
	Contact_1_city         string `json:"contact_1_city"`
	Contact_1_state        string `json:"contact_1_state"`
	Contact_1_zipcode      string `json:"contact_1_zipcode"`
	Reported_cost          string `json:"reported_cost"`
	Pin1                   string `json:"pin1"`
	Pin2                   string `json:"pin2"`
	Community_area         string `json:"community_area"`
	Census_tract           string `json:"census_tract"`
	Ward                   string `json:"ward"`
	Xcoordinate            string `json:"xcoordinate"`
	Ycoordinate            string `json:"ycoordinate"`
	Latitude               string `json:"latitude"`
	Longitude              string `json:"longitude"`
}

func main() {
	// Try connection
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

	for {
		GetTaxiTripsData(db)
		GetUnemploymentData(db)
		GetBuildingPermitsData(db)

		//Grab data every 24 hours
		time.Sleep(24 * time.Hour)
	}
}

func GetTaxiTripsData(db *sql.DB) {
	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	//Set API key for google geocoder
	apiKey := os.Getenv("GEOCODER_API_KEY")

	if apiKey == "" {
		fmt.Println("GEOCODER_API_KEY is not set")
		os.Exit(1)
	}

	geocoder.ApiKey(apiKey)

	//Create taxi_trips table
	drop_table := `drop table if exists taxi_trips`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`
	_, err = db.Exec(create_table)

	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully created taxi_trips table")

	//Change lmit when ready to run
	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Successfully got taxi_trips data from data source.")

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("Error reading response body: %v\n", err)
		return
	}
	var taxi_trips_list TaxiTripsJsonRecords
	err = json.Unmarshal(body, &taxi_trips_list)
	if err != nil {
		fmt.Printf("Error unmarshaling JSON: %v\n", err)
		return
	}

	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude
		//pickup_centroid_longitude := taxi_trips_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := taxi_trips_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := taxi_trips_list[i].DROPOFF_LONG

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		// Comment the following line while not unit-testing
		fmt.Println(pickup_location)

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}

	}

}

func GetUnemploymentData(db *sql.DB) {

}

func GetBuildingPermitsData(db *sql.DB) {

}
