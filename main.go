package main

import (
	"context"
	"fmt"
	"hinode/models"
	"log"
	"time"
	"github.com/jackc/pgx/v5"
)

func main() {

	connectionString := "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	mtModel := models.NewMultiTable("hinode", conn)
	mtModel.CreateSchema()

	start := time.Now()
	mtModel.ParseInput("merged_and_sorted_events.txt")
	parsingTime := time.Since(start)
	fmt.Println("Time elapsed parsing data: ", parsingTime.Seconds(),"seconds")

	start = time.Now()
	mtModel.GetAliveVertices("2011-01-01", "2012-01-01")
	aliveVerticesTime := time.Since(start)
	fmt.Println("Time elapsed getting alive vertices:", aliveVerticesTime.Seconds(), "seconds")
	

	// var bday string
	// row := mtModel.QueryRow("SELECT vattr ->> 'firstName' FROM attributes WHERE vid = '111'")
	// err = row.Scan(&bday)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(bday)

	// rows, _ := mtModel.Query("SELECT * FROM edges")
	// defer rows.Close()

	// var label, source, target, weight, start, end string
	// for rows.Next() {
	// 	err := rows.Scan(&label, &source, &target, &weight, &start, &end)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	fmt.Println(label, source, target, weight, start, end)
	// }

	// fmt.Println(mtModel.GetDegreeDistribution("111", "2010-01-01", "2012-01-23"))

}
