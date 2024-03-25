package main

import (
	"context"
	"fmt"
	"hinode/db"
)

var dbConn = db.GetConnection()

func main() {

	// Creating multi table model and importing data
	// mt := db.CreateModel("hinode", dbConn)
	// mt.ImportData("../hinode_data/merged_and_sorted_eventsSF3_extended.txt")

	mt := db.USE("hinode", dbConn)
	alv := mt.GetAliveVertices("2015-01-01", "2018-01-01")
	fmt.Println(len(alv))

	dbConn.Close(context.Background())
}
