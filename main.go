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
	_, aliveVertices := mt.GetAliveVertices("2010-01-01", "2024-03-27")
	for k, v := range(aliveVertices){
		fmt.Printf("%s : # of vertices = %d\n", k, len(v))
	}

	ll := mt.GetDegreeDistribution("2010-01-01", "2024-03-28")
	fmt.Println(ll["16882"])
	dbConn.Close(context.Background())
}

