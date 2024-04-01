package main

import (
	"context"
	"hinode/db"
)

var dbConn = db.GetConnection()
//var dbName = "hinode"
var testdbName = "testhinode"

func main() {

	//Creating multi table model and importing data
	//mt := db.CreateModel(dbName, dbConn)
	//mt.ImportData("../hinode_data/merged_and_sorted_eventsSF3_extended.txt")

	// Creating testing model
	mt := db.CreateModel(testdbName, dbConn)
	mt.ImportData("../hinode_data/100kdata.txt")

	// mt := db.USE(dbName, dbConn)
	// _, aliveVertices := mt.GetAliveVertices("2010-01-01", "2024-03-27")
	// for k, v := range aliveVertices {
	// 	fmt.Printf("%s : # of vertices = %d\n", k, len(v))
	// }

	// _, degrees := mt.GetDegreeDistribution("2010-01-01", "2024-03-28")
	// fmt.Println(degrees)

	// neighborhood := mt.GetOneHopNeighborhood("16882", "2013-12-31")
	// fmt.Println(len(neighborhood))

	dbConn.Close(context.Background())
}
