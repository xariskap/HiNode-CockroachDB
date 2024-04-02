package main

import (
	"context"
	"fmt"
	"hinode/db"
)

var dbConn = db.GetConnection()

var dbName = "sf3"

func main() {

	//Creating multi table model and importing data
	// mt := db.CreateModel(dbName, dbConn)
	// mt.ImportSF3("../hinode_data/merged_and_sorted_eventsSF3.txt")

	mt := db.USE(dbName, dbConn)
	// _, aliveVertices := mt.GetAliveVertices("2010-01-01", "2024-03-27")
	// for k, v := range aliveVertices {
	// 	fmt.Printf("%s : # of vertices = %d\n", k, len(v))
	// }

	neighborhood16882 := mt.GetOneHopNeighborhood("16882", "2013-12-31")
	fmt.Println("one hop for 16882: ", len(neighborhood16882))

	neighborhood5218 := mt.GetOneHopNeighborhood("5218", "2013-12-31")
	fmt.Println("one hop for 5218: ", len(neighborhood5218))

	neighborhood := mt.GetOneHopNeighborhood("6597069787743", "2013-12-31")
	fmt.Println("one hop for 6597069787743: ", len(neighborhood))

	dbConn.Close(context.Background())
}
