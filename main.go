package main

import (
	"context"
	"fmt"
	"hinode/db"
)

var dbConn = db.GetConnection()

var dbName = "sf3extended"

func main() {
	mt := db.USEmt(dbName, dbConn)
	defer dbConn.Close(context.Background())

	fmt.Print("CONCURRENTLY -> ")
	mt.GetDegreeDistributionConcurrently("2010-01-01", "2012-12-31")

	fmt.Print("LESS MEMORY -> ")
	mt.GetDegreeDistributionOptimized("2010-01-01", "2012-12-31")

	fmt.Print("MORE MEMORY -> ")
	mt.GetDegreeDistribution("2010-01-01", "2012-12-31")

	fmt.Print("OLD WAY -> ")
	mt.GetDegreeDistributionFetchAllVertices("2010-01-01", "2012-12-31")
}
