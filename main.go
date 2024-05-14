package main

import (
	"context"
	"fmt"
	"hinode/db"
	"runtime"
)

var dbConn = db.GetConnection()

// var dbConns = db.GetConnection()

var dbName = "sf3extended"

// var stdbName = "st_sf3extended"
var memStats runtime.MemStats

func main() {
	mt := db.USEmt(dbName, dbConn)
	//st := db.USEst(stdbName, dbConns)
	defer dbConn.Close(context.Background())
	//defer dbConns.Close(context.Background())

	// fmt.Print("CONCURRENTLY MT-> ")
	// mt.GetDegreeDistributionConcurrently("2010-01-01", "2012-12-31")

	// fmt.Print("CONCURRENTLY ST -> ")
	// st.GetDegreeDistributionConcurrently("2010-01-01", "2012-12-31")

	// fmt.Print("LESS MEMORY MT -> ")
	// mt.GetDegreeDistributionOptimized("2010-01-01", "2012-12-31")

	// fmt.Print("LESS MEMORY ST -> ")
	// st.GetDegreeDistributionOptimized("2010-01-01", "2012-12-31")

	// fmt.Print("MORE MEMORY MT -> ")
	// mt.GetDegreeDistribution("2010-01-01", "2012-12-31")

	// fmt.Print("MORE MEMORY ST -> ")
	// st.GetDegreeDistribution("2010-01-01", "2012-12-31")

	fmt.Print("OLD WAY MT -> ")
	mt.GetDegreeDistributionFetchAllVertices("2010-01-01", "2012-12-31")

	// fmt.Print("OLD WAY ST -> ")
	// st.GetDegreeDistributionFetchAllVertices("2010-01-01", "2012-12-31")

	// mt := db.CreateMtModel(dbName, dbConn)
	// mt.ImportGremlin("gremlin/gremlin.txt")

	// runtime.ReadMemStats(&memStats)

	// fmt.Printf("Total allocated memory (in bytes): %d\n", memStats.Alloc)
	// fmt.Printf("Heap memory (in bytes): %d\n", memStats.HeapAlloc)
	// fmt.Printf("Number of garbage collections: %d\n", memStats.NumGC)
}
