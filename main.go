package main

import (
	"context"
	"fmt"
	"hinode/db"
)

var dbConn = db.GetConnection()

var dbName = "sf3"

func main() {
	mt := db.USEmt(dbName, dbConn)

	// fmt.Print("2010-01-01 - 2010-12-31 -> ")
	// s1 := mt.GetDegreeDistribution("2010-01-01", "2010-12-31")
	// fmt.Println(s1)
	// fmt.Print("2010-01-01 - 2011-12-31 -> ")
	// mt.GetDegreeDistribution("2010-01-01", "2011-12-31")

	fmt.Print("2010-01-01 - 2012-12-31 -> ")
	s1 := mt.GetDegreeDistribution("2010-01-01", "2012-12-31")
	fmt.Println(s1["2011"])


	// fmt.Print("2010-01-01 - 2010-12-31 -> ")
	// s2 := mt.GetDegreeDistributionFetchAllVertices("2010-01-01", "2010-12-31")
	// fmt.Println(s2)

	// fmt.Print("2010-01-01 - 2011-12-31 -> ")
	// mt.GetDegreeDistributionFetchAllVertices("2010-01-01", "2011-12-31")

	fmt.Print("2010-01-01 - 2012-12-31 -> ")
	s2 := mt.GetDegreeDistributionFetchAllVertices("2010-01-01", "2012-12-31")
	fmt.Println(s2["2011"])

	dbConn.Close(context.Background())
}
