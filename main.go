package main

import (
	"hinode/db"
)

var dbConn = db.GetConnection()

var dbName = "gremlin"

func main() {
	// mt := db.USEmt(dbName, dbConn)
	// st := db.USEst(dbst, dbConns)
	// defer dbConn.Close(context.Background())
	// defer dbConns.Close(context.Background())

	// fmt.Print("CONCURRENTLY -> ")
	// mt.GetDegreeDistributionConcurrently("2010-01-01", "2012-12-31")

	// fmt.Print("LESS MEMORY -> ")
	// mt.GetDegreeDistributionOptimized("2010-01-01", "2012-12-31")

	// fmt.Print("MORE MEMORY -> ")
	// mt.GetDegreeDistribution("2010-01-01", "2012-12-31")

	// fmt.Print("OLD WAY -> ")
	// s1 := mt.GetDegreeDistributionFetchAllVertices("2010-01-01", "2012-12-31")
	// fmt.Println(s1["2010"])

	// fmt.Print("OLD WAY ST-> ")
	// s2 := st.GetDegreeDistributionConcurrently("2010-01-01", "2012-12-31")
	// fmt.Println(s2[2010])

	mt := db.CreateMtModel(dbName, dbConn)
	mt.ImportGremlin("gremlin/gremlin.txt")
}
