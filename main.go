package main

import (
	"context"
	"fmt"
	"hinode/db"
)

var dbConn = db.GetConnection()

var dbName = "st_sf10"

func main() {
	// st := db.CreateStModel(dbName, dbConn)
	// st.ImportNoLabelData("../hinode_data/merged_and_sorted_eventsSF3.txt")

	st := db.USEst(dbName, dbConn)

	fmt.Print("2010-01-01 - 2010-12-31 -> ")
	st.GetDegreeDistribution("2010-01-01", "2010-12-31")

	fmt.Print("2010-01-01 - 2011-12-31 -> ")
	st.GetDegreeDistribution("2010-01-01", "2011-12-31")

	fmt.Print("2010-01-01 - 2012-12-31 -> ")
	st.GetDegreeDistribution("2010-01-01", "2012-12-31")

	fmt.Println("one hop for 29190: ")
	st.GetOneHopNeighborhood("29190", "2013-12-31")

	fmt.Println("one hop for 928: ")
	st.GetOneHopNeighborhood("928", "2013-12-31")

	fmt.Println("one hop for 38233: ")
	st.GetOneHopNeighborhood("38233", "2013-12-31")

	dbConn.Close(context.Background())
}
