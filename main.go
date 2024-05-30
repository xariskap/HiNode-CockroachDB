package main

import (
	"context"
	"fmt"
	"hinode/db"
	"time"
)

func getBenchmarks(mtDBNames, stDBNames []string, iter int, oneHopIDs []string, tstart, tend string) {
	var dbConn = db.GetConnection()
	var dbConnst = db.GetConnection()

	defer dbConn.Close(context.Background())
	defer dbConnst.Close(context.Background())

	for i, mtdbname := range mtDBNames {
		mt := db.USEmt(mtdbname, dbConn)

		var avgTimeDegMTf time.Duration
		fmt.Println("Degree Distribution Fetch All Multiple Table: ")
		for i := 0; i < iter+1; i++ {

			fmt.Print("iteration ", i, ": ")
			_, time := mt.GetDegreeDistributionFetchAllVertices(tstart, tend)
			if i == 0 {
				fmt.Println("--------------DOES NOT COUNT--------------")
			}

			if i != 0 {
				avgTimeDegMTf += time
			}
		}
		fmt.Println("Avg time: ", (avgTimeDegMTf / time.Duration(iter)).Seconds())

		// var avgTimeDegMT time.Duration
		// fmt.Println("\nDegree Distribution Multiple Table: ")
		// for i := 0; i < iter+1; i++ {

		// 	fmt.Print("iteration ", i, ": ")
		// 	_, time := mt.GetDegreeDistribution(tstart, tend)
		// 	if i == 0 {
		// 		fmt.Println("--------------DOES NOT COUNT--------------")
		// 	}

		// 	if i != 0 {
		// 		avgTimeDegMT += time
		// 	}
		// }
		// fmt.Println("Avg time: ", (avgTimeDegMT / time.Duration(iter)).Seconds())

		// var avgTimeDegMTl time.Duration
		// fmt.Println("\nDegree Distribution Less Multiple Table: ")
		// for i := 0; i < iter+1; i++ {

		// 	fmt.Print("iteration ", i, ": ")
		// 	_, time := mt.GetDegreeDistribution(tstart, tend)
		// 	if i == 0 {
		// 		fmt.Println("--------------DOES NOT COUNT--------------")
		// 	}

		// 	if i != 0 {
		// 		avgTimeDegMTl += time
		// 	}
		// }
		// fmt.Println("Avg time: ", (avgTimeDegMTl / time.Duration(iter)).Seconds())

		// var avgTimeDegMTc time.Duration
		// fmt.Println("\nDegree Distribution Conc Multiple Table: ")
		// for i := 0; i < iter+1; i++ {

		// 	fmt.Print("iteration ", i, ": ")
		// 	_, time := mt.GetDegreeDistributionConcurrently(tstart, tend)
		// 	if i == 0 {
		// 		fmt.Println("--------------DOES NOT COUNT--------------")
		// 	}

		// 	if i != 0 {
		// 		avgTimeDegMTc += time
		// 	}
		// }
		// fmt.Println("Avg time: ", (avgTimeDegMTc / time.Duration(iter)).Seconds())

		if i == 1 {
			var avgTimeHopMT1 time.Duration
			fmt.Println("\nOne Hop Multi Table on", oneHopIDs[3], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := mt.GetOneHopNeighborhood(oneHopIDs[3], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopMT1 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopMT1 / time.Duration(iter)).Seconds())

			var avgTimeHopMT2 time.Duration
			fmt.Println("\nOne Hop Multi Table on", oneHopIDs[4], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := mt.GetOneHopNeighborhood(oneHopIDs[4], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopMT2 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopMT2 / time.Duration(iter)).Seconds())

			var avgTimeHopMT3 time.Duration
			fmt.Println("\nOne Hop Multi Table on", oneHopIDs[5], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := mt.GetOneHopNeighborhood(oneHopIDs[5], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopMT3 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopMT3 / time.Duration(iter)).Seconds())
		} else {
			var avgTimeHopMT1 time.Duration
			fmt.Println("\nOne Hop Multi Table on", oneHopIDs[0], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := mt.GetOneHopNeighborhood(oneHopIDs[0], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopMT1 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopMT1 / time.Duration(iter)).Seconds())

			var avgTimeHopMT2 time.Duration
			fmt.Println("\nOne Hop Multi Table on", oneHopIDs[1], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := mt.GetOneHopNeighborhood(oneHopIDs[1], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopMT2 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopMT2 / time.Duration(iter)).Seconds())

			var avgTimeHopMT3 time.Duration
			fmt.Println("\nOne Hop Multi Table on", oneHopIDs[2], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := mt.GetOneHopNeighborhood(oneHopIDs[2], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopMT3 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopMT3 / time.Duration(iter)).Seconds())
		}

		fmt.Println("----------------DONE WITH DATASET", mtdbname, "--------------------------------------")
	}

	fmt.Println("---------------------------------------------------------------------------------")

	for i, stdbname := range stDBNames {
		st := db.USEst(stdbname, dbConnst)

		var avgTimeDegSTf time.Duration
		fmt.Println("Degree Distribution Fetch All Single Table: ")
		for i := 0; i < iter+1; i++ {

			fmt.Print("iteration ", i, ": ")
			_, time := st.GetDegreeDistributionFetchAllVertices(tstart, tend)
			if i == 0 {
				fmt.Println("--------------DOES NOT COUNT--------------")
			}

			if i != 0 {
				avgTimeDegSTf += time
			}
		}
		fmt.Println("Avg time: ", (avgTimeDegSTf / time.Duration(iter)).Seconds())

		// var avgTimeDegST time.Duration
		// fmt.Println("\nDegree Distribution Single Table: ")
		// for i := 0; i < iter+1; i++ {

		// 	fmt.Print("iteration ", i, ": ")
		// 	_, time := st.GetDegreeDistribution(tstart, tend)
		// 	if i == 0 {
		// 		fmt.Println("--------------DOES NOT COUNT--------------")
		// 	}

		// 	if i != 0 {
		// 		avgTimeDegST += time
		// 	}
		// }
		// fmt.Println("Avg time: ", (avgTimeDegST / time.Duration(iter)).Seconds())

		// var avgTimeDegSTl time.Duration
		// fmt.Println("\nDegree Distribution Less Single Table: ")
		// for i := 0; i < iter+1; i++ {

		// 	fmt.Print("iteration ", i, ": ")
		// 	_, time := st.GetDegreeDistributionOptimized(tstart, tend)
		// 	if i == 0 {
		// 		fmt.Println("--------------DOES NOT COUNT--------------")
		// 	}

		// 	if i != 0 {
		// 		avgTimeDegSTl += time
		// 	}
		// }
		// fmt.Println("Avg time: ", (avgTimeDegSTl / time.Duration(iter)).Seconds())

		// var avgTimeDegSTc time.Duration
		// fmt.Println("\nDegree Distribution Conc Single Table: ")
		// for i := 0; i < iter+1; i++ {

		// 	fmt.Print("iteration ", i, ": ")
		// 	_, time := st.GetDegreeDistribution(tstart, tend)
		// 	if i == 0 {
		// 		fmt.Println("--------------DOES NOT COUNT--------------")
		// 	}

		// 	if i != 0 {
		// 		avgTimeDegSTc += time
		// 	}
		// }
		// fmt.Println("Avg time: ", (avgTimeDegSTc / time.Duration(iter)).Seconds())

		if i == 1 {
			var avgTimeHopST1 time.Duration
			fmt.Println("\nOne Hop Single Table on", oneHopIDs[3], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := st.GetOneHopNeighborhood(oneHopIDs[3], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopST1 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopST1 / time.Duration(iter)).Seconds())

			var avgTimeHopST2 time.Duration
			fmt.Println("\nOne Hop Single Table on", oneHopIDs[4], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := st.GetOneHopNeighborhood(oneHopIDs[4], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopST2 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopST2 / time.Duration(iter)).Seconds())

			var avgTimeHopST3 time.Duration
			fmt.Println("\nOne Hop Single Table on", oneHopIDs[5], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := st.GetOneHopNeighborhood(oneHopIDs[5], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopST3 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopST3 / time.Duration(iter)).Seconds())
		} else {
			var avgTimeHopST1 time.Duration
			fmt.Println("\nOne Hop Single Table on", oneHopIDs[0], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := st.GetOneHopNeighborhood(oneHopIDs[0], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopST1 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopST1 / time.Duration(iter)).Seconds())

			var avgTimeHopST2 time.Duration
			fmt.Println("\nOne Hop Single Table on", oneHopIDs[1], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := st.GetOneHopNeighborhood(oneHopIDs[1], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopST2 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopST2 / time.Duration(iter)).Seconds())

			var avgTimeHopST3 time.Duration
			fmt.Println("\nOne Hop Single Table on", oneHopIDs[2], ":")
			for i := 0; i < iter+1; i++ {

				fmt.Print("iteration ", i, ": ")
				_, time := st.GetOneHopNeighborhood(oneHopIDs[2], tend)
				if i == 0 {
					fmt.Println("--------------DOES NOT COUNT--------------")
				}

				if i != 0 {
					avgTimeHopST3 += time
				}
			}
			fmt.Println("Avg time: ", (avgTimeHopST3 / time.Duration(iter)).Seconds())
		}

		fmt.Println("----------------DONE WITH DATASET ", stdbname, " --------------------------------------")
	}
}

func getAllBenchmarks(mtDBNames, stDBNames []string, iter int, oneHopIDs []string, tstart string) {
	var endYears = []string{"2010-12-31", "2011-12-31", "2012-12-31"}
	for _, year := range endYears {
		getBenchmarks(mtDBNames, stDBNames, iter, oneHopIDs, tstart, year)

		fmt.Println("\n------COMPLETED FOR ", tstart, "-", year, "---------\n")
	}
}

func insertAllData(mtDBNames, stDBNames, datasets []string) {
	var dbConn = db.GetConnection()
	var dbConnst = db.GetConnection()

	defer dbConn.Close(context.Background())
	defer dbConnst.Close(context.Background())

	// import multiple table databases
	for i, mtdbname := range mtDBNames {
		mt := db.CreateMtModel(mtdbname, dbConn)
		if i == 0 {
			mt.ImportData(datasets[i]) // ONLY FOR SF3EXTENDED
		} else {
			mt.ImportNoLabelData(datasets[i]) // SF3 AND SF10
		}

	}

	// import single table databases
	for i, stdbname := range stDBNames {
		st := db.CreateStModel(stdbname, dbConn)
		if i == 0 {
			st.ImportData(datasets[i]) // ONLY FOR SF3EXTENDED
		} else {
			st.ImportNoLabelData(datasets[i]) // SF3 AND SF10
		}
	}
}

var mtDBNames = []string{"sf3extended", "sf10", "sf3"}
var stDBNames = []string{"st_sf3extended", "st_sf10", "st_sf3"}
var oneHop = []string{"16882", "5218", "6597069787743", "29190", "928", "38233"}
var dataPath = []string{"../test_data/data1.txt", "../test_data/data2.txt", "../test_data/data3.txt"}

func main() {
	insertAllData(mtDBNames, stDBNames, dataPath)
	getAllBenchmarks(mtDBNames, stDBNames, 5, oneHop, "2010-01-01")
}
