package main

import (
	"fmt"
	"hinode/utils"
)

func main() {
	// Create instances using the constructors
	interval := utils.NewInterval("value1", "0", "5")
	interval2 := utils.NewInterval("value2", "10", "15")
	interval3 := utils.NewInterval("value3", "15", "20")
	var intervalcomb []utils.Interval
	intervalcomb = append(intervalcomb, interval2, interval3)
	edge := utils.NewEdge("label1", "1", "target1", "0", "10")

	// Create a Dianode using the Dianode constructor
	dianode := utils.NewDianode(
		"1",
		"0",
		"10",
		map[string][]utils.Interval{"attr1": {interval}},
		map[string][]utils.Edge{"target1": {edge}},
		map[string][]utils.Edge{"source1": {edge}},
	)

	// Print the created instances
	dianode.InsertAttribute("attr1", intervalcomb)
	fmt.Println(dianode.GetAttributes())
	fmt.Println(dianode.Search("attr1", "10"))
}
