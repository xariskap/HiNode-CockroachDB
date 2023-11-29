package main

import (
	"fmt"
	"hinode/db"
	"hinode/utils"
)

func main() {
	edge := utils.NewEdge("label", "1", "2", "3", "4")
	fmt.Println(edge.String())
	db.ConnetToCockroach()
}
