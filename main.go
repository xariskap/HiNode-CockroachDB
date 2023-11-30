package main

import (
	"fmt"
	"hinode/utils"
)

func main() {
	edge := utils.NewEdge("label", "1", "2", "3", "4")
	edge2 := utils.NewEdge("label", "1", "2", "3", "4")
	// fmt.Println(edge.String())
	vertex := utils.NewVertex()

	hashmap := map[string]string{ //create a hashmap using map literal
		"apple":  "10",
		"mango":  "20",
		"banana": "30", //assign the values to the key
	}

	vertex.AddOutgoingEdge("1", edge)
	vertex.AddIncomingEdge("2", edge2)
	vertex.SetAttributes(hashmap)
	vertexAtrr := vertex.GetAttributes()
	vertexNeigh := vertex.GetNeighbors()
	vertex.AddIncomingEdge("3", edge)
	vertexInc := vertex.GetIncomingEdges()
	fmt.Println("Vertex Attributes: ", vertexAtrr)
	fmt.Println("Vertex Neighbors: ", vertexNeigh)
	fmt.Println("Vertex Incoming: ", vertexInc)
}
