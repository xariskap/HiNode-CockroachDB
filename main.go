package main

import (
	"context"
	"fmt"
	"hinode/models"
	"log"

	"github.com/jackc/pgx/v5"
)

func main() {

	// interval := utils.NewInterval("value1", "0", "5")
	// interval2 := utils.NewInterval("value2", "10", "15")
	// interval3 := utils.NewInterval("value3", "15", "20")
	// var intervalcomb []utils.Interval
	// intervalcomb = append(intervalcomb, interval2, interval3)
	// edge := utils.NewEdge("label1", "1", "target1", "0", "10")


	// dianode := utils.NewDianode(
	// 	"1",
	// 	"0",
	// 	"10",
	// 	map[string][]utils.Interval{"attr1": {interval}},
	// 	map[string][]utils.Edge{"target1": {edge}},
	// 	map[string][]utils.Edge{"source1": {edge}},
	// )


	// dianode.InsertAttribute("attr1", intervalcomb)
	// fmt.Println(dianode.GetAttributes())
	// fmt.Println(dianode.GetOutgoingEdges())

	connectionString := "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())

	model := models.NewSingleTable("hinode", conn)


	model.CreateSchema()

	rows := model.Query("SELECT vid, start, eend FROM dianode")
	defer rows.Close()
	var id, start, end string
	for rows.Next() {
		err := rows.Scan(&id, &start, &end)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, start, end)
	}
}
