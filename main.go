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

	model := models.NewMultiTable("hinode", conn)

	model.CreateSchema()
	model.ExecQuery("INSERT INTO vertex (vid, vstart, vend) VALUES (1, 4, 10)")
	erre := model.ExecQuery("INSERT INTO vertex (vid, vstart, vend) VALUES (2, 2020, '2022-12-14T21:11:54.229304359+02:00')")
	if erre != nil{
		log.Fatal("Failed to query2: ", err)
	}
	model.InsertVertex("2", "2021-12-14T21:11:54.229304359+02:00")

	rows, err := model.Query("SELECT vid, vstart, vend FROM vertex")
	if err != nil{
		log.Fatal("Failed to query: ", err)
	}
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
