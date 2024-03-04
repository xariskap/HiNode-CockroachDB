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

	mtModel := models.NewMultiTable("hinode", conn)

	mtModel.CreateSchema()
	mtModel.ParseInput("test_data.txt")

	rows := mtModel.GetAliveVertexes("2011-01-01", "2012-02-01")
	defer rows.Close()

	var id string
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id)
	}

	var bday string
	row := mtModel.QueryRow("SELECT vattr ->> 'firstName' FROM attributes WHERE vid = '111'")
	err = row.Scan(&bday)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(bday)

	rows, _ = mtModel.Query("SELECT * FROM edges")
	defer rows.Close()

	var label, source, target, weight, start string
	for rows.Next() {
		err := rows.Scan(&label, &source, &target, &weight, &start)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(label, source, target, weight, start)
	}

}
