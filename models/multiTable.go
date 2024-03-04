package models

import (
	"bufio"
	"context"
	"hinode/utils"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type MultiTable struct {
	db   string
	conn *pgx.Conn
}

func NewMultiTable(db string, conn *pgx.Conn) MultiTable {
	return MultiTable{db, conn}
}

func (mt MultiTable) GetDatabaseName() string {
	return mt.db
}

// returns the rows of the given query
func (mt MultiTable) Query(sql string, values ...any) (pgx.Rows, error) {
	rows, err := mt.conn.Query(context.Background(), sql, values...)
	return rows, err
}

func (mt MultiTable) QueryRow(sql string, values ...any) pgx.Row {
	row := mt.conn.QueryRow(context.Background(), sql, values...)
	return row
}

func (mt MultiTable) ExecQuery(sql string, values ...any) error {
	_, err := mt.conn.Exec(context.Background(), sql, values...)
	return err
}

func (mt MultiTable) ExecSQL(sql []string) {
	for _, stmt := range sql {
		if _, err := mt.conn.Exec(context.Background(), stmt); err != nil {
			log.Fatal(err)
		}
	}
}

func (mt MultiTable) ExecSQLConcurrently(sql []string) {
	var wg sync.WaitGroup

	for _, stmt := range sql {
		wg.Add(1)

		go func(stmt string) {
			defer wg.Done()

			if _, err := mt.conn.Exec(context.Background(), stmt); err != nil {
				log.Println(err)
			}
		}(stmt)
	}
	wg.Wait()
}

// ConvertToEdgeList()

func (mt MultiTable) CreateSchema() {
	// Create the schema using SQL statements
	sqlStatements := []string{
		"DROP DATABASE IF EXISTS " + mt.db,
		"CREATE DATABASE " + mt.db,
		"USE " + mt.db,
		"CREATE TABLE vertexes (vid STRING, vstart STRING, vend STRING)",
		"CREATE TABLE attributes (vid STRING, vattr JSONB)",
		"CREATE TABLE edges (label STRING, sourceID STRING, targetID STRING, weight STRING, start STRING)",
	}


	//create indexes
	// sqlStatemetns2 := []string{
	// 	"CREATE INDEX ON dianode (vid)",
	// 	"CREATE INDEX ON dianode (start, eend)",
	// 	"CREATE INDEX ON dianode (vid, start, eend)",
	// }

	//start := time.Now()

	mt.ExecSQL(sqlStatements)
	//fmt.Printf("SQL execution took %s\n", time.Since(start))
	//mt.ExecSQLConcurrently(sqlStatemetns2)
	//fmt.Printf("\nSQL execution took %s\n", time.Since(start))

}

func (mt MultiTable) insertVertex(vid, vstart string) {
	var s, e string

	// search for a vertex with a higher end time than the provided start time
	err := mt.QueryRow("SELECT vstart, vend FROM vertexes WHERE vid = $1 AND vend >= $2 ORDER BY vend ASC", vid, vstart).Scan(&s, &e)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal(err)
	}

	// if vertex is found, update it
	if e != "" {
		if err := mt.ExecQuery("UPDATE vertexes SET vend = $1 WHERE vid = $2 AND vstart = $3", vstart, vid, s); err != nil {
			log.Fatal("Failed to update vertex: ", err)
		}
	}

	// insert new vertex
	err = mt.ExecQuery("INSERT INTO vertexes (vid, vstart, vend) VALUES ($1, $2, $3)", vid, vstart, time.Now().Format(time.RFC3339Nano))
	if err != nil {
		log.Fatal("Failed to insert vertex: ", err)
	}
}

func (mt MultiTable) deleteVertex(vID, vEnd string){
	if err := mt.ExecQuery("UPDATE vertexes SET vend = $1 WHERE vid = $2", vEnd, vID); err != nil {
		log.Fatal("Failed to update vertex on deletion: ", err)
	}
}

func (mt MultiTable) insertAttribute(vID, label, attr string, interval utils.Interval) {
	jsonData := utils.AttributeToJSON(vID, label, attr, interval)

	err := mt.ExecQuery("INSERT INTO attributes (vid, vattr) VALUES ($1, $2)", vID, jsonData)
	if err != nil {
		log.Fatal("Failed to insert attribute: ", err)
	}
}

func (mt MultiTable) insertEdge(label, source, target, weight, start string){
	err := mt.ExecQuery("INSERT INTO edges (label, sourceID, targetID, weight, start) VALUES ($1, $2, $3, $4, $5)",label, source, target, weight, start )
	if err != nil{
		log.Fatal("Failed to insert edge", err)
	}
}

func (mt MultiTable) ParseInput(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	// var vid, vstart string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		line := scanner.Text()
		tokens := strings.Fields(line)

		if strings.HasPrefix(line, "vertex") {
			mt.insertVertex(tokens[1], tokens[3])

		} else if strings.HasPrefix(line, "delete vertex") {
			mt.deleteVertex(tokens[2], tokens[3])

		} else if strings.HasPrefix(line, "change vertex") {
			interv := utils.NewInterval(tokens[4], tokens[5], "2099")
			mt.insertAttribute(tokens[2], tokens[3], tokens[4], interv)

		} else if strings.HasPrefix(line, "edge") {
			mt.insertEdge("undefined", tokens[1], tokens[2], "1", tokens[4])
		}
	}
}

func (mt MultiTable) GetAliveVertexes(start, end string) pgx.Rows {
	aliveVertexes, err := mt.Query("SELECT vid FROM vertexes WHERE vstart >= $1 AND vend >= $2", start, end)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve alive vertexes:", err)
	}

	return aliveVertexes
}

// }
