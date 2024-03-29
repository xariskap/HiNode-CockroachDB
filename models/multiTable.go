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

	for _, stmnt := range sql {
		wg.Add(1)

		go func(stmt string) {
			defer wg.Done()

			if _, err := mt.conn.Exec(context.Background(), stmt); err != nil {
				log.Println(err)
			}
		}(stmnt)
	}
	wg.Wait()
}

func (mt MultiTable) CreateSchema() {
	// Create the schema using SQL statements
	databaseInit := []string{
		"DROP DATABASE IF EXISTS " + mt.db,
		"CREATE DATABASE " + mt.db,
		"USE " + mt.db,
		"CREATE TABLE vertices (vid STRING, vstart STRING, vend STRING)",
		"CREATE TABLE attributes (vid STRING, vattr JSONB)",
		"CREATE TABLE edges (label STRING, sourceID STRING, targetID STRING, weight STRING, estart STRING, eend STRING)",
	}

	//create indexes
	indexesInit := []string{
		"CREATE INDEX kakalo ON hinode.vertices (vstart) STORING (vid, vend)",
	}

	//start := time.Now()

	mt.ExecSQL(databaseInit)
	//fmt.Printf("SQL execution took %s\n", time.Since(start))
	mt.ExecSQLConcurrently(indexesInit)
	//fmt.Printf("\nSQL execution took %s\n", time.Since(start))

}

func (mt MultiTable) insertVertex(vid, vstart string) {
	var s, e string

	// search for a vertex with a higher end time than the provided start time
	err := mt.QueryRow("SELECT vstart, vend FROM vertices WHERE vid = $1 AND vend >= $2 ORDER BY vend ASC", vid, vstart).Scan(&s, &e)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal(err)
	}

	// if vertex is found, update it
	if e != "" {
		if err := mt.ExecQuery("UPDATE vertices SET vend = $1 WHERE vid = $2 AND vstart = $3", vstart, vid, s); err != nil {
			log.Fatal("Failed to update vertex: ", err)
		}
	}

	// insert new vertex
	err = mt.ExecQuery("INSERT INTO vertices (vid, vstart, vend) VALUES ($1, $2, $3)", vid, vstart, time.Now().Format(time.RFC3339Nano))
	if err != nil {
		log.Fatal("Failed to insert vertex: ", err)
	}
}

func (mt MultiTable) deleteVertex(vID, vEnd string) {
	if err := mt.ExecQuery("UPDATE vertices SET vend = $1 WHERE vid = $2", vEnd, vID); err != nil {
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

// TODO change eend to time.Now()
func (mt MultiTable) insertEdge(label, source, target, weight, start string) {
	err := mt.ExecQuery("INSERT INTO edges (label, sourceID, targetID, weight, estart, eend) VALUES ($1, $2, $3, $4, $5, $6)", label, source, target, weight, start, "2012-01-22T17:53:41.518+00:00")
	if err != nil {
		log.Fatal("Failed to insert edge ", err)
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

func (mt MultiTable) GetAliveVertices(start, end string) []string {
	rows, err := mt.Query("SELECT vid FROM vertices WHERE vstart >= $1 AND vend >= $2", start, end)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve alive vertices:", err)
	}

	var aliveVertices []string
	var id string
	for rows.Next() {
		err := rows.Scan(&id)
		if err != nil {
			log.Fatal(err)
		}
		aliveVertices = append(aliveVertices, id)
	}
	return aliveVertices
}

func (mt MultiTable) GetDegreeDistribution(id, start, end string) int{
	row := mt.QueryRow("SELECT COUNT(targetid) FROM edges WHERE sourceid = $1 AND estart >= $2 AND eend <= $3", id, start, end)
	var degree int
	err := row.Scan(&degree)
	if err != nil{
		log.Fatal("Could not parse degree: ", err) 
	}
	return degree
}
