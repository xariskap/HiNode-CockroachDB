package models

import (
	"bufio"
	"context"
	"fmt"
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
		"CREATE TABLE vertices (vid STRING, vstart STRING, vlabel STRING, vend STRING, PRIMARY KEY(vid, vstart))",
		"CREATE TABLE attributes (aid STRING, alabel STRING, attribute JSONB)",
		"CREATE TABLE edges (label STRING, sourceid STRING, targetid STRING, weight STRING, estart STRING, eend STRING, PRIMARY KEY(sourceid, targetid, estart))",
	}

	//create indexes
	indexesInit := []string{
		"CREATE INDEX ON hinode.vertices (vid, vend) STORING (vstart)",
		"CREATE INDEX ON hinode.edges (sourceid, targetid) STORING (estart)",
		// "CREATE INDEX ON hinode.attributes (aid) STORING (attribute)",
	}

	mt.ExecSQL(databaseInit)
	mt.ExecSQLConcurrently(indexesInit)
}

func (mt MultiTable) insertVertex(vid, vlabel, vstart, timeNow string) {
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
	err = mt.ExecQuery("INSERT INTO vertices (vid, vstart, vlabel, vend) VALUES ($1, $2, $3, $4)", vid, vstart, vlabel, timeNow)
	if err != nil {
		log.Fatal("Failed to insert vertex: ", err)
	}
}

func (mt MultiTable) deleteVertex(vID, vEnd string) {
	if err := mt.ExecQuery("UPDATE vertices SET vend = $1 WHERE vid = $2", vEnd, vID); err != nil {
		log.Fatal("Failed to update vertex on deletion: ", err)
	}
}

func (mt MultiTable) insertAttribute(id, label, attrlabel, attr string, interval utils.Interval) {
	jsonData := utils.AttributeToJSON(attrlabel, attr, interval)

	err := mt.ExecQuery("INSERT INTO attributes (aid, alabel, attribute) VALUES ($1, $2, $3)", id, label, jsonData)
	if err != nil {
		log.Fatal("Failed to insert attribute: ", err)
	}
}

func (mt MultiTable) insertEdge(label, source, target, weight, start, timeNow string) {
	err := mt.ExecQuery("INSERT INTO edges (label, sourceid, targetid, weight, estart, eend) VALUES ($1, $2, $3, $4, $5, $6)", label, source, target, weight, start, timeNow)
	if err != nil {
		log.Fatal("Failed to insert edge ", err)
	}
}

func (mt MultiTable) ImportData(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	lineNumber := 0
	timeNow := time.Now().Format(time.RFC3339Nano)

	scanner := bufio.NewScanner(file)
	timeStart := time.Now()
	for scanner.Scan() {
		lineNumber++
		if lineNumber%100000 == 0 {
			fmt.Println("...", lineNumber, time.Since(timeStart).Minutes())
		}

		line := scanner.Text()
		tokens := strings.Fields(line)

		if strings.HasPrefix(line, "edge") {
			mt.insertEdge(tokens[1], tokens[2], tokens[3], "1", tokens[5], timeNow)

		} else if strings.HasPrefix(line, "Add attribute") {
			interv := utils.NewInterval(tokens[5], tokens[len(tokens)-1], "2099")
			mt.insertAttribute(tokens[2], tokens[3], tokens[4], tokens[5], interv)

		} else if strings.HasPrefix(line, "vertex") {
			mt.insertVertex(tokens[1], tokens[2], tokens[4], timeNow)

		} else if strings.HasPrefix(line, "delete vertex") {
			mt.deleteVertex(tokens[2], tokens[3])

		}
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Minutes(), "minutes elapsed importing data")
}

// SELECT vid, year FROM(SELECT vid, EXTRACT(YEAR FROM CAST(vstart AS DATE)) as year FROM vertices) WHERE year BETWEEN '2010' AND '2011';
//
//	vid | year
//
// ------+-------
//
//	181 |  2010
func (mt MultiTable) GetAliveVertices(start, end string) ([]string, map[string][]string) {
	timeStart := time.Now()
	rows, err := mt.Query("SELECT vid, EXTRACT(YEAR FROM CAST(vstart AS DATE)) FROM vertices WHERE CAST(vstart AS DATE) BETWEEN $1 AND $2 OR CAST(vstart AS DATE) BETWEEN $1 AND $2 OR CAST(vstart AS DATE) <= $1 AND CAST(vstart AS DATE) >= $2", start, end)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve alive vertices:", err)
	}

	var allAliveVertices []string
	aliveVertices := make(map[string][]string)
	var id, year string
	for rows.Next() {
		err := rows.Scan(&id, &year)
		if err != nil {
			log.Fatal(err)
		}
		allAliveVertices = append(allAliveVertices, id)
		aliveVertices[year] = append(aliveVertices[year], id)
	}
	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the alive vertices")
	return allAliveVertices, aliveVertices
}

func (mt MultiTable) GetDegreeDistribution(start, end string) map[string][][2]any {
	var id, year string
	var degree int
	degreeDistribution := make(map[string][][2]any)

	timeStart := time.Now()
	rows, err := mt.Query("SELECT sourceid ,COUNT(targetid), EXTRACT(YEAR FROM CAST(estart AS DATE)) FROM edges WHERE CAST(estart AS DATE) BETWEEN $1 AND $2 OR CAST(estart AS DATE) BETWEEN $1 AND $2 OR CAST(estart AS DATE) <= $1 AND CAST(estart AS DATE) >= $2 GROUP BY sourceid,EXTRACT(YEAR FROM CAST(estart AS DATE))", start, end)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve vertex degree:", err)
	}

	for rows.Next() {
		err = rows.Scan(&id, &degree, &year)
		if err != nil {
			log.Fatal("Could not parse degree: ", err)
		}
		values := [2]any{year, degree}
		degreeDistribution[id] = append(degreeDistribution[id], values)
	}
	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution")
	return degreeDistribution
}

func (mt MultiTable) GetOneHopNeighborhood(vid, end string) []string {
	var neighborhood []string
	var targetid string

	timeStart := time.Now()
	rows, err := mt.Query("SELECT targetid FROM edges WHERE sourceid = $1 AND CAST(estart AS DATE) <= $2", vid, end)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve one hop neighborhood:", err)
	}

	for rows.Next() {
		err = rows.Scan(&targetid)
		if err != nil {
			log.Fatal("Could not parse target id (one hop neighborhood): ", err)
		}

		neighborhood = append(neighborhood, targetid)
	}
	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the one hop neighborhood")
	return neighborhood
}
