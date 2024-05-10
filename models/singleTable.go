package models

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type SingleTable struct {
	db   string
	conn *pgx.Conn
}

func NewSingleTable(db string, conn *pgx.Conn) SingleTable {
	return SingleTable{db, conn}
}

func (st SingleTable) Query(sql string, values ...any) (pgx.Rows, error) {
	rows, err := st.conn.Query(context.Background(), sql, values...)
	return rows, err
}

func (st SingleTable) QueryRow(sql string, values ...any) pgx.Row {
	row := st.conn.QueryRow(context.Background(), sql, values...)
	return row
}

func (st SingleTable) ExecQuery(sql string, values ...any) error {
	_, err := st.conn.Exec(context.Background(), sql, values...)
	return err
}

func (st SingleTable) ExecSQL(sql []string) {
	for _, stmt := range sql {
		if _, err := st.conn.Exec(context.Background(), stmt); err != nil {
			log.Fatal(err)
		}
	}
}

func (st SingleTable) ExecSQLConcurrently(sql []string) {
	var wg sync.WaitGroup

	for _, stmnt := range sql {
		wg.Add(1)

		go func(stmt string) {
			defer wg.Done()

			if _, err := st.conn.Exec(context.Background(), stmt); err != nil {
				log.Println(err)
			}
		}(stmnt)
	}
	wg.Wait()
}

func (st SingleTable) CreateSchema() {
	// Create the schema using SQL statements
	databaseInit := []string{
		"DROP DATABASE IF EXISTS " + st.db,
		"CREATE DATABASE " + st.db,
		"USE " + st.db,
		"CREATE TABLE dianode (vid STRING, vstart STRING, vend STRING, vlabel STRING, attributes JSONB, edge JSONB)",
	}

	indexesInit := []string{
		"CREATE INDEX ON " + st.db + ".dianode (vid, vstart, vend) STORING (vlabel, attributes, edge)",
	}

	st.ExecSQL(databaseInit)
	st.ExecSQLConcurrently(indexesInit)
}

func (st SingleTable) insertVertex(vid, vlabel, vstart, vend string) {
	var s, e string

	// search for a vertex with a higher end time than the provided start time
	err := st.QueryRow("SELECT vstart, vend FROM dianode WHERE vid = $1 AND date(vend) >= $2 ORDER BY vend ASC", vid, vstart).Scan(&s, &e)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal(err)
	}

	// if vertex is found, update it
	if e != "" {
		if err := st.ExecQuery("UPDATE dianode SET vend = $1 WHERE vid = $2 AND vstart = $3", vstart, vid, s); err != nil {
			log.Fatal("Failed to update vertex: ", err)
		}
	}

	// insert new vertex
	err = st.ExecQuery("INSERT INTO dianode (vid, vstart, vend, vlabel) VALUES ($1, $2, $3, $4)", vid, vstart, vend, vlabel)
	if err != nil {
		log.Fatal("Failed to insert vertex: ", err)
	}
}

func (st SingleTable) deleteVertex(vid, vend string) {
	if err := st.ExecQuery("UPDATE dianode SET vend = $1 WHERE vid = $2 AND vend = (SELECT MAX(vend) FROM vertices WHERE vid = $2)", vend, vid); err != nil {
		log.Fatal("Failed to delete vertex: ", err)
	}
}

func (st SingleTable) insertAttribute(vid, attrlabel, attr string, astart string) {

	sqlStatement := `
    UPDATE dianode
    SET attributes = COALESCE(attributes, '{}'::JSONB) || $1::JSONB
    WHERE vid = $2
	`
	err := st.ExecQuery(sqlStatement, fmt.Sprintf(`{"%s": ["%s", "%s"]}`, attrlabel, attr, astart), vid)
	if err != nil {
		log.Fatal("Failed to insert attribute: ", err)
	}
}

func (st SingleTable) insertEdge(label, source, target, weight, estart, eend string) {
	var vstart, vend, vlabel, attributes string

	err := st.QueryRow("SELECT vstart, vend, vlabel, attributes FROM dianode WHERE vid = $1 AND date(vstart) <= $2 ORDER BY vend ASC", source, estart).Scan(&vstart, &vend, &vlabel, &attributes)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal(err)
	}

	edge := fmt.Sprintf(`{"label": "%s", "targetid" : "%s", "weight" : "%s", "estart" : "%s", "eend" : "%s"}`, label, target, weight, estart, eend)
	err = st.ExecQuery("INSERT INTO dianode (vid, vstart, vend, vlabel, attributes, edge) VALUES ($1, $2, $3, $4, $5, $6)", source, vstart, vend, vlabel, attributes, edge)
	if err != nil {
		log.Fatal("Failed to insert edge ", err)
	}
}

func (st SingleTable) deleteEdge(source, target, eend string) {

	err := st.ExecQuery("UPDATE dianode SET edge = jsonb_set(edge, '{eend}', $3) WHERE vid = $1 AND edge->>'targetid' = $2", source, target, fmt.Sprintf("\"%s\"", eend))
	if err != nil {
		log.Fatal("Failed to delete edge ", err)
	}
}

func (st SingleTable) ImportData(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	lineNumber := 0

	scanner := bufio.NewScanner(file)
	timeStart := time.Now()
	for scanner.Scan() {
		lineNumber++
		if lineNumber%100000 == 0 {
			fmt.Println("-->", lineNumber, time.Since(timeStart).Minutes())
		}

		line := scanner.Text()
		tokens := strings.Fields(line)

		if strings.HasPrefix(line, "edge") {
			st.insertEdge(tokens[1], tokens[2], tokens[3], "1", tokens[5], "2099-01-01")

		} else if strings.HasPrefix(line, "vertex") {
			st.insertVertex(tokens[1], tokens[2], tokens[4], "2099-01-01")

		} else if strings.HasPrefix(line, "delete vertex") {
			st.deleteVertex(tokens[2], tokens[4])

		} else if strings.HasPrefix(line, "Add attribute") {
			st.insertAttribute(tokens[2], tokens[4], tokens[5], tokens[len(tokens)-1])

		} else if strings.HasPrefix(line, "delete edge") {
			st.deleteEdge(tokens[3], tokens[4], tokens[5])
		}
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Minutes(), "minutes elapsed importing data")
}

func (st SingleTable) ImportNoLabelData(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	lineNumber := 0

	scanner := bufio.NewScanner(file)
	timeStart := time.Now()
	for scanner.Scan() {
		lineNumber++
		if lineNumber%100000 == 0 {
			fmt.Println("-->", lineNumber, time.Since(timeStart).Minutes())
		}

		line := scanner.Text()
		tokens := strings.Fields(line)

		if strings.HasPrefix(line, "edge") {
			st.insertEdge("label", tokens[1], tokens[2], "1", tokens[4], "2099-01-01")

		} else if strings.HasPrefix(line, "vertex") {
			st.insertVertex(tokens[1], "label", tokens[3], "2099-01-01")

		} else if strings.HasPrefix(line, "delete vertex") {
			st.deleteVertex(tokens[2], tokens[3])

		} else if strings.HasPrefix(line, "Add attribute") {
			st.insertAttribute(tokens[2], tokens[3], tokens[4], tokens[len(tokens)-1])

		} else if strings.HasPrefix(line, "delete edge") {
			st.deleteEdge(tokens[2], tokens[3], tokens[4])
		}
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Minutes(), "minutes elapsed importing data")
}

func (st SingleTable) GetDegreeDistribution(start, end string) map[int]map[int]int {
	var estart, eend, degree int
	var sourceid string
	var c = 0
	vertexDistribution := make(map[string]map[int]int)
	degreeDistribution := make(map[int]map[int]int)
	timeStart := time.Now()
	rows, err := st.Query(`
	SELECT
		vid,                                                                                                                                    
	    COUNT(edge->>'targetid'),                                     
	    EXTRACT(YEAR FROM DATE(edge->>'estart'))::int AS start,
	    least(EXTRACT(YEAR FROM DATE(edge->>'eend'))::int, EXTRACT(YEAR FROM DATE($2))::int)::int AS end
	FROM dianode WHERE DATE(edge->>'eend') >= $1 AND DATE(edge->>'estart') <= $2
	GROUP BY
		vid,
		EXTRACT(YEAR FROM DATE(edge->>'estart')),
		EXTRACT(YEAR FROM DATE(edge->>'eend'))`, start, end)

	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve degree distribution:", err)
	}

	afterQ := time.Now()
	for rows.Next() {
		c++
		err = rows.Scan(&sourceid, &degree, &estart, &eend)
		if err != nil {
			log.Fatal("Could not parse degree: ", err)
		}
		if _, ok := vertexDistribution[sourceid]; !ok {
			vertexDistribution[sourceid] = make(map[int]int)
		}

		for year := estart; year <= eend; year++ {
			vertexDistribution[sourceid][year] += degree
		}
	}

	for _, v := range vertexDistribution {
		for year, deg := range v {

			if _, ok := degreeDistribution[year]; !ok {
				degreeDistribution[year] = make(map[int]int)
			}
			degreeDistribution[year][deg]++
		}
	}
	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution and", time.Since(afterQ).Seconds(), "seconds elapsed processing the data", c, "LINES")
	return degreeDistribution
}

func (st SingleTable) GetDegreeDistributionOptimized(start, end string) map[int]map[int]int {
	var estart, eend, degree int
	var sourceid, prevsourceid string
	vertexDistribution := make(map[int]int)
	degreeDistribution := make(map[int]map[int]int)
	timeStart := time.Now()
	rows, err := st.Query(`
	SELECT
		vid,
	    COUNT(edge->>'targetid'),
	    EXTRACT(YEAR FROM DATE(edge->>'estart'))::int AS start,
	    least(EXTRACT(YEAR FROM DATE(edge->>'eend'))::int, EXTRACT(YEAR FROM DATE($2))::int)::int AS end
	FROM dianode WHERE DATE(edge->>'eend') >= $1 AND DATE(edge->>'estart') <= $2
	GROUP BY
		vid,
		EXTRACT(YEAR FROM DATE(edge->>'estart')),
		EXTRACT(YEAR FROM DATE(edge->>'eend'))
	ORDER BY vid DESC`, start, end)

	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve degree distribution:", err)
	}

	afterQ := time.Now()
	for rows.Next() {

		err = rows.Scan(&sourceid, &degree, &estart, &eend)
		if err != nil {
			log.Fatal("Error scanning the rows of degree distribution: ", err)
		}

		if prevsourceid != sourceid && prevsourceid != "" {

			for k, v := range vertexDistribution {
				if _, ok := degreeDistribution[k]; !ok {
					degreeDistribution[k] = make(map[int]int)
				}
				degreeDistribution[k][v]++
			}
			vertexDistribution = make(map[int]int)
		}

		for year := estart; year <= eend; year++ {
			vertexDistribution[year] += degree
		}

		prevsourceid = sourceid
	}

	for k, v := range vertexDistribution {
		if _, ok := degreeDistribution[k]; !ok {
			degreeDistribution[k] = make(map[int]int)
		}
		degreeDistribution[k][v]++
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution and", time.Since(afterQ).Seconds(), "seconds elapsed processing the data")
	return degreeDistribution
}

func (st SingleTable) GetDegreeDistributionConcurrently(start, end string) map[int]map[int]int {
	var estart, eend, degree int
	var sourceid, prevsourceid string
	vertexDistribution := make(map[int]int)
	degreeDistribution := make(map[int]map[int]int)
	timeStart := time.Now()
	rows, err := st.Query(`
	SELECT
		vid,
	    COUNT(edge->>'targetid'),
	    EXTRACT(YEAR FROM DATE(edge->>'estart'))::int AS start,
	    least(EXTRACT(YEAR FROM DATE(edge->>'eend'))::int, EXTRACT(YEAR FROM DATE($2))::int)::int AS end
	FROM dianode WHERE DATE(edge->>'eend') >= $1 AND DATE(edge->>'estart') <= $2
	GROUP BY                     
		vid,
		EXTRACT(YEAR FROM DATE(edge->>'estart')),
		EXTRACT(YEAR FROM DATE(edge->>'eend'))
	ORDER BY vid DESC`, start, end)

	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve degree distribution:", err)
	}

	var mutex sync.Mutex
	var wg sync.WaitGroup

	afterQ := time.Now()
	for rows.Next() {

		err = rows.Scan(&sourceid, &degree, &estart, &eend)
		if err != nil {
			log.Fatal("Error scanning the rows of degree distribution: ", err)
		}

		if prevsourceid != sourceid && prevsourceid != "" {
			mutex.Lock()
			temp := vertexDistribution
			mutex.Unlock()

			vertexDistribution = make(map[int]int)
			wg.Add(1)

			go func(temp map[int]int) {
				defer wg.Done()
				mutex.Lock()
				defer mutex.Unlock()
				for k, v := range temp {
					if _, ok := degreeDistribution[k]; !ok {
						degreeDistribution[k] = make(map[int]int)
					}
					degreeDistribution[k][v]++
				}
			}(temp)
		}

		for year := estart; year <= eend; year++ {
			vertexDistribution[year] += degree
		}

		prevsourceid = sourceid
	}

	wg.Wait()
	for k, v := range vertexDistribution {
		if _, ok := degreeDistribution[k]; !ok {
			degreeDistribution[k] = make(map[int]int)
		}
		degreeDistribution[k][v]++
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution and", time.Since(afterQ).Seconds(), "seconds elapsed processing the data")
	return degreeDistribution
}

func (st SingleTable) GetOneHopNeighborhood(vid, end string) ([]string, int) {
	var neighborhood []string
	var targetid string

	timeStart := time.Now()
	rows, err := st.Query("SELECT edge->>'targetid' FROM dianode WHERE vid = $1 AND DATE(edge->>'estart') <= $2", vid, end)
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
	return neighborhood, len(neighborhood)
}

func (st SingleTable) GetDegreeDistributionFetchAllVertices(instart, inend string) map[string]map[string]int {
	var sourceid, estart, eend string
	results := make(map[string]map[string]int)
	vertexDegreeInAllInstances := make(map[string]map[string]int)
	var c = 0
	timeStart := time.Now()
	rows, err := st.Query("SELECT vid, edge->>'estart', edge->>'eend' FROM dianode WHERE DATE(edge->>'eend') >= $1 AND DATE(edge->>'estart') <= $2", instart, inend)

	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve vertex degree:", err)
	}

	afterQ := time.Now()
	for rows.Next() {
		c++
		err = rows.Scan(&sourceid, &estart, &eend)
		if err != nil {
			log.Fatal("Could not parse degree: ", err)
		}

		rowstart, _ := strconv.Atoi(estart[:4])
		rowend, _ := strconv.Atoi(eend[:4])
		firstInt, _ := strconv.Atoi(instart[:4])
		lastInt, _ := strconv.Atoi(inend[:4])

		start := max(rowstart, firstInt)
		end := min(rowend, lastInt)

		if _, ok := vertexDegreeInAllInstances[sourceid]; !ok {
			vertexDegreeInAllInstances[sourceid] = make(map[string]int)
		}

		for i := start; i <= end; i++ {
			year := strconv.Itoa(i)
			vertexDegreeInAllInstances[sourceid][year]++
		}
	}

	for _, vertices := range vertexDegreeInAllInstances {
		for year, count := range vertices {
			if _, ok := results[year]; !ok {
				results[year] = make(map[string]int)
			}
			results[year][strconv.Itoa(count)]++
		}
	}
	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution and", time.Since(afterQ).Seconds(), "seconds elapsed processing the data", c, "LINES")

	return results
}
