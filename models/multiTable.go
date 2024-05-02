package models

import (
	"bufio"
	"context"
	"fmt"
	"hinode/gremlin"
	"hinode/utils"
	"log"
	"os"
	"strconv"
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
		"CREATE INDEX ON " + mt.db + ".vertices (vid, vend) STORING (vstart)",
		"CREATE INDEX ON " + mt.db + ".vertices (vstart, vend) STORING(vid)",
		"CREATE INDEX ON " + mt.db + ".edges (sourceid, targetid) STORING (estart)",
		"CREATE INDEX ON " + mt.db + ".edges (eend) STORING (sourceid, targetid)",
	}

	mt.ExecSQL(databaseInit)
	mt.ExecSQLConcurrently(indexesInit)
}

func (mt MultiTable) insertVertex(vid, vlabel, vstart, vend string) {
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
	err = mt.ExecQuery("INSERT INTO vertices (vid, vstart, vlabel, vend) VALUES ($1, $2, $3, $4)", vid, vstart, vlabel, vend)
	if err != nil {
		log.Fatal("Failed to insert vertex: ", err)
	}
}

func (mt MultiTable) deleteVertex(vid, vend string) {
	if err := mt.ExecQuery("UPDATE vertices SET vend = $1 WHERE vid = $2", vend, vid); err != nil {
		log.Fatal("Failed to delete vertex: ", err)
	}
}

func (mt MultiTable) insertAttribute(id, label, attrlabel, attr string, interval utils.Interval) {
	jsonData := utils.AttributeToJSON(attrlabel, attr, interval)

	err := mt.ExecQuery("INSERT INTO attributes (aid, alabel, attribute) VALUES ($1, $2, $3)", id, label, jsonData)
	if err != nil {
		log.Fatal("Failed to insert attribute: ", err)
	}
}

func (mt MultiTable) insertEdge(label, source, target, weight, start, eend string) {
	err := mt.ExecQuery("INSERT INTO edges (label, sourceid, targetid, weight, estart, eend) VALUES ($1, $2, $3, $4, $5, $6)", label, source, target, weight, start, eend)
	if err != nil {
		log.Fatal("Failed to insert edge ", err)
	}
}

func (mt MultiTable) deleteEdge(source, target, eend string) {
	err := mt.ExecQuery("UPDATE edges SET eend = $3 WHERE sourceid = $1 AND targetid = $2", source, target, eend)
	if err != nil {
		log.Fatal("Failed to delete edge ", err)
	}
}

func (mt MultiTable) ImportData(path string) {
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
			mt.insertEdge(tokens[1], tokens[2], tokens[3], "1", tokens[5], "2099-01-01")

		} else if strings.HasPrefix(line, "Add attribute") {
			interv := utils.NewInterval(tokens[5], tokens[len(tokens)-1], "2099-01-01")
			mt.insertAttribute(tokens[2], tokens[3], tokens[4], tokens[5], interv)

		} else if strings.HasPrefix(line, "vertex") {
			mt.insertVertex(tokens[1], tokens[2], tokens[4], "2099-01-01")

		} else if strings.HasPrefix(line, "delete vertex") {
			mt.deleteVertex(tokens[2], tokens[4])

		} else if strings.HasPrefix(line, "delete edge") {
			mt.deleteEdge(tokens[3], tokens[4], tokens[5])
		}
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Minutes(), "minutes elapsed importing data")
}

func (mt MultiTable) ImportNoLabelData(path string) {
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
			mt.insertEdge("label", tokens[1], tokens[2], "1", tokens[4], "2099-01-01")

		} else if strings.HasPrefix(line, "Add attribute") {
			interv := utils.NewInterval(tokens[5], tokens[len(tokens)-1], "2099-01-01")
			mt.insertAttribute(tokens[2], "label", tokens[3], tokens[4], interv)

		} else if strings.HasPrefix(line, "vertex") {
			mt.insertVertex(tokens[1], "label", tokens[3], "2099-01-01")

		} else if strings.HasPrefix(line, "delete vertex") {
			mt.deleteVertex(tokens[2], tokens[3])

		} else if strings.HasPrefix(line, "delete edge") {
			mt.deleteEdge(tokens[2], tokens[3], tokens[4])
		}
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Minutes(), "minutes elapsed importing SF3")
}

func (mt MultiTable) GetAliveVertices(start, end string) ([]string, map[string][]string) {
	timeStart := time.Now()
	rows, err := mt.Query("SELECT vid, EXTRACT(YEAR FROM DATE(vstart)) FROM vertices WHERE DATE(vstart) <= $2 AND DATE(vend) >= $1", start, end)
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

func (mt MultiTable) GetDegreeDistribution(start, end string) map[int]map[int]int {
	var estart, eend, degree int
	var sourceid string
	vertexDistribution := make(map[string]map[int]int)
	degreeDistribution := make(map[int]map[int]int)
	timeStart := time.Now()
	rows, err := mt.Query(`
	SELECT
		sourceid,                                                                                                                                    
	    COUNT(targetid),                                     
	    EXTRACT(YEAR FROM DATE(estart))::int AS start_year,
	    least(EXTRACT(YEAR FROM DATE(eend))::int, EXTRACT(YEAR FROM DATE($2))::int)::int AS end_year
	FROM edges WHERE DATE(eend) >= $1 AND DATE(estart) <= $2
	GROUP BY                                                                                         
		sourceid,
		EXTRACT(YEAR FROM DATE(estart)),
		EXTRACT(YEAR FROM DATE(eend))`, start, end)

	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve degree distribution:", err)
	}

	afterQ := time.Now()
	for rows.Next() {

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
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution and", time.Since(afterQ).Seconds(), "seconds elapsed processing the data")
	return degreeDistribution
}

func (mt MultiTable) GetDegreeDistributionConcurrently(start, end string) map[int]map[int]int {
	var estart, eend, degree int
	var sourceid, prevsourceid string
	vertexDistribution := make(map[int]int)
	degreeDistribution := make(map[int]map[int]int)
	timeStart := time.Now()
	rows, err := mt.Query(`
	SELECT
		sourceid,                                                                                                                                    
	    COUNT(targetid),                                     
	    EXTRACT(YEAR FROM DATE(estart))::int AS start_year,
	    least(EXTRACT(YEAR FROM DATE(eend))::int, EXTRACT(YEAR FROM DATE($2))::int)::int AS end_year
	FROM edges WHERE DATE(eend) >= $1 AND DATE(estart) <= $2
	GROUP BY                                                                                         
		sourceid,
		EXTRACT(YEAR FROM DATE(estart)),
		EXTRACT(YEAR FROM DATE(eend))
	ORDER BY sourceid DESC`, start, end)

	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve degree distribution:", err)
	}

	var mutex sync.Mutex

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

			vertexDistribution = make(map[int]int) // clear the map

			go func(temp map[int]int) {
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

	for k, v := range vertexDistribution { // covering the last vertexDistribution data
		if _, ok := degreeDistribution[k]; !ok {
			degreeDistribution[k] = make(map[int]int)
		}
		degreeDistribution[k][v]++
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution and", time.Since(afterQ).Seconds(), "seconds elapsed processing the data")
	return degreeDistribution
}

func (mt MultiTable) GetDegreeDistributionOptimized(start, end string) map[int]map[int]int {
	var estart, eend, degree int
	var sourceid, prevsourceid string
	vertexDistribution := make(map[int]int)
	degreeDistribution := make(map[int]map[int]int)
	timeStart := time.Now()
	rows, err := mt.Query(`
	SELECT
		sourceid,                                                                                                                                    
	    COUNT(targetid),                                     
	    EXTRACT(YEAR FROM DATE(estart))::int AS start_year,
	    least(EXTRACT(YEAR FROM DATE(eend))::int, EXTRACT(YEAR FROM DATE($2))::int)::int AS end_year
	FROM edges WHERE DATE(eend) >= $1 AND DATE(estart) <= $2
	GROUP BY                                                                                         
		sourceid,
		EXTRACT(YEAR FROM DATE(estart)),
		EXTRACT(YEAR FROM DATE(eend))
	ORDER BY sourceid DESC`, start, end)

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

	for k, v := range vertexDistribution { // covering the last vertexDistribution data
		if _, ok := degreeDistribution[k]; !ok {
			degreeDistribution[k] = make(map[int]int)
		}
		degreeDistribution[k][v]++
	}

	elapsedTime := time.Since(timeStart)
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution and", time.Since(afterQ).Seconds(), "seconds elapsed processing the data")
	return degreeDistribution
}

func (mt MultiTable) GetOneHopNeighborhood(vid, end string) []string {
	var neighborhood []string
	var targetid string

	timeStart := time.Now()
	rows, err := mt.Query("SELECT targetid FROM edges WHERE sourceid = $1 AND DATE(estart) <= $2", vid, end)
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

func (mt MultiTable) GetDegreeDistributionFetchAllVertices(instart, inend string) map[string]map[string]int {
	var sourceid, estart, eend string
	results := make(map[string]map[string]int)
	vertexDegreeInAllInstances := make(map[string]map[string]int)
	timeStart := time.Now()
	rows, err := mt.Query("SELECT sourceid, estart, eend FROM edges WHERE DATE(eend) >= $1 AND DATE(estart) <= $2", instart, inend)

	if err != nil && err != pgx.ErrNoRows {
		log.Fatal("Failed to retrieve vertex degree:", err)
	}

	afterQ := time.Now()
	for rows.Next() {
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
	fmt.Println(elapsedTime.Seconds(), "seconds elapsed getting the degree distribution and", time.Since(afterQ).Seconds(), "seconds elapsed processing the data")

	return results
}

func (mt MultiTable) ImportGremlin(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		data := gremlin.GremlinParse(line)

		if strings.HasPrefix(line, "g.insertE") {
			mt.insertEdge(data[0], data[1], data[2], data[3], data[4], data[5])

		} else if strings.HasPrefix(line, "g.addV") {
			mt.insertVertex(data[0], data[1], data[2], data[3])

		} else if strings.HasPrefix(line, "g.deleteV") {
			mt.deleteVertex(data[0], data[1])

		} else if strings.HasPrefix(line, "g.deleteE") {
			mt.deleteEdge(data[0], data[1], data[2])
		}
	}
}
