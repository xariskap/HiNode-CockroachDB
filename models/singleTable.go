package models

import (
	"context"
	"fmt"
	"log"
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

func (sg SingleTable) GetDatabaseName() string {
	return sg.db
}

// returns the rows of the given query
func (sg SingleTable) Query(sql string) pgx.Rows {
	rows, err := sg.conn.Query(context.Background(), sql)
	if err != nil{
		log.Fatal(err)
	}
	return rows
}

func (sg SingleTable) ExecQuery(sql string){
	if _, err := sg.conn.Exec(context.Background(), sql); err != nil {
		log.Fatal(err)
	}
}

func (sg SingleTable) ExecSQL(sql []string) {
	for _, stmt := range sql {
		if _, err := sg.conn.Exec(context.Background(), stmt); err != nil {
			log.Fatal(err)
		}
	}
}

func (sg SingleTable) ExecSQLConcurrently(sql []string) {
	var wg sync.WaitGroup

	for _, stmt := range sql {
		wg.Add(1)

		go func(stmt string) {
			defer wg.Done()

			if _, err := sg.conn.Exec(context.Background(), stmt); err != nil {
				log.Println(err)
			}
		}(stmt)
	}
	wg.Wait()
}

// ConvertToEdgeList()

func (sg SingleTable) CreateSchema() {
	// Create the schema using SQL statements
	sqlStatements := []string{
		"DROP DATABASE IF EXISTS " + sg.db,
		"CREATE DATABASE " + sg.db,
		"USE " + sg.db,
		"CREATE TABLE dianode (vid STRING, start STRING, eend STRING)",
	}

	sqlStatemetns2 := []string{
		"CREATE INDEX ON dianode (vid)",
		"CREATE INDEX ON dianode (start, eend)",
		"CREATE INDEX ON dianode (vid, start, eend)",
	}

	start := time.Now()

	sg.ExecSQL(sqlStatements)
	fmt.Printf("SQL execution took %s\n", time.Since(start))
	sg.ExecSQLConcurrently(sqlStatemetns2)
	fmt.Printf("\nSQL execution took %s\n", time.Since(start))

}

func (sg SingleTable) GetAllAliveVertices(first, last string) {
	
	rows := sg.Query("SELECT vid, start, eend FROM dianode")
	defer rows.Close()
	

	

}
