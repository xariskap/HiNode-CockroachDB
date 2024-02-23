package models

import (
	"context"
	"fmt"
	"log"
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

func (mg MultiTable) GetDatabaseName() string {
	return mg.db
}

// returns the rows of the given query
func (mg MultiTable) Query(sql string, values ...any) (pgx.Rows, error) {
	rows, err := mg.conn.Query(context.Background(), sql, values...)
	return rows, err
}

func (mg MultiTable) QueryRow(sql string, values ...any) pgx.Row {
	row := mg.conn.QueryRow(context.Background(), sql, values...)
	return row
}

func (mg MultiTable) ExecQuery(sql string, values ...any) error {
	_, err := mg.conn.Exec(context.Background(), sql, values...)
	return err
}

func (mg MultiTable) ExecSQL(sql []string) {
	for _, stmt := range sql {
		if _, err := mg.conn.Exec(context.Background(), stmt); err != nil {
			log.Fatal(err)
		}
	}
}

func (mg MultiTable) ExecSQLConcurrently(sql []string) {
	var wg sync.WaitGroup

	for _, stmt := range sql {
		wg.Add(1)

		go func(stmt string) {
			defer wg.Done()

			if _, err := mg.conn.Exec(context.Background(), stmt); err != nil {
				log.Println(err)
			}
		}(stmt)
	}
	wg.Wait()
}

// ConvertToEdgeList()

func (mg MultiTable) CreateSchema() {
	// Create the schema using SQL statements
	sqlStatements := []string{
		"DROP DATABASE IF EXISTS " + mg.db,
		"CREATE DATABASE " + mg.db,
		"USE " + mg.db,
		"CREATE TABLE vertex (vid STRING, vstart STRING, vend STRING)",
	}

	//create indexes
	// sqlStatemetns2 := []string{
	// 	"CREATE INDEX ON dianode (vid)",
	// 	"CREATE INDEX ON dianode (start, eend)",
	// 	"CREATE INDEX ON dianode (vid, start, eend)",
	// }

	start := time.Now()

	mg.ExecSQL(sqlStatements)
	fmt.Printf("SQL execution took %s\n", time.Since(start))
	//mg.ExecSQLConcurrently(sqlStatemetns2)
	//fmt.Printf("\nSQL execution took %s\n", time.Since(start))

}


func (mg MultiTable) InsertVertex(vid, vstart string) {
	var s, e string

	// search for a vertex with a higher end time than the provided start time
	err := mg.QueryRow("SELECT vstart, vend FROM vertex WHERE vid = $1 AND vend >= $2 ORDER BY vend ASC", vid, vstart).Scan(&s,&e)
	if err != nil && err != pgx.ErrNoRows {
		log.Fatal(err)
	}
	fmt.Println(e, "eeee")

	// if a vertex is found, delete it
	//UPDATE VERTEX NOT DELETE
	if e != "" {
		if err := mg.ExecQuery("UPDATE vertex SET vend = $1 WHERE vid = $2 AND vstart = $3",vstart, vid, s); err != nil{
			log.Fatal("Failed to update vertex: ", err)
		}
	}

	// insert new vertex
	err = mg.ExecQuery("INSERT INTO vertex (vid, vstart, vend) VALUES ($1, $2, $3)", vid, vstart, time.Now().Format(time.RFC3339Nano))
	if err != nil{
		log.Fatal("Failed to insert vertex: ", err)
	}

}


// func (mg MultiTable) GetAllAliveVertices(first, last string) {

// 	rows := mg.Query("SELECT vid, start, eend FROM dianode")
// 	defer rows.Close()

// }
