package db

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
)

func ConnetToCockroach() {

	conn, err := pgx.Connect(context.Background(), "postgresql://root@localhost:26257/defaultdb?sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	fmt.Println("Connected to CockroachDB")
}
