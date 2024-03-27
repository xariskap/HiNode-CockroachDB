package db

import (
	"context"
	"hinode/models"
	"log"

	"github.com/jackc/pgx/v5"
)

func GetConnection() *pgx.Conn {
	connectionString := "postgresql://root:root@localhost:26257/defaultdb"
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func CreateModel(db string, conn *pgx.Conn) *models.MultiTable {
	mtModel := models.NewMultiTable("hinode", conn)
	mtModel.CreateSchema()

	return &mtModel
}

func USE(db string, conn *pgx.Conn) *models.MultiTable {
	mtModel := models.NewMultiTable(db, conn)
	mtModel.ExecQuery("USE " + db)
	
	return &mtModel
}
