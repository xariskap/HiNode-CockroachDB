package db

import (
	"context"
	"hinode/models"
	"log"

	"github.com/jackc/pgx/v5"
)

func GetConnection() *pgx.Conn {
	//connectionString := "postgresql://root:root@localhost:26257/defaultdb"
	conn, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func CreateMtModel(db string, conn *pgx.Conn) *models.MultiTable {
	mtModel := models.NewMultiTable(db, conn)
	mtModel.CreateSchema()

	return &mtModel
}

func CreateStModel(db string, conn *pgx.Conn) *models.SingleTable {
	stModel := models.NewSingleTable(db, conn)
	stModel.CreateSchema()

	return &stModel
}

func USEmt(db string, conn *pgx.Conn) *models.MultiTable {
	mtModel := models.NewMultiTable(db, conn)
	mtModel.ExecQuery("USE " + db)

	return &mtModel
}

func USEst(db string, conn *pgx.Conn) *models.SingleTable {
	stModel := models.NewSingleTable(db, conn)
	stModel.ExecQuery("USE " + db)

	return &stModel
}
