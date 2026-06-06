package mysql_client

import (
	"context"
)

type MigrationWorker struct {
	MySQLClient *mysqlClient
	Tables      []string
}

func InitMigrationWorker(mysqlClient *mysqlClient) *MigrationWorker {
	return &MigrationWorker{
		MySQLClient: mysqlClient,
	}
}

func (m *MigrationWorker) RegisterMigration(queries ...string) {
	for _, query := range queries {
		m.Tables = append(m.Tables, query)
	}
}

// Migration runs database migrations within the given context.
// Currently returns nil as migrations are not implemented.
func (m *MigrationWorker) Migration(ctx context.Context) error {
	for _, migration := range m.Tables {
		_, err := m.MySQLClient.db.ExecContext(ctx, migration)
		if err != nil {
			return err
		}
	}
	return nil
}
