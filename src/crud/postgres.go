package crud

import (
	"fmt"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/geometry-labs/icon-blocks/config"
)

func formatPostgresDSN(host string, port string, user string, password string, dbname string, sslmode string, timezone string) string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=%s TimeZone=%s",
		host, user, password, dbname, port, sslmode, timezone)
}

func getPostgresConn() *gorm.DB {
	dsn := formatPostgresDSN(
		config.Config.DbHost,
		config.Config.DbPort,
		config.Config.DbUser,
		config.Config.DbPassword,
		config.Config.DbName,
		config.Config.DbSslmode,
		config.Config.DbTimezone,
	)

	session, err := retryGetPostgresSession(dsn)
	if err != nil {
		zap.S().Warn("Cannot create a connection to postgres", err)
	}

	zap.S().Info("Successful connection to postgres")
	return session
}

func createSession(dsn string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		zap.S().Info("err:", err)
	}

	return db, err
}

func retryGetPostgresSession(dsn string) (*gorm.DB, error) {
	var session *gorm.DB
	operation := func() error {
		sess, err := createSession(dsn)
		if err != nil {
			zap.S().Info("POSTGRES SESSION Error : ", err.Error())
		} else {
			session = sess
		}
		return err
	}
	neb := backoff.NewExponentialBackOff()
	err := backoff.Retry(operation, neb)
	return session, err
}
