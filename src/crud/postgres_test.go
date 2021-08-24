package crud

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/geometry-labs/icon-blocks/config"
	"github.com/geometry-labs/icon-blocks/logging"
)

func init() {
	// Read env
	// Defaults should work
	config.ReadEnvironment()

	// Set up logging
	logging.Init()
}

func TestFormatPostgresDSN(t *testing.T) {
	assert := assert.New(t)

	var (
		dsn string

		host     string
		port     string
		user     string
		password string
		dbname   string
		sslmode  string
		timezone string
	)

	dsn = "host=localhost user=postgres password=changeme dbname=postgres port=5432 sslmode=disable TimeZone=UTC"

	host = "localhost"
	port = "5432"
	user = "postgres"
	password = "changeme"
	dbname = "postgres"
	sslmode = "disable"
	timezone = "UTC"

	assert.Equal(dsn, formatPostgresDSN(host, port, user, password, dbname, sslmode, timezone))
}

func TestGetPostgressConn(t *testing.T) {
	assert := assert.New(t)

	conn := getPostgresConn()
	assert.NotEqual(nil, conn)
}
