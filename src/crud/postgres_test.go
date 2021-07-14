//+build unit

package crud_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/geometry-labs/icon-blocks/crud"
)

var _ = Describe("Postgres", func() {

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

	BeforeEach(func() {
		dsn = "host=localhost user=postgres password=changeme dbname=postgres port=5432 sslmode=disable TimeZone=UTC"

		host = "localhost"
		port = "5432"
		user = "postgres"
		password = "changeme"
		dbname = "postgres"
		sslmode = "disable"
		timezone = "UTC"
	})

	Describe("create DSN string", func() {
		Context("new dsn string", func() {
			It("dsn string valid", func() {
				Expect(crud.NewDsn(host, port, user, password, dbname, sslmode, timezone)).To(Equal(dsn))
			})
		})
	})
})
