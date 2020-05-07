package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

// This backsup schemas (needed if they are empty)

type schema struct {
	name string
}

func (s *schema) Schema() string { return s.name }
func (s *schema) Name() string   { return "" }

func BackupSchemas(src *exasol.Conn, dst string, crit Criteria, dropExtras bool) {
	log.Noticef("Backing up schemas")

	schemas, dbObjs := getSchemasToBackup(src, crit)
	if dropExtras {
		removeExtraObjects("schemas", dbObjs, dst, crit)
	}

	if len(schemas) == 0 {
		log.Warning("Object criteria did not match any schemas")
		return
	}

	dir := filepath.Join(dst, "schemas")
	os.MkdirAll(dir, os.ModePerm)
	for _, schema := range schemas {
		createSchema(dir, schema)
	}

	log.Notice("Done backing up schemas")
}

func getSchemasToBackup(conn *exasol.Conn, crit Criteria) ([]*schema, []dbObj) {
	sql := fmt.Sprintf(`
		SELECT schema_name AS s,
			   schema_name AS o
		FROM exa_schemas
		WHERE %s
		ORDER BY local.s
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		log.Fatal(err)
	}
	schemas := []*schema{}
	dbObjs := []dbObj{}
	for _, row := range res {
		s := &schema{name: row[0].(string)}
		schemas = append(schemas, s)
		dbObjs = append(dbObjs, s)
	}
	return schemas, dbObjs
}

func createSchema(dst string, s *schema) {
	log.Noticef("Backing up schema %s", s.name)
	sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;\n", s.name)

	dir := filepath.Join(dst, s.name)
	os.MkdirAll(dir, os.ModePerm)

	file := filepath.Join(dir, "schema.sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup schema", sql, err)
	}
}
