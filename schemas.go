package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/GrantStreetGroup/go-exasol-client"
)

// This backs up schemas and virtual schemas.

type schema struct {
	name         string
	comment      string
	sizeLimit    uint64
}

func (s *schema) Schema() string { return s.name }
func (s *schema) Name() string   { return "" }

func BackupSchemas(src *exasol.Conn, dst string, crit Criteria, dropExtras bool) error {
	log.Infof("Backing up schemas")

	schemas, dbObjs, err := getSchemasToBackup(src, crit)
	if err != nil {
		return err
	}
	if dropExtras {
		removeExtraObjects("schemas", dbObjs, dst, crit)
	}

	if len(schemas) == 0 {
		log.Warning("Object criteria did not match any schemas")
		return nil
	}

	dir := filepath.Join(dst, "schemas")
	os.MkdirAll(dir, os.ModePerm)
	for _, schema := range schemas {
		err = createSchema(dir, schema)
		if err != nil {
			return err
		}
	}

	log.Info("Done backing up schemas")
	return nil
}

func getSchemasToBackup(conn *exasol.Conn, crit Criteria) ([]*schema, []dbObj, error) {
	sql := fmt.Sprintf(`
		SELECT s.schema_name AS s,
			   s.schema_name AS o,
			   schema_comment,
			   raw_object_size_limit
		FROM exa_schemas AS s
		JOIN exa_all_object_sizes AS os
		  ON s.schema_name = os.object_name
		 AND os.object_type = 'SCHEMA'
		WHERE %s
		ORDER BY local.s
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to get schemas: %s", err)
	}
	schemas := []*schema{}
	dbObjs := []dbObj{}
	for _, row := range res {
		s := &schema{
			name:      row[0].(string),
		}
		if row[2] != nil {
			s.comment = row[2].(string)
		}
		if row[3] != nil {
			s.sizeLimit = uint64(row[3].(float64))
		}
		schemas = append(schemas, s)
		dbObjs = append(dbObjs, s)
	}
	return schemas, dbObjs, nil
}

func createSchema(dst string, s *schema) error {
	log.Infof("Backing up schema %s", s.name)
	sql := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS [%s];\n", s.name)

	if s.comment != "" {
		sql += fmt.Sprintf("COMMENT ON SCHEMA [%s] IS '%s';\n", s.name, qStr(s.comment))
	}
	if s.sizeLimit > 0 {
		sql += fmt.Sprintf("ALTER SCHEMA [%s] SET RAW_SIZE_LIMIT = %d;\n", s.name, s.sizeLimit)
	}

	dir := filepath.Join(dst, s.name)
	os.MkdirAll(dir, os.ModePerm)

	file := filepath.Join(dir, "schema.sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup schema: %s", err)
	}
	return nil
}
