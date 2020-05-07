package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

// This backs up schemas and virtual schemas.

type schema struct {
	name         string
	comment      string
	isVirtual    bool
	adapter      string
	vSchemaProps []*vSchemaProp
}

type vSchemaProp struct {
	name  string
	value string
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

	addVirtualSchemaProps(src, schemas, crit)

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
			   schema_name AS o,
			   schema_comment,
			   schema_is_virtual,
			   adapter_script
		FROM exa_schemas
		LEFT JOIN exa_all_virtual_schemas
			USING(schema_name)
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
		s := &schema{
			name:      row[0].(string),
			isVirtual: row[3].(bool),
		}
		if row[2] != nil {
			s.comment = row[2].(string)
		}
		if row[4] != nil {
			s.adapter = row[4].(string)
		}
		schemas = append(schemas, s)
		dbObjs = append(dbObjs, s)
	}
	return schemas, dbObjs
}

func addVirtualSchemaProps(conn *exasol.Conn, schemas []*schema, crit Criteria) {
	sql := fmt.Sprintf(`
		SELECT schema_name AS s,
			   schema_name AS o,
			   property_name,
			   property_value
		FROM exa_dba_virtual_schema_properties
		WHERE %s
		ORDER BY schema_name, property_name
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		log.Fatal(err)
	}

	for _, row := range res {
		schemaName := row[0].(string)
		prop := &vSchemaProp{name: row[2].(string)}
		if row[3] != nil {
			prop.value = row[3].(string)
		}
		var schema *schema
		for _, s := range schemas {
			if s.name == schemaName {
				schema = s
				break
			}
		}
		if schema == nil {
			log.Fatal("Cannot find schema", schemaName)
		}

		schema.vSchemaProps = append(schema.vSchemaProps, prop)
	}
}

func createSchema(dst string, s *schema) {
	log.Noticef("Backing up schema %s", s.name)
	sql := ""
	if s.isVirtual {
		props := ""
		if len(s.vSchemaProps) > 0 {
			props = "\nWITH"
			for _, p := range s.vSchemaProps {
				props += fmt.Sprintf("\n  %s = '%s'", p.name, qStr(p.value))
			}
		}
		sql = fmt.Sprintf(
			"CREATE VIRTUAL SCHEMA IF NOT EXISTS [%s]\nUSING %s%s;\n",
			s.name, s.adapter, props,
		)
	} else {
		sql = fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS [%s];\n", s.name)
	}

	if s.comment != "" {
		sql += fmt.Sprintf("COMMENT ON SCHEMA [%s] IS '%s';\n", s.name, qStr(s.comment))
	}

	dir := filepath.Join(dst, s.name)
	os.MkdirAll(dir, os.ModePerm)

	file := filepath.Join(dir, "schema.sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup schema", sql, err)
	}
}
