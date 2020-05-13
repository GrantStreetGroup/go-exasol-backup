package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/grantstreetgroup/go-exasol-client"
)

// This backs up schemas and virtual schemas.

type schema struct {
	name         string
	comment      string
	isVirtual    bool
	adapter      string
	sizeLimit    uint64
	vSchemaProps []*vSchemaProp
}

type vSchemaProp struct {
	name  string
	value string
}

func (s *schema) Schema() string { return s.name }
func (s *schema) Name() string   { return "" }

func BackupSchemas(src *exasol.Conn, dst string, crit Criteria, dropExtras bool) error {
	log.Noticef("Backing up schemas")

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

	err = addVirtualSchemaProps(src, schemas, crit)
	if err != nil {
		return err
	}

	dir := filepath.Join(dst, "schemas")
	os.MkdirAll(dir, os.ModePerm)
	for _, schema := range schemas {
		err = createSchema(dir, schema)
		if err != nil {
			return err
		}
	}

	log.Notice("Done backing up schemas")
	return nil
}

func getSchemasToBackup(conn *exasol.Conn, crit Criteria) ([]*schema, []dbObj, error) {
	sql := fmt.Sprintf(`
		SELECT s.schema_name AS s,
			   s.schema_name AS o,
			   schema_comment,
			   schema_is_virtual,
			   adapter_script,
			   raw_object_size_limit
		FROM exa_schemas AS s
		JOIN exa_all_object_sizes AS os
		  ON s.schema_name = os.object_name
		 AND os.object_type = 'SCHEMA'
		LEFT JOIN exa_all_virtual_schemas AS vs
		  ON s.schema_name = vs.schema_name
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
			isVirtual: row[3].(bool),
		}
		if row[2] != nil {
			s.comment = row[2].(string)
		}
		if row[4] != nil {
			s.adapter = row[4].(string)
		}
		if row[5] != nil {
			s.sizeLimit = uint64(row[5].(float64))
		}
		schemas = append(schemas, s)
		dbObjs = append(dbObjs, s)
	}
	return schemas, dbObjs, nil
}

func addVirtualSchemaProps(conn *exasol.Conn, schemas []*schema, crit Criteria) error {
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
		return fmt.Errorf("Unable to get virtual schema properties: %s", err)
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
			return fmt.Errorf("Cannot find schema %s for virtual property", schemaName)
		}

		schema.vSchemaProps = append(schema.vSchemaProps, prop)
	}
	return nil
}

func createSchema(dst string, s *schema) error {
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
		adapter := strings.Split(s.adapter, ".")
		sql = fmt.Sprintf(
			"CREATE VIRTUAL SCHEMA IF NOT EXISTS [%s]\nUSING [%s].[%s]%s;\n",
			s.name, adapter[0], adapter[1], props,
		)
	} else {
		sql = fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS [%s];\n", s.name)
	}

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
