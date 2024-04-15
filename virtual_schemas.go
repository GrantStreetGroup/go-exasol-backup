package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/GrantStreetGroup/go-exasol-client"
)

// This backs up schemas and virtual schemas.

type virtual_schema struct {
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

func (s *virtual_schema) Schema() string { return s.name }
func (s *virtual_schema) Name() string   { return "" }

func BackupVirtualSchemas(src *exasol.Conn, dst string, crit Criteria, dropExtras bool) error {
	log.Infof("Backing up schemas")

	schemas, dbObjs, err := getVirtualSchemasToBackup(src, crit)
	if err != nil {
		return err
	}
	if dropExtras {
		removeExtraObjects("virtual_schemas", dbObjs, dst, crit)
	}

	if len(schemas) == 0 {
		log.Warning("Object criteria did not match any virtual schemas")
		return nil
	}

	err = addVirtualSchemaProps(src, schemas, crit)
	if err != nil {
		return err
	}

	dir := filepath.Join(dst, "schemas")
	os.MkdirAll(dir, os.ModePerm)
	for _, schema := range schemas {
		err = createVirtualSchema(dir, schema)
		if err != nil {
			return err
		}
	}

	log.Info("Done backing up virtual schemas")
	return nil
}

func getVirtualSchemasToBackup(conn *exasol.Conn, crit Criteria) ([]*virtual_schema, []dbObj, error) {
	adapterColumn := "adapter_script"
	if capability.version >= 8 {
		adapterColumn = `CONCAT(adapter_script_schema,'.',adapter_script_name)`
	}

	sql := fmt.Sprintf(`
		SELECT s.schema_name AS s,
			   s.schema_name AS o,
			   s.schema_comment,
			   %s
		FROM exa_schemas AS s
		JOIN exa_all_virtual_schemas AS vs
		  ON s.schema_name = vs.schema_name
		WHERE %s
		ORDER BY local.s
		`, adapterColumn, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to get virtual schemas: %s", err)
	}
	schemas := []*virtual_schema{}
	dbObjs := []dbObj{}
	for _, row := range res {
		s := &virtual_schema{
			name:      row[0].(string),
		}
		if row[2] != nil {
			s.comment = row[2].(string)
		}
		if row[3] != nil {
			s.adapter = row[3].(string)
		}
		schemas = append(schemas, s)
		dbObjs = append(dbObjs, s)
	}
	return schemas, dbObjs, nil
}

func addVirtualSchemaProps(conn *exasol.Conn, schemas []*virtual_schema, crit Criteria) error {
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
		var schema *virtual_schema
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

func createVirtualSchema(dst string, s *virtual_schema) error {
	log.Infof("Backing up virtual schema %s", s.name)
	props := ""
	if len(s.vSchemaProps) > 0 {
		props = "\nWITH"
		for _, p := range s.vSchemaProps {
			props += fmt.Sprintf("\n  %s = '%s'", p.name, qStr(p.value))
		}
	}
	adapter := strings.Split(s.adapter, ".")
	sql := fmt.Sprintf(
		"CREATE VIRTUAL SCHEMA IF NOT EXISTS [%s]\nUSING [%s].[%s]%s;\n",
		s.name, adapter[0], adapter[1], props,
	)

	if s.comment != "" {
		sql += fmt.Sprintf("COMMENT ON SCHEMA [%s] IS '%s';\n", s.name, qStr(s.comment))
	}

	dir := filepath.Join(dst, s.name)
	os.MkdirAll(dir, os.ModePerm)

	file := filepath.Join(dir, "schema.sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup virtual schema: %s", err)
	}
	return nil
}
