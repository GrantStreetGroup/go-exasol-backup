package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/grantstreetgroup/go-exasol-client"
)

type function struct {
	schema  string
	name    string
	text    string
	comment string
}

func (f *function) Schema() string { return f.schema }
func (f *function) Name() string   { return f.name }

func BackupFunctions(src *exasol.Conn, dst string, crit Criteria, dropExtras bool) error {
	log.Info("Backing up functions")

	allFuncs, dbObjs, err := getFunctionsToBackup(src, crit)
	if err != nil {
		return err
	}
	if dropExtras {
		removeExtraObjects("functions", dbObjs, dst, crit)
	}

	if len(allFuncs) == 0 {
		log.Warning("Object criteria did not match any functions")
		return nil
	}

	for _, f := range allFuncs {
		dir := filepath.Join(dst, "schemas", f.schema, "functions")
		os.MkdirAll(dir, os.ModePerm)
		err = createFunction(dir, f)
		if err != nil {
			return err
		}
	}
	log.Info("Done backing up functions")
	return nil
}

func getFunctionsToBackup(conn *exasol.Conn, crit Criteria) ([]*function, []dbObj, error) {
	sql := fmt.Sprintf(`
		SELECT function_schema AS s,
			   function_name   AS o,
			   function_text,
			   function_comment
		FROM exa_all_functions
		WHERE %s
		ORDER BY local.s, local.o
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to get functions to backup: %s", err)
	}
	functions := []*function{}
	dbObjs := []dbObj{}
	for _, row := range res {
		f := &function{
			schema: row[0].(string),
			name:   row[1].(string),
			text:   row[2].(string),
		}
		if row[3] != nil {
			f.comment = row[3].(string)
		}
		functions = append(functions, f)
		dbObjs = append(dbObjs, f)
	}
	return functions, dbObjs, nil
}

func createFunction(dst string, f *function) error {
	log.Infof("Backing up function %s.%s", f.schema, f.name)
	fText := regexp.MustCompile(`(?m)/\s*$`).ReplaceAllString(f.text, "")
	sql := fmt.Sprintf(
		"OPEN SCHEMA [%s];\n--/\nCREATE OR REPLACE %s\n/\n",
		f.schema, fText,
	)
	if f.comment != "" {
		sql += fmt.Sprintf(
			"COMMENT ON FUNCTION [%s].[%s] IS '%s';\n",
			f.schema, f.name, qStr(f.comment),
		)
	}
	file := filepath.Join(dst, f.name+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup function: %s", err)
	}
	return nil
}
