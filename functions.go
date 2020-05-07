package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

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

func BackupFunctions(src *exasol.Conn, dst string, crit Criteria, dropExtras bool) {
	log.Notice("Backing up functions")

	allFuncs, dbObjs := getFunctionsToBackup(src, crit)
	if dropExtras {
		removeExtraObjects("functions", dbObjs, dst, crit)
	}

	if len(allFuncs) == 0 {
		log.Warning("Object criteria did not match any functions")
		return
	}

	for _, f := range allFuncs {
		dir := filepath.Join(dst, "schemas", f.schema, "functions")
		os.MkdirAll(dir, os.ModePerm)
		createFunction(dir, f)
	}
	log.Info("Done backing up functions")
}

func getFunctionsToBackup(conn *exasol.Conn, crit Criteria) ([]*function, []dbObj) {
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
		log.Fatal(err)
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
	return functions, dbObjs
}

func createFunction(dst string, f *function) {
	log.Noticef("Backing up function %s.%s", f.schema, f.name)
	createFunction := "CREATE OR REPLACE " + f.text
	sql := fmt.Sprintf("OPEN SCHEMA %s;\n--/\n%s;\n", f.schema, createFunction)
	if f.comment != "" {
		sql += fmt.Sprintf("COMMENT ON FUNCTION %s IS '%s';\n", f.name, exasol.QuoteStr(f.comment))
	}
	file := filepath.Join(dst, f.name+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup function", sql, err)
	}
}
