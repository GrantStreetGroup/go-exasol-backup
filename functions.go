package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

type function struct {
	schema   string
	function string
	text     string
}

func (f *function) Schema() string { return f.schema }
func (f *function) Name() string   { return f.function }

func BackupFunctions(src *exasol.Conn, dst string, crit criteria, dropExtras bool) {
	log.Notice("Backingup functions")

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
	log.Info("Done backingup functions")
}

func getFunctionsToBackup(conn *exasol.Conn, crit criteria) ([]*function, []dbObj) {
	sql := fmt.Sprintf(`
		SELECT function_schema AS s,
			   function_name   AS o,
			   function_text
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
			schema:   row[0].(string),
			function: row[1].(string),
			text:     row[2].(string),
		}
		functions = append(functions, f)
		dbObjs = append(dbObjs, f)
	}
	return functions, dbObjs
}

func createFunction(dst string, f *function) {
	log.Noticef("Backingup function %s.%s", f.schema, f.function)
	createFunction := "CREATE OR REPLACE " + f.text
	sql := fmt.Sprintf("OPEN SCHEMA %s;\n--/\n%s;\n", f.schema, createFunction)
	file := filepath.Join(dst, f.function+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup function", sql, err)
	}
}
