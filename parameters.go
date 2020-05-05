package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

// This backsup system parameters

type parameter struct {
	name  string
	value string
}

func BackupParameters(src *exasol.Conn, dst string) {
	log.Notice("Backingup parameters")

	parameters := getParametersToBackup(src)
	if len(parameters) == 0 {
		log.Warning("No parameters found")
		return
	}

	var sql string
	for _, parameter := range parameters {
		sql += createParameter(parameter)
	}

	os.MkdirAll(dst, os.ModePerm)
	file := filepath.Join(dst, "parameters.sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup parameters", sql, err)
	}

	log.Info("Done backingup parameters")
}

func getParametersToBackup(conn *exasol.Conn) []*parameter {
	sql := fmt.Sprintf(`
		SELECT parameter_name AS s,
			   parameter_name AS o,
			   system_value
		FROM exa_parameters
		WHERE parameter_name != 'NICE'
		ORDER BY local.s`,
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		log.Fatal(err)
	}
	parameters := []*parameter{}
	for _, row := range res {
		p := &parameter{name: row[0].(string)}

		if row[2] != nil {
			p.value = row[2].(string)
		}
		parameters = append(parameters, p)
	}
	return parameters
}

func createParameter(p *parameter) string {
	log.Noticef("Backingup parameter %s", p.name)
	q := "'"
	if p.name == "NLS_FIRST_DAY_OF_WEEK" {
		// This param is numeric so no quotes
		q = ""
	}
	return fmt.Sprintf("ALTER SYSTEM SET %s=%s%s%s;\n", p.name, q, p.value, q)
}
