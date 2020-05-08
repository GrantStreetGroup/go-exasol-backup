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

func BackupParameters(src *exasol.Conn, dst string) error {
	log.Notice("Backing up parameters")

	parameters, err := getParametersToBackup(src)
	if err != nil {
		return err
	}
	if len(parameters) == 0 {
		log.Warning("No parameters found")
		return nil
	}

	var sql string
	for _, parameter := range parameters {
		sql += createParameter(parameter)
	}

	os.MkdirAll(dst, os.ModePerm)
	file := filepath.Join(dst, "parameters.sql")
	err = ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup parameters: %s", err)
	}

	log.Info("Done backing up parameters")
	return nil
}

func getParametersToBackup(conn *exasol.Conn) ([]*parameter, error) {
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
		return nil, fmt.Errorf("Unable to get parameters to backup: %s", err)
	}
	parameters := []*parameter{}
	for _, row := range res {
		p := &parameter{name: row[0].(string)}

		if row[2] != nil {
			p.value = row[2].(string)
		}
		parameters = append(parameters, p)
	}
	return parameters, nil
}

func createParameter(p *parameter) string {
	log.Noticef("Backing up parameter %s", p.name)
	q := "'"
	if p.name == "NLS_FIRST_DAY_OF_WEEK" {
		// This param is numeric so no quotes
		q = ""
	}
	return fmt.Sprintf("ALTER SYSTEM SET %s=%s%s%s;\n", p.name, q, p.value, q)
}
