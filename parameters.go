package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/GrantStreetGroup/go-exasol-client"
)

// This backsup system parameters

type parameter struct {
	name  string
	value string
}

func BackupParameters(src *exasol.Conn, dst string) error {
	log.Info("Backing up parameters")

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
	sql := `
		SELECT parameter_name,
			   system_value
		FROM exa_parameters
		WHERE parameter_name != 'NICE'
		ORDER BY parameter_name
	`
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, fmt.Errorf("Unable to get parameters to backup: %s", err)
	}
	parameters := []*parameter{}
	for _, row := range res {
		p := &parameter{name: row[0].(string)}

		if row[1] != nil {
			p.value = row[1].(string)
		}
		parameters = append(parameters, p)
	}
	return parameters, nil
}

func createParameter(p *parameter) string {
	log.Infof("Backing up parameter %s", p.name)
	q := "'"
	if p.name == "NLS_FIRST_DAY_OF_WEEK" ||
		p.name == "QUERY_TIMEOUT" ||
		p.name == "ST_MAX_DECIMAL_DIGITS" ||
		p.name == "SQL_PREPROCESSOR_SCRIPT" ||
		p.name == "DEFAULT_PRIORITY_GROUP" ||
		p.name == "DEFAULT_CONSUMER_GROUP" {
		// These params don't need quotes
		q = ""
	}
	return fmt.Sprintf("ALTER SYSTEM SET %s=%s%s%s;\n", p.name, q, p.value, q)
}
