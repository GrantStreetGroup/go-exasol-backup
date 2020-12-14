package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	"github.com/grantstreetgroup/go-exasol-client"
)

type script struct {
	schema  string
	name    string
	text    string
	comment string
}

func (s *script) Schema() string { return s.schema }
func (s *script) Name() string   { return s.name }

func BackupScripts(src *exasol.Conn, dst string, crit Criteria, dropExtras bool) error {
	log.Info("Backing up scripts")

	scripts, dbObjs, err := getScriptsToBackup(src, crit)
	if err != nil {
		return err
	}
	if dropExtras {
		removeExtraObjects("scripts", dbObjs, dst, crit)
	}
	if len(scripts) == 0 {
		log.Warning("Object criteria did not match any scripts")
		return nil
	}

	for _, s := range scripts {
		dir := filepath.Join(dst, "schemas", s.schema, "scripts")
		os.MkdirAll(dir, os.ModePerm)
		err = backupScript(dir, s)
		if err != nil {
			return err
		}
	}

	log.Info("Done backing up scripts")
	return nil
}

func getScriptsToBackup(conn *exasol.Conn, crit Criteria) ([]*script, []dbObj, error) {
	sql := fmt.Sprintf(`
		SELECT script_schema AS s,
			   script_name   AS o,
			   script_text,
			   script_comment
		FROM exa_all_scripts
		WHERE %s
		ORDER BY local.s, local.o
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to get scripts: %s", err)
	}
	scripts := []*script{}
	dbObjs := []dbObj{}
	for _, row := range res {
		s := &script{
			schema: row[0].(string),
			name:   row[1].(string),
			text:   row[2].(string),
		}
		if row[3] != nil {
			s.comment = row[3].(string)
		}
		scripts = append(scripts, s)
		dbObjs = append(dbObjs, s)
	}
	return scripts, dbObjs, nil
}

func backupScript(dst string, s *script) error {
	log.Infof("Backing up script %s.%s", s.schema, s.name)
	sText := regexp.MustCompile(`^CREATE `).
		ReplaceAllString(s.text, "CREATE OR REPLACE ")
	sql := fmt.Sprintf("OPEN SCHEMA [%s];\n--/\n%s\n/\n", s.schema, sText)
	if s.comment != "" {
		sql += fmt.Sprintf(
			"COMMENT ON SCRIPT [%s].[%s] IS '%s';\n",
			s.schema, s.name, qStr(s.comment),
		)
	}

	file := filepath.Join(dst, s.name+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup script: %s", err)
	}
	return nil
}
