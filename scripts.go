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
	schema string
	script string
	text   string
}

func (s *script) Schema() string { return s.schema }
func (s *script) Name() string   { return s.script }

func BackupScripts(src *exasol.Conn, dst string, crit Criteria, dropExtras bool) {
	log.Notice("Backing up scripts")

	scripts, dbObjs := getScriptsToBackup(src, crit)
	if dropExtras {
		removeExtraObjects("scripts", dbObjs, dst, crit)
	}
	if len(scripts) == 0 {
		log.Warning("Object criteria did not match any scripts")
		return
	}

	for _, s := range scripts {
		dir := filepath.Join(dst, "schemas", s.schema, "scripts")
		os.MkdirAll(dir, os.ModePerm)
		backupScript(dir, s)
	}

	log.Info("Done backing up scripts")
}

func getScriptsToBackup(conn *exasol.Conn, crit Criteria) ([]*script, []dbObj) {
	sql := fmt.Sprintf(`
		SELECT script_schema AS s,
			   script_name   AS o,
			   script_text
		FROM exa_all_scripts
		WHERE %s
		ORDER BY local.s, local.o
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		log.Fatal(err)
	}
	scripts := []*script{}
	dbObjs := []dbObj{}
	for _, row := range res {
		s := &script{
			schema: row[0].(string),
			script: row[1].(string),
			text:   row[2].(string),
		}
		scripts = append(scripts, s)
		dbObjs = append(dbObjs, s)
	}
	return scripts, dbObjs
}

func backupScript(dst string, s *script) {
	log.Noticef("Backing up script %s.%s", s.schema, s.script)
	r := regexp.MustCompile(`^(?i)CREATE\s+(OR\s+REPLACE)?`)
	createScript := r.ReplaceAllString(s.text, "CREATE OR REPLACE ")
	sql := fmt.Sprintf("OPEN SCHEMA %s;\n%s;\n", s.schema, createScript)

	file := filepath.Join(dst, s.script+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup script", sql, err)
	}
}
