package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	"github.com/grantstreetgroup/go-exasol-client"
)

type view struct {
	schema string
	view   string
	scope  string
	text   string
}

func (v *view) Schema() string { return v.schema }
func (v *view) Name() string   { return v.view }

func BackupViews(src *exasol.Conn, dst string, crit Criteria, maxRows int, dropExtras bool) {
	log.Notice("Backing up views")

	views, dbObjs := getViewsToBackup(src, crit)
	if dropExtras {
		removeExtraObjects("views", dbObjs, dst, crit)
	}
	if len(views) == 0 {
		log.Warning("Object criteria did not match any views")
		return
	}

	for _, v := range views {
		dir := filepath.Join(dst, "schemas", v.schema, "views")
		os.MkdirAll(dir, os.ModePerm)
		backupView(dir, v)
		if shouldBackupViewData(src, v, maxRows) {
			log.Noticef("Backing up view data for %s.%s", v.schema, v.view)
			wg := &sync.WaitGroup{}
			wg.Add(2)
			data := make(chan []byte)
			go readViewData(src, v, data, wg)
			go writeViewData(dir, v, data, wg)
			wg.Wait()
		}
	}

	log.Info("Done backing up views")
}

func getViewsToBackup(conn *exasol.Conn, crit Criteria) ([]*view, []dbObj) {
	sql := fmt.Sprintf(`
		SELECT view_schema AS s,
			   view_name   AS o,
			   scope_schema,
			   view_text
		FROM exa_all_views
		WHERE %s
		ORDER BY local.s, local.o
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		log.Fatal(err)
	}
	views := []*view{}
	dbObjs := []dbObj{}
	for _, row := range res {
		v := &view{
			schema: row[0].(string),
			view:   row[1].(string),
			text:   row[3].(string),
		}
		if row[2] == nil {
			v.scope = row[0].(string)
		} else {
			v.scope = row[2].(string)
		}
		views = append(views, v)
		dbObjs = append(dbObjs, v)
	}
	return views, dbObjs
}

func backupView(dir string, v *view) {
	log.Noticef("Backing up view %s.%s", v.schema, v.view)

	// We have to swap out the name too because if the view got renamed
	// the v.text still references the original name.
	r := regexp.MustCompile(`^(?is).*?CREATE[^V]+?VIEW\s+("?[\w_-]+"?\.)?"?[\w_-]+"?`)
	replacement := fmt.Sprintf(`CREATE OR REPLACE FORCE VIEW "%s"."%s"`, v.schema, v.view)
	createView := r.ReplaceAllString(v.text, replacement)

	sql := fmt.Sprintf("OPEN SCHEMA %s;\n%s;\n", v.scope, createView)
	file := filepath.Join(dir, v.view+".sql")

	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup view", sql, err)
	}
}

func shouldBackupViewData(conn *exasol.Conn, v *view, maxRows int) bool {
	if maxRows == 0 {
		return false
	}
	sql := fmt.Sprintf(
		`SELECT COUNT(*)FROM %s.%s`,
		conn.QuoteIdent(v.schema), conn.QuoteIdent(v.view),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		log.Fatal(err)
	}
	numRows := int(res[0][0].(float64))
	return numRows > 0 && numRows <= maxRows
}

func readViewData(conn *exasol.Conn, v *view, data chan<- []byte, wg *sync.WaitGroup) {
	defer func() {
		close(data)
		wg.Done()
	}()

	exportSQL := fmt.Sprintf(
		"EXPORT (SELECT * FROM %s.%s) INTO CSV AT '%%s' FILE 'data.csv'",
		conn.QuoteIdent(v.schema), conn.QuoteIdent(v.view),
	)
	_, err := conn.StreamQuery(exportSQL, data)
	if err != nil {
		log.Fatal("Unable to read:", err)
	}
}

func writeViewData(dst string, v *view, data <-chan []byte, wg *sync.WaitGroup) {
	defer func() { wg.Done() }()
	fp := filepath.Join(dst, v.view+".csv")
	f, err := os.Create(fp)
	if err != nil {
		log.Fatal("Unable to create file", fp, err)
	}
	for d := range data {
		_, err = f.Write(d)
		if err != nil {
			log.Fatal("Unable to write to file", fp, err)
		}
	}
	f.Close()
}
