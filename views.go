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
	name   string
	scope  string
	text   string
}

func (v *view) Schema() string { return v.schema }
func (v *view) Name() string   { return v.name }

func BackupViews(src *exasol.Conn, dst string, crit Criteria, maxRows int, dropExtras bool) error {
	log.Info("Backing up views")

	views, dbObjs, err := getViewsToBackup(src, crit)
	if err != nil {
		return err
	}
	if dropExtras {
		removeExtraObjects("views", dbObjs, dst, crit)
	}
	if len(views) == 0 {
		log.Warning("Object criteria did not match any views")
		return nil
	}

	for _, v := range views {
		dir := filepath.Join(dst, "schemas", v.schema, "views")
		os.MkdirAll(dir, os.ModePerm)
		err = backupView(dir, v)
		if err != nil {
			return err
		}
		shouldBackup, err := shouldBackupViewData(src, v, maxRows)
		if err != nil {
			return err
		}
		if shouldBackup {
			log.Infof("Backing up view data for %s.%s", v.schema, v.name)
			wg := &sync.WaitGroup{}
			wg.Add(2)
			data := make(chan []byte)
			errors := make(chan error, 2)
			go readViewData(src, v, data, errors, wg)
			go writeViewData(dir, v, data, errors, wg)
			wg.Wait()
			select {
			case err = <-errors:
				return err
			default:
			}
		}
	}

	log.Info("Done backing up views")
	return nil
}

func getViewsToBackup(conn *exasol.Conn, crit Criteria) ([]*view, []dbObj, error) {
	// Not that view and view-column comments can only be added
	// directly in the context of a CREATE VIEW so as long as we
	// pull out view_text we got them all.
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
		return nil, nil, fmt.Errorf("Unable to get views: %s", err)
	}
	views := []*view{}
	dbObjs := []dbObj{}
	for _, row := range res {
		v := &view{
			schema: row[0].(string),
			name:   row[1].(string),
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
	return views, dbObjs, nil
}

func backupView(dir string, v *view) error {
	log.Infof("Backing up view %s.%s", v.schema, v.name)

	// We have to swap out the name too because if the view got renamed
	// the v.text still references the original name.
	r := regexp.MustCompile(`^(?is).*?CREATE[^V]+?VIEW\s+("?[\w_-]+"?\.)?"?[\w_-]+"?`)
	replacement := fmt.Sprintf(`CREATE OR REPLACE FORCE VIEW "%s"."%s"`, v.schema, v.name)
	createView := r.ReplaceAllString(v.text, replacement)

	sql := fmt.Sprintf("OPEN SCHEMA [%s];\n%s;\n", v.scope, createView)
	file := filepath.Join(dir, v.name+".sql")

	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup view %s: %s", v.name, err)
	}
	return nil
}

func shouldBackupViewData(conn *exasol.Conn, v *view, maxRows int) (bool, error) {
	if maxRows == 0 {
		return false, nil
	}
	sql := fmt.Sprintf(`SELECT COUNT(*) FROM [%s].[%s]`, v.schema, v.name)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return false, fmt.Errorf("Unable to number of view rows: %s", err)
	}
	numRows := int(res[0][0].(float64))
	return numRows > 0 && numRows <= maxRows, nil
}

func readViewData(conn *exasol.Conn, v *view, data chan<- []byte, errors chan<- error, wg *sync.WaitGroup) {
	defer func() {
		close(data)
		wg.Done()
	}()

	exportSQL := fmt.Sprintf(
		"EXPORT (SELECT * FROM [%s].[%s]) INTO CSV AT '%%s' FILE 'data.csv'",
		v.schema, v.name,
	)
	res := conn.StreamQuery(exportSQL)
	if res.Error != nil {
		errors <- fmt.Errorf("Unable to read view %s: %s", v.name, res.Error)
		return
	}
	for d := range res.Data {
		data <- d
	}
}

func writeViewData(dst string, v *view, data <-chan []byte, errors chan<- error, wg *sync.WaitGroup) {
	defer func() { wg.Done() }()
	fp := filepath.Join(dst, v.name+".csv")
	f, err := os.Create(fp)
	if err != nil {
		errors <- fmt.Errorf("Unable to create view file %s: %s", fp, err)
		return
	}
	for d := range data {
		_, err = f.Write(d)
		if err != nil {
			errors <- fmt.Errorf("Unable to write view file %s: %s", fp, err)
			return
		}
	}
	f.Close()
}
