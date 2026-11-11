package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/GrantStreetGroup/go-exasol-client"
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
		if maxRows > 0 {
			err = backupViewData(src, dir, v, maxRows)
			if err != nil {
				return err
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
	res, err := fetchSlice(conn, sql)
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
	r := regexp.MustCompile(`^(?is).*?CREATE[^V]+?VIEW\s+(("[^"]+"|[\w_-]+)\.)?("[^"]+"|[\w_-]+)`)
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

// backupViewData exports the view's data to a single CSV file when the
// view has at least one and at most maxRows rows. The total row count is
// prepended to every exported record as an extra column and stripped out
// of the stream on the way to the file, so the view only gets evaluated
// once rather than once for the count and again for the export.
func backupViewData(conn *exasol.Conn, dir string, v *view, maxRows int) error {
	orderBy, err := getViewOrderBy(conn, v)
	if err != nil {
		return err
	}
	// The ORDER BY is required for correctness, not just stable diffs:
	// without it Exasol applies the LIMIT before COUNT(*) OVER () inside
	// an EXPORT (a plain SELECT is unaffected) so the total would be
	// capped at maxRows and the skip check below would be silently
	// defeated.
	exportSQL := fmt.Sprintf(
		`EXPORT (SELECT COUNT(*) OVER () AS "BACKUP_TOTAL_ROWS__", v.* FROM [%s].[%s] v%s LIMIT %d) INTO CSV AT '%%s' FILE 'data.csv'`,
		v.schema, v.name, orderBy, maxRows,
	)
	res := conn.StreamQuery(exportSQL)

	stripper := newTotalRowsStripper()
	fp := filepath.Join(dir, v.name+".csv")
	var file *os.File
	var retErr error
	skip := false
	for d := range res.Data {
		// Always drain res.Data, even after an error or when skipping
		// the view: the producer goroutine blocks until it's read.
		if skip || retErr != nil {
			continue
		}
		out, err := stripper.strip(d)
		if err != nil {
			retErr = err
			continue
		}
		if file == nil && stripper.total >= 0 {
			if stripper.total > maxRows {
				log.Infof("Skipping view data for %s.%s: %d rows exceeds the maximum of %d",
					v.schema, v.name, stripper.total, maxRows)
				skip = true
				continue
			}
			log.Infof("Backing up view data for %s.%s", v.schema, v.name)
			if file, err = os.Create(fp); err != nil {
				retErr = fmt.Errorf("Unable to create view file %s: %s", fp, err)
				continue
			}
		}
		if file != nil {
			if _, err = file.Write(out); err != nil {
				retErr = fmt.Errorf("Unable to write view file %s: %s", fp, err)
			}
		}
	}
	if res.Error != nil && retErr == nil {
		// res.Error is set before res.Data is closed so it is safe to
		// check it now rather than before draining the channel.
		retErr = fmt.Errorf("Unable to read view %s: %s", v.name, res.Error)
	}
	if file != nil {
		if err = file.Close(); err != nil && retErr == nil {
			retErr = fmt.Errorf("Unable to write view file %s: %s", fp, err)
		}
	}
	return retErr
}

// totalRowsStripper removes the leading BACKUP_TOTAL_ROWS__ column that
// backupViewData prepends to every exported CSV record, and remembers the
// total row count found in the first record. Records are processed a byte
// at a time so it works however the stream happens to be chunked.
type totalRowsStripper struct {
	inTotal bool   // inside the leading total column of a record
	inQuote bool   // inside a quoted CSV field
	digits  []byte // count digits buffered while the total is still unknown
	total   int    // total row count, -1 until known
}

func newTotalRowsStripper() *totalRowsStripper {
	return &totalRowsStripper{inTotal: true, total: -1}
}

func (t *totalRowsStripper) strip(in []byte) ([]byte, error) {
	out := make([]byte, 0, len(in))
	for _, b := range in {
		if t.inTotal {
			if b != ',' {
				if t.total < 0 {
					t.digits = append(t.digits, b)
				}
				continue
			}
			t.inTotal = false
			if t.total < 0 {
				n, err := strconv.Atoi(string(t.digits))
				if err != nil {
					return out, fmt.Errorf("Unable to parse view row count %q: %s", t.digits, err)
				}
				t.total = n
			}
			t.digits = nil
			continue
		}
		out = append(out, b)
		if b == '"' {
			// Toggling on every '"' correctly skips the "" escape.
			t.inQuote = !t.inQuote
		} else if b == '\n' && !t.inQuote {
			t.inTotal = true
		}
	}
	return out, nil
}

func getViewOrderBy(conn *exasol.Conn, v *view) (string, error) {
	// DESCRIBE returns one row per column; view columns may not be
	// present in exa_all_columns.
	res, err := fetchSlice(conn, fmt.Sprintf("DESCRIBE [%s].[%s]", v.schema, v.name))
	if err != nil {
		return "", fmt.Errorf("Unable to get columns for view %s.%s: %s", v.schema, v.name, err)
	}
	numCols := len(res)
	if numCols == 0 {
		return "", nil
	}
	positions := make([]string, numCols)
	for i := range positions {
		// Column 1 is BACKUP_TOTAL_ROWS__ so the data starts at 2.
		positions[i] = strconv.Itoa(i + 2)
	}
	return " ORDER BY " + strings.Join(positions, ","), nil
}
