// TODO add partition and distribution keys

package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/grantstreetgroup/go-exasol-client"
)

type table struct {
	schema      string
	name        string
	rowCount    float64
	columns     []*column
	constraints []*constraint
	data        chan []byte
	comment     string
}

type column struct {
	name       string
	colType    string
	colDefault string
	identity   string
	comment    string
}

type constraint struct {
	columns []string
	conType string
	enabled bool
}

func (t *table) Schema() string { return t.schema }
func (t *table) Name() string   { return t.name }

func BackupTables(src *exasol.Conn, dst string, crit Criteria, maxRows int, dropExtras bool) error {
	log.Notice("Backing up tables")
	wg := &sync.WaitGroup{}
	wg.Add(2)

	tables := make(chan *table, 10)
	errors := make(chan error, 2)
	go readTables(src, tables, crit, maxRows, dst, dropExtras, errors, wg)
	go writeTables(dst, tables, crit, maxRows, errors, wg)

	wg.Wait()
	log.Info("Done backing up tables")
	select {
	case err := <-errors:
		return err
	default:
		return nil
	}
}

func readTables(conn *exasol.Conn, out chan<- *table, crit Criteria, maxRows int, dst string, dropExtras bool, errors chan<- error, wg *sync.WaitGroup) {
	defer func() {
		close(out)
		wg.Done()
	}()

	tables, dbObjs, err := getTablesToBackup(conn, crit)
	if err != nil {
		errors <- err
		return
	}
	if dropExtras {
		removeExtraObjects("tables", dbObjs, dst, crit)
	}
	if len(tables) == 0 {
		log.Warning("Object criteria did not match any tables")
		return
	}

	err = addTableColumns(conn, tables, crit)
	if err != nil {
		errors <- err
		return
	}
	err = addTableConstraints(conn, tables, crit)
	if err != nil {
		errors <- err
		return
	}

	for _, table := range tables {
		err = readTable(conn, table, out, maxRows)
		if err != nil {
			errors <- err
			return
		}
	}
}

func readTable(conn *exasol.Conn, t *table, out chan<- *table, maxRows int) error {
	log.Noticef("Backing up %s.%s", t.schema, t.name)
	if t.rowCount == 0 || t.rowCount > float64(maxRows) {
		out <- t
		return nil
	}
	t.data = make(chan []byte, 10000)
	out <- t

	var orderBys []string
	for _, cnst := range t.constraints {
		if cnst.conType == "PRIMARY KEY" {
			orderBys = cnst.columns
		}
	}
	if len(orderBys) == 0 {
		for _, col := range t.columns {
			orderBys = append(orderBys, col.name)
		}
	}
	exportSQL := fmt.Sprintf(
		"EXPORT (SELECT * FROM [%s].[%s] ORDER BY [%s]) INTO CSV AT '%%s' FILE 'data.csv'",
		t.schema, t.name, strings.Join(orderBys, `],[`),
	)

	start := time.Now()
	bytesRead, err := conn.StreamQuery(exportSQL, t.data)
	if err != nil {
		return fmt.Errorf("Unable to read table %s.%s: %s", t.schema, t.name, err)
	}
	close(t.data)
	duration := time.Since(start).Seconds()

	totalMB := float64(bytesRead) / 1048576
	mbps := totalMB / duration
	rps := t.rowCount / duration
	log.Infof("Read %0.fMB in %0.fs @ %0.fMBps and %0.frps", totalMB, duration, mbps, rps)
	return nil
}

func getTablesToBackup(conn *exasol.Conn, crit Criteria) ([]*table, []dbObj, error) {
	sql := fmt.Sprintf(`
		SELECT table_schema AS s,
			   table_name AS o,
			   table_row_count,
			   table_comment
		FROM exa_all_tables
		WHERE table_is_virtual = FALSE
		  AND (%s)
		ORDER BY table_schema, table_name
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, nil, fmt.Errorf("Unable to get tables: %s", err)
	}
	tables := []*table{}
	dbObjs := []dbObj{}
	for _, row := range res {
		t := &table{
			schema: row[0].(string),
			name:   row[1].(string),
		}
		if row[2] != nil {
			t.rowCount = row[2].(float64)
		}
		if row[3] != nil {
			t.comment = row[3].(string)
		}
		tables = append(tables, t)
		dbObjs = append(dbObjs, t)
	}
	return tables, dbObjs, nil
}

func addTableColumns(conn *exasol.Conn, tables []*table, crit Criteria) error {
	sql := fmt.Sprintf(`
		SELECT column_schema AS s,
			   column_table  AS o,
			   column_name,    column_type,
			   column_default, column_identity,
			   column_comment
		FROM exa_all_columns
		WHERE column_object_type = 'TABLE'
		  AND column_is_virtual = FALSE
		  AND (%s)
		ORDER BY column_schema, column_table, column_ordinal_position
		`, crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get table columns: %s", err)
	}

	for _, row := range res {
		schemaName := row[0].(string)
		tableName := row[1].(string)
		col := &column{
			name:    row[2].(string),
			colType: row[3].(string),
		}
		if row[4] != nil {
			col.colDefault = row[4].(string)
		}
		if row[5] != nil {
			col.identity = row[5].(string)
		}
		if row[6] != nil {
			col.comment = row[6].(string)
		}
		var table *table
		for _, t := range tables {
			if t.schema == schemaName &&
				t.name == tableName {
				table = t
				break
			}
		}
		if table == nil {
			return fmt.Errorf("Unable to find table %s.%s for column", schemaName, tableName)
		}

		table.columns = append(table.columns, col)
	}
	return nil
}

func addTableConstraints(conn *exasol.Conn, tables []*table, crit Criteria) error {
	sql := fmt.Sprintf(`
		SELECT con.constraint_schema AS s,
			   con.constraint_table  AS o,
			   con.constraint_type,
			   con.constraint_enabled,
			   cols.columns
		FROM exa_all_constraints AS con
		JOIN (
			SELECT constraint_schema AS s,
				   constraint_table  AS o,
				   constraint_name,
				   GROUP_CONCAT(
					   column_name
					   ORDER BY ordinal_position
					   SEPARATOR ','
				   ) AS columns
		    FROM exa_all_constraint_columns
			WHERE %s
			GROUP BY local.s, local.o, constraint_name
		) AS cols USING(constraint_name)
		WHERE con.constraint_type IN ('PRIMARY KEY','NOT NULL')
	      AND (%s)
		ORDER BY local.s, local.o
		`, crit.getSQLCriteria(), crit.getSQLCriteria(),
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get table constraints: %s", err)
	}

	for _, row := range res {
		schemaName := row[0].(string)
		tableName := row[1].(string)
		con := &constraint{
			conType: row[2].(string),
			enabled: row[3].(bool),
			columns: strings.Split(row[4].(string), ","),
		}

		var table *table
		for _, t := range tables {
			if t.schema == schemaName &&
				t.name == tableName {
				table = t
				break
			}
		}
		if table == nil {
			return fmt.Errorf("Unable to find table %s.%s for constraint", schemaName, tableName)
		}

		table.constraints = append(table.constraints, con)
	}
	return nil
}

func writeTables(dst string, in <-chan *table, crit Criteria, maxRows int, errors chan<- error, wg *sync.WaitGroup) {
	for t := range in {
		dir := filepath.Join(dst, "schemas", t.schema, "tables")
		os.MkdirAll(dir, os.ModePerm)
		err := createTable(dir, t)
		if err != nil {
			errors <- err
			return
		}
		err = writeTableData(dir, t, maxRows)
		if err != nil {
			errors <- err
			return
		}
		t.data = nil // otherwise seems to leak mem
	}

	wg.Done()
}

func createTable(dir string, t *table) error {
	var cols []string
	for _, c := range t.columns {
		col := fmt.Sprintf(`"%s" %s`, c.name, c.colType)
		if c.colDefault != "" {
			col += fmt.Sprintf(" DEFAULT %s", c.colDefault)
		}
		if c.identity != "" {
			col += fmt.Sprintf(" IDENTITY %s", c.identity)
		}
		for _, cnst := range t.constraints {
			if cnst.conType == "NOT NULL" &&
				cnst.columns[0] == c.name {
				col += " NOT NULL"
				if cnst.enabled {
					col += " ENABLE"
				} else {
					col += " DISABLE"
				}
				break
			}
		}
		if c.comment != "" {
			col += fmt.Sprintf(" COMMENT IS '%s'", qStr(c.comment))
		}
		cols = append(cols, col)
	}

	for _, cnst := range t.constraints {
		if cnst.conType == "PRIMARY KEY" {
			col := fmt.Sprintf(
				`PRIMARY KEY ("%s")`,
				strings.Join(cnst.columns, `","`),
			)
			if cnst.enabled {
				col += " ENABLE"
			} else {
				col += " DISABLE"
			}
			cols = append(cols, col)
		}
	}

	sql := fmt.Sprintf(
		"CREATE OR REPLACE TABLE \"%s\".\"%s\" (\n\t%s\n);\n",
		t.schema, t.name, strings.Join(cols, ",\n\t"),
	)
	if t.comment != "" {
		sql += fmt.Sprintf("COMMENT ON TABLE [%s] IS '%s';\n", t.name, qStr(t.comment))
	}
	file := filepath.Join(dir, t.name+".sql")

	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup table %s.%s: %s", t.schema, t.name, err)
	}
	return nil
}

func writeTableData(dir string, t *table, maxRows int) error {
	if t.rowCount == 0 || t.rowCount > float64(maxRows) {
		return nil
	}
	fp := filepath.Join(dir, t.name+".csv")
	f, err := os.Create(fp)
	if err != nil {
		return fmt.Errorf("Unable to create file %s: %s", fp, err)
	}
	for d := range t.data {
		_, err = f.Write(d)
		if err != nil {
			return fmt.Errorf("Unable to write to file %s: %s", fp, err)
		}
	}
	f.Close()
	return nil
}
