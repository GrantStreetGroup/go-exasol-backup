package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

// This backsup connections

type connection struct {
	name     string
	connStr  string
	username string
	comment  string
}

func BackupConnections(src *exasol.Conn, dst string) error {
	log.Notice("Backing up connections")

	connections, err := getConnectionsToBackup(src)
	if err != nil {
		return err
	}
	if len(connections) == 0 {
		log.Warning("No connections found")
		return nil
	}

	var sql string
	for _, connection := range connections {
		sql += createConnection(connection)
	}
	os.MkdirAll(dst, os.ModePerm)
	file := filepath.Join(dst, "connections.sql")
	err = ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup connections: %s", err)
	}

	log.Info("Done backing up connections")
	return nil
}

func getConnectionsToBackup(conn *exasol.Conn) ([]*connection, error) {
	sql := fmt.Sprintf(`
		SELECT connection_name AS s,
			   connection_name AS o,
			   connection_string,
			   user_name,
			   connection_comment
		FROM exa_dba_connections
		ORDER BY local.s`,
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, fmt.Errorf("Unable to get connections to backup: %s", err)
	}
	connections := []*connection{}
	for _, row := range res {
		c := &connection{
			name:    row[0].(string),
			connStr: row[2].(string),
		}
		if row[3] != nil {
			c.username = row[3].(string)
		}
		if row[4] != nil {
			c.comment = row[4].(string)
		}
		connections = append(connections, c)
	}
	return connections, nil
}

func createConnection(c *connection) string {
	log.Noticef("Backing up connection %s", c.name)
	sql := fmt.Sprintf(
		"CREATE CONNECTION [%s] TO '%s' USER '%s' IDENTIFIED BY ********;\n",
		c.name, qStr(c.connStr), c.username,
	)
	if c.comment != "" {
		sql += fmt.Sprintf(
			"COMMENT ON CONNECTION [%s] IS '%s';\n",
			c.name, qStr(c.comment),
		)
	}
	return sql
}
