package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

type role struct {
	name     string
	priority string
	comment  string
}

func BackupRoles(src *exasol.Conn, dst string, dropExtras bool) {
	log.Notice("Backing up roles")

	roles := getRolesToBackup(src)
	if len(roles) == 0 {
		log.Warning("No roles found")
		return
	}

	dir := filepath.Join(dst, "roles")
	if dropExtras {
		log.Noticef("Remove extraneous backedup roles")
		os.RemoveAll(dir)
	}
	os.MkdirAll(dir, os.ModePerm)

	roleNames := []string{"PUBLIC"}
	for _, role := range roles {
		createRole(dir, role)
		roleNames = append(roleNames, role.name)
	}

	BackupPrivileges(src, dir, roleNames)

	log.Info("Done backing up roles")
}

func getRolesToBackup(conn *exasol.Conn) []*role {
	sql := fmt.Sprintf(`
		SELECT role_name AS s,
			   role_name AS o,
			   role_priority,
			   role_comment
		FROM exa_all_roles
		ORDER BY local.s`,
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		log.Fatal(err)
	}
	roles := []*role{}
	for _, row := range res {
		r := &role{name: row[0].(string)}

		if row[2] != nil {
			r.priority = row[2].(string)
		}
		if row[3] != nil {
			r.comment = row[3].(string)
		}
		roles = append(roles, r)
	}
	return roles
}

func createRole(dst string, r *role) {
	log.Noticef("Backing up role %s", r.name)

	var sql string
	if r.name != "DBA" && r.name != "PUBLIC" {
		sql = "CREATE ROLE " + r.name + ";\n"
	}
	if r.priority != "" {
		sql += fmt.Sprintf("GRANT PRIORITY %s TO %s;\n", r.priority, r.name)
	}
	if r.comment != "" {
		sql += fmt.Sprintf("COMMENT ON ROLE %s IS '%s';\n", r.name, exasol.QuoteStr(r.comment))
	}

	file := filepath.Join(dst, r.name+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup role", sql, err)
	}
}
