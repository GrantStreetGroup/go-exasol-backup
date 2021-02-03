package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

type role struct {
	name           string
	consumer_group string
	comment        string
}

func BackupRoles(src *exasol.Conn, dst string, dropExtras bool) error {
	log.Info("Backing up roles")

	roles, err := getRolesToBackup(src)
	if err != nil {
		return err
	}
	if len(roles) == 0 {
		log.Warning("No roles found")
		return nil
	}

	dir := filepath.Join(dst, "roles")
	if dropExtras {
		log.Infof("Remove extraneous backedup roles")
		os.RemoveAll(dir)
	}
	os.MkdirAll(dir, os.ModePerm)

	roleNames := []string{}
	for _, role := range roles {
		err = createRole(dir, role)
		if err != nil {
			return err
		}
		if role.name != "DBA" {
			roleNames = append(roleNames, role.name)
		}
	}

	err = BackupPrivileges(src, dir, roleNames)
	if err != nil {
		return err
	}

	log.Info("Done backing up roles")
	return nil
}

func getRolesToBackup(conn *exasol.Conn) ([]*role, error) {
	groupType := "role_priority"
	if capability.consumerGroups {
		groupType = "role_consumer_group"
	}
	sql := fmt.Sprintf(`
		SELECT role_name AS s,
			   role_name AS o,
			   %s,
			   role_comment
		FROM exa_all_roles
		ORDER BY local.s`,
		groupType,
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, fmt.Errorf("Unable to get roles: %s", err)
	}
	roles := []*role{}
	for _, row := range res {
		r := &role{name: row[0].(string)}

		if row[2] != nil {
			r.consumer_group = row[2].(string)
		}
		if row[3] != nil {
			r.comment = row[3].(string)
		}
		roles = append(roles, r)
	}
	return roles, nil
}

func createRole(dst string, r *role) error {
	log.Infof("Backing up role %s", r.name)

	var sql string
	if r.name != "DBA" && r.name != "PUBLIC" {
		sql = "CREATE ROLE " + r.name + ";\n"
	}
	if r.consumer_group != "" {
		if capability.consumerGroups {
			sql += fmt.Sprintf("ALTER ROLE %s SET CONSUMER_GROUP = [%s];\n", r.name, r.consumer_group)
		} else {
			sql += fmt.Sprintf("GRANT PRIORITY GROUP [%s] TO %s;\n", r.consumer_group, r.name)
		}
	}
	if r.comment != "" {
		sql += fmt.Sprintf("COMMENT ON ROLE %s IS '%s';\n", r.name, qStr(r.comment))
	}

	file := filepath.Join(dst, r.name+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup role: %s", err)
	}
	return nil
}
