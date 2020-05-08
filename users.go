package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

type user struct {
	name     string
	ldapDN   string
	priority string
	comment  string
}

func BackupUsers(src *exasol.Conn, dst string, dropExtras bool) error {
	log.Notice("Backing up users")

	users, err := getUsersToBackup(src)
	if err != nil {
		return err
	}
	if len(users) == 0 {
		log.Warning("No users found")
		return nil
	}

	dir := filepath.Join(dst, "users")
	if dropExtras {
		log.Noticef("Removing extraneous backedup users")
		os.RemoveAll(dir)
	}
	os.MkdirAll(dir, os.ModePerm)

	var userNames []string
	for _, user := range users {
		err = backupUser(dir, user)
		if err != nil {
			return err
		}
		userNames = append(userNames, user.name)
	}

	err = BackupPrivileges(src, dir, userNames)
	if err != nil {
		return err
	}

	log.Info("Done backing up users")
	return nil
}

func getUsersToBackup(conn *exasol.Conn) ([]*user, error) {
	sql := fmt.Sprintf(`
		SELECT user_name AS s,
			   user_name AS o,
			   distinguished_name,
			   user_priority,
			   user_comment
		FROM exa_dba_users
		WHERE user_name != 'SYS'
		ORDER BY local.s`,
	)
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, fmt.Errorf("Unable to get users: %s", err)
	}
	users := []*user{}
	for _, row := range res {
		u := &user{name: row[0].(string)}

		if row[2] != nil {
			u.ldapDN = row[2].(string)
		}
		if row[3] != nil {
			u.priority = row[3].(string)
		}
		if row[4] != nil {
			u.comment = row[4].(string)
		}
		users = append(users, u)
	}
	return users, nil
}

func backupUser(dst string, u *user) error {
	log.Noticef("Backing up user %s", u.name)

	sql := ""
	dn := u.ldapDN
	if dn == "" {
		// If the user is setup with a non-LDAP account
		// we can't backup the password. If the user already
		// exists on the destination site then the create-user
		// will just fail and the existing user (and its
		// password) will remain and all will be well.
		// If the user doesn't exist then we'll need to go
		// in manually later and change the password so in
		// the meantime we set an invalid password by
		// setting it to an invalid LDAP distinguished name.
		sql = fmt.Sprintf("CREATE USER %s IDENTIFIED BY ********;\n", u.name)
	} else {
		sql = fmt.Sprintf("CREATE USER %s IDENTIFIED AT LDAP AS '%s';\n", u.name, dn)
	}

	if u.priority != "" {
		sql += fmt.Sprintf("GRANT PRIORITY %s TO %s;\n", u.priority, u.name)
	}

	if u.comment != "" {
		sql += fmt.Sprintf("COMMENT ON USER %s IS '%s';\n", u.name, qStr(u.comment))
	}

	file := filepath.Join(dst, u.name+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup user %s: %s", u.name, err)
	}
	return nil
}
