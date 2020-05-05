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

func BackupUsers(src *exasol.Conn, dst string, dropExtras bool) {
	log.Notice("Backingup users")

	users := getUsersToBackup(src)
	if len(users) == 0 {
		log.Warning("No users found")
		return
	}

	dir := filepath.Join(dst, "users")
	if dropExtras {
		log.Noticef("Removing extraneous backedup users")
		os.RemoveAll(dir)
	}
	os.MkdirAll(dir, os.ModePerm)

	var userNames []string
	for _, user := range users {
		backupUser(dir, user)
		userNames = append(userNames, user.name)
	}

	BackupPrivileges(src, dir, userNames)

	log.Info("Done backingup users")
}

func getUsersToBackup(conn *exasol.Conn) []*user {
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
		log.Fatal(err)
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
	return users
}

func backupUser(dst string, u *user) {
	log.Noticef("Backingup user %s", u.name)

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
		dn = "MUST CHANGE PASSWORD"
	}
	sql := fmt.Sprintf("CREATE USER %s IDENTIFIED AT LDAP AS '%s';\n", u.name, dn)

	if u.priority != "" {
		sql += fmt.Sprintf("GRANT PRIORITY %s TO %s;\n", u.priority, u.name)
	}

	if u.comment != "" {
		sql += fmt.Sprintf("COMMENT ON USER %s IS '%s';\n", u.name, exasol.QuoteStr(u.comment))
	}

	file := filepath.Join(dst, u.name+".sql")
	err := ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		log.Fatal("Unable to backup user", sql, err)
	}
}
