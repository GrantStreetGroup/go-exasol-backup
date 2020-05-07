package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/grantstreetgroup/go-exasol-client"
)

func BackupPrivileges(src *exasol.Conn, dst string, grantees []string) {
	for i := range grantees {
		grantees[i] = "'" + grantees[i] + "'"
	}
	backupConnectionPrivs(src, dst, grantees)
	backupObjectPrivs(src, dst, grantees)
	backupRestrictedObjectPrivs(src, dst, grantees)
	backupRolePrivs(src, dst, grantees)
	backupSystemPrivs(src, dst, grantees)
	backupSchemaOwners(src, dst, grantees)

	log.Info("Done backing up privileges")
}

func backupConnectionPrivs(src *exasol.Conn, dst string, grantees []string) {
	log.Notice("Backing up connection privileges")

	sql := fmt.Sprintf(`
		SELECT grantee, granted_connection, admin_option
		FROM exa_dba_connection_privs
		WHERE grantee IN (%s)
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		log.Fatal("Unable to get connection privs", err)
	}
	for _, row := range res {
		grantee := row[0].(string)
		connection := row[1].(string)
		adminOption := row[2].(bool)
		sql := fmt.Sprintf("GRANT CONNECTION %s TO %s", connection, grantee)
		if adminOption {
			sql += " WITH ADMIN OPTION"
		}
		sql += ";\n"
		appendToObjFile(dst, grantee, sql)
	}
}

func backupObjectPrivs(src *exasol.Conn, dst string, grantees []string) {
	log.Notice("Backing up object privileges")

	sql := fmt.Sprintf(`
		SELECT object_schema, object_name, object_type,
		       privilege, grantee
		FROM exa_dba_obj_privs
		WHERE grantee IN (%s)
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		log.Fatal("Unable to get object privs", err)
	}
	for _, row := range res {
		objType := row[2].(string)
		privilege := row[3].(string)
		grantee := row[4].(string)

		var object string
		if objType == "SCHEMA" {
			object = row[1].(string)
		} else {
			object = row[0].(string) + "." + row[1].(string)
		}

		sql := fmt.Sprintf("GRANT %s ON %s %s TO %s;\n", privilege, objType, object, grantee)
		appendToObjFile(dst, grantee, sql)
	}
}

func backupRestrictedObjectPrivs(src *exasol.Conn, dst string, grantees []string) {
	log.Notice("Backing up restricted object privileges")

	sql := fmt.Sprintf(`
		SELECT object_schema, object_name, object_type,
			   for_object_schema, for_object_name, for_object_type,
		       privilege, grantee
		FROM exa_dba_restricted_obj_privs
		WHERE grantee IN (%s)
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		log.Fatal("Unable to get restricted object privs", err)
	}
	for _, row := range res {
		objType := row[2].(string)
		forObjType := row[5].(string)
		privilege := row[6].(string)
		grantee := row[7].(string)

		var object string
		if row[0] == nil {
			object = row[1].(string)
		} else {
			object = row[0].(string) + `"."` + row[1].(string)
		}
		var forObject string
		if row[3] == nil {
			forObject = row[4].(string)
		} else {
			forObject = row[3].(string) + `"."` + row[4].(string)
		}

		sql := fmt.Sprintf(
			`GRANT %s ON %s "%s" FOR %s "%s" TO "%s";`+"\n",
			privilege, objType, object, forObjType, forObject, grantee,
		)
		appendToObjFile(dst, grantee, sql)
	}
}

func backupRolePrivs(src *exasol.Conn, dst string, grantees []string) {
	log.Notice("Backing up role privileges")

	sql := fmt.Sprintf(`
		SELECT grantee, granted_role, admin_option
		FROM exa_dba_role_privs
		WHERE grantee IN (%s)
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		log.Fatal("Unable to get role privs", err)
	}
	for _, row := range res {
		grantee := row[0].(string)
		role := row[1].(string)
		adminOption := row[2].(bool)

		sql := fmt.Sprintf("GRANT %s TO %s", role, grantee)
		if adminOption {
			sql += " WITH ADMIN OPTION"
		}
		sql += ";\n"
		appendToObjFile(dst, grantee, sql)
	}
}

func backupSystemPrivs(src *exasol.Conn, dst string, grantees []string) {
	log.Notice("Backing up system privileges")

	sql := fmt.Sprintf(`
		SELECT grantee, privilege, admin_option
		FROM exa_dba_sys_privs
		WHERE grantee IN (%s)
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		log.Fatal("Unable to get sys privs", err)
	}
	for _, row := range res {
		grantee := row[0].(string)
		privilege := row[1].(string)
		adminOption := row[2].(bool)

		sql := fmt.Sprintf("GRANT %s TO %s", privilege, grantee)
		if adminOption {
			sql += " WITH ADMIN OPTION"
		}
		sql += ";\n"
		appendToObjFile(dst, grantee, sql)
	}
}

func backupSchemaOwners(src *exasol.Conn, dst string, grantees []string) {
	log.Notice("Backing up schema owners")

	sql := fmt.Sprintf(`
	    SELECT DISTINCT s.schema_name, s.schema_owner,
            (vs.schema_name IS NOT NULL) AS is_virtual
        FROM exa_schemas AS s
        LEFT JOIN exa_dba_virtual_schemas AS vs
          ON s.schema_name = vs.schema_name
        WHERE s.schema_owner IN (%s)
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		log.Fatal("Unable to get schema owner", err)
	}
	for _, row := range res {
		schema := row[0].(string)
		owner := row[1].(string)
		isVirtual := row[2].(bool)
		var virtual string
		if isVirtual {
			virtual = "VIRTUAL "
		}

		sql := fmt.Sprintf("ALTER %sSCHEMA %s CHANGE OWNER %s;\n", virtual, schema, owner)
		appendToObjFile(dst, owner, sql)
	}
}

func appendToObjFile(dst, user, sql string) {
	fp := filepath.Join(dst, user+".sql")
	f, err := os.OpenFile(fp, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Unable to open file", fp, err)
	}
	_, err = f.Write([]byte(sql))
	if err != nil {
		log.Fatal("Unable to write ", fp, sql, err)
	}
	f.Close()
}
