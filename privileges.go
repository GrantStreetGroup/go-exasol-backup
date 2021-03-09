package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/grantstreetgroup/go-exasol-client"
)

func BackupPrivileges(src *exasol.Conn, dst string, grantees []string) error {
	for i := range grantees {
		grantees[i] = "'" + grantees[i] + "'"
	}
	privs := []func(*exasol.Conn, string, []string) error{
		backupConnectionPrivs,
		backupObjectPrivs,
		backupRestrictedObjectPrivs,
		backupRolePrivs,
		backupSystemPrivs,
		backupImpersonationPrivs,
		backupSchemaOwners,
	}
	for _, backupPriv := range privs {
		err := backupPriv(src, dst, grantees)
		if err != nil {
			return err
		}
	}

	log.Info("Done backing up privileges")
	return nil
}

func backupConnectionPrivs(src *exasol.Conn, dst string, grantees []string) error {
	log.Info("Backing up connection privileges")

	sql := fmt.Sprintf(`
		SELECT grantee, granted_connection, admin_option
		FROM exa_dba_connection_privs
		WHERE grantee IN (%s)
		ORDER BY 1, 2
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get connection privs: %s", err)
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
		err = appendToObjFile(dst, grantee, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func backupObjectPrivs(src *exasol.Conn, dst string, grantees []string) error {
	log.Info("Backing up object privileges")

	sql := fmt.Sprintf(`
		SELECT object_schema, object_name, object_type,
		       privilege, grantee
		FROM exa_dba_obj_privs
		WHERE grantee IN (%s)
		ORDER BY 1, 2, 3, 4, 5
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get object privs: %s", err)
	}
	for _, row := range res {
		objType := row[2].(string)
		privilege := row[3].(string)
		grantee := row[4].(string)

		var object string
		if objType == "SCHEMA" {
			object = row[1].(string)
		} else {
			object = row[0].(string) + "].[" + row[1].(string)
		}

		sql := fmt.Sprintf("GRANT %s ON %s [%s] TO %s;\n", privilege, objType, object, grantee)
		err = appendToObjFile(dst, grantee, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func backupRestrictedObjectPrivs(src *exasol.Conn, dst string, grantees []string) error {
	log.Info("Backing up restricted object privileges")

	sql := fmt.Sprintf(`
		SELECT object_schema, object_name, object_type,
			   for_object_schema, for_object_name, for_object_type,
		       privilege, grantee
		FROM exa_dba_restricted_obj_privs
		WHERE grantee IN (%s)
		ORDER BY 1, 2, 3, 4, 5, 6, 7, 8
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get restricted object privs: %s", err)
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
			object = row[0].(string) + "].[" + row[1].(string)
		}
		var forObject string
		if row[3] == nil {
			forObject = row[4].(string)
		} else {
			forObject = row[3].(string) + "].[" + row[4].(string)
		}

		sql := fmt.Sprintf(
			`GRANT %s ON %s [%s] FOR %s [%s] TO %s;`+"\n",
			privilege, objType, object, forObjType, forObject, grantee,
		)
		err = appendToObjFile(dst, grantee, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func backupRolePrivs(src *exasol.Conn, dst string, grantees []string) error {
	log.Info("Backing up role privileges")

	sql := fmt.Sprintf(`
		SELECT grantee, granted_role, admin_option
		FROM exa_dba_role_privs
		WHERE grantee IN (%s)
		ORDER BY 1, 2
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get role privs: %s", err)
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
		err = appendToObjFile(dst, grantee, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func backupSystemPrivs(src *exasol.Conn, dst string, grantees []string) error {
	log.Info("Backing up system privileges")

	sql := fmt.Sprintf(`
		SELECT grantee, privilege, admin_option
		FROM exa_dba_sys_privs
		WHERE grantee IN (%s)
		ORDER BY 1, 2
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get sys privs: %s", err)
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
		err = appendToObjFile(dst, grantee, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func backupImpersonationPrivs(src *exasol.Conn, dst string, grantees []string) error {
	log.Info("Backing up impersonation privileges")

	sql := fmt.Sprintf(`
		SELECT grantee, impersonation_on
		FROM exa_dba_impersonation_privs
		WHERE grantee IN (%s)
		ORDER BY 1, 2
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get impersonation privs: %s", err)
	}
	for _, row := range res {
		grantee := row[0].(string)
		impersonationOn := row[1].(string)

		sql := fmt.Sprintf("GRANT IMPERSONATION ON %s TO %s;\n", impersonationOn, grantee)
		err = appendToObjFile(dst, grantee, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func backupSchemaOwners(src *exasol.Conn, dst string, grantees []string) error {
	log.Info("Backing up schema owners")

	sql := fmt.Sprintf(`
	    SELECT DISTINCT s.schema_name, s.schema_owner,
            (vs.schema_name IS NOT NULL) AS is_virtual
        FROM exa_schemas AS s
        LEFT JOIN exa_all_virtual_schemas AS vs
          ON s.schema_name = vs.schema_name
        WHERE s.schema_owner IN (%s)
		ORDER BY 1, 2
		`, strings.Join(grantees, ","),
	)
	res, err := src.FetchSlice(sql)
	if err != nil {
		return fmt.Errorf("Unable to get schema owner: %s", err)
	}
	for _, row := range res {
		schema := row[0].(string)
		owner := row[1].(string)
		isVirtual := row[2].(bool)
		var virtual string
		if isVirtual {
			virtual = "VIRTUAL "
		}

		sql := fmt.Sprintf("ALTER %sSCHEMA [%s] CHANGE OWNER %s;\n", virtual, schema, owner)
		err = appendToObjFile(dst, owner, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func appendToObjFile(dst, user, sql string) error {
	fp := filepath.Join(dst, user+".sql")
	f, err := os.OpenFile(fp, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Unable to open file '%s': %s", fp, err)
	}
	_, err = f.Write([]byte(sql))
	if err != nil {
		return fmt.Errorf("Unable to write to file '%s': %s", fp, err)
	}
	f.Close()
	return nil
}
