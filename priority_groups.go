package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/GrantStreetGroup/go-exasol-client"
)

// This backs up priority groups

type priorityGroup struct {
	name    string
	weight  int
	comment string
}

func BackupPriorityGroups(src *exasol.Conn, dst string) error {
	log.Info("Backing up priority groups")

	priorityGroups, err := getPriorityGroupsToBackup(src)
	if err != nil {
		return err
	}
	if len(priorityGroups) == 0 {
		log.Warning("No priority groups found")
		return nil
	}

	var sql string
	for _, priorityGroup := range priorityGroups {
		sql += createPriorityGroup(priorityGroup)
	}

	os.MkdirAll(dst, os.ModePerm)
	file := filepath.Join(dst, "priority_groups.sql")
	err = ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup priority groups: %s", err)
	}

	log.Info("Done backing up priority groups")
	return nil
}

func getPriorityGroupsToBackup(conn *exasol.Conn) ([]*priorityGroup, error) {
	sql := `
		SELECT priority_group_name,
			   priority_group_weight,
			   priority_group_comment
		FROM exa_priority_groups
		ORDER BY priority_group_name
	`
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, fmt.Errorf("Unable to get priority groups to backup: %s", err)
	}
	priorityGroups := []*priorityGroup{}
	for _, row := range res {
		p := &priorityGroup{
			name:   row[0].(string),
			weight: int(row[1].(float64)),
		}
		if row[2] != nil {
			p.comment = row[2].(string)
		}
		priorityGroups = append(priorityGroups, p)
	}
	return priorityGroups, nil
}

func createPriorityGroup(p *priorityGroup) string {
	log.Infof("Backing up priority group %s", p.name)
	sql := ""
	if p.name == "MEDIUM" {
		sql = fmt.Sprintf("ALTER PRIORITY GROUP [%s] SET WEIGHT = %d;\n", p.name, p.weight)
	} else {
		sql = fmt.Sprintf(
			"DROP PRIORITY GROUP [%s];\n"+
				"CREATE PRIORITY GROUP [%s] WITH WEIGHT = %d;\n",
			p.name, p.name, p.weight,
		)
	}
	if p.comment != "" {
		sql += fmt.Sprintf(
			"COMMENT ON PRIORITY GROUP [%s] IS '%s';\n",
			p.name, qStr(p.comment),
		)
	}
	return sql
}
