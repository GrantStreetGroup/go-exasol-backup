package backup

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grantstreetgroup/go-exasol-client"
)

// This backs up consumer groups

type consumerGroup struct {
	name                  string
	isDefault             bool
	precedence            int
	cpuWeight             int
	groupTempDBRAMLimit   int
	userTempDBRAMLimit    int
	sessionTempDBRAMLimit int
	queryTimeout          int
	idleTimeout           int
	comment               string
}

func BackupConsumerGroups(src *exasol.Conn, dst string) error {
	log.Info("Backing up consumer groups")

	consumerGroups, err := getConsumerGroupsToBackup(src)
	if err != nil {
		return err
	}
	if len(consumerGroups) == 0 {
		log.Warning("No consumer groups found")
		return nil
	}

	var sql string
	for _, consumerGroup := range consumerGroups {
		sql += createConsumerGroup(consumerGroup)
	}

	os.MkdirAll(dst, os.ModePerm)
	file := filepath.Join(dst, "consumer_groups.sql")
	err = ioutil.WriteFile(file, []byte(sql), 0644)
	if err != nil {
		return fmt.Errorf("Unable to backup consumer groups: %s", err)
	}

	// Drop the legacy priority groups file to avoid confusion.
	// Depending on the Exasol version we have either consumer or priority groups.
	os.Remove(filepath.Join(dst, "priority_groups.sql"))

	log.Info("Done backing up consumer groups")
	return nil
}

func getConsumerGroupsToBackup(conn *exasol.Conn) ([]*consumerGroup, error) {
	sql := `
		SELECT system_value
		FROM exa_parameters
		WHERE parameter_name = 'DEFAULT_CONSUMER_GROUP'
	`
	res, err := conn.FetchSlice(sql)
	if err != nil {
		return nil, fmt.Errorf("Unable to get default consumer group: %s", err)
	}
	defaultGroup := res[0][0].(string)

	sql = `
		SELECT consumer_group_name,
			   precedence,
			   cpu_weight,
			   group_temp_db_ram_limit,
			   user_temp_db_ram_limit,
			   session_temp_db_ram_limit,
			   query_timeout,
			   idle_timeout,
			   consumer_group_comment
		FROM exa_consumer_groups
		ORDER BY consumer_group_name
	`
	res, err = conn.FetchSlice(sql)
	if err != nil {
		return nil, fmt.Errorf("Unable to get consumer groups to backup: %s", err)
	}
	consumerGroups := []*consumerGroup{}
	for _, row := range res {
		p := &consumerGroup{
			name:       row[0].(string),
			isDefault:  (row[0].(string) == defaultGroup),
			precedence: int(row[1].(float64)),
			cpuWeight:  int(row[2].(float64)),
		}
		if row[3] != nil {
			p.groupTempDBRAMLimit = int(row[3].(float64))
		}
		if row[4] != nil {
			p.userTempDBRAMLimit = int(row[4].(float64))
		}
		if row[5] != nil {
			p.sessionTempDBRAMLimit = int(row[5].(float64))
		}
		if row[6] != nil {
			p.queryTimeout = int(row[6].(float64))
		}
		if row[7] != nil {
			p.idleTimeout = int(row[7].(float64))
		}
		if row[8] != nil {
			p.comment = row[8].(string)
		}
		consumerGroups = append(consumerGroups, p)
	}
	return consumerGroups, nil
}

func createConsumerGroup(p *consumerGroup) string {
	log.Infof("Backing up consumer group %s", p.name)
	sql := ""
	if p.name == "SYS_CONSUMER_GROUP" || p.isDefault {
		sql = fmt.Sprintf("ALTER CONSUMER GROUP [%s] SET", p.name)
	} else {
		sql = fmt.Sprintf(
			"DROP CONSUMER GROUP [%s];\nCREATE CONSUMER GROUP [%s] WITH",
			p.name, p.name,
		)
	}
	limit := func(i int) string {
		if i == 0 {
			return "OFF"
		} else {
			return fmt.Sprintf("%d", i)
		}
	}
	sql = fmt.Sprintf("%s\n"+
		"   PRECEDENCE = %d,\n"+
		"   CPU_WEIGHT = %d,\n"+
		"   GROUP_TEMP_DB_RAM_LIMIT = '%s',\n"+
		"   USER_TEMP_DB_RAM_LIMIT = '%s',\n"+
		"   SESSION_TEMP_DB_RAM_LIMIT = '%s',\n"+
		"   QUERY_TIMEOUT = %d,\n"+
		"   IDLE_TIMEOUT = %d;\n",
		sql, p.precedence, p.cpuWeight,
		limit(p.groupTempDBRAMLimit),
		limit(p.userTempDBRAMLimit),
		limit(p.sessionTempDBRAMLimit),
		p.queryTimeout, p.idleTimeout,
	)
	if p.comment != "" {
		sql += fmt.Sprintf(
			"COMMENT ON CONSUMER GROUP [%s] IS '%s';\n",
			p.name, qStr(p.comment),
		)
	}
	return sql
}
