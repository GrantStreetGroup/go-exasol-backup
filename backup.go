/*
	This library backs up metadata (and optionally data) from an Exasol instance to text files
*/

package backup

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/grantstreetgroup/go-exasol-client"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("exasol-backup")

/* Public Interface */

type Object byte

const (
	ALL Object = iota
	CONNECTIONS
	FUNCTIONS
	PARAMETERS
	ROLES
	SCHEMAS // This would backup the (virtual)schema itself but nothing under it
	SCRIPTS
	TABLES
	USERS
	VIEWS
)

type Conf struct {
	// Exasol instance to backup from
	ExaConn *exasol.Conn

	// Local filesystem directory underwhich to store the backup
	Destination string

	// The list of object types to backup
	Objects []Object

	// Match is a comma delimited set of wildcard matching patterns.
	// Any schema object matching one of these patterns will be backedup.
	// Each pattern should be in the form of "schema.object".
	// If the object is not specified "schema.*" is assumed.
	// If not specified then "*.*" is assumed.
	// Non-schema objects (users, roles, connections, parameters) are
	// not affected by this config. i.e. they will all be backup if requested.
	Match string

	// Skip is the inverse of Match.
	// Any schema objects matching it will be skipped.
	Skip string

	// If > 0 then tables with this many or fewer rows
	// will have the their data backed up to CSV files
	MaxTableRows int
	// If > 0 then views with this many or fewer rows
	// will have the their data backed up to CSV files
	MaxViewRows int

	// If true then any text files existing in the destination
	// but no longer existing in Exasol will be removed.
	// If false then the backup is purely additive
	DropExtras bool

	LogLevel string // Defaults to "error"
}

func Backup(cfg Conf) error {
	initLogging(cfg.LogLevel)
	log.Noticef("Backing up to %s", cfg.Destination)

	// Set defaults
	if cfg.Match == "" {
		cfg.Match = "*.*"
	}
	if cfg.ExaConn == nil {
		return errors.New("You must specify an ExaConn")
	}
	if cfg.Destination == "" {
		return errors.New("You must specify a Destination")
	}
	fi, err := os.Stat(cfg.Destination)
	if os.IsNotExist(err) || !fi.Mode().IsDir() {
		return errors.New("The Destination must be a valid directory path")
	}

	backup := map[Object]bool{}
	for _, o := range cfg.Objects {
		backup[o] = true
	}
	src := cfg.ExaConn
	dst := cfg.Destination
	drop := cfg.DropExtras
	crit := Criteria{cfg.Match, cfg.Skip}

	// TODO capture and restore original values of these 2 settings
	src.DisableAutoCommit()
	src.Execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3'")

	if backup[PARAMETERS] || backup[ALL] {
		BackupParameters(src, dst)
	}
	if backup[SCHEMAS] || backup[ALL] {
		BackupSchemas(src, dst, crit, drop)
	}
	if backup[TABLES] || backup[ALL] {
		BackupTables(src, dst, crit, cfg.MaxTableRows, drop)
	}
	if backup[VIEWS] || backup[ALL] {
		BackupViews(src, dst, crit, cfg.MaxViewRows, drop)
	}
	if backup[SCRIPTS] || backup[ALL] {
		BackupScripts(src, dst, crit, drop)
	}
	if backup[FUNCTIONS] || backup[ALL] {
		BackupFunctions(src, dst, crit, drop)
	}
	if backup[CONNECTIONS] || backup[ALL] {
		BackupConnections(src, dst)
	}
	if backup[ROLES] || backup[ALL] {
		BackupRoles(src, dst, drop)
	}
	if backup[USERS] || backup[ALL] {
		BackupUsers(src, dst, drop)
	}

	log.Notice("Done backing up")
	return nil
}

type Criteria struct {
	match string
	skip  string
}

/* Private routines */

type dbObj interface {
	Name() string
	Schema() string
}

func initLogging(logLevelStr string) {
	if logLevelStr == "" {
		logLevelStr = "error"
	}
	logLevel, err := logging.LogLevel(logLevelStr)
	if err != nil {
		log.Fatal("Unrecognized log level", err)
	}
	logFormat := logging.MustStringFormatter(
		"%{color}%{time:15:04:05.000} %{shortfunc}: " +
			"%{level:.4s} %{id:03x}%{color:reset} %{message}",
	)
	backend := logging.NewLogBackend(os.Stdout, "[exasol-backup]", 0)
	formattedBackend := logging.NewBackendFormatter(backend, logFormat)
	leveledBackend := logging.AddModuleLevel(formattedBackend)
	leveledBackend.SetLevel(logLevel, "exasol-backup")
	log.SetBackend(leveledBackend)
}

func (c *Criteria) getSQLCriteria() string {
	whereClause := buildCriteria(c.match)
	if c.skip != "" {
		whereClause = fmt.Sprintf(
			"(%s) AND NOT (%s)",
			whereClause, buildCriteria(c.skip),
		)
	}
	return whereClause
}

func (c *Criteria) matches(schema, object string) bool {
	return matchesCriteria(c.match, schema, object) &&
		(c.skip == "" || !matchesCriteria(c.skip, schema, object))
}

func matchesCriteria(matchStr, schema, object string) bool {
	// Convert wildcards to regexp wildcard match
	for _, match := range strings.Split(matchStr, ",") {
		parts := strings.Split(match, ".")
		var schemaPattern = regexp.MustCompile(`\*`).ReplaceAllString(parts[0], ".*")
		var objectPattern string
		if len(parts) > 1 {
			objectPattern = regexp.MustCompile(`\*`).ReplaceAllString(parts[1], ".*")
		} else {
			objectPattern = ".*"
		}
		if regexp.MustCompile("(?i)" + schemaPattern).MatchString(schema) {
			if object == "" || regexp.MustCompile("(?i)"+objectPattern).MatchString(object) {
				return true
			}
		}
	}
	return false
}

func buildCriteria(argStr string) string {
	// Convert wildcards to SQL wildcard match
	arg := regexp.MustCompile(`\*`).ReplaceAllString(argStr, "%")
	var whereClause []string
	for _, st := range strings.Split(arg, ",") {
		parts := strings.Split(st, ".")
		var schema = parts[0]
		var object string
		if len(parts) > 1 {
			object = parts[1]
		} else {
			object = "%"
		}
		criteria := fmt.Sprintf(`(
				UPPER(local.s) LIKE UPPER('%s') AND
				UPPER(local.o) LIKE UPPER('%s')
			)`, schema, object,
		)
		whereClause = append(whereClause, criteria)
	}
	return strings.Join(whereClause, " OR ")
}

func removeExtraObjects(objType string, srcObjs []dbObj, dst string, crit Criteria) {
	log.Noticef("Removing extraneous %s", objType)

	schemaDir := filepath.Join(dst, "schemas")
	os.MkdirAll(schemaDir, os.ModePerm) // May be the first time we're backing up the env

	dstSchemas, err := ioutil.ReadDir(schemaDir)
	if err != nil {
		// If this is the first time we're backing up the environment
		// there may be no directory to read yet.
		log.Warning(err)
		return
	}

SCHEMA:
	for _, dstSchema := range dstSchemas {
		if dstSchema.IsDir() && crit.matches(dstSchema.Name(), "") {
			if objType == "schemas" {
				for _, srcObj := range srcObjs {
					// Check if existing destination schema still exists
					// in the source. If not we'll remove it
					if srcObj.Schema() == dstSchema.Name() {
						continue SCHEMA
					}
				}
				os.RemoveAll(filepath.Join(schemaDir, dstSchema.Name()))

			} else { // Non-Schema objects
				objDir := filepath.Join(schemaDir, dstSchema.Name(), objType)
				objs, err := ioutil.ReadDir(objDir)
				if err != nil {
					// No objects in this schema
					continue SCHEMA
				}
			OBJ:
				for _, obj := range objs {
					objBaseName := strings.TrimSuffix(obj.Name(), filepath.Ext(obj.Name()))
					if crit.matches(dstSchema.Name(), objBaseName) {
						for _, srcObj := range srcObjs {
							// Check if existing destination object still exists
							// in the source. If not we'll remove it
							if dstSchema.Name() == srcObj.Schema() &&
								objBaseName == srcObj.Name() {
								continue OBJ
							}
						}
						log.Noticef("Dropping %s.%s %s", dstSchema.Name(), objBaseName, objType)
						os.Remove(filepath.Join(objDir, obj.Name()))
					}
				}
			}
		}
	}
}

func openSchema(conn *exasol.Conn, schema string) {
	conn.Conf.SuppressError = true

	openSchema := fmt.Sprintf("OPEN SCHEMA %s", schema)
	_, err := conn.Execute(openSchema)
	if err != nil {
		if regexp.MustCompile(`schema .* not found`).MatchString(err.Error()) {

			createSchema := fmt.Sprintf("CREATE SCHEMA %s", schema)
			_, err = conn.Execute(createSchema)
			if err != nil {
				log.Fatal("Unable to create schema", createSchema, err)
			}

			_, err = conn.Execute(openSchema)
			if err != nil {
				log.Fatal("Unable to open schema", openSchema, err)
			}

		} else {
			log.Fatal("Unable to open schema", openSchema, err)
		}
	}
	conn.Conf.SuppressError = false
}
