/*
	When running this test suite be sure to run it against a completely
	blank and default Exasol instance having only the SYS user.

	We recommend using an Exasol docker container for this:
		https://github.com/exasol/docker-db

	These tests expect Exasol v7.0+

	Run tests via: go test -v -args -testify.m pattern
*/
package backup

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/GrantStreetGroup/go-exasol-client"
	"github.com/stretchr/testify/suite"
)

var testHost = flag.String("host", "127.0.0.1", "Exasol hostname")
var testPort = flag.Int("port", 8563, "Exasol port")
var testPass = flag.String("pass", "exasol", "Exasol SYS password")
var testLoglevel = flag.String("loglevel", "warning", "Output loglevel")
var testTmpdir = flag.String("tmpdir", "/var/tmp/", "Temp directory for backup destination")

type testSuite struct {
	suite.Suite
	exaConn   *exasol.Conn
	tmpDir    string
	testDir   string
	loglevel  string
	schemaSQL string
}

func TestBackups(t *testing.T) {
	var err error
	initLogging(*testLoglevel)
	s := new(testSuite)
	s.tmpDir = *testTmpdir
	s.loglevel = *testLoglevel
	s.exaConn, err = exasol.Connect(exasol.ConnConf{
		Host:         *testHost,
		Port:         uint16(*testPort),
		Username:     "SYS",
		Password:     *testPass,
		QueryTimeout: 10 * time.Second,
		Logger:       log,
		TLSConfig:    &tls.Config{InsecureSkipVerify: true},
	})
	if err != nil {
		log.Fatalf("Unable to connect to Exasol: %s", err)
	}
	s.exaConn.DisableAutoCommit()
	defer s.exaConn.Disconnect()
	setCapabilities(s.exaConn)

	suite.Run(t, s)
}

func (s *testSuite) SetupTest() {
	var err error
	s.testDir, err = ioutil.TempDir(s.tmpDir, "exasol-test-data-")
	if err != nil {
		log.Fatal(err)
	}

	s.execute("DROP SCHEMA IF EXISTS [test] CASCADE")
	s.schemaSQL = "CREATE SCHEMA IF NOT EXISTS [test];\n"
	s.execute(s.schemaSQL)
}

func (s *testSuite) TearDownTest() {
	err := os.RemoveAll(s.testDir)
	if err != nil {
		fmt.Printf("Unable to remove test dir %s: %s", s.testDir, err)
	}
	s.exaConn.Rollback()
}

func (s *testSuite) execute(args ...string) {
	for _, arg := range args {
		_, err := s.exaConn.Execute(arg)
		if !s.exaConn.Conf.SuppressError {
			s.NoError(err, "Unable to execute SQL")
		}
	}
}

func (s *testSuite) backup(cnf Conf, args ...Object) {
	cnf.Source = s.exaConn
	cnf.Destination = s.testDir
	cnf.LogLevel = s.loglevel
	cnf.Objects = args
	Backup(cnf)
}

type dt map[string]interface{} // Directory/file Tree

func (s *testSuite) expect(expected dt) {
	s.expectDir(s.testDir, expected)
}

// This checks to see if the actual directory/file tree under 'dir'
// matches the expected directory/file tree specified by 'dt'
func (s *testSuite) expectDir(dir string, expected dt) {
	got, err := ioutil.ReadDir(dir)
	s.NoErrorf(err, "Unable to read dir %s: %s", dir, err)

	for _, fd := range got {
		name := fd.Name()
		fullPath := filepath.Join(dir, name)
		if fd.IsDir() {
			s.Containsf(expected, name, "Extra directory in backup %s", fullPath)
			expDir := expected[name]
			if expDir == nil {
				continue
			}
			s.expectDir(fullPath, expDir.(dt))
			delete(expected, name)

		} else {
			s.Containsf(expected, name, "Extra file in backup %s", fullPath)
			expContent := expected[name]
			if expContent == nil {
				continue
			}
			gotContent, err := ioutil.ReadFile(fullPath)
			s.NoErrorf(err, "Unable to read file %s: %s", fullPath, err)

			normalize := func(s string) string {
				s = regexp.MustCompile(`[[:blank]]`).ReplaceAllString(s, " ")
				s = regexp.MustCompile(`(?m)^\s+`).ReplaceAllString(s, "")
				s = regexp.MustCompile(`(?m)\s+$`).ReplaceAllString(s, "\n")
				return s
			}
			exp := normalize(expContent.(string))
			got := normalize(string(gotContent))

			s.Equal(exp, got)
			delete(expected, name)
		}
	}
	s.Emptyf(expected, "Missing backup entries under %s:\n%v", dir, expected)
}

func (s *testSuite) TestParameters() {
	s.backup(Conf{}, PARAMETERS)
	s.expect(dt{
		"parameters.sql": `
            ALTER SYSTEM SET CONSTRAINT_STATE_DEFAULT='ENABLE';
            ALTER SYSTEM SET DEFAULT_CONSUMER_GROUP=MEDIUM;
            ALTER SYSTEM SET DEFAULT_LIKE_ESCAPE_CHARACTER='\';
            ALTER SYSTEM SET HASHTYPE_FORMAT='HEX';
			ALTER SYSTEM SET IDLE_TIMEOUT='86400';
            ALTER SYSTEM SET NLS_DATE_FORMAT='YYYY-MM-DD';
            ALTER SYSTEM SET NLS_DATE_LANGUAGE='ENG';
            ALTER SYSTEM SET NLS_FIRST_DAY_OF_WEEK=7;
            ALTER SYSTEM SET NLS_NUMERIC_CHARACTERS='.,';
            ALTER SYSTEM SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF6';
            ALTER SYSTEM SET PASSWORD_EXPIRY_POLICY='OFF';
            ALTER SYSTEM SET PASSWORD_SECURITY_POLICY='OFF';
            ALTER SYSTEM SET PROFILE='OFF';
            ALTER SYSTEM SET QUERY_CACHE='ON';
            ALTER SYSTEM SET QUERY_TIMEOUT=0;
			ALTER SYSTEM SET REPLICATION_BORDER='100000';
            ALTER SYSTEM SET SCRIPT_LANGUAGES='R=builtin_r JAVA=builtin_java PYTHON3=builtin_python3';
            ALTER SYSTEM SET SCRIPT_OUTPUT_ADDRESS='';
			ALTER SYSTEM SET SESSION_TEMP_DB_RAM_LIMIT='OFF';
			ALTER SYSTEM SET SNAPSHOT_MODE='SYSTEM TABLES';
            ALTER SYSTEM SET SQL_PREPROCESSOR_SCRIPT=;
			ALTER SYSTEM SET ST_MAX_DECIMAL_DIGITS=16;
			ALTER SYSTEM SET TEMP_DB_RAM_LIMIT='OFF';
            ALTER SYSTEM SET TIMESTAMP_ARITHMETIC_BEHAVIOR='INTERVAL';
            ALTER SYSTEM SET TIME_ZONE='EUROPE/BERLIN';
            ALTER SYSTEM SET TIME_ZONE_BEHAVIOR='INVALID SHIFT AMBIGUOUS ST';
			ALTER SYSTEM SET USER_TEMP_DB_RAM_LIMIT='OFF';
        `,
	})
}

func (s *testSuite) TestSchemas() {
	adapterSQL := `
		CREATE LUA ADAPTER SCRIPT [test].vs_adapter AS
		require 'cjson'
		function adapter_call(req)
			return cjson.encode({
				type = cjson.decode(req).type,
				schemaMetadata = {
					tables = {{ name = 'T', columns = {{ name = 'C', dataType = { type = 'DATE' }}}}}
				},
				capabilities = {},
				sql = 'SELECT 1',
			})
		end
	`
	vSchemaSQL := `
		CREATE VIRTUAL SCHEMA IF NOT EXISTS [testvs]
		USING [test].[VS_ADAPTER]
	`

	commentSQL := "COMMENT ON SCHEMA [test] IS 'HI MOM!!!';\n"
	sizeSQL := "ALTER SCHEMA [test] SET RAW_SIZE_LIMIT = 1234567890;\n"

	s.execute(adapterSQL, vSchemaSQL, commentSQL, sizeSQL)
	s.backup(Conf{}, SCHEMAS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"schema.sql": s.schemaSQL + commentSQL + sizeSQL,
			},
		},
	})
}

func (s *testSuite) TestVirtualSchemas() {
	adapterSQL := `
		CREATE LUA ADAPTER SCRIPT [test].vs_adapter AS
		require 'cjson'
		function adapter_call(req)
			return cjson.encode({
				type = cjson.decode(req).type,
				schemaMetadata = {
					tables = {{ name = 'T', columns = {{ name = 'C', dataType = { type = 'DATE' }}}}}
				},
				capabilities = {},
				sql = 'SELECT 1',
			})
		end
	`
	vSchemaSQL := `
		CREATE VIRTUAL SCHEMA IF NOT EXISTS [testvs]
	 	USING [test].[VS_ADAPTER]
		WITH
		  A = 'b'
		  P = 'v';
	`

	s.execute(adapterSQL, vSchemaSQL)
	s.backup(Conf{}, VIRTUAL_SCHEMAS)
	s.expect(dt{
		"schemas": dt{
			"testvs": dt{
				"schema.sql": vSchemaSQL + "\n",
			},
		},
	})

	s.execute("DROP VIRTUAL SCHEMA IF EXISTS [testvs] CASCADE")
	s.execute("DROP ADAPTER SCRIPT [test].vs_adapter")
}

func (s *testSuite) TestTables() {
	table1SQL := `
		CREATE OR REPLACE TABLE "test"."T1" (
			"A" DECIMAL(18,0),
			"B" DECIMAL(18,0),
			PRIMARY KEY ("A","B")
		);
	`
	table2SQL := `
		CREATE OR REPLACE TABLE "test"."T2" (
			"A" DECIMAL(18,0) IDENTITY 321 NOT NULL COMMENT IS 'column A comment',
			"B" DECIMAL(18,0) COMMENT IS 'column B comment',
			"C" DECIMAL(18,0) DEFAULT 123 CONSTRAINT "cnst" NOT NULL DISABLE,
			FOREIGN KEY ("B","C") REFERENCES "test"."T1" ("A","B") DISABLE,
			CONSTRAINT "mypk" PRIMARY KEY ("A","C"),
			DISTRIBUTE BY "A","B",
			PARTITION BY "B","C"
		) COMMENT IS 'table comment';
	`
	data1SQL := `INSERT INTO [test].T1 VALUES (2,3), (3,4);`
	data2SQL := `INSERT INTO [test].T2 VALUES (1,2,3), (2,3,4);`
	s.execute(table1SQL, table2SQL, data1SQL, data2SQL)
	s.backup(Conf{MaxTableRows: 0}, TABLES)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"tables": dt{
					"T1.sql": table1SQL,
					"T2.sql": table2SQL,
				},
			},
		},
	})

	// Test --max-table-rows
	s.backup(Conf{MaxTableRows: 100}, TABLES)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"tables": dt{
					"T1.sql": table1SQL,
					"T2.sql": table2SQL,
					"T1.csv": "2,3\n3,4\n",
					"T2.csv": "1,2,3\n2,3,4\n",
				},
			},
		},
	})

	// Test --drop-extras
	s.execute("DROP TABLE t2")
	s.backup(Conf{DropExtras: true}, TABLES)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"tables": dt{
					"T1.sql": table1SQL,
					"T1.csv": "2,3\n3,4\n",
				},
			},
		},
	})
}

func (s *testSuite) TestViews() {
	openSchemaSQL := "OPEN SCHEMA [test];\n"
	view1SQL := `CREATE OR REPLACE FORCE VIEW "test"."V1"
		  (c COMMENT IS 'column comment') AS
			SELECT 'Hi Mom!!' AS col
		  COMMENT IS 'view comment'`
	view2SQL := `CREATE OR REPLACE FORCE VIEW "test"."V2" AS SELECT 1 c`
	s.execute(openSchemaSQL, view1SQL, view2SQL)
	s.backup(Conf{MaxViewRows: 0}, VIEWS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"views": dt{
					"V1.sql": openSchemaSQL + view1SQL + ";\n",
					"V2.sql": openSchemaSQL + view2SQL + ";\n",
				},
			},
		},
	})

	// Test --max-view-rows
	s.backup(Conf{MaxViewRows: 100}, VIEWS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"views": dt{
					"V1.sql": openSchemaSQL + view1SQL + ";\n",
					"V1.csv": "\"Hi Mom!!\"\n",
					"V2.sql": openSchemaSQL + view2SQL + ";\n",
					"V2.csv": "1\n",
				},
			},
		},
	})

	// Test --drop-extras
	s.execute("DROP VIEW v2")
	s.backup(Conf{DropExtras: true}, VIEWS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"views": dt{
					"V1.sql": openSchemaSQL + view1SQL + ";\n",
					"V1.csv": "\"Hi Mom!!\"\n",
				},
			},
		},
	})

	// Test renamed views
	s.execute("RENAME VIEW v1 TO v3")
	view3SQL := regexp.MustCompile("V1").ReplaceAllString(view1SQL, "V3")
	s.backup(Conf{DropExtras: true}, VIEWS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"views": dt{
					"V3.sql": openSchemaSQL + view3SQL + ";\n",
				},
			},
		},
	})
}

func (s *testSuite) TestFunctions() {
	openSchemaSQL := "OPEN SCHEMA [test];"
	func1SQL := `--/
	    CREATE OR REPLACE FUNCTION "test"."F1" ()
		/* Test Comment */
		RETURN DECIMAL res DECIMAL; BEGIN RETURN 1; END;
		/
	`
	func2SQL := `CREATE OR REPLACE FUNCTION "test"."F2" ()
		RETURN DECIMAL res DECIMAL; BEGIN RETURN 1; END
	`
	commentSQL := "COMMENT ON FUNCTION [test].[F1] IS 'func comment';\n"
	s.execute(openSchemaSQL, func1SQL, func2SQL, commentSQL)
	s.backup(Conf{}, FUNCTIONS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"functions": dt{
					"F1.sql": openSchemaSQL + "\n" + func1SQL + commentSQL,
					"F2.sql": openSchemaSQL + "\n--/\n" + func2SQL + "\n/\n",
				},
			},
		},
	})

	// Test --drop-extras
	s.execute("DROP FUNCTION f2")
	s.backup(Conf{DropExtras: true}, FUNCTIONS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"functions": dt{
					"F1.sql": openSchemaSQL + "\n" + func1SQL + commentSQL,
				},
			},
		},
	})
}

func (s *testSuite) TestScripts() {
	openSchemaSQL := "OPEN SCHEMA [test];"
	script1SQL := `--/
		CREATE OR REPLACE LUA SCRIPT "SCRIPTING_SCRIPT" () RETURNS ROWCOUNT AS
		function hi()
			output('hello')
		end
/
`
	script2SQL := `CREATE OR REPLACE LUA SCALAR SCRIPT "UDF_SCRIPT" () RETURNS DECIMAL(18,0) AS
		function run(ctx)
			return 1
		end
	`
	script3SQL := `CREATE OR REPLACE PYTHON3  ADAPTER SCRIPT "ADAPTER_SCRIPT" AS
		local str = 'hello'
	`
	commentSQL := "COMMENT ON SCRIPT [test].[SCRIPTING_SCRIPT] IS 'script comment';\n"
	s.execute(openSchemaSQL, script1SQL, script2SQL, script3SQL, commentSQL)
	s.backup(Conf{}, SCRIPTS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"scripts": dt{
					"SCRIPTING_SCRIPT.sql": fmt.Sprintf("%s\n%s%s",
						openSchemaSQL, script1SQL, commentSQL,
					),
					"UDF_SCRIPT.sql":     openSchemaSQL + "\n--/\n" + script2SQL + "\n/\n",
					"ADAPTER_SCRIPT.sql": openSchemaSQL + "\n--/\n" + script3SQL + "\n/\n",
				},
			},
		},
	})

	// Test --drop-extras
	s.execute("DROP SCRIPT SCRIPTING_SCRIPT")
	s.execute("DROP ADAPTER SCRIPT ADAPTER_SCRIPT")
	s.backup(Conf{DropExtras: true}, SCRIPTS)
	s.expect(dt{
		"schemas": dt{
			"test": dt{
				"scripts": dt{
					"UDF_SCRIPT.sql": openSchemaSQL + "\n--/\n" + script2SQL + "\n/\n",
				},
			},
		},
	})
}

func (s *testSuite) TestUsers() {
	password := regexp.MustCompile(`"12345678"`)
	user1SQL := "CREATE USER [JOE] IDENTIFIED BY \"12345678\";\n"
	user2SQL := "CREATE USER [JANE] IDENTIFIED BY KERBEROS PRINCIPAL 'jane';\n"
	user3SQL := "CREATE USER [JOHN] IDENTIFIED AT LDAP AS 'john'"
	user4SQL := "CREATE USER [JENN] IDENTIFIED BY OPENID SUBJECT 'jenn';\n"
	commentSQL := "COMMENT ON USER [JOE] IS 'a tough guy';\n"
	policySQL := "ALTER USER [JOE] SET PASSWORD_EXPIRY_POLICY='EXPIRY_DAYS=180:GRACE_DAYS=30';\n"
	expireSQL := "ALTER USER [JOE] PASSWORD EXPIRE;\n"
	cleanUser1SQL := password.ReplaceAllString(user1SQL, "********")

	s.execute("DROP USER IF EXISTS joe")
	s.execute("DROP USER IF EXISTS jane")
	s.execute("DROP USER IF EXISTS john")
	s.execute("DROP USER IF EXISTS jenn")
	s.execute(user1SQL, user2SQL, user3SQL+" FORCE", user4SQL, commentSQL, policySQL, expireSQL)
	s.backup(Conf{}, USERS)
	s.expect(dt{
		"users": dt{
			"JOE.sql":  cleanUser1SQL + commentSQL + policySQL + expireSQL,
			"JANE.sql": user2SQL,
			"JOHN.sql": user3SQL + ";\n",
			"JENN.sql": user4SQL,
		},
	})
}

func (s *testSuite) TestRoles() {
	roleSQL := "CREATE ROLE [LUMBERJACKS];\n"
	groupSQL := "ALTER ROLE [LUMBERJACKS] SET CONSUMER_GROUP = [LOW];\n"
	commentSQL := "COMMENT ON ROLE [LUMBERJACKS] IS 'tough guys';\n"

	s.execute("DROP ROLE IF EXISTS lumberjacks")
	s.execute(roleSQL, groupSQL, commentSQL)
	s.backup(Conf{}, ROLES)
	s.expect(dt{
		"roles": dt{
			"DBA.sql":         "COMMENT ON ROLE [DBA] IS 'DBA stands for database administrator and has all possible privileges. This role should only be assigned to very few users because it provides these with full access to the database.';\n",
			"PUBLIC.sql":      "COMMENT ON ROLE [PUBLIC] IS 'The PUBLIC role stands apart because every user receives this role automatically. This makes it very simple to grant and later withdraw certain privileges to/from all users of the database. However, this should only occur if one is quite sure that it is safe to grant the respective rights and the shared data should be publicly accessible.';\nGRANT USE ANY SCHEMA TO [PUBLIC];\n",
			"LUMBERJACKS.sql": roleSQL + groupSQL + commentSQL,
		},
	})
}

func (s *testSuite) TestConnections() {
	password := regexp.MustCompile(`'12345678'`)
	connSQL := "CREATE OR REPLACE CONNECTION CONN TO 'someplace' USER 'joe' IDENTIFIED BY '12345678';\n"
	commentSQL := "COMMENT ON CONNECTION CONN IS 'teleporter';\n"
	cleanConnSQL := password.ReplaceAllString(connSQL, "********")

	s.execute("DROP CONNECTION IF EXISTS conn")
	s.execute(connSQL, commentSQL)
	s.backup(Conf{}, CONNECTIONS)
	s.expect(dt{
		"connections.sql": cleanConnSQL + commentSQL,
	})
}

func (s *testSuite) TestEmptyConnections() {
	connSQL := "CREATE OR REPLACE CONNECTION CONN TO '' USER '' IDENTIFIED BY '';\n"
	cleanConnSQL := regexp.MustCompile(`'';`).ReplaceAllString(connSQL, "********;")
	s.execute("DROP CONNECTION IF EXISTS conn")
	s.execute(connSQL)
	s.backup(Conf{}, CONNECTIONS)
	s.expect(dt{"connections.sql": cleanConnSQL})
}

func (s *testSuite) TestConsumerGroups() {
	if capability.consumerGroups {
		groupSQL := []string{
			"DROP CONSUMER GROUP [Low]",
			"CREATE CONSUMER GROUP [Low] WITH\n" +
				"   PRECEDENCE = 1,\n" +
				"   CPU_WEIGHT = 234,\n" +
				"   GROUP_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   USER_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   SESSION_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   QUERY_TIMEOUT = 0,\n" +
				"   IDLE_TIMEOUT = 86400",
			"ALTER CONSUMER GROUP [MEDIUM] SET\n" +
				"   PRECEDENCE = 1,\n" +
				"   CPU_WEIGHT = 456,\n" +
				"   GROUP_TEMP_DB_RAM_LIMIT = '500',\n" +
				"   USER_TEMP_DB_RAM_LIMIT = '678',\n" +
				"   SESSION_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   QUERY_TIMEOUT = 0,\n" +
				"   IDLE_TIMEOUT = 86400",
			"ALTER CONSUMER GROUP [SYS_CONSUMER_GROUP] SET\n" +
				"   PRECEDENCE = 1000,\n" +
				"   CPU_WEIGHT = 1000,\n" +
				"   GROUP_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   USER_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   SESSION_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   QUERY_TIMEOUT = 0,\n" +
				"   IDLE_TIMEOUT = 86400",
			"DROP CONSUMER GROUP [custom]",
			"CREATE CONSUMER GROUP [custom] WITH\n" +
				"  PRECEDENCE = 123,\n" +
				"  CPU_WEIGHT = 456,\n" +
				"  GROUP_TEMP_DB_RAM_LIMIT = '1234',\n" +
				"  USER_TEMP_DB_RAM_LIMIT = '2345',\n" +
				"  SESSION_TEMP_DB_RAM_LIMIT = '3456',\n" +
				"  QUERY_TIMEOUT = 78,\n" +
				"  IDLE_TIMEOUT = 90",
			"COMMENT ON CONSUMER GROUP [custom] IS 'the big cheeses'",
			"DROP CONSUMER GROUP [high]",
			"CREATE CONSUMER GROUP [high] WITH\n" +
				"   PRECEDENCE = 123,\n" +
				"   CPU_WEIGHT = 456,\n" +
				"   GROUP_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   USER_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   SESSION_TEMP_DB_RAM_LIMIT = 'OFF',\n" +
				"   QUERY_TIMEOUT = 0,\n" +
				"   IDLE_TIMEOUT = 86400",
		}
		s.execute("DROP CONSUMER GROUP HIGH")
		s.execute("DROP CONSUMER GROUP LOW")
		s.execute("CREATE CONSUMER GROUP [Low] WITH CPU_WEIGHT = 12")
		s.execute("CREATE CONSUMER GROUP [high] WITH CPU_WEIGHT = 12")
		s.execute("CREATE CONSUMER GROUP [custom] WITH CPU_WEIGHT = 12")
		s.execute(groupSQL...)
		s.backup(Conf{}, CONSUMER_GROUPS)
		s.expect(dt{"consumer_groups.sql": strings.Join(groupSQL, ";\n") + ";\n"})
		s.execute("DROP CONSUMER GROUP [custom]")
	} else {
		groupSQL := []string{
			"DROP PRIORITY GROUP [Low]",
			"CREATE PRIORITY GROUP [Low] WITH WEIGHT = 234",
			"ALTER PRIORITY GROUP [MEDIUM] SET WEIGHT = 345",
			"DROP PRIORITY GROUP [custom]",
			"CREATE PRIORITY GROUP [custom] WITH WEIGHT = 456",
			"COMMENT ON PRIORITY GROUP [custom] IS 'the big cheeses'",
			"DROP PRIORITY GROUP [high]",
			"CREATE PRIORITY GROUP [high] WITH WEIGHT = 123",
		}
		s.exaConn.Conf.SuppressError = true // The groups may not exist
		s.execute("DROP PRIORITY GROUP HIGH")
		s.execute("DROP PRIORITY GROUP LOW")
		s.execute(groupSQL...)
		s.exaConn.Conf.SuppressError = false
		s.backup(Conf{}, PRIORITY_GROUPS)
		s.expect(dt{"priority_groups.sql": strings.Join(groupSQL, ";\n") + ";\n"})
		s.execute("DROP PRIORITY GROUP [custom]")
	}
}

func (s *testSuite) TestPrivileges() {
	prioritySQL := "GRANT PRIORITY GROUP [LOW] TO [JOE]"
	if capability.consumerGroups {
		prioritySQL = "ALTER USER [JOE] SET CONSUMER_GROUP = [LOW]"
	}
	sql := []string{
		"CREATE USER [JOE] IDENTIFIED BY KERBEROS PRINCIPAL 'joe'",
		prioritySQL, // Priority Priv
		"GRANT CONNECTION CONN TO [JOE] WITH ADMIN OPTION",                   // Connection Priv
		"GRANT ACCESS ON CONNECTION [CONN] FOR SCRIPT [test].[SCR] TO [JOE]", // Connection Restricted Priv
		"GRANT ACCESS ON CONNECTION [CONN] FOR SCHEMA [test] TO [JOE]",       // Connection Restricted Priv
		"GRANT SELECT ON SCHEMA [test] TO [JOE]",                             // Object Priv
		"GRANT [DBA] TO [JOE] WITH ADMIN OPTION",                             // Role Priv
		"GRANT SELECT ANY TABLE TO [JOE] WITH ADMIN OPTION",                  // System Priv
		"GRANT IMPERSONATION ON [DBA] TO [JOE]",                              // Impersonation Priv
		"ALTER SCHEMA [test] CHANGE OWNER [JOE]",                             // Schema Owner
	}
	s.execute("DROP USER IF EXISTS joe")
	s.execute("DROP CONNECTION IF EXISTS conn")
	s.execute("CREATE CONNECTION conn TO 'someplace'")
	s.execute(`
		CREATE OR REPLACE LUA SCALAR SCRIPT [test].[SCR]() RETURNS BOOLEAN AS
			function run(ctx)
				ctx.emit(true)
			end
	`)
	s.execute(sql...)
	s.backup(Conf{}, USERS)
	s.expect(dt{
		"users": dt{
			"JOE.sql": strings.Join(sql, ";\n") + ";\n",
		},
	})
}

func (s *testSuite) TestCriteria() {
	tests := [][]string{
		// matchCriteria, skipCriteria, schemaToBeChecked, objectToBeChecked, expectedReturn
		{"sch", "", "sch", "", "true"},
		{"sch", "", "sch", "obj", "true"},
		{"sch", "", "hcs", "", "false"},
		{"sch", "", "hcs", "jbo", "false"},
		{"sch.obj", "", "sch", "", "true"},
		{"sch.obj", "", "sch", "obj", "true"},
		{"sch.obj", "", "sch", "jbo", "false"},
		{"sch.obj", "", "hcs", "", "false"},
		{"sch.obj", "", "hcs", "jbo", "false"},

		{"sch", "hcs", "sch", "", "true"},
		{"sch", "hcs", "sch", "obj", "true"},
		{"sch", "hcs", "hcs", "", "false"},
		{"sch", "hcs", "hcs", "jbo", "false"},
		{"sch.obj", "hcs", "sch", "", "true"},
		{"sch.obj", "hcs", "sch", "obj", "true"},
		{"sch.obj", "hcs", "sch", "jbo", "false"},
		{"sch.obj", "hcs", "hcs", "", "false"},
		{"sch.obj", "hcs", "hcs", "jbo", "false"},

		{"sch", "hcs.bjo", "sch", "", "true"},
		{"sch", "hcs.bjo", "sch", "obj", "true"},
		{"sch", "hcs.bjo", "hcs", "", "false"},
		{"sch", "hcs.bjo", "hcs", "jbo", "false"},
		{"sch.obj", "hcs.bjo", "sch", "", "true"},
		{"sch.obj", "hcs.bjo", "sch", "obj", "true"},
		{"sch.obj", "hcs.bjo", "sch", "jbo", "false"},
		{"sch.obj", "hcs.bjo", "hcs", "", "false"},
		{"sch.obj", "hcs.bjo", "hcs", "jbo", "false"},

		{"sch", "sch.bjo", "sch", "", "true"},
		{"sch", "sch.bjo", "sch", "obj", "true"},
		{"sch", "sch.bjo", "hcs", "", "false"},
		{"sch", "sch.bjo", "hcs", "jbo", "false"},
		{"sch.obj", "sch.bjo", "sch", "", "true"},
		{"sch.obj", "sch.bjo", "sch", "obj", "true"},
		{"sch.obj", "sch.bjo", "sch", "jbo", "false"},
		{"sch.obj", "sch.bjo", "hcs", "", "false"},
		{"sch.obj", "sch.bjo", "hcs", "jbo", "false"},

		{"sch", "sch.obj", "sch", "", "true"},
		{"sch", "sch.obj", "sch", "obj", "false"},
		{"sch", "sch.obj", "hcs", "", "false"},
		{"sch", "sch.obj", "hcs", "jbo", "false"},
	}
	for _, test := range tests {
		crit := Criteria{
			match: test[0],
			skip:  test[1],
		}
		exp := test[4]
		got := fmt.Sprintf("%v", crit.matches(test[2], test[3]))
		s.Equal(exp, got)
	}
}
