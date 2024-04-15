
# go-exasol-backup

A Go utility for backing up Exasol metadata and, optionally, data to text files.
Metadata includes DDL, users, roles, connections, permissions and system parameters.
The metadata is stored as SQL and data is stored as CSV.

## Synopsis

```go
import (
    "github.com/GrantStreetGroup/go-exasol-backup"
    "github.com/GrantStreetGroup/go-exasol-client"
)

func main() {
    err := backup.Backup(backup.Conf{
        Source:      exasol.Connect(exasol.ConnConf{ ... })
        Destination: "/directory/to/backup/to/",
        Objects:     []backup.Object{backup.ALL},
    })
    if err != nil {
        log.Fatal(err)
    }
}
```
## Configs

 - **Source**: Pointer to an Exasol connection to backup from.
 - **Destination**: Path to a filesystem directory to store the backup SQL/CSV
 - **Objects**: List of object types to backup. It can be one or more of the following constants: `CONNECTIONS, FUNCTIONS, PARAMETERS, PRIORITY_GROUPS, ROLES, SCHEMAS, SCRIPTS, TABLES, USERS, VIEWS,` or `ALL`
 - **Match**:  You can restrict which objects are backed up using the Match and Skip configs. Match is a comma delimited set of wildcard matching patterns. Any schema object matching one of these patterns will be backedup. Each pattern should be in the form of `schema.object`. If the object is not specified `schema.*` is assumed. If the schema is not specified then `*.*` is assumed.  Non-schema objects (users, roles, connections, parameters) are not affected by this config. i.e. they will be backed up all-or-none.
 - **Skip**: Skip is the inverse of Match. Any schema objects matching it will be skipped. Same rules apply.
 -  **MaxTableRows**: If > 0 then tables with this many or fewer rows will have the their data backed up to CSV files. If 0 then no table data will be backed up (Default).
 - **MaxViewRows**: If > 0 then views with this many or fewer rows will have the their data backed up to CSV files. If 0 then no view data will be backed up (Default).
 - **DropExtras**: If true then any text files existing in the destination but no longer existing in Exasol will be removed. If false then the backup is purely additive (Default).
 - **LogLevel**: Defaults to `warning`

# Author

Grant Street Group <developers@grantstreet.com>

# Copyright and License

This software is Copyright (c) 2020 by Grant Street Group.

This is free software, licensed under:

    MIT License

