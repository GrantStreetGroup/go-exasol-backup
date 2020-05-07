# go-exasol-backup

A Go library for backingup Exasol metadata and optionally data to text files.
Metadata includes DDL, users, roles, connections, permissions and system parameters.
The metadata is stored as SQL and data is stored as CSV.

## Synopsis

```go
import (
	"github.com/grantstreetgroup/go-exasol-backup"
	"github.com/grantstreetgroup/go-exasol-client"
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

TBD



    TODO:
            - Backup priority groups
                    - Backup schema raw size limits


# Author

Grant Street Group <developers@grantstreet.com>

# Copyright and License

This software is Copyright (c) 2020 by Grant Street Group.

This is free software, licensed under:

    MIT License

# Contributors

- Peter Kioko <peter.kioko@grantstreet.com>
