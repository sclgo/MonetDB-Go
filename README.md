go-monetdb
==========

MonetDB driver for Go.


## Installation

To install the `monetdb` package to your `$GOPATH`, simply use
the `go` tool. Make sure you have [Git](http://git-scm.com/downloads) installed.

```bash
$ go get github.com/MonetDB/MonetDB-Go
```

Then import it:

```
import(
	_ "github.com/MonetDB/MonetDB-Go/src"
)
```

This ensures that this repo will be used.

## Usage

This Go MonetDB driver implements Go's
[`database/sql/driver`](http://golang.org/pkg/database/sql/driver/) interface.
Once you import it, you can use the standard Go database API to access MonetDB.

```go
import (
	"database/sql"
	_ "github.com/fajran/go-monetdb"
)
```

Then use `monetdb` as the driver name and Data Source Name (DSN) as specified
in the next section.

```go
db, err := sql.Open("monetdb", "username:password@hostname:50000/database")
```

## Data Source Name (DSN)

The format of the DSN is the following

```
[username[:password]@]hostname[:port]/database
```


If the `port` is blank, then the default port `50000` will be used.

## API Documentation

https://pkg.go.dev/github.com/fajran/go-monetdb


