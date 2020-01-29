package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

type Param struct {
	Project  string
	Instance string
	Database string
	Sql      string
}

func main() {
	ctx := context.Background()

	param, err := getFlag()
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", param.Project, param.Instance, param.Database)
	fmt.Println(db)

	ss, err := NewSpannerService(ctx, db)
	if err != nil {
		panic(err)
	}
	fmt.Println(param.Sql)

	count, err := ss.PartitionedDML(ctx, param.Sql)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Success:RowCount:%d", count)
}

func getFlag() (*Param, error) {
	var (
		project  = flag.String("project", "", "project is spanner project")
		instance = flag.String("instance", "", "instance is spanner insntace")
		database = flag.String("database", "", "database is spanner database")
		sql      = flag.String("sql", "", "sql is execute query")
	)
	flag.Parse()

	var emsg string
	if len(*project) < 1 {
		emsg += "project is required\n"
	}
	if len(*instance) < 1 {
		emsg += "instance is required\n"
	}
	if len(*database) < 1 {
		emsg += "database is required\n"
	}
	if len(*sql) < 1 {
		emsg += "sql is required\n"
	}

	if len(emsg) > 0 {
		return nil, fmt.Errorf("%s", emsg)
	}

	return &Param{
		Project:  *project,
		Instance: *instance,
		Database: *database,
		Sql:      *sql,
	}, nil
}
