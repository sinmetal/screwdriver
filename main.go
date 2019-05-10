package main

import (
	"context"
	"fmt"
	"time"
)

func main() {
	spannerDatabsase := GetOSEnvStr("SPANNER_DATABASE")

	ctx := context.Background()

	ss, err := NewSpannerService(ctx, spannerDatabsase)
	if err != nil {
		panic(err)
	}

	// sql := "DELETE From Tweet WHERE Author = 'dia'"
	sql := "UPDATE Tweet SET Sort = 1 WHERE Author = 'dia' AND Sort != 1"
	//sql := "SELECT 1 as Count"
	fmt.Println(time.Now())
	fmt.Println(sql)
	//ss.ExactStalenessQuery(ctx, sql)

	count, err := ss.PartitionedDML(ctx, sql)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Success:RowCount:%d", count)
}
