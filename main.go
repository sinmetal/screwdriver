package main

import (
	"context"
)

func main() {
	spannerDatabsase := GetOSEnvStr("SPANNER_DATABASE")

	ctx := context.Background()

	ss, err := NewSpannerService(ctx, spannerDatabsase)
	if err != nil {
		panic(err)
	}

	sql := "SELECT count(1) as Count From Tweet Limit 1"
	//sql := "SELECT 1 as Count"

	ss.ExactStalenessQuery(ctx, sql)
}
