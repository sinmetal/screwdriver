package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

func main() {
	spannerDatabsase := GetOSEnvStr("SPANNER_DATABASE")

	ctx := context.Background()

	ss, err := NewSpannerService(ctx, spannerDatabsase)
	if err != nil {
		panic(err)
	}

	// sql := "DELETE From Tweet WHERE Author = 'dia'"
	sql := "UPDATE TweetHashKey SET Sort = 1 WHERE STARTS_WITH(Id, @Id)"
	//sql := "SELECT 1 as Count"
	parallelPartitionedDML(ctx, ss, sql)
	fmt.Println(time.Now())
	fmt.Println(sql)
	//ss.ExactStalenessQuery(ctx, sql)
}

func partitionedDML(ctx context.Context, ss *SpannerService, sql string) {
	const maxRetryCount = 5

	var updateCount int64
	var abortCount int
	var retryCount int
	for {
		count, err := ss.PartitionedDML(ctx, sql)
		if err != nil {
			if spanner.ErrCode(err) != codes.Aborted {
				fmt.Printf("failed PartitionedDML:%+v\n", err)
				retryCount++
				if retryCount > maxRetryCount {
					fmt.Println("over retry...")
					os.Exit(1)
				}
				continue
			}
			abortCount++
			continue
		}
		updateCount = count
		break
	}
	fmt.Printf("Success: RowCount:%d, AbortCount:%d RetryCount:%d\n", updateCount, abortCount, retryCount)
}

func parallelPartitionedDML(ctx context.Context, ss *SpannerService, sql string) {
	prefix := GenerateUUIDPrefix()

	rcs, errs := ss.ParallelPartitionedDML(ctx, sql, prefix) // TODO マルチエラーがいるのか・・・
	for i := 0; i < len(rcs); i++ {
		if errs[i] != nil {
			fmt.Printf("%d:%+v\n", i, errs[i])
			continue
		}
		fmt.Printf("%d:%+v\n", i, rcs[i])
	}
}
