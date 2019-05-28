package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type SpannerService struct {
	sc *spanner.Client
}

func NewSpannerService(ctx context.Context, db string) (*SpannerService, error) {
	c, err := createSpannerClient(ctx, db)
	if err != nil {
		return nil, err
	}
	return &SpannerService{
		sc: c,
	}, nil
}

func createSpannerClient(ctx context.Context, db string, o ...option.ClientOption) (*spanner.Client, error) {
	dataClient, err := spanner.NewClient(ctx, db, o...)
	if err != nil {
		return nil, err
	}

	return dataClient, nil
}

func (s *SpannerService) ExactStalenessQuery(ctx context.Context, sql string) {
	fmt.Printf("Start Query : %s\n", sql)
	iter := s.sc.Single().WithTimestampBound(spanner.ExactStaleness(time.Second*15)).QueryWithStats(ctx, spanner.Statement{
		SQL: sql,
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
		var count int64
		if err := row.ColumnByName("Count", &count); err != nil {
			panic(err)
		}
		fmt.Printf("Count:%d\n", count)
	}
	fmt.Printf("QueryPlan: %+v\n", iter.QueryPlan)
	fmt.Printf("QueryStats: %+v\n", iter.QueryStats)
}

func (s *SpannerService) PartitionedDML(ctx context.Context, sql string) (int64, error) {
	defer func(n time.Time) {
		d := time.Since(n)
		fmt.Printf("PartitionedDML:Time: %v \n", d)
	}(time.Now())

	stmt := spanner.Statement{SQL: sql}
	rowCount, err := s.sc.PartitionedUpdate(ctx, stmt)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (s *SpannerService) ParallelPartitionedDML(ctx context.Context, sql string, shards []string) ([]int64, []error) {
	defer func(n time.Time) {
		d := time.Since(n)
		fmt.Printf("ParallelPartitionedDML:Time: %v \n", d)
	}(time.Now())

	rowCounts := make([]int64, len(shards))
	errors := make([]error, len(shards))
	wg := &sync.WaitGroup{}
	for i, shard := range shards {
		i := i
		shard := shard
		wg.Add(1)
		go func(i int, shard string) {
			defer wg.Done()

			q := fmt.Sprintf(sql, shard)
			fmt.Println(q)
			stmt := spanner.Statement{
				SQL: q,
			}
			//fmt.Printf("%s:%v\n", sql, shard)
			//stmt := spanner.Statement{
			//	SQL: sql,
			//	Params: map[string]interface{}{
			//		"Shard": shard,
			//	},
			//}
			//var rowCount int64
			//_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			//	count, err := txn.Update(ctx, stmt)
			//	if err != nil {
			//		rowCount = count
			//	}
			//	return err
			//})
			rowCount, err := s.sc.PartitionedUpdate(ctx, stmt)
			if err != nil {
				errors[i] = err
				return
			}
			rowCounts[i] = rowCount
		}(i, shard)
	}
	wg.Wait()

	return rowCounts[:], errors[:]
}

func GenerateUUIDPrefix() []string {
	const a = 'a'
	//var prefix []string

	var runeList []string
	for i := 0; i < 10; i++ {
		runeList = append(runeList, fmt.Sprintf("%d", i))
	}
	for i := 0; i < 6; i++ {
		r := rune('a' + i)
		runeList = append(runeList, fmt.Sprintf("%v", string(r)))
	}

	var resluts []string
	for _, i := range runeList {
		for _, j := range runeList {
			for _, k := range runeList {
				resluts = append(resluts, fmt.Sprintf("%s%s%s", i, j, k))
			}
		}
	}

	return resluts
}
