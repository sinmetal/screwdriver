package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BatchSpannerService struct {
	sc *spanner.Client
}

func NewBatchSpannerService(ctx context.Context, db string) (*BatchSpannerService, error) {
	client, err := createSpannerClientForBatch(ctx, db)
	if err != nil {
		return nil, err
	}
	return &BatchSpannerService{
		sc: client,
	}, nil
}

func createSpannerClientForBatch(ctx context.Context, db string) (*spanner.Client, error) {
	return spanner.NewClientWithConfig(ctx, db, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened:     1,  // 1query投げておしまいので、1でOK
			MaxOpened:     10, // 1query投げておしまいなので、そんなにたくさんは要らない
			WriteSessions: 0,  // さほどPerformanceは気にしてないので、WriteSessionsは要らない
		},
	}, option.WithEndpoint("batch-spanner.googleapis.com:443"))
}

func (s *BatchSpannerService) Close() {
	s.sc.Close()
}

func (s *BatchSpannerService) ExecuteQuery(ctx context.Context, sql string) error {
	fmt.Printf("Start Query : %s\n", sql)
	tx, err := s.sc.BatchReadOnlyTransaction(ctx, spanner.ExactStaleness(time.Second*15))
	if err != nil {
		return err
	}
	defer tx.Close()

	iter := tx.QueryWithStats(ctx, spanner.Statement{
		SQL: sql,
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Printf("%+v\n", row.ColumnNames())
	}
	fmt.Printf("QueryPlan: %+v\n", iter.QueryPlan)
	fmt.Printf("QueryStats: %+v\n", iter.QueryStats)
	return nil
}
