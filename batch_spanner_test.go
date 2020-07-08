package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestBatchSpannerService_ExecuteQuery(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", "gcpug-public-spanner", "merpay-sponsored-instance", "sinmetal_benchmark_b")
	s, err := NewBatchSpannerService(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.ExecuteQuery(ctx, "SELECT * FROM OrderDetail1M")
	if err != nil {
		t.Fatal(err)
	}
}
