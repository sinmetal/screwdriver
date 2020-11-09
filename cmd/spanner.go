package cmd

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/gcpug/hake"
	"github.com/google/uuid"
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

func (s *SpannerService) ExecuteUpdateDML(ctx context.Context, sql string) error {
	fmt.Printf("Start Query : %s\n", sql)
	fmt.Println("-------------------------------------------------------")

	var rowCounts []int64
	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var err error
		rowCounts, err = tx.BatchUpdate(ctx, []spanner.Statement{
			spanner.NewStatement(sql),
			spanner.NewStatement(fmt.Sprintf("INSERT INTO OperationPITR (OperationPITRID, Comment, CommitedAt)\nVALUES (\"%s\", \"%s\", PENDING_COMMIT_TIMESTAMP());", uuid.New().String(), "update from screwdriver")),
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("Update RowCount : %d\n", rowCounts[0])
	return nil
}

func (s *SpannerService) ExactStalenessQuery(ctx context.Context, sql string) error {
	fmt.Printf("Start Query : %s\n", sql)
	fmt.Println("-------------------------------------------------------")
	iter := s.sc.Single().WithTimestampBound(spanner.ExactStaleness(time.Second*15)).QueryWithStats(ctx, spanner.Statement{
		SQL: sql,
	})
	defer iter.Stop()

	csvw := csv.NewWriter(os.Stdout)
	w := hake.NewWriter(csvw, true)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	csvw.Flush()

	return nil
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
