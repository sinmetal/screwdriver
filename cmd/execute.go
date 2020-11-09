package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
)

var ExecuteCmd = &cobra.Command{
	Use:   "execute",
	Short: "execute",
	RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("Command name argument expected.")
	},
}

// executeStalenessSQLCmd is ExactStaleness を実行する
func executeStalenessSQLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "staleness",
		Short: "Execute SQL with ExactStaleness (before 15sec)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			db, err := SpannerDatabase()
			if err != nil {
				return err
			}
			ss, err := NewSpannerService(ctx, db)
			if err != nil {
				return fmt.Errorf("%s : %s", err.Error(), db)
			}

			return ss.ExactStalenessQuery(ctx, sql)
		},
	}

	return cmd
}

// executeUpdateSQLCmd is 更新系の DML を実行する
// OperationPITR Table への INSERT も同時に行うことで、クエリ実行時のCommitTimestampを分かりやすくしている
func executeUpdateSQLCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update",
		Short: "",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			db, err := SpannerDatabase()
			if err != nil {
				return err
			}
			ss, err := NewSpannerService(ctx, db)
			if err != nil {
				return fmt.Errorf("%s : %s", err.Error(), db)
			}

			return ss.ExecuteUpdateDML(ctx, sql)
		},
	}

	return cmd
}
