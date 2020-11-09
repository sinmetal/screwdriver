package cmd

import (
	"context"
	"fmt"

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
