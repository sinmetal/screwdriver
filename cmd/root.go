package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	projectID string
	instance  string
	database  string
	sql       string

	// 環境変数で設定されているケース
	spannerDatabase string
)

// RootCmd is root command
var RootCmd = &cobra.Command{
	Use:   "screwdriver",
	Short: "sql to spanner",
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func init() {
	cobra.OnInitialize()
	ExecuteCmd.AddCommand(
		executeStalenessSQLCmd(),
	)

	RootCmd.AddCommand(
		ExecuteCmd,
	)

	spannerDatabase = os.Getenv("SPANNER_DATABASE")
	RootCmd.PersistentFlags().StringVar(&projectID, "project", "hogeproject", "project")
	RootCmd.PersistentFlags().StringVar(&instance, "instance", "hogeinstance", "instance")
	RootCmd.PersistentFlags().StringVar(&database, "database", "hogedb", "database")

	RootCmd.PersistentFlags().StringVar(&sql, "sql", "SELECT 1", "sql")
	if err := RootCmd.MarkPersistentFlagRequired("sql"); err != nil {
		fmt.Println(err)
	}
}

func SpannerDatabase() (string, error) {
	if projectID != "hogeproject" {
		return fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, database), nil
	}

	if len(spannerDatabase) < 1 {
		return spannerDatabase, nil
	}

	return "", fmt.Errorf("Spanner Database is not set. Set $SPANNER_DATABASE = projects/{PROJECT}/instances/{INSTACE}/databases/{DATABASE} or set flag --project --instance --database.")
}
