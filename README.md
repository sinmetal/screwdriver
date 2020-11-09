# screwdriver
Spannerに適当にクエリを投げるツール

## Config

何度も同じ SpannerDatabase に SQL を実行する場合は $SPANNER_DATABASE を使うと便利

```
export SPANNER_DATABASE=projects/PROJECT_ID/instances/INSTANCE_ID/databases/DATABASE_ID
```

## Usage

```
go run . execute staleness --project gcpug-public-spanner --instance merpay-sponsored-instance --database sinmetal --sql "SELECT 1"
```

or Config を設定している場合

```
go run . execute staleness --sql "SELECT 1"
```