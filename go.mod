module clashking_tracking

go 1.26.4

replace github.com/clashkinginc/clashy.go => ../../GolandProjects/clashy.go

require (
	github.com/clashkinginc/clashy.go v0.1.2
	github.com/golang/snappy v1.0.0
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.9.2
	github.com/joho/godotenv v1.5.1
	github.com/turnage/graw v0.0.0-20250321203609-ee225b526649
	github.com/valkey-io/valkey-go v1.0.74
	golang.org/x/oauth2 v0.35.0
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
)

require (
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/turnage/redditproto v0.0.0-20151223012412-afedf1b6eddb // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.43.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260401024825-9d38bb4040a9 // indirect
)
