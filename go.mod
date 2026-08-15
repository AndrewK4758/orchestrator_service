module orchestrator_service

go 1.26.4

require (
	buf.build/go/protovalidate v1.3.0
	github.com/AndrewK4758/shared_protos v0.0.0-20260815145145-c2c7e8d11702
	github.com/AndrewK4758/shared_utils v0.0.0-00010101000000-000000000000
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.3
	github.com/nats-io/nats.go v1.53.1
	github.com/stretchr/testify v1.11.1
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260810153831-ec0a7760b754
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.12
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260709200747-435963d16310.1 // indirect
	cel.dev/expr v0.25.3 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/google/cel-go v0.31.0 // indirect
	github.com/joho/godotenv v1.5.1 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/exp v0.0.0-20260813180055-c1d0aacb2297 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260810153831-ec0a7760b754 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/AndrewK4758/shared_utils => ../shared_utils
