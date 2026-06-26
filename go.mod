module orchestrator_service

go 1.26.4

require (
	ai_service v0.0.0-00010101000000-000000000000
	github.com/pdfcpu/pdfcpu v0.13.0
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/hhrutter/lzw v1.0.0 // indirect
	github.com/hhrutter/pkcs7 v0.2.2 // indirect
	github.com/hhrutter/tiff v1.0.3 // indirect
	github.com/mattn/go-runewidth v0.0.24 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/image v0.41.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260622175928-b703f567277d // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace ai_service => ../ai_service
