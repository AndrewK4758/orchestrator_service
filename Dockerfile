FROM golang:alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

COPY --from=shared_protos . /app/shared_protos
COPY --from=shared_utils . /app/shared_utils
COPY . /app/orchestrator_service
WORKDIR /app/orchestrator_service
RUN go mod edit -replace github.com/AndrewK4758/shared_protos=../shared_protos

# Download dependencies
RUN go mod edit -replace github.com/AndrewK4758/shared_utils=../shared_utils
RUN go mod download

# Build the executable
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o orchestrator_bin cmd/server/main.go

# --- Final minimal stage ---
FROM alpine:latest  
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/orchestrator_service/orchestrator_bin .

EXPOSE 50052

CMD ["./orchestrator_bin"]
