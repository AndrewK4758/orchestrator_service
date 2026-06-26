FROM golang:alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

WORKDIR /app

# In order to support the local `replace ai_service => ../ai_service` go.mod directive,
# we set the Docker build context to the root doc_processor_Service directory 
# and copy both services into the builder container.
COPY ai_service/ ./ai_service/
COPY orchestrator_service/ ./orchestrator_service/

WORKDIR /app/orchestrator_service

# Download dependencies
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
