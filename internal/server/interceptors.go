package server

import (
	"context"
	"errors"

	"buf.build/go/protovalidate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ValidationInterceptor creates a gRPC interceptor that validates incoming requests.
func ValidationInterceptor(v protovalidate.Validator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		msg, ok := req.(proto.Message)
		if !ok {
			// Not a proto message, pass through
			return handler(ctx, req)
		}

		// Run protovalidate
		if err := v.Validate(msg); err != nil {
			var valErr *protovalidate.ValidationError
			if errors.As(err, &valErr) {
				st := status.New(codes.InvalidArgument, "Request validation failed")

				// Map protovalidate violations to gRPC errdetails.BadRequest
				badReq := &errdetails.BadRequest{}

				for _, violation := range valErr.Violations {
					badReq.FieldViolations = append(badReq.FieldViolations, &errdetails.BadRequest_FieldViolation{
						Field:       string(violation.FieldDescriptor.Name()),
						Description: violation.Proto.GetMessage(),
					})
				}

				// Attach the rich error details to the status
				stWithDetails, attachErr := st.WithDetails(badReq)
				if attachErr == nil {
					return nil, stWithDetails.Err()
				}
			}
			// Fallback if it's not a ValidationError or WithDetails fails
			return nil, status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
		}

		// Request is valid, proceed to handler
		return handler(ctx, req)
	}
}
