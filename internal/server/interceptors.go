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
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		msg, ok := req.(proto.Message)
		if !ok {
			return handler(ctx, req)
		}

		if err := v.Validate(msg); err != nil {
			if valErr, ok := errors.AsType[*protovalidate.ValidationError](err); ok {
				st := status.New(codes.InvalidArgument, "Request validation failed")
				badReq := &errdetails.BadRequest{}

				for _, violation := range valErr.Violations {
					badReq.FieldViolations = append(badReq.FieldViolations, &errdetails.BadRequest_FieldViolation{
						Field:       string(violation.FieldDescriptor.Name()),
						Description: violation.Proto.GetMessage(),
					})
				}

				stWithDetails, attachErr := st.WithDetails(badReq)
				if attachErr == nil {
					return nil, stWithDetails.Err()
				}
			}
			return nil, status.Errorf(codes.InvalidArgument, "invalid request: %v", err)
		}

		return handler(ctx, req)
	}
}
