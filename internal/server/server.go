package server

import (
	"context"
	"fmt"
	"log"

	pb "orchestrator_service/proto"
)

// OrchestratorServer implements the OrchestratorService gRPC interface.
type OrchestratorServer struct {
	pb.UnimplementedOrchestratorServiceServer
}

// NewServer creates a new instance of the OrchestratorServer.
func NewServer() *OrchestratorServer {
	return &OrchestratorServer{}
}

// SubmitDocument handles the initial client request to process a PDF.
// It returns a job_id instantly while starting the async work.
func (s *OrchestratorServer) SubmitDocument(ctx context.Context, req *pb.SubmitDocumentRequest) (*pb.SubmitDocumentResponse, error) {
	log.Printf("Received document: %s, size: %d bytes", req.GetFileName(), len(req.GetPdfContent()))

	// TODO: Generate a real UUID for the job
	jobID := "job_" + req.GetFileName()

	// TODO: Push the job to an internal channel or queue to be picked up by the state machine

	return &pb.SubmitDocumentResponse{
		JobId:  jobID,
		Status: "QUEUED",
	}, nil
}

// ListenForProgress streams real-time updates back to the client.
func (s *OrchestratorServer) ListenForProgress(req *pb.ListenRequest, stream pb.OrchestratorService_ListenForProgressServer) error {
	jobID := req.GetJobId()
	log.Printf("Client listening for progress on job: %s", jobID)

	// TODO: Subscribe to the internal channel or Redis/NATS for this specific JobID
	// For now, we simulate a quick progress update.
	updates := []string{"CHUNKING", "PROCESSING", "STITCHING", "COMPLETE"}
	
	for _, status := range updates {
		msg := &pb.ProgressUpdate{
			JobId:   jobID,
			Status:  status,
			Message: fmt.Sprintf("Entering state: %s", status),
		}
		if status == "COMPLETE" {
			msg.FinalResultsJson = `[{"document": 1, "status": "success"}]`
		}

		if err := stream.Send(msg); err != nil {
			log.Printf("Error sending stream to client for job %s: %v", jobID, err)
			return err
		}
	}

	return nil
}
