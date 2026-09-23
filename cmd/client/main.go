package main

import (
	"context"
	"fmt"
	"log"
	"time"

	searchv1 "github.com/Rebira678/Retriever/pkg/api/search/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := searchv1.NewSearchServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	fmt.Println("Making gRPC request to Search API...")
	start := time.Now()
	
	r, err := c.Search(ctx, &searchv1.SearchRequest{
		Query: "What is Retrieval-Augmented Generation?",
		TopK:  2,
	})
	if err != nil {
		log.Fatalf("could not search: %v", err)
	}
	
	fmt.Printf("Search took %s\n", time.Since(start))
	for i, res := range r.GetResults() {
		fmt.Printf("Result %d (Score: %f): %s...\n", i+1, res.Score, res.ChunkText[:50])
	}
}
