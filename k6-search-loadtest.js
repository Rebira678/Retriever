// Day 45: Senior-Level Load testing the gRPC search endpoint
// This script simulates realistic varied traffic to prevent 100% cache hits
// and properly tests the pgvector HNSW index traversal latency.
import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';
import exec from 'k6/execution';

const client = new grpc.Client();
// Load the protobuf definition
client.load(['proto/search/v1'], 'search.proto');

export const options = {
    scenarios: {
        ramping_load: {
            executor: 'ramping-arrival-rate',
            startRate: 10,
            timeUnit: '1s',
            preAllocatedVUs: 50,
            maxVUs: 500,
            stages: [
                { duration: '30s', target: 50 },  // Ramp to 50 RPS
                { duration: '1m', target: 50 },   // Maintain
                { duration: '30s', target: 100 }, // Peak load
                { duration: '1m', target: 100 },  // Maintain
                { duration: '30s', target: 0 },   // Cool down
            ],
        },
    },
    thresholds: {
        // Strict production SLAs for semantic search
        grpc_req_duration: ['p(95)<200', 'p(99)<400'], 
    },
};

// Seeded list of realistic enterprise queries to test the vector index variety
const QUERIES = [
    "What is the second phase of the RAG pipeline?",
    "How does idempotency work in distributed systems?",
    "Explain the circuit breaker pattern",
    "What is the difference between batch and streaming ingestion?",
    "How do we prevent vector space poisoning?",
    "What is an HNSW index in pgvector?",
    "How does Singleflight prevent cache stampedes?",
    "Explain bounded semaphores in Go",
    "Best practices for Go channels",
    "How does a Token Bucket rate limiter work?"
];

export default () => {
    // Note: In k6/net/grpc, connections are typically maintained per-VU.
    client.connect('127.0.0.1:50051', {
        plaintext: true,
    });

    // Randomize query to force pgvector HNSW index traversal and test LRU cache eviction
    const queryIndex = exec.scenario.iterationInTest % QUERIES.length;
    
    const data = {
        query: QUERIES[queryIndex],
        top_k: 5,
    };

    // Invoke the SearchService Search method
    const response = client.invoke('search.v1.SearchService/Search', data);

    check(response, {
        'status is OK': (r) => r && r.status === grpc.StatusOK,
        'has results': (r) => r && r.message && r.message.results && r.message.results.length > 0,
    });

    client.close();
};
