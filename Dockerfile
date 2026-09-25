# Stage 1: Build the Go binary
FROM golang:1.22-alpine AS builder

# Set the Current Working Directory inside the container
WORKDIR /app

# Install git, required for fetching Go dependencies
RUN apk add --no-cache git

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app. 
# We use CGO_ENABLED=0 to ensure a statically linked binary.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /retriever-bin cmd/retriever/main.go

# Stage 2: Create a minimal image for execution
FROM gcr.io/distroless/static-debian12

# Set the Current Working Directory inside the container
WORKDIR /

# Copy the Pre-built binary file from the previous stage
COPY --from=builder /retriever-bin /retriever-bin

# Expose the gRPC port
EXPOSE 50051

# Command to run the executable
ENTRYPOINT ["/retriever-bin"]
