FROM golang:1.15 AS builder

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Move to working directory /build
WORKDIR /build

# Copy and download dependency using go mod
COPY go.mod .
COPY go.sum .
RUN go mod download

# Copy the code into the container
COPY . .

# Build the application
RUN NOW=$(date +'%Y-%m-%d_%T') && go build -ldflags "-X version.Revision=$SOURCE_COMMIT -X version.BuildDate=$NOW -X version.Version=$DOCKER_TAG" -o eventscheduler ./app

# Move to /dist directory as the place for resulting binary folder
WORKDIR /dist

# Copy binary from build to main folder
RUN cp /build/eventscheduler .

FROM gcr.io/distroless/base

COPY --from=builder /dist/eventscheduler /

# Export necessary port
# EXPOSE 3000

ENTRYPOINT ["/eventscheduler"]