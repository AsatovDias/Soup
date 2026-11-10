FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go schema.sql ./
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -o /queue .

FROM debian:bookworm-slim
COPY --from=build /queue /queue
USER 65532:65532
ENTRYPOINT ["/queue"]
