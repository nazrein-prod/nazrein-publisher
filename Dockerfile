
FROM golang:1.23-alpine AS builder
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY main.go .

RUN CGO_ENABLED=0 GOOS=linux go build -o publisher

FROM gcr.io/distroless/base-debian12:nonroot
WORKDIR /app

COPY --from=builder /build/publisher .

COPY --from=builder /build/migrations ./migrations

USER nonroot:nonroot

CMD ["/app/publisher"]
