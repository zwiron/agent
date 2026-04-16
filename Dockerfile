FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o /agent .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /agent /usr/local/bin/zwiron-agent

ENTRYPOINT ["zwiron-agent"]
CMD ["run"]
