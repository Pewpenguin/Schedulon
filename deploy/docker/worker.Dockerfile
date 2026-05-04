FROM golang:1.23-alpine AS build
WORKDIR /app
ENV GOTOOLCHAIN=auto
COPY . .
RUN CGO_ENABLED=0 go build -o worker cmd/worker/main.go

FROM alpine
RUN apk add --no-cache ca-certificates docker-cli
COPY --from=build /app/worker /worker
ENTRYPOINT ["/worker"]
