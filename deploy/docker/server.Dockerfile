FROM golang:1.23-alpine AS build
WORKDIR /app
RUN apk add --no-cache gcc musl-dev
ENV CGO_ENABLED=1 GOTOOLCHAIN=auto
COPY . .
RUN go build -o scheduler ./cmd/server/

FROM alpine
RUN apk add --no-cache ca-certificates
COPY --from=build /app/configs /configs
COPY --from=build /app/scheduler /scheduler
ENTRYPOINT ["/scheduler"]
CMD ["-config", "/configs/scheduler.yaml"]
