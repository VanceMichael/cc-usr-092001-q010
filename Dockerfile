FROM golang:1.24-alpine AS build
WORKDIR /src
COPY . .
RUN go test ./... && go build -o /service ./cmd/server

FROM alpine:3.22
WORKDIR /app
COPY --from=build /service /app/service
ENV PORT=8080
EXPOSE 8080
CMD ["/app/service"]
