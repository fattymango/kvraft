# app/ is a single self-contained module (the lab packages this needs live
# under internal/ as regular internal packages), so the build context is
# just app/ itself.
FROM golang:1.22-alpine AS build
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -o /out/kvnode ./cmd/kvnode && \
    CGO_ENABLED=0 go build -o /out/kvgateway ./cmd/kvgateway && \
    CGO_ENABLED=0 go build -o /out/shardnode ./cmd/shardnode && \
    CGO_ENABLED=0 go build -o /out/shardctrl ./cmd/shardctrl && \
    CGO_ENABLED=0 go build -o /out/shardgateway ./cmd/shardgateway

FROM scratch
COPY --from=build /out/ /
ENTRYPOINT ["/kvnode"]
