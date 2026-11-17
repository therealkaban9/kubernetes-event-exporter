FROM --platform=$BUILDPLATFORM golang:1.25.3-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/event-exporter ./cmd/event-exporter

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/therealkaban9/kubernetes-event-exporter" \
      org.opencontainers.image.version="0.1.0"
COPY --from=build /out/event-exporter /event-exporter
USER 65532:65532
EXPOSE 2112
ENTRYPOINT ["/event-exporter"]
