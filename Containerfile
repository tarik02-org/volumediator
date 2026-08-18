FROM golang:1.26-bookworm AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /volumediator ./cmd/volumediator

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates e2fsprogs mount \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /volumediator /usr/local/bin/volumediator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/volumediator"]
