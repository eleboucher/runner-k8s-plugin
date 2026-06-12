FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=docker
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -trimpath \
    -o /forgejo-runner-k8s \
    .

FROM alpine:3.23

ARG VERSION=dev
ARG REVISION=unknown
ARG CREATED=
LABEL org.opencontainers.image.title="runner-k8s-plugin" \
      org.opencontainers.image.description="Forgejo Runner backend plugin that executes CI/CD jobs as Kubernetes pods" \
      org.opencontainers.image.source="https://git.erwanleboucher.dev/eleboucher/runner-k8s-plugin" \
      org.opencontainers.image.url="https://git.erwanleboucher.dev/eleboucher/runner-k8s-plugin" \
      org.opencontainers.image.documentation="https://git.erwanleboucher.dev/eleboucher/runner-k8s-plugin/src/branch/main/README.md" \
      org.opencontainers.image.licenses="GPL-3.0-or-later" \
      org.opencontainers.image.authors="Erwan Leboucher <erwanleboucher@gmail.com>" \
      org.opencontainers.image.vendor="Erwan Leboucher" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}"

RUN apk add --no-cache ca-certificates

COPY --from=builder /forgejo-runner-k8s /usr/local/bin/forgejo-runner-k8s

USER 1000:1000

ENTRYPOINT ["/usr/local/bin/forgejo-runner-k8s"]
CMD ["--listen", ":9090"]
