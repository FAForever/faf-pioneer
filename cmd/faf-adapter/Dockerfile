FROM golang:1.24-alpine AS builder
WORKDIR /build

ARG TARGET_OS
ARG TARGET_ARCH
ENV CGO_ENABLED=0
ENV GOOS=${TARGET_OS}
ENV GOARCH=${TARGET_ARCH}

COPY . /build/

RUN go build -o pioneer ./cmd/faf-adapter

FROM scratch AS app
COPY --from=builder /build/pioneer /pioneer
