# Distroless, non-root, static. The service needs no credentials of any kind and
# no shell; keeping it that way is a property worth preserving.
FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies in their own layer so source edits do not re-download them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO off gives a static binary with no libc to keep patched. Trimpath and the
# stripped symbol table keep the image small and the build reproducible.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath -ldflags="-s -w" \
      -o /out/router ./cmd/router

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/router /router

# The cursor is the only thing written at runtime, and it lives on a mounted
# volume — so the root filesystem can stay read-only.
VOLUME ["/var/lib/jetstream-router"]

USER nonroot:nonroot
EXPOSE 9090
ENTRYPOINT ["/router"]
