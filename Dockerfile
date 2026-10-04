# Builds kafka-mcp from source. Releases do not use this file: goreleaser
# packages its own binaries with ci/Dockerfile. This one is for builders that
# start from the repository, such as the Docker MCP Catalog.
ARG GO=golang:1.27-alpine
ARG ALPINE=alpine:3.24.2

FROM $GO AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /kafka-mcp ./cmd/server

FROM $ALPINE

RUN apk --no-cache --no-progress add tzdata ca-certificates

LABEL io.modelcontextprotocol.server.name="io.github.denizgursoy/kafka-mcp"

COPY --from=build /kafka-mcp /
ENTRYPOINT [ "/kafka-mcp" ]
CMD [ "--server" ]
