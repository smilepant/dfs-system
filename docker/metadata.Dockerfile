FROM alpine:latest

RUN apk --no-cache add ca-certificates wget

WORKDIR /app

# Copy the pre-built Linux binary directly from the docker context (dfs-system/)
COPY docker/metadata-server-linux ./metadata-server
COPY configs/ /app/configs/
COPY web/     /app/web/

EXPOSE 8000
CMD ["./metadata-server", "-config", "configs/config.yaml"]
