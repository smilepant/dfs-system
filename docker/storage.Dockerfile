FROM alpine:latest

RUN apk --no-cache add ca-certificates wget

WORKDIR /app

# Copy the pre-built Linux binary directly from the docker context (dfs-system/)
COPY docker/storage-node-linux ./storage-node
COPY configs/ /app/configs/

ENV RUNNING_IN_DOCKER=true
EXPOSE 8081
CMD ["./storage-node", "-config", "configs/config.yaml"]
