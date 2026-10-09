# Binaries are cross-compiled on the host (scripts/start.ps1 or scripts/start.sh),
# which is far faster than compiling Go inside the Docker Desktop VM.
FROM alpine:3.20
RUN apk --no-cache add ca-certificates wget
WORKDIR /app
COPY docker/storage-node-linux ./storage-node
COPY configs/ /app/configs/

ENV RUNNING_IN_DOCKER=true
EXPOSE 8081
CMD ["./storage-node", "-config", "configs/config.yaml"]
