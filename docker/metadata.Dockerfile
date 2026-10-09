# Binaries are cross-compiled on the host (scripts/start.ps1 or scripts/start.sh),
# which is far faster than compiling Go inside the Docker Desktop VM.
FROM alpine:3.20
RUN apk --no-cache add ca-certificates wget
WORKDIR /app
COPY docker/metadata-server-linux ./metadata-server
COPY configs/ /app/configs/
COPY web/     /app/web/

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s CMD wget -qO- http://localhost:8000/files >/dev/null || exit 1
CMD ["./metadata-server", "-config", "configs/config.yaml"]
