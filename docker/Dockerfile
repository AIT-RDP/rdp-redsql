FROM ghcr.io/astral-sh/uv:python3.13-alpine

WORKDIR /usr/redsql
COPY pyproject.toml uv.lock ./

RUN --mount=type=secret,id=repo-env,target=/kaniko/.secret.env \
    ls -la /kaniko/ && export $(cat /kaniko/.secret.env | grep -v -e '^\s*#.*' | sed 's/\r//g' | xargs) && \
    touch README.md && uv sync --compile-bytecode --no-cache --no-managed-python && \
    addgroup -S redsql && adduser -g redsql -S -s /bin/sh redsql

COPY --chmod=755 redsql ./redsql
COPY --chmod=755 docker/etc/redsql /etc/redsql

USER redsql:redsql
ENV PATH="/usr/redsql/.venv/bin:$PATH"
CMD ["python", "-m", "redsql", "--config", "/etc/redsql/config.yml"]