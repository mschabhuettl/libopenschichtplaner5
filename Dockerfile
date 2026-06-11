# ==============================================================================
# libopenschichtplaner5 — CLI-/Tools-Image + Build-/Test-Stage
#
# Die Library wird als pip-Paket konsumiert; standalone nutzbar ist sie über
# das sp5lib-CLI (Default-Stage "cli", slim, non-root):
#
#   docker build -t libopenschichtplaner5 .
#   docker run --rm -v /pfad/zu/SP5/Daten:/data:ro libopenschichtplaner5 info /data
#   docker run --rm -v /pfad/zu/SP5/Daten:/data:ro libopenschichtplaner5 dump /data 5EMPL --limit 5
#   docker run --rm -v /pfad/zu/SP5/Daten:/data:ro libopenschichtplaner5 validate /data
#   docker run --rm -v /pfad/zu/SP5/Daten:/data -v /pfad/out:/out \
#     libopenschichtplaner5 sync /data --target sqlite:/out/sp5.db
#
# Stage "test" lässt Lint + Testsuite reproduzierbar im Container laufen:
#
#   docker compose run --rm test        # ruff + pytest
#   docker build --target test -t libopenschichtplaner5:test .
# ==============================================================================

FROM python:3.12-slim AS base
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /lib

# Paket + Dev-/Postgres-Extras installieren (Metadaten zuerst für Layer-Cache)
COPY pyproject.toml README.md LICENSE ./
COPY sp5lib/ sp5lib/
RUN pip install --no-cache-dir -e ".[dev,postgres]"

# ── Stage test: ruff + pytest ─────────────────────────────────────────────────
FROM base AS test
COPY tests/ tests/
CMD ["sh", "-c", "ruff check . && pytest -v"]

# ── Stage cli (Default): sp5lib-CLI, slim + non-root ──────────────────────────
FROM python:3.12-slim AS cli
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY pyproject.toml README.md LICENSE /src/
COPY sp5lib/ /src/sp5lib/
RUN pip install --no-cache-dir "/src[postgres]" && rm -rf /src

RUN useradd --uid 1001 --create-home --shell /usr/sbin/nologin sp5
USER sp5
WORKDIR /data

ENTRYPOINT ["sp5lib"]
CMD ["--help"]
