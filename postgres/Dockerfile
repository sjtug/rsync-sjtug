FROM postgres:15-alpine AS pg_builder

ARG pg_cron_version=1.5.2

RUN apk add --no-cache --virtual .build-deps \
        build-base \
        clang15-dev \
        llvm15-dev \
        make \
        wget
#        postgresql-dev \

RUN wget -O pg_cron.tar.gz https://github.com/citusdata/pg_cron/archive/refs/tags/v${pg_cron_version}.tar.gz \
    && mkdir -p /usr/src/pg_cron \
    && tar -xzf pg_cron.tar.gz -C /usr/src/pg_cron --strip-components=1 \
    && rm pg_cron.tar.gz

RUN cd /usr/src/pg_cron \
    && make \
    && make install

FROM rust:1.71-alpine AS rs_builder

RUN apk add --no-cache --virtual .build-deps build-base

COPY oneshot-migration /usr/src/oneshot-migration

COPY migrations /usr/src/migrations

RUN cd /usr/src/oneshot-migration && cargo build --release

FROM postgres:15-alpine

COPY --from=pg_builder /usr/local/lib/postgresql/pg_cron.so /usr/local/lib/postgresql/

COPY --from=pg_builder /usr/local/lib/postgresql/bitcode/pg_cron.index.bc /usr/local/lib/postgresql/bitcode/

COPY --from=pg_builder /usr/local/lib/postgresql/bitcode/pg_cron /usr/local/lib/postgresql/bitcode/pg_cron/

COPY --from=pg_builder /usr/local/share/postgresql/extension/pg_cron* /usr/local/share/postgresql/extension/

COPY --from=rs_builder /usr/src/oneshot-migration/target/release/oneshot-migration /usr/local/bin/oneshot-migration

COPY ./setup.sh /docker-entrypoint-initdb.d/setup.sh

ENV POSTGRES_USER=rsync-sjtug

ENV POSTGRES_DB=rsync-sjtug

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]