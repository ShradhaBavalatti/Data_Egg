from __future__ import annotations

import io
import sys

import pandas as pd
from psycopg2 import sql

import config
from streaming_utils import get_logger, pg_connect, trip_business_key

log = get_logger("load_source")

COLMAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "ratecode_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
    "airport_fee": "airport_fee",
}

COPY_COLUMNS = [
    "trip_key",
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]


def ensure_database():
    conn = pg_connect("postgres")
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s",
                (config.PG["dbname"],),
            )

            if cur.fetchone() is None:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(config.PG["dbname"])
                    )
                )
                log.info("Created database %s", config.PG["dbname"])
            else:
                log.info("Database %s already exists", config.PG["dbname"])
    finally:
        conn.close()


def apply_setup_sql():
    conn = pg_connect()
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            cur.execute((config.BASE_DIR / "setup.sql").read_text())
    finally:
        conn.close()

    log.info("Applied setup.sql")


def load_zones():
    df = pd.read_csv(config.ZONE_LOOKUP_CSV)

    df.columns = df.columns.str.strip()

    df = df.rename(
        columns={
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
        }
    )

    df = df[
        [
            "location_id",
            "borough",
            "zone",
            "service_zone",
        ]
    ]

    buf = io.StringIO()
    df.to_csv(
        buf,
        index=False,
        header=False,
        na_rep="",
    )
    buf.seek(0)

    conn = pg_connect()

    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {config.ZONE_TABLE}")

            cur.copy_expert(
                f"COPY {config.ZONE_TABLE} "
                "(location_id,borough,zone,service_zone) "
                "FROM STDIN WITH (FORMAT csv, NULL '')",
                buf,
            )

        conn.commit()

    finally:
        conn.close()

    return len(df)


def load_trips():
    df = (
        pd.read_parquet(config.RAW_PARQUET)
        .head(config.SOURCE_ROW_LIMIT)
        .rename(columns=COLMAP)
    )

    ints = [
        "vendor_id",
        "passenger_count",
        "ratecode_id",
        "pu_location_id",
        "do_location_id",
        "payment_type",
    ]

    for c in ints:
        df[c] = pd.to_numeric(
            df[c],
            errors="coerce",
        ).astype("Int64")

    df["trip_key"] = [
        trip_business_key(
            *r
        )
        for r in zip(
            df.vendor_id,
            df.tpep_pickup_datetime,
            df.pu_location_id,
            df.do_location_id,
            df.total_amount,
            df.fare_amount,
        )
    ]

    df = df[COPY_COLUMNS]

    buf = io.StringIO()

    df.to_csv(
        buf,
        index=False,
        header=False,
        na_rep="",
    )

    buf.seek(0)

    conn = pg_connect()

    try:
        with conn.cursor() as cur:
            cur.execute(
                f"TRUNCATE {config.SRC_TABLE} RESTART IDENTITY"
            )

            cur.copy_expert(
                f"COPY {config.SRC_TABLE} "
                f"({', '.join(COPY_COLUMNS)}) "
                "FROM STDIN WITH (FORMAT csv, NULL '')",
                buf,
            )

            cur.execute(
                f"SELECT count(*) FROM {config.SRC_TABLE}"
            )

            n = cur.fetchone()[0]

        conn.commit()

    finally:
        conn.close()

    log.info("Loaded %s trips", n)

    return n


def main():
    ensure_database()
    apply_setup_sql()

    z = load_zones()
    n = load_trips()

    log.info(
        "SOURCE READY — %s trips + %s zones",
        n,
        z,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())