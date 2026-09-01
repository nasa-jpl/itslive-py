#!/usr/bin/env python
"""Diagnostic stats for the ITS_LIVE geoparquet warehouse.

Queries the partitioned STAC geoparquet files directly with duckdb (no
Iceberg/SQLite catalog) and prints aggregate statistics useful for
understanding when/where items were lost relative to the live pgstac catalog.

Without ``--bbox`` it scans the whole catalog; with ``--bbox`` it prunes to
the overlapping H3 tiles first.

Usage:
    python scripts/catalog_stats.py                       # whole catalog
    python scripts/catalog_stats.py --compare-pgstac      # per (year, platform) delta vs pgstac
    python scripts/catalog_stats.py --bbox 74.5,36.3,74.7,36.5
    python scripts/catalog_stats.py --json
"""

import argparse
import json
import logging
import sys
import time

DEFAULT_WAREHOUSE = "s3://its-live-data/test-space/stac/catalog/warehouse"
GLOB = "grid=h3/level=1/**/*.parquet"
STAC_API = "https://stac.itslive.cloud"


def _duckdb_connection():
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial")
    con.execute("LOAD spatial")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("SET preserve_insertion_order=false")
    return con


def _polygon(bbox: list[float]) -> dict:
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [bbox[0], bbox[1]],
                [bbox[2], bbox[1]],
                [bbox[2], bbox[3]],
                [bbox[0], bbox[3]],
                [bbox[0], bbox[1]],
            ]
        ],
    }


def _parquet_sources(warehouse: str, bbox: list[float] | None) -> tuple[list[str], str]:
    """Return (list of parquet globs, geometry WHERE clause).

    Whole catalog: a single glob and no spatial filter. With a bbox, prune to
    the overlapping H3 tiles first (like the search path) so the scan does not
    read the entire warehouse.
    """
    glob = f"{warehouse}/{GLOB}"
    if not bbox:
        return [glob], ""

    import json as _json

    from itslive._search import _sql_quote, get_overlapping_grid_names

    poly = _polygon(bbox)
    tiles = get_overlapping_grid_names(
        geojson_geometry=poly,
        base_href=warehouse,
        partition_type="h3",
        resolution=1,
        use_hive_partitions=True,
    )
    globs = [f"{t}/**/*.parquet" for t in tiles]
    where = (
        "WHERE ST_Intersects(geometry, "
        f"ST_GeomFromGeoJSON('{_sql_quote(_json.dumps(poly))}'))"
    )
    return globs, where


def _from_clause(globs: list[str], filename: bool) -> str:
    from itslive._search import _sql_quote

    glob_list = ", ".join(f"'{_sql_quote(g)}'" for g in globs)
    return f"FROM read_parquet([{glob_list}], union_by_name=true" + (
        ", filename=true)" if filename else ")"
    )


def _query(
    con, globs: list[str], select: str, where: str, group_by: str, order_by: str
):
    """Run an aggregate query over the parquet globs and return list of dicts."""
    sql = (
        f"SELECT {select} {_from_clause(globs, 'filename' in select)} "
        f"{where} {group_by} {order_by}"
    )
    reader = con.execute(sql).arrow()
    out = []
    if reader is None:
        return out
    for rb in reader:
        names = rb.schema.names
        cols = {name: rb.column(name).to_pylist() for name in names}
        n = len(cols[names[0]])
        out.extend({name: cols[name][i] for name in names} for i in range(n))
    return out


def stats(warehouse: str, bbox: list[float] | None, approx: bool) -> dict:
    con = _duckdb_connection()
    globs, where = _parquet_sources(warehouse, bbox)
    distinct_expr = "approx_count_distinct(id)" if approx else "count(DISTINCT id)"

    t0 = time.perf_counter()
    row = con.execute(
        f"""
        SELECT
            count(*)                     AS total_rows,
            {distinct_expr}              AS distinct_items,
            count(DISTINCT filename)     AS total_files,
            CAST(min(created) AS VARCHAR)  AS created_min,
            CAST(max(created) AS VARCHAR)  AS created_max,
            CAST(min(updated) AS VARCHAR)  AS updated_min,
            CAST(max(updated) AS VARCHAR)  AS updated_max
        {_from_clause(globs, filename=True)}
        {where}
        """
    ).fetchone()
    if row is None:
        raise RuntimeError("empty summary query result")
    summary = {
        "total_rows": row[0],
        "distinct_items": row[1],
        "total_files": row[2],
        "created_min": row[3],
        "created_max": row[4],
        "updated_min": row[5],
        "updated_max": row[6],
    }
    summary["approx"] = approx
    summary["summary_seconds"] = round(time.perf_counter() - t0, 1)

    by_year = _query(
        con,
        globs,
        select=(
            "CAST(regexp_extract(filename, 'year=([0-9]+)', 1) AS INTEGER) AS year, "
            "count(*) AS rows, count(DISTINCT id) AS items, "
            "count(DISTINCT filename) AS files, "
            "CAST(min(created) AS VARCHAR) AS created_min, "
            "CAST(max(created) AS VARCHAR) AS created_max"
        ),
        where=where,
        group_by="GROUP BY 1",
        order_by="ORDER BY 1",
    )

    by_platform = _query(
        con,
        globs,
        select="platform, count(*) AS rows, count(DISTINCT id) AS items",
        where=where,
        group_by="GROUP BY 1",
        order_by="ORDER BY items DESC",
    )

    by_version = _query(
        con,
        globs,
        select="version, count(*) AS rows, count(DISTINCT id) AS items",
        where=where,
        group_by="GROUP BY 1",
        order_by="ORDER BY items DESC",
    )

    by_year_platform = _query(
        con,
        globs,
        select=(
            "CAST(regexp_extract(filename, 'year=([0-9]+)', 1) AS INTEGER) AS year, "
            "platform, count(DISTINCT id) AS items"
        ),
        where=where,
        group_by="GROUP BY 1, 2",
        order_by="ORDER BY year, items DESC",
    )

    created_monthly = _query(
        con,
        globs,
        select="strftime(CAST(created AS TIMESTAMP), '%Y-%m') AS month, count(DISTINCT id) AS items",
        where=where,
        group_by="GROUP BY 1",
        order_by="ORDER BY 1",
    )

    by_tile = _query(
        con,
        globs,
        select=(
            "regexp_extract(filename, 'tile=([^/]+)', 1) AS tile, "
            "count(DISTINCT id) AS items, count(*) AS rows"
        ),
        where=where,
        group_by="GROUP BY 1",
        order_by="ORDER BY items DESC",
    )

    return {
        "warehouse": warehouse,
        "bbox": bbox,
        "summary": summary,
        "by_year": by_year,
        "by_platform": by_platform,
        "by_version": by_version,
        "by_year_platform": by_year_platform,
        "created_monthly": created_monthly,
        "by_tile": by_tile,
    }


def pgstac_compare(bbox: list[float], by_year_platform: list[dict]) -> list[dict]:
    """Per (year, platform) item counts from the live pgstac catalog.

    Discovers platforms dynamically by iterating pgstac items for the region
    in a single pass (no hard-coded platform list, so platforms entirely
    absent from the geoparquet are still compared). The ``year`` is the item's
    midpoint ``datetime`` year, matching the geoparquet ``year`` partition.
    """
    import pystac_client

    client = pystac_client.Client.open(STAC_API)
    geo = _polygon(bbox)

    counts: dict[tuple[int, str], int] = {}
    search = client.search(
        intersects=geo,
        collections=["itslive-granules"],
        limit=10000,
    )
    for item in search.items():
        dt = item.properties.get("datetime")
        plat = item.properties.get("platform")
        if dt is None or plat is None:
            continue
        year = int(str(dt)[:4])
        key = (year, plat)
        counts[key] = counts.get(key, 0) + 1

    return [
        {"year": year, "platform": plat, "pgstac_items": n}
        for (year, plat), n in sorted(counts.items())
    ]


def _print_table(headers: list[str], rows: list[list], width: int = 22) -> None:
    header = "  " + " ".join(f"{h:>{width}}" for h in headers)
    print(header)
    print("  " + "-" * (len(header) - 2))
    for r in rows:
        print("  " + " ".join(f"{str(v):>{width}}" for v in r))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stream diagnostic stats from the ITS_LIVE geoparquet warehouse (whole catalog by default)."
    )
    parser.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    parser.add_argument(
        "--approx",
        action="store_true",
        help="Use approx_count_distinct for the global distinct count.",
    )
    parser.add_argument(
        "--bbox",
        type=str,
        default=None,
        help="Comma-separated min_lon,min_lat,max_lon,max_lat to restrict the scan.",
    )
    parser.add_argument(
        "--compare-pgstac",
        action="store_true",
        help="Also query the live pgstac catalog per (year, platform).",
    )
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--top-tiles", type=int, default=10)
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    bbox = [float(x) for x in args.bbox.split(",")] if args.bbox else None

    if args.compare_pgstac and bbox is None:
        print(
            "ERROR: --compare-pgstac requires a --bbox region (pgstac has no "
            "whole-catalog distinct/aggregate endpoint to stream cheaply).",
            file=sys.stderr,
        )
        return 1

    s = stats(args.warehouse, bbox, args.approx)

    if args.compare_pgstac:
        assert bbox is not None
        s["pgstac"] = pgstac_compare(bbox, s["by_year_platform"])

    if args.json:
        print(json.dumps(s, indent=2, default=str))
        return 0

    line = "=" * 72
    sm = s["summary"]
    print(f"\n{line}")
    print("  Warehouse summary (from parquet files, duckdb)")
    print(f"{line}")
    print(f"  Warehouse     : {s['warehouse']}")
    print(f"  Region        : {s['bbox'] or 'global'}")
    print(f"  Distinct mode : {'approx (HLL)' if sm['approx'] else 'exact'}")
    print(f"\n  UNIQUE ITEMS  : {sm['distinct_items']:,}")
    print(f"  Total rows    : {sm['total_rows']:,}")
    print(f"  Total files   : {sm['total_files']:,}")
    print(f"  created range : {sm['created_min']}  ..  {sm['created_max']}")
    print(f"  updated range : {sm['updated_min']}  ..  {sm['updated_max']}")
    print(f"  scan time     : {sm['summary_seconds']}s")

    if s["by_platform"]:
        print(f"\n{line}\n  By platform\n{line}")
        _print_table(
            ["platform", "rows", "items"],
            [[r["platform"], r["rows"], r["items"]] for r in s["by_platform"]],
        )

    if s["by_version"]:
        print(f"\n{line}\n  By version\n{line}")
        _print_table(
            ["version", "rows", "items"],
            [[r["version"], r["rows"], r["items"]] for r in s["by_version"]],
        )

    if s["by_year"]:
        print(f"\n{line}\n  By year\n{line}")
        _print_table(
            ["year", "rows", "items", "files", "created_min", "created_max"],
            [
                [
                    r["year"],
                    r["rows"],
                    r["items"],
                    r["files"],
                    (r["created_min"] or "")[:10],
                    (r["created_max"] or "")[:10],
                ]
                for r in s["by_year"]
            ],
            width=18,
        )

    if s["by_year_platform"]:
        print(f"\n{line}\n  Distinct items by (year, platform)\n{line}")
        _print_table(
            ["year", "platform", "items"],
            [[r["year"], r["platform"], r["items"]] for r in s["by_year_platform"]],
            width=14,
        )

    if args.compare_pgstac and s.get("pgstac"):
        print(f"\n{line}\n  Delta vs pgstac (year, platform)\n{line}")
        geo_map = {
            (r["year"], r["platform"]): r["items"] for r in s["by_year_platform"]
        }
        for r in s["pgstac"]:
            g = geo_map.get((r["year"], r["platform"]), 0)
            diff = r["pgstac_items"] - g
            flag = "  <-- missing" if diff > 0 else ""
            print(
                f"  {r['year']} {r['platform']:<5s} geoparquet={g:>9,} pgstac={r['pgstac_items']:>9,} "
                f"delta={diff:>+9,}{flag}"
            )

    if s["created_monthly"]:
        print(f"\n{line}\n  Distinct items created per month (ingest timeline)\n{line}")
        _print_table(
            ["month", "items"],
            [[r["month"], r["items"]] for r in s["created_monthly"]],
            width=14,
        )

    if s["by_tile"]:
        print(f"\n{line}\n  Top {args.top_tiles} tiles by distinct items\n{line}")
        _print_table(
            ["tile", "items", "rows"],
            [
                [r["tile"], r["items"], r["rows"]]
                for r in s["by_tile"][: args.top_tiles]
            ],
            width=16,
        )

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
