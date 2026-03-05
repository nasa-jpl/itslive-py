import csv
import datetime
import json
import sys

import rich_click as click
from rich import print as rprint

from itslive.search import EQ, GT, GTE, LT, LTE, NEQ

# Use Rich markup
click.rich_click.USE_RICH_MARKUP = True


class Mutex(click.Option):
    # Taken from https://bit.ly/3hWeXVj
    def __init__(self, *args, **kwargs):
        self.not_required_if: list = kwargs.pop("not_required_if")

        assert self.not_required_if, "'not_required_if' parameter required"
        kwargs["help"] = (
            f"{kwargs.get('help', '')} Option is mutually exclusive with "
            f"{', '.join(self.not_required_if).strip()}."
        )
        super(Mutex, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        current_opt: bool = self.name in opts
        for mutex_opt in self.not_required_if:
            if mutex_opt in opts:
                if current_opt:
                    error_msg = (
                        f"Illegal usage: {self.name}"
                        f" is mutually exclusive with {str(mutex_opt)}"
                    )
                    raise click.UsageError(error_msg)
                else:
                    self.prompt = None
        return super(Mutex, self).handle_parse_result(ctx, opts, args)


def validate_bbox(ctx, param, value):
    if value:
        try:
            parts = [float(x) for x in value.split(",")]
            if len(parts) != 4:
                raise ValueError
            if not (
                -180 <= parts[0] <= 180
                and -90 <= parts[1] <= 90
                and -180 <= parts[2] <= 180
                and -90 <= parts[3] <= 90
            ):
                raise ValueError
            return parts
        except ValueError:
            raise click.BadParameter(
                "bbox must be 'min_lon,min_lat,max_lon,max_lat' e.g., -50,65,-40,75"
            )
    return value


def validate_polygon(ctx, param, value):
    if value:
        try:
            parts = [float(x) for x in value.split(",")]
            if len(parts) < 6 or len(parts) % 2 != 0:
                raise ValueError
            return parts
        except ValueError:
            raise click.BadParameter(
                "polygon must be comma-separated lon,lat pairs "
                "e.g., lon1,lat1,lon2,lat2,lon3,lat3,lon1,lat1"
            )
    return value


def validate_date(ctx, param, value):
    if value:
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            raise click.BadParameter(
                "date must be in YYYY-MM-DD format, e.g., 2020-01-01"
            )
    return value


def validate_filter(ctx, param, value):
    """
    Parse filter string(s) in format 'property:operator:value'.

    The operator is used as the delimiter, so property names that contain
    colons (e.g. 'proj:code') are handled correctly.

    When used with multiple=True, this receives a tuple of filter strings.

    Examples:
        - platform:=:S2               # platform equals S2
        - percent_valid_pixels:>=:50  # percent_valid_pixels greater or equal 50
        - created:>=:2020-01-01       # created on or after date
        - version:!=:002              # version not equal to 002
        - proj:code:=:EPSG:32717      # namespaced property with colon in value

    Operators: = (equals), >= (greater or equal), <= (less or equal),
               > (greater), < (less), != (not equal)

    Returns:
        List of tuples: [(property_name, PropertyFilter), ...]
    """
    if not value:
        return None

    import re

    # Match the first occurrence of a known operator token surrounded (or
    # preceded) by colons: :>=:  :<=:  :!=:  :=:  :>:  :<:
    _OP_RE = re.compile(r":(>=|<=|!=|=|>|<):")

    result = []

    for filter_str in value:
        try:
            m = _OP_RE.search(filter_str)
            if not m:
                raise ValueError(f"Invalid filter format in '{filter_str}'")

            prop_name = filter_str[: m.start()]
            op = m.group(1)
            value_str = filter_str[m.end() :]

            # Map operator strings to filter helpers
            op_map = {
                "=": EQ,
                ">=": GTE,
                "<=": LTE,
                ">": GT,
                "<": LT,
                "!=": NEQ,
            }

            if op not in op_map:
                raise ValueError(f"Invalid operator: {op} in filter '{filter_str}'")

            # Parse value (try int, then float, then string)
            try:
                parsed_value = int(value_str)
            except ValueError:
                try:
                    parsed_value = float(value_str)
                except ValueError:
                    # Keep as string
                    parsed_value = value_str

            result.append((prop_name, op_map[op](parsed_value)))
        except ValueError as e:
            raise click.BadParameter(
                f"{str(e)}. "
                "Expected format: property:operator:value "
                "(e.g., platform:=:S2 or percent_valid_pixels:>:50). "
                "Valid operators: =, >=, <=, >, <, !="
            )

    return result


@click.command()
@click.option(
    "--bbox",
    cls=Mutex,
    not_required_if=["polygon"],
    callback=validate_bbox,
    help=(
        "Bounding box as 'min_lon,min_lat,max_lon,max_lat'. "
        "[dim]Example: -50,65,-40,75[/]"
    ),
)
@click.option(
    "--polygon",
    cls=Mutex,
    not_required_if=["bbox"],
    callback=validate_polygon,
    help=(
        "Polygon as comma-separated lon,lat pairs. "
        "[dim]Example: lon1,lat1,lon2,lat2,lon3,lat3,lon1,lat1[/]"
    ),
)
@click.option(
    "--engine",
    type=click.Choice(["stac", "duckdb", "rustac"], case_sensitive=False),
    default="stac",
    help=(
        "Search engine backend. "
        "[dim]stac: STAC API (default) - uses https://stac.its-live.org[/] "
        "[dim]duckdb/rustac: Geoparquet with S3 - must specify --base-catalog-href[/]"
    ),
)
@click.option(
    "--collection",
    default="itslive-granules",
    help="STAC collection name [dim](default: itslive-granules)[/]",
)
@click.option(
    "--percent-valid-pixels",
    type=int,
    default=1,
    help="Minimum percent of valid pixels [dim](default: 1)[/]",
)
@click.option(
    "--mission",
    type=str,
    help="Filter by satellite mission [dim](e.g., landsatOLI, sentinel1, sentinel2)[/]",
)
@click.option(
    "--start",
    callback=validate_date,
    help="Start date in YYYY-MM-DD format",
)
@click.option(
    "--end",
    callback=validate_date,
    help="End date in YYYY-MM-DD format",
)
@click.option(
    "--min-interval",
    type=int,
    help="Minimum time interval in days",
)
@click.option(
    "--max-interval",
    type=int,
    help="Maximum time interval in days",
)
@click.option(
    "--base-catalog-href",
    type=str,
    help=(
        "Explicit geoparquet catalog path (optional for duckdb/rustac). "
        "[dim]If not specified, uses default H3 paths:[/] "
        "[dim]h3r2: s3://its-live-data/test-space/stac/geoparquet/h3r2 (default)[/] "
        "[dim]h3r1: s3://its-live-data/test-space/stac/geoparquet/h3r1[/]"
    ),
)
@click.option(
    "--partition-type",
    type=click.Choice(["h3", "latlon"], case_sensitive=False),
    default="h3",
    show_default=True,
    help=(
        "Geoparquet spatial partitioning scheme [dim](required for duckdb/rustac)[/] "
        "[dim]h3: H3 hexagonal grid - specify --resolution (r1 or r2)[/] "
        "[dim]latlon: Lat/Lon geographic grid[/]"
    ),
)
@click.option(
    "--resolution",
    type=click.Choice([1, 2], case_sensitive=False),
    default=1,
    show_default=True,
    help="H3 hexagonal resolution [dim]default: 1 (finer), 2 (coarser)[/]. "
    "[dim]Only used with --partition-type h3[/]",
)
@click.option(
    "--overlap",
    type=str,
    default="bbox_overlap",
    show_default=True,
    help="H3 cell overlap mode [dim]options: bbox_overlap, center, edge[/]",
)
@click.option(
    "--reduce-spatial-search/--no-reduce-spatial-search",
    default=True,
    show_default=True,
    help="Pre-filter geoparquet files to overlapping spatial partitions [dim](speeds up large queries)[/]",
)
@click.option(
    "--use-hive-partitions",
    is_flag=True,
    help="Use Hive-style partition paths instead of integer prefixes",
)
@click.option(
    "--filter",
    "-f",
    "filters",
    multiple=True,
    callback=validate_filter,
    help=(
        "Property filter in format 'property:operator:value'. "
        "Can be used multiple times. "
        "[dim]Examples: -f platform:=:S2 -f percent_valid_pixels:>:50[/]"
    ),
)
@click.option(
    "--format",
    type=click.Choice(["url", "json", "csv"], case_sensitive=False),
    default="url",
    help="Output format [dim]url: one URL per line (default), json: JSON array, csv: CSV with metadata[/]",
)
@click.option(
    "--count-only",
    is_flag=True,
    help="Only print the count of matching URLs, don't output URLs",
)
@click.option(
    "--quiet",
    is_flag=True,
    default=True,
    help="Suppress progress messages to stderr",
)
def search(
    bbox,
    polygon,
    engine,
    collection,
    percent_valid_pixels,
    mission,
    start,
    end,
    min_interval,
    max_interval,
    base_catalog_href,
    partition_type,
    resolution,
    overlap,
    reduce_spatial_search,
    use_hive_partitions,
    filters,
    format,
    count_only,
    quiet,
):
    """
    Search for ITS_LIVE velocity granules and stream URLs to stdout.
    
    This command is optimized for large result sets (e.g., 1M+ URLs) by
    streaming results to stdout one at a time, avoiding loading everything into memory.
    
    [bold]Examples:[/]
    
      [dim]# Example 1: Search using STAC API (default)[/]
      $ itslive-search --bbox -50,65,-40,75 --start 2020-01-01 --end 2022-12-31 > urls.txt
    
      [dim]# Example 2: Search using geoparquet with H3 hexagonal partitioning (resolution 2)[/]
      $ itslive-search --bbox -50,65,-40,75 --engine duckdb \\
          --partition-type h3 --resolution 2 > urls.txt
    
      [dim]# Example 3: Search using geoparquet with H3 resolution 3 (finer grid)[/]
      $ itslive-search --bbox -50,65,-40,75 --engine rustac \\
          --partition-type h3 --resolution 3 > urls.txt
    
      [dim]# Example 4: Search using geoparquet with lat/lon geographic partitioning[/]
      $ itslive-search --bbox -50,65,-40,75 --engine duckdb \\
          --partition-type latlon --base-catalog-href \\
          s3://its-live-data/test-space/stac/geoparquet/latlon > urls.txt
    
      [dim]# Example 5: Search with filters[/]
      $ itslive-search --bbox -50,65,-40,75 --mission sentinel1 \\
          --min-interval 12 --max-interval 36
    
      [dim]# Example 6: Count results only[/]
      $ itslive-search --bbox -50,65,-40,75 --count-only
    """
    import itslive

    # Build custom filters dict from --filter options
    custom_filters = {}
    if filters:
        for prop_name, prop_filter in filters:
            custom_filters[prop_name] = prop_filter

    # Prepare kwargs for search
    stac_kwargs = {
        "collection": collection,
        "partition_type": partition_type,
        "resolution": resolution,
        "overlap": overlap,
        "reduce_spatial_search": reduce_spatial_search,
        "use_hive_partitions": use_hive_partitions,
        "filters": custom_filters if custom_filters else None,
    }

    if base_catalog_href:
        stac_kwargs["base_catalog_href"] = base_catalog_href

    # Validate required parameters
    if not bbox and not polygon:
        rprint("[red]Error: Either --bbox or --polygon is required[/]")
        sys.exit(1)

    # Build geometry parameter
    if polygon:
        geometry_arg = polygon
        bbox_arg = None
    else:
        geometry_arg = None
        bbox_arg = bbox

    # Perform streaming search
    url_generator = itslive.velocity_pairs.find_streaming(
        bbox=bbox_arg,
        polygon=geometry_arg,
        percent_valid_pixels=percent_valid_pixels,
        mission=mission,
        start=start,
        end=end,
        min_interval=min_interval,
        max_interval=max_interval,
        engine=engine,
        **stac_kwargs,
    )

    # Stream results to stdout
    if not quiet:
        rprint(f"[dim]Using {engine.upper()} engine[/]")

    if format == "json":
        urls_list = []
        for url in url_generator:
            urls_list.append(url)

        if count_only:
            print(len(urls_list))
        else:
            print(json.dumps(urls_list, indent=2))

    elif format == "csv":
        writer = None
        for i, url in enumerate(url_generator):
            if count_only:
                pass
            else:
                # Extract filename from URL
                filename = url.split("/")[-1]

                if writer is None:
                    writer = csv.writer(sys.stdout)
                    writer.writerow(["url", "filename"])

                writer.writerow([url, filename])

        if count_only and url_generator:
            print(i + 1)

    else:  # url format (default)
        count = 0
        for url in url_generator:
            count += 1
            if not count_only:
                print(url)

        if not quiet:
            rprint(f"[green]Total URLs: {count}[/]")
