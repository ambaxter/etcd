import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from polars.io.plugins import register_io_source
    from typing import Iterator
    import io
    import re
    import altair as alt

    return Iterator, alt, pl, re, register_io_source


@app.cell(hide_code=True)
def _(Iterator, pl, re, register_io_source):
    def my_scan_csv(csv_path: str, db_name: str) -> pl.LazyFrame:
        schema = {
            "commit": pl.String,
            "key_size": pl.UInt64,
            "key_space_size": pl.UInt64,
            "backend_size": pl.UInt64,
            "range_result_limit": pl.UInt64,
            "kv_type": pl.String,
            "cmp_oriole": pl.Int8,
            "cmp_wal" : pl.String,
            "init_ms" : pl.UInt64,
            "empty_compact_ms": pl.UInt64,
            "empty_defrag_ms": pl.UInt64,
            "init_db_size": pl.UInt64,
            "ratio": pl.String,
            "conn_size": pl.UInt64,
            "value_size" : pl.UInt64,
            "read_ops_per_s": pl.List(pl.Float32),
            "read_average_response_s": pl.List(pl.Float32),
            "read_slowest_response_s": pl.List(pl.Float32),
            "read_fastest_response_s": pl.List(pl.Float32),
            "read_stddev_response_s": pl.List(pl.Float32),
            "write_ops_per_s": pl.List(pl.Float32),
            "write_average_response_s": pl.List(pl.Float32),
            "write_slowest_response_s": pl.List(pl.Float32),
            "write_fastest_response_s": pl.List(pl.Float32),
            "write_stddev_response_s": pl.List(pl.Float32),
            "completed_db_size" : pl.UInt64,
            "final_compact_ms" : pl.UInt64,
            "final_defrag_ms" : pl.UInt64,
            "final_db_size" : pl.UInt64,
        }
        p = re.compile(r"(?:\[(.+?)\]|(.+?))(?:,|$)")
        def source_generator(
            with_columns: list[str] | None,
            predicate: pl.Expr | None,
            n_rows: int | None,
            batch_size: int | None,
        ) -> Iterator[pl.DataFrame]:
            """
            Generator function that creates the source.
            This function will be registered as IO source.
            """
            if batch_size is None:
                batch_size = 100

            # Initialize the reader.
            with open(csv_path, "r") as fin:
                _ = next(fin)
                # Ensure we don't read more rows than requested from the engine
                while n_rows is None or n_rows > 0:
                    if n_rows is not None:
                        batch_size = min(batch_size, n_rows)

                    rows = []

                    for _ in range(batch_size):
                        try:
                            file_row = next(fin)
                            file_row = file_row.replace("][","],[")
                            matches = p.findall(file_row)
                            row = {
                                "commit": matches[0][1],
                                "key_size": int(matches[1][1]),
                                "key_space_size": int(matches[2][1]),
                                "backend_size": int(matches[3][1]),
                                "range_result_limit": int(matches[4][1]),
                                "kv_type": matches[5][1],
                                "cmp_oriole": int(matches[6][1]),
                                "cmp_wal" : matches[7][1],
                                "init_ms" : int(matches[8][1]),
                                "empty_compact_ms": int(matches[9][1]),
                                "empty_defrag_ms": int(matches[10][1]),
                                "init_db_size": int(matches[11][1]),
                                "ratio": matches[12][1],
                                "conn_size": int(matches[13][1]),
                                "value_size" : int(matches[14][1]),
                                "read_ops_per_s": [float(x) for x in matches[15][0].split(",") if len(x) > 0],
                                "read_average_response_s": [float(x) for x in matches[16][0].split(",") if len(x) > 0],
                                "read_fastest_response_s": [float(x) for x in matches[18][0].split(",") if len(x) > 0],
                                "read_slowest_response_s": [float(x) for x in matches[17][0].split(",") if len(x) > 0],
                                "read_stddev_response_s": [float(x) for x in matches[19][0].split(",") if len(x) > 0],
                                "write_ops_per_s": [float(x) for x in matches[20][0].split(",") if len(x) > 0],
                                "write_average_response_s": [float(x) for x in matches[21][0].split(",") if len(x) > 0],
                                "write_slowest_response_s": [float(x) for x in matches[22][0].split(",") if len(x) > 0],
                                "write_fastest_response_s": [float(x) for x in matches[23][0].split(",") if len(x) > 0],
                                "write_stddev_response_s": [float(x) for x in matches[24][0].split(",") if len(x) > 0],
                                "completed_db_size" : int(matches[25][1]),
                                "final_compact_ms" : int(matches[26][1]),
                                "final_defrag_ms" : int(matches[27][1]),
                                "final_db_size" : int(matches[28][1]),
                            }
                        except StopIteration:
                            n_rows = 0
                            break
                        rows.append(row)

                    df = pl.from_records(rows, schema=schema, orient="row")
                    if n_rows is not None:
                        n_rows -= df.height

                    # If we would make a performant reader, we would not read these
                    # columns at all.
                    if with_columns is not None:
                        df = df.select(with_columns)

                    # If the source supports predicate pushdown, the expression can be parsed
                    # to skip rows/groups.
                    if predicate is not None:
                        df = df.filter(predicate)

                    yield df


        df = register_io_source(io_source=source_generator, schema=schema)
        return df.with_columns(
            pl.lit(db_name).alias("database"),
            pl.col("read_ops_per_s").list.mean().alias("read_ops_per_s_avg"),
            pl.col("read_average_response_s").list.mean().alias("read_average_response_s_avg"),
            pl.col("read_slowest_response_s").list.mean().alias("read_slowest_response_s_avg"),
            pl.col("read_fastest_response_s").list.mean().alias("read_fastest_response_s_avg"),
            pl.col("read_stddev_response_s").list.mean().alias("read_stddev_response_s_avg"),
            pl.col("write_ops_per_s").list.mean().alias("write_ops_per_s_avg"),
            pl.col("write_average_response_s").list.mean().alias("write_average_response_s_avg"),
            pl.col("write_slowest_response_s").list.mean().alias("write_slowest_response_s_avg"),
            pl.col("write_fastest_response_s").list.mean().alias("write_fastest_response_s_avg"),
            pl.col("write_stddev_response_s").list.mean().alias("write_stddev_response_s_avg"),
            pl.col("init_db_size").truediv(1024 * 1024).alias("init_db_size_mb"),
            pl.col("completed_db_size").truediv(1024 * 1024).alias("completed_db_size_mb"),
            pl.col("final_db_size").truediv(1024 * 1024).alias("final_db_size_mb"),
        )

    return (my_scan_csv,)


@app.cell
def _(my_scan_csv, pl):
    run1 = pl.concat([
        my_scan_csv("results/20250206/bolt-202602061052.csv", "bolt"),
        my_scan_csv("results/20250206/postgres18_result-202602061851.csv", "pg"),
        my_scan_csv("results/20250206/postgres18_result-202602080807.csv", "pg")
    ])
    run1_write_df = run1.filter([pl.col("ratio") == ".0078", pl.col("write_ops_per_s_avg") > 0])
    run1_read_df = run1.filter([pl.col("ratio") == "128.0000", pl.col("conn_size") == 2048, pl.col("cmp_wal")=="off"])

    run1_read_ops = run1_read_df.select(
        pl.col("value_size"),
        pl.col("database"),
        pl.col("kv_type"),
        pl.concat_str([pl.col("database"),pl.col("kv_type")], separator=": ").alias("config"),
        pl.col("init_db_size_mb"),
        pl.col("read_ops_per_s_avg"),
        pl.col("read_average_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_stddev_response_s_avg"),
    ).sort(["value_size","config"])

    run_1_write_ops_cmp_wal = run1_write_df.select(
        pl.col("value_size"),
        pl.col("conn_size"),
        pl.col("database"),
        pl.col("kv_type"),
        pl.col("cmp_wal"),
        (pl.concat_str([pl.col("database"),pl.col("kv_type")], separator=": ") + ", wc: " + pl.col("cmp_wal")).alias("config"),
        pl.col("init_db_size_mb"),
        pl.col("completed_db_size_mb"),
        pl.col("final_db_size_mb"),
        pl.col("read_ops_per_s_avg"),
        pl.col("read_average_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_stddev_response_s_avg"),
        pl.col("write_ops_per_s_avg"),
        pl.col("write_average_response_s_avg"),
        pl.col("write_fastest_response_s_avg"),
        pl.col("write_slowest_response_s_avg"),
        pl.col("write_stddev_response_s_avg"),
        pl.col("final_compact_ms"),
        pl.col("final_defrag_ms"),
    ).sort(["value_size","conn_size","config"])

    run_1_write_ops_no_wal = run1_write_df.select(
        pl.col("value_size"),
        pl.col("conn_size"),
        pl.col("database"),
        pl.col("kv_type"),
        pl.col("cmp_wal"),
        (pl.concat_str([pl.col("database"),pl.col("kv_type")], separator=": ")).alias("config"),
        pl.col("init_db_size_mb"),
        pl.col("completed_db_size_mb"),
        pl.col("final_db_size_mb"),
        pl.col("read_ops_per_s_avg"),
        pl.col("read_average_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_stddev_response_s_avg"),
        pl.col("write_ops_per_s_avg"),
        pl.col("write_average_response_s_avg"),
        pl.col("write_fastest_response_s_avg"),
        pl.col("write_slowest_response_s_avg"),
        pl.col("write_stddev_response_s_avg"),
        (pl.col("final_compact_ms") / 1000).alias("final_compact_s"),
        (pl.col("final_defrag_ms") / 1000).alias("final_defrag_s"),
    ).filter(pl.col("cmp_wal")=="off").sort(["value_size","conn_size","config"])

    # Run 1 Questions
    # Does naive KV table layout improve performance?
    # Does naive KV foreign key have a performance penalty?
    # Which database configuration has the best read response avg?
    # Which database configuration has the best read response stddev?
    # Which database configuration has the best write ops?
    # Which database configuration has the best write response min?
    # Which database configuration has the best write response avg?
    # Which database configuration has the best write response stddev?
    # Which database configuration has the best compaction time?
    # Which database configuration has the best defrag time?
    return run1_read_ops, run_1_write_ops_no_wal


@app.cell
def _(pl, run_1_write_ops_no_wal):
    run_1_write_ops_no_wal.filter(pl.col("kv_type").str.contains("lbr")).collect()
    return


@app.cell
def _():
    # Bolt and PG Bucket Entries
    return


@app.cell
def _(alt, pl, run1_read_ops):
    _chart = (
        alt.Chart(run1_read_ops.filter((pl.col("database") == "bolt") | ((pl.col("database") == "pg") & (pl.col("kv_type").str.contains("bucket")))).collect())
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_ops_per_s_avg', type='quantitative', title='Read Ops/S (Higher Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='read_ops_per_s_avg', format=',.2f', title='Read Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title="Database Configuration")
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Read-Heavy Read Ops/S',
                subtitle='2048 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart
    return


@app.cell
def _(alt, pl, run1_read_ops):
    run1_read_response_times = run1_read_ops.filter((pl.col("database") == "bolt") | ((pl.col("database") == "pg") & (pl.col("kv_type").str.contains("bucket")))).select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_average_response_s_avg"),
        (pl.col("read_average_response_s_avg") - pl.col("read_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("read_average_response_s_avg") + pl.col("read_stddev_response_s_avg")).alias("stddev_upper")
    ).collect()

    # TODO: Bar - average latency + ErrorBar - stddev + Points min, max
    _chart = (
        alt.Chart(run1_read_response_times)
        .mark_bar(size=18)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_average_response_s_avg', type='quantitative', title='Read Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run1_read_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_read_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Read-Heavy Read Latency',
                subtitle='2048 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(pl, run1_read_ops):
    run1_read_join = run1_read_ops.filter((pl.col("database") == "bolt")).join(run1_read_ops.filter((pl.col("database") == "pg") & (pl.col("kv_type").str.contains("bucket"))), on=["value_size"])

    run1_read_join.select(
        pl.col("config_right"),
        pl.col("value_size"),
        pl.col("read_ops_per_s_avg").alias("bolt_read_ops_per_s_avg"),
        pl.col("read_ops_per_s_avg_right").alias("pg_read_ops_per_s_avg"),
        (pl.col("read_ops_per_s_avg") - pl.col("read_ops_per_s_avg_right")) / pl.col("read_ops_per_s_avg"),
        pl.col("read_average_response_s_avg").alias("bolt_read_average_response_s_avg"),
        pl.col("read_average_response_s_avg_right").alias("pg_read_average_response_s_avg"),
        (pl.col("read_average_response_s_avg") - pl.col("read_average_response_s_avg_right")) / pl.col("read_average_response_s_avg"),
        pl.col("read_stddev_response_s_avg").alias("bolt_stddev_respone_s"),
        pl.col("read_stddev_response_s_avg_right").alias("pg_stddev_respone_s"),
        (pl.col("read_stddev_response_s_avg") - pl.col("read_stddev_response_s_avg_right")) / pl.col("read_stddev_response_s_avg"),
        pl.col("read_slowest_response_s_avg").alias("bolt_slowest_respone_s"),
        pl.col("read_slowest_response_s_avg_right").alias("pg_slowest_respone_s"),
        ((pl.col("read_slowest_response_s_avg") - pl.col("read_slowest_response_s_avg_right")) / pl.col("read_slowest_response_s_avg")).alias("pg_percent_slowest_difference"),
    ).collect()
    return


@app.cell
def _(pl, run_1_write_ops_no_wal):
    run1_write_join = run_1_write_ops_no_wal.filter([(pl.col("database") == "bolt"),pl.col("conn_size") == 512]).join(run_1_write_ops_no_wal.filter([(pl.col("database") == "pg") & (pl.col("kv_type").str.contains("bucket")),pl.col("conn_size") == 512]), on=["value_size", "conn_size"])

    run1_write_join.select(
        pl.col("config_right"),
        pl.col("value_size"),
        pl.col("conn_size"),
        pl.col("read_ops_per_s_avg").alias("bolt_read_ops_per_s_avg"),
        pl.col("read_ops_per_s_avg_right").alias("pg_read_ops_per_s_avg"),
        (pl.col("read_ops_per_s_avg") - pl.col("read_ops_per_s_avg_right")) / pl.col("read_ops_per_s_avg"),
        pl.col("read_average_response_s_avg").alias("bolt_read_average_response_s_avg"),
        pl.col("read_average_response_s_avg_right").alias("pg_read_average_response_s_avg"),
        (pl.col("read_average_response_s_avg") - pl.col("read_average_response_s_avg_right")) / pl.col("read_average_response_s_avg"),
        pl.col("read_stddev_response_s_avg").alias("bolt_read_stddev_respone_s"),
        pl.col("read_stddev_response_s_avg_right").alias("pg_read_stddev_respone_s"),
        (pl.col("read_stddev_response_s_avg") - pl.col("read_stddev_response_s_avg_right")) / pl.col("read_stddev_response_s_avg"),
        pl.col("read_slowest_response_s_avg").alias("bolt_slowest_respone_s"),
        pl.col("read_slowest_response_s_avg_right").alias("pg_slowest_respone_s"),
        ((pl.col("read_slowest_response_s_avg") - pl.col("read_slowest_response_s_avg_right")) / pl.col("read_slowest_response_s_avg")).alias("pg_read_percent_slowest_difference"),
    
        pl.col("write_ops_per_s_avg").alias("bolt_write_ops_per_s_avg"),
        pl.col("write_ops_per_s_avg_right").alias("pg_write_ops_per_s_avg"),
        (pl.col("write_ops_per_s_avg") - pl.col("write_ops_per_s_avg_right")) / pl.col("write_ops_per_s_avg"),
        pl.col("write_average_response_s_avg").alias("bolt_write_average_response_s_avg"),
        pl.col("write_average_response_s_avg_right").alias("pg_write_average_response_s_avg"),
        (pl.col("write_average_response_s_avg") - pl.col("write_average_response_s_avg_right")) / pl.col("write_average_response_s_avg"),
        pl.col("write_stddev_response_s_avg").alias("bolt_write_stddev_respone_s"),
        pl.col("write_stddev_response_s_avg_right").alias("pg_write_stddev_respone_s"),
        (pl.col("write_stddev_response_s_avg") - pl.col("write_stddev_response_s_avg_right")) / pl.col("write_stddev_response_s_avg"),
        pl.col("write_slowest_response_s_avg").alias("bolt_write_slowest_respone_s"),
        pl.col("write_slowest_response_s_avg_right").alias("pg_write_slowest_respone_s"),
        ((pl.col("write_slowest_response_s_avg") - pl.col("write_slowest_response_s_avg_right")) / pl.col("write_slowest_response_s_avg")).alias("pg_write_percent_slowest_difference"),
        pl.col("final_compact_s").alias("bolt_final_compact_s"),
        pl.col("final_compact_s_right").alias("pg_final_compact_s"),
            ((pl.col("final_compact_s") - pl.col("final_compact_s_right")) / pl.col("final_compact_s")).alias("pg_final_compact_s_difference"),
    ).collect()
    return


@app.cell
def _(pl, run_1_write_ops_no_wal):
    run1_write_ops_cmp_off = run_1_write_ops_no_wal.filter([(pl.col("database") == "bolt") | ((pl.col("database") == "pg") & (pl.col("kv_type").str.contains("bucket"))), pl.col("conn_size") == 512]).collect()
    return (run1_write_ops_cmp_off,)


@app.cell
def _(alt, run1_write_ops_cmp_off):
    _chart = (
        alt.Chart(run1_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_ops_per_s_avg', type='quantitative', title='Write Ops/S (Higher Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='write_ops_per_s_avg', format=',.2f', title='Write Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Write-Heavy Write Ops/S',
                subtitle='512 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, run1_write_ops_cmp_off):
    _chart = (
        alt.Chart(run1_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_ops_per_s_avg', type='quantitative', title='Read Ops/S (Higher Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='read_ops_per_s_avg', format=',.2f', title='Read Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Write-Heavy Read Ops/S',
                subtitle='512 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, pl, run1_write_ops_cmp_off):
    run1_write_read_response_times = run1_write_ops_cmp_off.select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_average_response_s_avg"),
        (pl.col("read_average_response_s_avg") - pl.col("read_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("read_average_response_s_avg") + pl.col("read_stddev_response_s_avg")).alias("stddev_upper")
    )

    _chart = (
        alt.Chart(run1_write_read_response_times)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_average_response_s_avg', type='quantitative', title='Read Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run1_write_read_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_write_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_write_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_write_read_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Write-Heavy Read Latency',
                subtitle='512 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(alt, pl, run1_write_ops_cmp_off):
    run1_write_write_response_times = run1_write_ops_cmp_off.select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("write_slowest_response_s_avg"),
        pl.col("write_fastest_response_s_avg"),
        pl.col("write_average_response_s_avg"),
        (pl.col("write_average_response_s_avg") - pl.col("write_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("write_average_response_s_avg") + pl.col("write_stddev_response_s_avg")).alias("stddev_upper")
    )

    _chart = (
        alt.Chart(run1_write_write_response_times)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_average_response_s_avg', type='quantitative', title='Write Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run1_write_write_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_write_write_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_write_write_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_write_write_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Write-Heavy Write Latency',
                subtitle='512 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(alt, run1_write_ops_cmp_off):
    _chart = (
        alt.Chart(run1_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='final_compact_s', type='quantitative', title='Compaction Time (S) (Lower Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='final_compact_s', format=',.2f', title='Compaction Time (S)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Compaction Time',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, run1_write_ops_cmp_off):
    _chart = (
        alt.Chart(run1_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='final_defrag_s', type='quantitative', title='Defrag Time (S) (Lower Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='final_defrag_s', format=',.2f', title='Defrag Time (S)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Defrag Time',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _():
    # Bolt and PG LBR Entries
    return


@app.cell
def _(alt, pl, run1_read_ops):
    _chart = (
        alt.Chart(run1_read_ops.filter((pl.col("database") == "bolt") | ((pl.col("database") == "pg") & (pl.col("kv_type").str.contains("lbr")))).collect())
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_ops_per_s_avg', type='quantitative', title='Read Ops/S (Higher Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='read_ops_per_s_avg', format=',.2f', title='Read Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title="Database Configuration")
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Read-Heavy Read Ops/S',
                subtitle='2048 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart
    return


@app.cell
def _(alt, pl, run1_read_ops):
    run1_lbr_read_response_times = run1_read_ops.filter((pl.col("database") == "bolt") | ((pl.col("database") == "pg") & (pl.col("kv_type").str.contains("lbr")))).select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_average_response_s_avg"),
        (pl.col("read_average_response_s_avg") - pl.col("read_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("read_average_response_s_avg") + pl.col("read_stddev_response_s_avg")).alias("stddev_upper")
    ).collect()

    # TODO: Bar - average latency + ErrorBar - stddev + Points min, max
    _chart = (
        alt.Chart(run1_lbr_read_response_times)
        .mark_bar(size=18)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_average_response_s_avg', type='quantitative', title='Read Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run1_lbr_read_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_read_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Read-Heavy Read Latency',
                subtitle='2048 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(pl, run_1_write_ops_no_wal):
    run1_lbr_write_ops_cmp_off = run_1_write_ops_no_wal.filter([(pl.col("database") == "bolt") | ((pl.col("database") == "pg") & (pl.col("kv_type").str.contains("lbr"))), pl.col("conn_size") == 512]).collect()
    return (run1_lbr_write_ops_cmp_off,)


@app.cell
def _(alt, run1_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run1_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_ops_per_s_avg', type='quantitative', title='Write Ops/S (Higher Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='write_ops_per_s_avg', format=',.2f', title='Write Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Write-Heavy Write Ops/S',
                subtitle='512 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, run1_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run1_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_ops_per_s_avg', type='quantitative', title='Read Ops/S (Higher Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='read_ops_per_s_avg', format=',.2f', title='Read Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Write-Heavy Read Ops/S',
                subtitle='512 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, pl, run1_lbr_write_ops_cmp_off):
    run1_lbr_write_read_response_times = run1_lbr_write_ops_cmp_off.select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_average_response_s_avg"),
        (pl.col("read_average_response_s_avg") - pl.col("read_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("read_average_response_s_avg") + pl.col("read_stddev_response_s_avg")).alias("stddev_upper")
    )

    _chart = (
        alt.Chart(run1_lbr_write_read_response_times)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_average_response_s_avg', type='quantitative', title='Read Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run1_lbr_write_read_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_write_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_write_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_write_read_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Write-Heavy Read Latency',
                subtitle='512 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(alt, pl, run1_lbr_write_ops_cmp_off):
    run1_lbr_write_write_response_times = run1_lbr_write_ops_cmp_off.select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("write_slowest_response_s_avg"),
        pl.col("write_fastest_response_s_avg"),
        pl.col("write_average_response_s_avg"),
        (pl.col("write_average_response_s_avg") - pl.col("write_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("write_average_response_s_avg") + pl.col("write_stddev_response_s_avg")).alias("stddev_upper")
    )

    _chart = (
        alt.Chart(run1_lbr_write_write_response_times)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_average_response_s_avg', type='quantitative', title='Write Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run1_lbr_write_write_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_write_write_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_write_write_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run1_lbr_write_write_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Write-Heavy Write Latency',
                subtitle='512 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(alt, run1_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run1_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='final_compact_s', type='quantitative', title='Compaction Time (S) (Lower Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='final_compact_s', format=',.2f', title='Compaction Time (S)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Compaction Time',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, run1_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run1_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='final_defrag_s', type='quantitative', title='Defrag Time (S) (Lower Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='final_defrag_s', format=',.2f', title='Defrag Time (S)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Defrag Time',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(my_scan_csv, pl):
    run2 = pl.concat([
        my_scan_csv("results/20260216/bolt-result-202602161815.csv", "bolt"),
        my_scan_csv("results/20260216/bolt-result-202602161844.csv", "bolt"),
        my_scan_csv("results/20260216/postgres18_result-202602161921.csv", "pg"),
        my_scan_csv("results/20260216/postgres18_result-202602162108.csv", "pg"),
        my_scan_csv("results/20260216/oriole17_result-202602161235.csv", "or"),
        my_scan_csv("results/20260216/oriole17_result-202602161623.csv", "or")
    ])
    run2_write_df = run2.filter([pl.col("ratio") == ".0078", pl.col("write_ops_per_s_avg") > 0])
    run2_read_df = run2.filter([pl.col("ratio") == "128.0000", pl.col("conn_size") == 2048])

    run2_read_ops = run2_read_df.select(
        pl.col("value_size"),
        pl.col("database"),
        pl.col("kv_type"),
        pl.col("cmp_oriole"),
        (pl.concat_str([pl.col("database"),pl.col("kv_type")], separator=": ") + pl.when(pl.col("cmp_oriole") > 0).then(pl.lit(", compressed")).otherwise(pl.lit(""))).alias("config"),
        pl.col("init_db_size_mb"),
        pl.col("read_ops_per_s_avg"),
        pl.col("read_average_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_stddev_response_s_avg"),
    ).sort(["value_size","config"])

    run_2_write_ops_no_wal = run2_write_df.select(
        pl.col("value_size"),
        pl.col("conn_size"),
        pl.col("database"),
        pl.col("kv_type"),
        pl.col("cmp_wal"),
        (pl.concat_str([pl.col("database"),pl.col("kv_type")], separator=": ") + pl.when(pl.col("cmp_oriole") > 0).then(pl.lit(", compressed")).otherwise(pl.lit(""))).alias("config"),
        pl.col("init_db_size_mb"),
        pl.col("completed_db_size_mb"),
        pl.col("final_db_size_mb"),
        pl.col("read_ops_per_s_avg"),
        pl.col("read_average_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_stddev_response_s_avg"),
        pl.col("write_ops_per_s_avg"),
        pl.col("write_average_response_s_avg"),
        pl.col("write_fastest_response_s_avg"),
        pl.col("write_slowest_response_s_avg"),
        pl.col("write_stddev_response_s_avg"),
        (pl.col("final_compact_ms") / 1000).alias("final_compact_s"),
        (pl.col("final_defrag_ms") / 1000).alias("final_defrag_s"),
    ).sort(["value_size","conn_size","config"])

    # Run 2 Questions
    # Which database configuration has the best read ops?
    # Which database configuration has the best read response min?
    # Which database configuration has the best read response avg?
    # Which database configuration has the best read response stddev?
    # Which database configuration has the best write ops?
    # Which database configuration has the best write response min?
    # Which database configuration has the best write response avg?
    # Which database configuration has the best write response stddev?
    # Which database configuration has the best compaction time?
    # Which database configuration has the best defrag time?

    return run2_read_ops, run2_write_df, run_2_write_ops_no_wal


@app.cell
def _(run2_write_df):
    run2_write_df.collect()
    return


@app.cell
def _(alt, pl, run2_read_ops):
    _chart = (
        alt.Chart(run2_read_ops.filter((pl.col("database") == "bolt") | (pl.col("kv_type").str.contains("nifs"))).collect())
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_ops_per_s_avg', type='quantitative', title='Read Ops/S (Higher Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='read_ops_per_s_avg', format=',.2f', title='Read Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title="Database Configuration")
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Read-Heavy Read Ops/S',
                subtitle='2048 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart
    return


@app.cell
def _(pl, run2_read_ops):
    run2_read_response_times = run2_read_ops.filter((pl.col("database") == "bolt") | (pl.col("kv_type").str.contains("nifs"))).select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_average_response_s_avg"),
        (pl.col("read_average_response_s_avg") - pl.col("read_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("read_average_response_s_avg") + pl.col("read_stddev_response_s_avg")).alias("stddev_upper")
    ).collect()
    run2_read_response_times
    return (run2_read_response_times,)


@app.cell
def _(alt, run2_read_response_times):
    _chart = (
        alt.Chart(run2_read_response_times)
        .mark_bar(size=18)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_average_response_s_avg', type='quantitative', title='Read Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run2_read_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_read_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Read-Heavy Read Latency',
                subtitle='2048 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(pl, run_2_write_ops_no_wal):
    run2_lbr_write_ops_cmp_off = run_2_write_ops_no_wal.filter([(pl.col("database") == "bolt") | (pl.col("kv_type").str.contains("nifs")), pl.col("conn_size") == 256]).collect()
    run2_lbr_write_ops_cmp_off
    return (run2_lbr_write_ops_cmp_off,)


@app.cell
def _(alt, run2_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run2_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_ops_per_s_avg', type='quantitative', title='Write Ops/S (Higher Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='write_ops_per_s_avg', format=',.2f', title='Write Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Write-Heavy Write Ops/S',
                subtitle='256 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, run2_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run2_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_ops_per_s_avg', type='quantitative', title='Read Ops/S (Higher Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='read_ops_per_s_avg', format=',.2f', title='Read Ops/S (Higher Is Better)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Write-Heavy Read Ops/S',
                subtitle='256 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, pl, run2_lbr_write_ops_cmp_off):
    run2_lbr_write_read_response_times = run2_lbr_write_ops_cmp_off.select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("read_slowest_response_s_avg"),
        pl.col("read_fastest_response_s_avg"),
        pl.col("read_average_response_s_avg"),
        (pl.col("read_average_response_s_avg") - pl.col("read_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("read_average_response_s_avg") + pl.col("read_stddev_response_s_avg")).alias("stddev_upper")
    )

    _chart = (
        alt.Chart(run2_lbr_write_read_response_times)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_average_response_s_avg', type='quantitative', title='Read Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run2_lbr_write_read_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_lbr_write_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_lbr_write_read_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_lbr_write_read_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='read_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Write-Heavy Read Latency',
                subtitle='256 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(alt, pl, run2_lbr_write_ops_cmp_off):
    run2_lbr_write_write_response_times = run2_lbr_write_ops_cmp_off.select(
        pl.col("config"),
        pl.col("value_size"),
        pl.col("write_slowest_response_s_avg"),
        pl.col("write_fastest_response_s_avg"),
        pl.col("write_average_response_s_avg"),
        (pl.col("write_average_response_s_avg") - pl.col("write_stddev_response_s_avg")).clip(lower_bound=0).alias("stddev_lower"),
        (pl.col("write_average_response_s_avg") + pl.col("write_stddev_response_s_avg")).alias("stddev_upper")
    )

    _chart = (
        alt.Chart(run2_lbr_write_write_response_times)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_average_response_s_avg', type='quantitative', title='Write Latency (S) (Lower Is Better)'),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
        ) + alt.Chart(run2_lbr_write_write_response_times)
        .mark_rule()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            y2=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_lbr_write_write_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_upper', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_lbr_write_write_response_times)
        .mark_point(color='black', size=10)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='stddev_lower', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        ) +
        alt.Chart(run2_lbr_write_write_response_times)
        .mark_point(color='red', size=8)
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='write_slowest_response_s_avg', type='quantitative'),
            xOffset=alt.XOffset(field="config"),
        )
    ).properties(
            title=alt.TitleParams(
                text='Write-Heavy Write Latency',
                subtitle='256 Connections',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    _chart
    return


@app.cell
def _(alt, run2_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run2_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='final_compact_s', type='quantitative', title='Compaction Time (S) (Lower Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='final_compact_s', format=',.2f', title='Compaction Time (S)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Compaction Time',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, run2_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run2_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='final_defrag_s', type='quantitative', title='Defrag Time (S) (Lower Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='final_defrag_s', format=',.2f', title='Defrag Time (S)'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Defrag Time',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, run2_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run2_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='completed_db_size_mb', type='quantitative', title='Pre-Compact DB Size (MB) (Lower Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='completed_db_size_mb', format=',.2f', title='Pre-Compact DB Size (MB'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Pre-Compact DB Size',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


@app.cell
def _(alt, run2_lbr_write_ops_cmp_off):
    _chart = (
        alt.Chart(run2_lbr_write_ops_cmp_off)
        .mark_bar()
        .encode(
            x=alt.X(field='value_size', type='nominal', title='Value Size (bytes)'),
            y=alt.Y(field='final_db_size_mb', type='quantitative', title='Post-Compact DB Size (MB) (Lower Is Better)', stack=False),
            color=alt.Color(field='config', type='nominal', title="Database Configuration", scale={
                'scheme': 'category10'
            }),
            xOffset=alt.XOffset(field="config"),
            tooltip=[
                alt.Tooltip(field='value_size', format=',.0f', title='Value Size (bytes)'),
                alt.Tooltip(field='completed_db_size_mb', format=',.2f', title='Post-Compact DB Size (MB'),
                alt.Tooltip(field='config', title='Database Configuration')
            ]
        )
        .properties(
            title=alt.TitleParams(
                text='Post-Compact DB Size',
            ),
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart.configure_title(
        align="center",
        anchor="middle",
    )
    return


if __name__ == "__main__":
    app.run()
