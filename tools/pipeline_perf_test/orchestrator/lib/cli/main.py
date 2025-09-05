"""
Main CLI entry point for the test orchestrator.

This module handles:
- Parsing command-line arguments
- Setting up telemetry (tracing/metrics)
- Loading the test configuration from file
- Instantiating and running the test suite

"""

from lib.cli.parser import build_parser
from lib.runner.schema.loader import load_config_from_file
from lib.runner.factory import build_test_suite
from lib.cli.telemetry import setup_telemetry, TelemetryClient

# Trigger all the default strategy / action registrations
# pylint: disable=unused-import
from lib.impl import strategies  # Do not remove
from lib.impl import actions  # Do not remove

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def write_dataframe_to_parquet(
    df: pd.DataFrame, file_path: str, compression: str = "snappy"
):
    """
    Write a pandas DataFrame to a Parquet file on disk using pyarrow.

    Parameters:
        df (pd.DataFrame): The DataFrame to write.
        file_path (str): The full path where the Parquet file will be saved.
        compression (str): Compression type ('snappy', 'gzip', 'brotli', etc.). Default is 'snappy'.
    """
    # Convert the pandas DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Write the table to Parquet
    pq.write_table(table, file_path, compression=compression)

    print(f"DataFrame written to {file_path} with {compression} compression.")


def main():
    """
    Entrypoint for the CLI.

    This function:
    - Parses command-line arguments using `build_parser()`
    - Sets up OpenTelemetry tracing/metrics if enabled via args
    - Loads the test suite configuration from a file
    - Builds and runs the test suite
    """
    parser = build_parser()
    args = parser.parse_args()
    tr = setup_telemetry(args)

    tsc = load_config_from_file(args.config)
    ts = build_test_suite(tsc, tr, args)

    ts.run()

    if args.print_spans or args.print_events or args.print_metrics or args.save_telemetry:
        import pandas as pd

        pd.set_option("display.max_columns", None)
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_colwidth", 150)
        tc: TelemetryClient = ts.context.get_telemetry_client()
        if args.save_telemetry:
            write_dataframe_to_parquet(tc.metrics.query_metrics(), "metrics.parquet")
            write_dataframe_to_parquet(tc.spans.query_span_events(), "events.parquet")
            write_dataframe_to_parquet(tc.spans.query_spans(), "spans.parquet")
        if args.print_spans:
            print(tc.spans.query_spans().to_string())
        if args.print_events:
            events = tc.spans.query_span_events(
                where=lambda df: df[df["name"] != "log"]
            )
            print(events)
        if args.print_metrics:
            print(tc.metrics.query_metrics().to_string())


if __name__ == "__main__":
    main()
