#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "requests",
# ]
# ///
"""
Query SigNoz logs via API.

Usage:
    python query_signoz_logs.py [--minutes N] [--filter EXPR] [--severity LEVEL] [--limit N]

Examples:
    python query_signoz_logs.py --minutes 10
    python query_signoz_logs.py --tx                    # Query [TX] transaction logs
    python query_signoz_logs.py --errors                # Query ERROR logs
    python query_signoz_logs.py --tx --errors           # Query both TX and ERROR logs
    python query_signoz_logs.py --severity DEBUG --filter "[TX]"
    python query_signoz_logs.py --start 14:30:00 --end 14:40:00
    python query_signoz_logs.py --trace-id abc123      # Filter by trace_id
    python query_signoz_logs.py --meta key=value       # Filter by any metadata field
    python query_signoz_logs.py --fields trace_id span_id app.component  # Show specific metadata fields
    python query_signoz_logs.py --show-all-fields      # Show all available metadata fields
"""

import requests
import json
import argparse
from datetime import datetime, timedelta

# SigNoz API configuration
SIGNOZ_URL = "http://signoz.signoz.orb.local:8080/api/v5/query_range"
SIGNOZ_API_KEY = "y3YzHqd21oXDCyPuk6getciHvJzqtXhdmrhCK84NpeU="
# Note: Bearer token may expire - get a fresh one from browser DevTools if needed
SIGNOZ_BEARER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NjQwNjE2MzQsImlhdCI6MTc2NDA1OTgzNCwiaWQiOiIwMTlhYTNlMS1mMjYxLTc4NGMtOGExYS03ZmM2ZjU0ZmMxOGQiLCJlbWFpbCI6Im1hcnRpbkBtYXVjaC5kZXYiLCJyb2xlIjoiQURNSU4iLCJvcmdJZCI6IjAxOWFhM2UxLWYyNjAtNzY0Ni04YTM0LTc1MDBhNmM0YjAyNiJ9.QG1IaX3IPkW9cjKOPPf-0YcjUspHSRinTpniQKfVaz0"


def query_logs(minutes=60, severity=None, body_filter=None, limit=200, start_utc=None, end_utc=None, verbose=False, trace_id=None, metadata_filters=None):
    """Query SigNoz logs.

    Args:
        minutes: Minutes to look back (default: 60)
        severity: Filter by severity level (DEBUG, INFO, WARN, ERROR)
        body_filter: Filter by body content (ILIKE match)
        limit: Maximum logs to retrieve
        start_utc: Start time in UTC (HH:MM:SS or ISO format)
        end_utc: End time in UTC (HH:MM:SS or ISO format)
        verbose: Show verbose output
        trace_id: Filter by trace_id
        metadata_filters: Dict of metadata field filters (e.g., {"span_id": "abc123"})
    """
    if start_utc and end_utc:
        # Parse ISO format or HH:MM:SS format
        if "T" in start_utc:
            start_time = datetime.fromisoformat(start_utc.replace("Z", ""))
            end_time = datetime.fromisoformat(end_utc.replace("Z", ""))
        else:
            # Assume HH:MM:SS format for today (treat as UTC)
            today = datetime.utcnow().date()
            start_time = datetime.combine(today, datetime.strptime(start_utc, "%H:%M:%S").time())
            end_time = datetime.combine(today, datetime.strptime(end_utc, "%H:%M:%S").time())
    else:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=minutes)

    # Convert to milliseconds since epoch
    # Use calendar.timegm for UTC (not timestamp() which uses local timezone)
    import calendar
    start_ms = int(calendar.timegm(start_time.timetuple()) * 1000)
    end_ms = int(calendar.timegm(end_time.timetuple()) * 1000)

    print(f"Querying logs from {start_time} to {end_time} UTC")
    print(f"Time range: {start_ms} - {end_ms}")

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {SIGNOZ_BEARER_TOKEN}",
        "Content-Type": "application/json",
        "signoz-api-key": SIGNOZ_API_KEY
    }

    # Build filter expression
    filter_parts = []
    if severity:
        filter_parts.append(f'severity_text="{severity}"')
    if body_filter:
        filter_parts.append(f'body ILIKE "%{body_filter}%"')
    if trace_id:
        filter_parts.append(f'trace_id="{trace_id}"')

    # Add any additional metadata filters
    if metadata_filters:
        for field, value in metadata_filters.items():
            filter_parts.append(f'{field}="{value}"')

    filter_expr = " AND ".join(filter_parts) if filter_parts else ""

    payload = {
        "schemaVersion": "v1",
        "start": start_ms,
        "end": end_ms,
        "requestType": "raw",
        "compositeQuery": {
            "queries": [{
                "type": "builder_query",
                "spec": {
                    "name": "A",
                    "signal": "logs",
                    "stepInterval": None,
                    "disabled": False,
                    "filter": {"expression": filter_expr},
                    "limit": limit,
                    "offset": 0,
                    "order": [
                        {"key": {"name": "timestamp"}, "direction": "desc"},
                        {"key": {"name": "id"}, "direction": "desc"}
                    ],
                    "having": {"expression": ""}
                }
            }]
        },
        "formatOptions": {"formatTableResultForUI": False, "fillGaps": False},
        "variables": {}
    }

    response = requests.post(SIGNOZ_URL, headers=headers, json=payload, timeout=30)

    if response.status_code != 200:
        print(f"Error: HTTP {response.status_code}")
        print(response.text[:500])
        return []

    data = response.json()

    if data.get("status") != "success":
        print(f"Query failed: {json.dumps(data, indent=2)}")
        return []

    results = data.get("data", {}).get("data", {}).get("results", [])
    rows = results[0].get("rows") or [] if results else []
    meta = data.get("data", {}).get("meta", {})

    print(f"Scanned: {meta.get('rowsScanned', 0)} rows")
    print(f"Found: {len(rows)} results")
    if filter_expr:
        print(f"Filter: {filter_expr}")

    # Show time range of actual results
    if rows and verbose:
        timestamps = [r.get("timestamp", "") for r in rows if r.get("timestamp")]
        if timestamps:
            print(f"Result time range: {min(timestamps)} to {max(timestamps)}")
    print()

    return rows


def extract_all_metadata_fields(rows):
    """Extract all unique metadata field names from rows."""
    all_fields = set()
    for row in rows:
        row_data = row.get("data", {})
        if isinstance(row_data, dict):
            all_fields.update(row_data.keys())
    return sorted(all_fields)


def format_metadata_fields(row_data, fields_to_show=None):
    """Format metadata fields for display.

    Args:
        row_data: Dictionary containing log data/metadata
        fields_to_show: List of field names to show, or None to show all

    Returns:
        Formatted string with metadata fields
    """
    if not isinstance(row_data, dict):
        return ""

    # If no fields specified, show all
    if fields_to_show is None:
        fields_to_show = [k for k in row_data.keys() if k not in ["body", "severity_text"]]

    if not fields_to_show:
        return ""

    # Build metadata string
    metadata_parts = []
    for field in fields_to_show:
        if field in row_data:
            value = row_data[field]
            # Format value nicely
            if isinstance(value, (dict, list)):
                value = json.dumps(value, separators=(',', ':'))
            elif value is None:
                value = ""
            else:
                value = str(value)

            # Truncate long values
            if len(value) > 50:
                value = value[:47] + "..."

            metadata_parts.append(f"{field}={value}")

    if metadata_parts:
        return " | " + " | ".join(metadata_parts)
    return ""


def display_logs(rows, highlight_keywords=None, body_contains=None, fields_to_show=None, show_all_fields=False):
    """Display logs with optional keyword highlighting and filtering.

    Args:
        rows: List of log rows
        highlight_keywords: Keywords to highlight
        body_contains: Filter by body content
        fields_to_show: List of metadata field names to display (None = show common fields)
        show_all_fields: If True, show all available metadata fields
    """
    if highlight_keywords is None:
        highlight_keywords = ["[TX]", "ERROR", "COMMIT", "BEGIN", "Autocommit"]

    # Determine which fields to show
    if show_all_fields:
        # Extract all unique fields from all rows
        all_fields = extract_all_metadata_fields(rows)
        # Exclude body and severity_text as they're shown separately
        fields_to_show = [f for f in all_fields if f not in ["body", "severity_text"]]
    elif fields_to_show is None:
        # Default: show common interesting fields
        fields_to_show = ["trace_id", "span_id", "app.component"]

    displayed = 0
    for row in rows:
        row_data = row.get("data", {})
        ts = row.get("timestamp", "")
        severity = row_data.get("severity_text", "INFO")
        body = row_data.get("body", "")

        # Filter by body content if specified
        if body_contains and body_contains not in body:
            continue

        # Check if any highlight keyword is in the body
        is_highlighted = any(kw in body for kw in highlight_keywords)
        marker = ">>>" if is_highlighted or severity == "ERROR" else "   "

        # Truncate body for display
        body_display = body[:150] if len(body) > 150 else body

        # Format metadata fields
        metadata_str = format_metadata_fields(row_data, fields_to_show)

        print(f"{marker} [{ts}] {severity:5} | {body_display}{metadata_str}")
        displayed += 1

    return displayed


def query_tx_logs(minutes=60, limit=500, start_utc=None, end_utc=None, verbose=False, trace_id=None, metadata_filters=None, fields_to_show=None, show_all_fields=False):
    """Query [TX] transaction logs (DEBUG level)."""
    print("=== Querying [TX] Transaction Logs ===\n")
    rows = query_logs(
        minutes=minutes,
        severity="DEBUG",
        limit=limit,
        start_utc=start_utc,
        end_utc=end_utc,
        verbose=verbose,
        trace_id=trace_id,
        metadata_filters=metadata_filters
    )

    # Filter to only [TX] logs
    tx_rows = [r for r in rows if "[TX]" in r.get("data", {}).get("body", "")]
    print(f"[TX] logs found: {len(tx_rows)}\n")

    # Determine which fields to show
    if show_all_fields:
        all_fields = extract_all_metadata_fields(tx_rows)
        fields_to_show = [f for f in all_fields if f not in ["body", "severity_text"]]
    elif fields_to_show is None:
        fields_to_show = ["trace_id", "span_id", "app.component"]

    for row in tx_rows:
        ts = row.get("timestamp", "")
        body = row.get("data", {}).get("body", "")[:150]
        row_data = row.get("data", {})
        metadata_str = format_metadata_fields(row_data, fields_to_show)
        print(f"[{ts}] {body}{metadata_str}")

    return tx_rows


def query_error_logs(minutes=60, limit=100, start_utc=None, end_utc=None, verbose=False, trace_id=None, metadata_filters=None, fields_to_show=None, show_all_fields=False):
    """Query ERROR logs."""
    print("=== Querying ERROR Logs ===\n")
    rows = query_logs(
        minutes=minutes,
        severity="ERROR",
        limit=limit,
        start_utc=start_utc,
        end_utc=end_utc,
        verbose=verbose,
        trace_id=trace_id,
        metadata_filters=metadata_filters
    )

    # Determine which fields to show
    if show_all_fields:
        all_fields = extract_all_metadata_fields(rows)
        fields_to_show = [f for f in all_fields if f not in ["body", "severity_text"]]
    elif fields_to_show is None:
        fields_to_show = ["trace_id", "span_id", "app.component"]

    for row in rows:
        ts = row.get("timestamp", "")
        body = row.get("data", {}).get("body", "")[:300]
        row_data = row.get("data", {})
        metadata_str = format_metadata_fields(row_data, fields_to_show)
        print(f"[{ts}] {body}{metadata_str}")

    return rows


def correlate_tx_and_errors(minutes=60, limit=500, start_utc=None, end_utc=None, verbose=False, trace_id=None, metadata_filters=None, fields_to_show=None, show_all_fields=False):
    """Query both TX and ERROR logs, correlating them by timestamp."""
    print("=== Correlating TX and ERROR Logs ===\n")

    # Query DEBUG logs for TX
    debug_rows = query_logs(
        minutes=minutes,
        severity="DEBUG",
        limit=limit,
        start_utc=start_utc,
        end_utc=end_utc,
        verbose=verbose,
        trace_id=trace_id,
        metadata_filters=metadata_filters
    )
    tx_rows = [r for r in debug_rows if "[TX]" in r.get("data", {}).get("body", "")]

    # Query ERROR logs
    error_rows = query_logs(
        minutes=minutes,
        severity="ERROR",
        limit=100,
        start_utc=start_utc,
        end_utc=end_utc,
        verbose=verbose,
        trace_id=trace_id,
        metadata_filters=metadata_filters
    )

    # Combine and sort by timestamp
    all_rows = []
    for row in tx_rows:
        row["_type"] = "TX"
        all_rows.append(row)
    for row in error_rows:
        row["_type"] = "ERROR"
        all_rows.append(row)

    # Sort by timestamp (ascending for chronological order)
    all_rows.sort(key=lambda r: r.get("timestamp", ""))

    # Determine which fields to show
    if show_all_fields:
        all_fields = extract_all_metadata_fields(all_rows)
        fields_to_show = [f for f in all_fields if f not in ["body", "severity_text"]]
    elif fields_to_show is None:
        fields_to_show = ["trace_id", "span_id", "app.component"]

    print(f"\nFound {len(tx_rows)} [TX] logs and {len(error_rows)} ERROR logs\n")
    print("=== Combined Timeline (chronological) ===\n")

    for row in all_rows:
        ts = row.get("timestamp", "")
        body = row.get("data", {}).get("body", "")[:150]
        row_type = row.get("_type", "?")
        severity = row.get("data", {}).get("severity_text", "")
        row_data = row.get("data", {})
        metadata_str = format_metadata_fields(row_data, fields_to_show)

        if row_type == "ERROR":
            print(f">>> [{ts}] ERROR | {body}{metadata_str}")
        else:
            print(f"    [{ts}] DEBUG | {body}{metadata_str}")


def parse_metadata_filters(meta_args):
    """Parse --meta key=value arguments into a dict."""
    if not meta_args:
        return None

    filters = {}
    for meta in meta_args:
        if "=" in meta:
            key, value = meta.split("=", 1)
            filters[key.strip()] = value.strip()
        else:
            print(f"Warning: Invalid metadata filter format '{meta}', expected key=value")

    return filters if filters else None


def main():
    parser = argparse.ArgumentParser(
        description="Query SigNoz logs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    %(prog)s --tx                        # Query [TX] transaction logs
    %(prog)s --errors                    # Query ERROR logs
    %(prog)s --tx --errors               # Correlate TX and ERROR logs
    %(prog)s --severity DEBUG --limit 50 # Query DEBUG logs
    %(prog)s --start 14:30:00 --end 14:40:00  # Query specific time window
    %(prog)s --trace-id abc123def456     # Filter by trace_id
    %(prog)s --meta span_id=xyz          # Filter by any metadata field
    %(prog)s --meta key1=val1 --meta key2=val2  # Multiple metadata filters
    %(prog)s --fields trace_id span_id   # Show specific metadata fields
    %(prog)s --show-all-fields           # Show all available metadata fields
    %(prog)s --fields app.component service.name  # Show custom metadata fields
        """
    )
    parser.add_argument("--minutes", type=int, default=60, help="Minutes to look back (default: 60)")
    parser.add_argument("--severity", type=str, help="Filter by severity (DEBUG, INFO, WARN, ERROR)")
    parser.add_argument("--filter", type=str, dest="body_filter", help="Filter by body content")
    parser.add_argument("--limit", type=int, default=200, help="Max logs to retrieve (default: 200)")
    parser.add_argument("--highlight", type=str, nargs="*", help="Keywords to highlight")
    parser.add_argument("--start", type=str, help="Start time UTC (HH:MM:SS or ISO)")
    parser.add_argument("--end", type=str, help="End time UTC (HH:MM:SS or ISO)")
    parser.add_argument("--tx", action="store_true", help="Query [TX] transaction logs")
    parser.add_argument("--errors", action="store_true", help="Query ERROR logs")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show verbose output including result time ranges")
    parser.add_argument("--trace-id", type=str, dest="trace_id", help="Filter by trace_id")
    parser.add_argument("--meta", type=str, action="append", dest="metadata",
                        help="Filter by metadata field (format: key=value). Can be used multiple times.")
    parser.add_argument("--fields", type=str, nargs="*", dest="fields",
                        help="Metadata fields to display (e.g., --fields trace_id span_id app.component). "
                             "If not specified, shows common fields (trace_id, span_id, app.component).")
    parser.add_argument("--show-all-fields", action="store_true", dest="show_all_fields",
                        help="Show all available metadata fields from the logs")

    args = parser.parse_args()

    # Parse metadata filters
    metadata_filters = parse_metadata_filters(args.metadata)

    # Prepare fields to show
    fields_to_show = args.fields if args.fields else None

    # Special modes
    if args.tx and args.errors:
        correlate_tx_and_errors(
            minutes=args.minutes,
            limit=args.limit,
            start_utc=args.start,
            end_utc=args.end,
            verbose=args.verbose,
            trace_id=args.trace_id,
            metadata_filters=metadata_filters,
            fields_to_show=fields_to_show,
            show_all_fields=args.show_all_fields
        )
    elif args.tx:
        query_tx_logs(
            minutes=args.minutes,
            limit=args.limit,
            start_utc=args.start,
            end_utc=args.end,
            verbose=args.verbose,
            trace_id=args.trace_id,
            metadata_filters=metadata_filters,
            fields_to_show=fields_to_show,
            show_all_fields=args.show_all_fields
        )
    elif args.errors:
        query_error_logs(
            minutes=args.minutes,
            limit=args.limit,
            start_utc=args.start,
            end_utc=args.end,
            verbose=args.verbose,
            trace_id=args.trace_id,
            metadata_filters=metadata_filters,
            fields_to_show=fields_to_show,
            show_all_fields=args.show_all_fields
        )
    else:
        # General query
        rows = query_logs(
            minutes=args.minutes,
            severity=args.severity,
            body_filter=args.body_filter,
            limit=args.limit,
            start_utc=args.start,
            end_utc=args.end,
            verbose=args.verbose,
            trace_id=args.trace_id,
            metadata_filters=metadata_filters
        )

        if rows:
            highlight = args.highlight if args.highlight else None
            display_logs(
                rows,
                highlight_keywords=highlight,
                body_contains=args.body_filter,
                fields_to_show=fields_to_show,
                show_all_fields=args.show_all_fields
            )
        else:
            print("No logs found.")


if __name__ == "__main__":
    main()
