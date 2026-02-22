from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, available_timezones

import pandas as pd


# Fallback path used when no --input is provided and no local .ics exists.
NOTEBOOK_ICS_FALLBACK = Path(
    "C:/Users/Mr. Paul/Downloads/carl@uni.minerva.edu.ical (1)/carl@uni.minerva.edu.ics"
)


def unfold_ics_lines(text: str) -> list[str]:
    """Unfold RFC5545 line continuations.

    In .ics files, long lines can wrap and continuation lines start with a space
    or tab. This function restores the original logical property lines.
    """
    lines = text.splitlines()
    unfolded: list[str] = []
    for line in lines:
        if (line.startswith(" ") or line.startswith("\t")) and unfolded:
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)
    return unfolded


def split_vevent_blocks(ics_text: str) -> list[str]:
    """Return raw text payload for each VEVENT block."""
    pattern = re.compile(r"BEGIN:VEVENT\r?\n(.*?)\r?\nEND:VEVENT", re.DOTALL)
    return [match.group(1) for match in pattern.finditer(ics_text)]


def parse_block(block: str) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Parse one VEVENT block into two property maps.

    base_props:
      Key = base property name (e.g., DTSTART), parameters removed.
    full_props:
      Key = full property key including parameters (e.g., DTSTART;TZID=...).
    """
    base_props: dict[str, list[str]] = {}
    full_props: dict[str, list[str]] = {}

    for line in unfold_ics_lines(block):
        if ":" not in line:
            continue
        key_with_params, value = line.split(":", 1)
        key_with_params = key_with_params.strip()
        value = value.strip()

        base_key = key_with_params.split(";", 1)[0].strip().upper()
        full_key = key_with_params.strip()

        base_props.setdefault(base_key, []).append(value)
        full_props.setdefault(full_key, []).append(value)

    return base_props, full_props


def first(props: dict[str, list[str]], key: str) -> str:
    """Get first value for a property key, or empty string if missing."""
    values = props.get(key, [])
    return values[0] if values else ""


def first_full_key(props: dict[str, list[str]], base_key: str) -> str:
    """Return first full key that matches a base key (case-insensitive)."""
    target = base_key.upper()
    for key in props.keys():
        if key.split(";", 1)[0].upper() == target:
            return key
    return target


def extract_tzid(full_key: str) -> str:
    """Extract TZID parameter from a full key like DTSTART;TZID=America/New_York."""
    for part in full_key.split(";")[1:]:
        if "=" in part:
            k, v = part.split("=", 1)
            if k.strip().upper() == "TZID":
                return v.strip()
    return ""


def normalize_tzid(tzid: str) -> str:
    """Normalize timezone ID casing so ZoneInfo can resolve it reliably."""
    if not tzid:
        return ""

    try:
        ZoneInfo(tzid)
        return tzid
    except Exception:
        pass

    tzid_lower = tzid.lower()
    for candidate in available_timezones():
        if candidate.lower() == tzid_lower:
            return candidate
    return tzid


def parse_ics_datetime(raw_value: str, tzid: str = "") -> datetime | None:
    """Parse ICS datetime formats into datetime.

    Supported inputs:
      - YYYYMMDDTHHMMSSZ (UTC)
      - YYYYMMDDTHHMM
      - YYYYMMDD (date-only)
    """
    if not raw_value:
        return None

    value = raw_value.strip()
    is_utc = value.endswith("Z")
    if is_utc:
        value = value[:-1]

    dt: datetime | None = None
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%dT%H%M", "%Y%m%d"):
        try:
            dt = datetime.strptime(value, fmt)
            break
        except ValueError:
            continue

    if dt is None:
        return None

    if is_utc:
        return dt.replace(tzinfo=timezone.utc)

    if tzid:
        tzid = normalize_tzid(tzid)
        try:
            return dt.replace(tzinfo=ZoneInfo(tzid))
        except Exception:
            return dt

    return dt


def parse_ics_duration(raw_duration: str) -> timedelta | None:
    """Parse ISO8601-like ICS duration (e.g., PT30M, P1DT2H) into timedelta."""
    if not raw_duration:
        return None

    value = raw_duration.strip().upper()
    match = re.fullmatch(
        r"P(?:(?P<weeks>\d+)W)?(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?",
        value,
    )
    if not match:
        return None

    weeks = int(match.group("weeks") or 0)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = int(match.group("seconds") or 0)
    return timedelta(
        weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds
    )


def format_dt(dt: datetime | None) -> str:
    """Format datetime as readable text for Excel export."""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        return dt.isoformat(sep=" ", timespec="seconds")
    return dt.isoformat(sep=" ", timespec="seconds")


def format_offset(dt: datetime | None) -> str:
    """Format UTC offset from +HHMM to +HH:MM."""
    if dt is None or dt.tzinfo is None:
        return ""
    offset = dt.strftime("%z")
    if offset and len(offset) == 5:
        return offset[:3] + ":" + offset[3:]
    return offset


def timezone_fields(
    start_key: str, dtstart_raw: str, dtstart_dt: datetime | None
) -> tuple[str, str, str]:
    """Build timezone_name, utc_offset, is_dst fields for one event."""
    if ";VALUE=DATE" in start_key:
        return "DATE_ONLY", "", ""

    if dtstart_raw.endswith("Z"):
        return "UTC", "+00:00", "0"

    tzid = normalize_tzid(extract_tzid(start_key))
    if tzid:
        offset = format_offset(dtstart_dt)
        is_dst = ""
        if dtstart_dt is not None and dtstart_dt.tzinfo is not None:
            dst_delta = dtstart_dt.dst()
            if dst_delta is not None:
                is_dst = "1" if dst_delta != timedelta(0) else "0"
        return tzid, offset, is_dst

    return "UNSPECIFIED", "", ""


def tz_label(start_key: str, dtstart_raw: str, dtstart_dt: datetime | None) -> str:
    """Human-readable composite timezone label, e.g., America/Los_Angeles (-07:00)."""
    timezone_name, utc_offset, _ = timezone_fields(start_key, dtstart_raw, dtstart_dt)
    if timezone_name in {"DATE_ONLY", "UNSPECIFIED"}:
        return timezone_name
    return f"{timezone_name} ({utc_offset})" if utc_offset else timezone_name


def duration_minutes(dtstart: datetime | None, dtend: datetime | None, raw_duration: str) -> str:
    """Return duration in minutes using DURATION if present, else DTEND-DTSTART."""
    parsed = parse_ics_duration(raw_duration)
    if parsed is not None:
        return str(int(parsed.total_seconds() // 60))

    if dtstart is not None and dtend is not None:
        return str(int((dtend - dtstart).total_seconds() // 60))

    return ""


def build_rows(ics_text: str) -> list[dict[str, str]]:
    """Transform VEVENT blocks into flat rows for DataFrame export."""
    blocks = split_vevent_blocks(ics_text)
    rows: list[dict[str, str]] = []

    for block in blocks:
        base_props, full_props = parse_block(block)

        dtstart_raw = first(base_props, "DTSTART")
        dtend_raw = first(base_props, "DTEND")
        duration_raw = first(base_props, "DURATION")

        dtstart_key = first_full_key(full_props, "DTSTART")
        dtend_key = first_full_key(full_props, "DTEND")

        dtstart_tzid = extract_tzid(dtstart_key)
        dtend_tzid = extract_tzid(dtend_key) or dtstart_tzid

        dtstart_dt = parse_ics_datetime(dtstart_raw, dtstart_tzid)
        dtend_dt = parse_ics_datetime(dtend_raw, dtend_tzid)

        # If DTEND is missing, treat event as instant: dtend == dtstart.
        missing_dtend = not dtend_raw
        if missing_dtend and dtstart_raw:
            dtend_raw = dtstart_raw
            dtend_dt = dtstart_dt

        duration_value = duration_minutes(dtstart_dt, dtend_dt, duration_raw)
        if missing_dtend and dtstart_raw and not duration_raw:
            duration_value = "0"

        timezone_name, utc_offset, is_dst = timezone_fields(
            dtstart_key, dtstart_raw, dtstart_dt
        )

        row = {
            "summary": first(base_props, "SUMMARY"),
            "dtstart": format_dt(dtstart_dt) or dtstart_raw,
            "dtend": format_dt(dtend_dt) or dtend_raw,
            "duration": duration_value,
            "location": first(base_props, "LOCATION"),
            "timezone": tz_label(dtstart_key, dtstart_raw, dtstart_dt),
            "timezone_name": timezone_name,
            "utc_offset": utc_offset,
            "is_dst": is_dst,
        }
        rows.append(row)

    return rows


def resolve_input_path(cli_input: str | None, cwd: Path) -> Path:
    """Resolve input .ics path with fallback priority.

    Order:
      1) --input argument
      2) first *.ics in current directory
      3) NOTEBOOK_ICS_FALLBACK
    """
    if cli_input:
        p = Path(cli_input)
        return p if p.is_absolute() else (cwd / p)

    local_ics_files = sorted(cwd.glob("*.ics"))
    if local_ics_files:
        return local_ics_files[0]

    if NOTEBOOK_ICS_FALLBACK.exists():
        return NOTEBOOK_ICS_FALLBACK

    raise FileNotFoundError("No .ics input file found. Provide one with --input.")


def main() -> None:
    """CLI entry point: read .ics, extract features, write .xlsx."""
    parser = argparse.ArgumentParser(
        description="Convert ICS VEVENT data to XLSX while preserving all event properties."
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to .ics file. Defaults to first .ics in current folder, then notebook fallback path.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="calendar_events_to_be_labeled.xlsx",
        help="Output .xlsx filename (written to current folder if relative).",
    )
    args = parser.parse_args()

    cwd = Path.cwd()
    input_path = resolve_input_path(args.input, cwd)
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = cwd / output_path

    ics_text = input_path.read_text(encoding="utf-8")
    rows = build_rows(ics_text)
    if not rows:
        raise ValueError(f"No VEVENT entries found in {input_path}")

    # Keep stable column order for downstream labeling/model scripts.
    df = pd.DataFrame(
        rows,
        columns=[
            "summary",
            "dtstart",
            "dtend",
            "duration",
            "location",
            "timezone",
            "timezone_name",
            "utc_offset",
            "is_dst",
        ],
    )

    # Keep values as strings to avoid Excel timezone-type limitations.
    for col in df.columns:
        df[col] = df[col].astype(str)

    df.to_excel(output_path, index=False)
    print(f"Wrote {len(df)} events to: {output_path}")


if __name__ == "__main__":
    main()
