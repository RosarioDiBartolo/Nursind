from __future__ import annotations

import argparse
import os
from typing import Any, Iterable

import holidays
import pandas as pd

from drive_scripts.fs_utils import ensure_parent_dir
from drive_scripts.logging_utils import get_logger, setup_logging
from drive_scripts.map_index_service import MapIndex
from drive_scripts.names import safe_name

logger = get_logger()
_HOLIDAY_CACHE: dict[int, set] = {}


def _italian_holidays_for_years(years: Iterable[int]) -> set:
    dates: set = set()
    for year in years:
        year_i = int(year)
        if year_i not in _HOLIDAY_CACHE:
            _HOLIDAY_CACHE[year_i] = set(holidays.country_holidays("IT", years=year_i).keys())
        dates.update(_HOLIDAY_CACHE[year_i])
    return dates


def _normalize_employee(name: str | None) -> str:
    return " ".join((name or "").strip().lower().split()) or "unknown"


def _employee_key(name: str | None, employee_id: str | None) -> str:
    if employee_id:
        return f"id:{employee_id}"
    norm = _normalize_employee(name)
    return f"name:{norm}"


def _group_files_by_employee(files: list[Any]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in files:
        name = getattr(item, "employee", None) or "unknown"
        employee_id = getattr(item, "employee_id", None)
        key = _employee_key(name, employee_id)
        if key not in grouped:
            grouped[key] = {
                "employee": name,
                "employee_id": employee_id,
                "files": [],
                "key": key,
            }
        grouped[key]["files"].append(item)
    return list(grouped.values())


def _expected_pairs_path(
    index_path: str, emp_name: str, file_name: str | None, file_id: str | None
) -> str:
    base_dir = os.path.dirname(os.path.abspath(index_path))
    safe_emp = safe_name(emp_name or "unknown")
    base_name = safe_name(file_name or "unknown.pdf")
    if not base_name.lower().endswith(".pdf"):
        base_name = f"{base_name}.pdf"
    if file_id:
        file_tag = f"{os.path.splitext(base_name)[0]}__{file_id[:8]}"
    else:
        file_tag = os.path.splitext(base_name)[0]
    return os.path.abspath(os.path.join(base_dir, safe_emp, file_tag, "pairs.csv"))


def _to_datetime_series(values: pd.Series) -> pd.Series:
    try:
        return pd.to_datetime(values, errors="coerce", format="mixed")
    except TypeError:
        return pd.to_datetime(values, errors="coerce")


def _close_incomplete_pairs(df: pd.DataFrame, max_gap_hours: float = 16.0) -> pd.DataFrame:
    if df.empty:
        return df
    if "entry_ts" not in df.columns or "exit_ts" not in df.columns:
        return df

    working = df.copy()
    working["entry_ts"] = _to_datetime_series(working["entry_ts"])
    working["exit_ts"] = _to_datetime_series(working["exit_ts"])

    complete_mask = working["entry_ts"].notna() & working["exit_ts"].notna()
    closed_rows = working.loc[complete_mask].copy()

    incomplete = working.loc[~complete_mask].copy()
    events: list[tuple[pd.Timestamp, str, pd.Series]] = []
    for _, row in incomplete.iterrows():
        if pd.notna(row.get("entry_ts")):
            events.append((row["entry_ts"], "entry", row))
        elif pd.notna(row.get("exit_ts")):
            events.append((row["exit_ts"], "exit", row))

    events.sort(key=lambda item: item[0])
    pending_entry: pd.Series | None = None
    max_gap = pd.Timedelta(hours=max_gap_hours)

    for ts, kind, row in events:
        if kind == "entry":
            pending_entry = row
            continue
        if pending_entry is None:
            continue

        entry_ts = pending_entry["entry_ts"]
        exit_ts = ts
        if exit_ts < entry_ts:
            exit_ts = exit_ts + pd.Timedelta(days=1)
        if max_gap_hours > 0 and (exit_ts - entry_ts) > max_gap:
            pending_entry = None
            continue

        merged = pending_entry.copy()
        merged["exit_ts"] = exit_ts
        closed_rows = pd.concat([closed_rows, pd.DataFrame([merged])], ignore_index=True)
        pending_entry = None

    return closed_rows.reset_index(drop=True)


def _parse_years(spec: str | None) -> list[int]:
    if not spec:
        return []
    if spec.strip().lower() in {"auto"}:
        return []
    tokens = [token.strip() for token in spec.split(",") if token.strip()]
    years: set[int] = set()
    for token in tokens:
        if ":" in token:
            start_s, end_s = token.split(":", 1)
            start = int(start_s)
            end = int(end_s)
            if end < start:
                start, end = end, start
            years.update(range(start, end + 1))
        else:
            years.add(int(token))
    return sorted(years)


def _count_by_year(
    df: pd.DataFrame,
    mask: pd.Series,
    years: Iterable[int],
) -> dict[int, int]:
    counts: dict[int, int] = {year: 0 for year in years}
    if df.empty:
        return counts
    filtered = df.loc[mask]
    if filtered.empty:
        return counts
    grouped = filtered.groupby("anno").size()
    for year, value in grouped.items():
        if int(year) in counts:
            counts[int(year)] += int(value)
    return counts


def build_turni_summary(
    index_path: str,
    years: Iterable[int] | None,
    *,
    close_gap_hours: float = 16.0,
    employee_filter: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int], list[int]]:
    report = MapIndex.load_index(index_path, strict=True, allow_legacy=True)
    employees = _group_files_by_employee(list(report.files.values()))
    total_employees = len(employees)
    if employee_filter:
        norm = _normalize_employee(employee_filter)
        employees = [
            emp for emp in employees if _normalize_employee(emp.get("employee")) == norm
        ]
        if not employees:
            logger.warning("Nessun dipendente trovato per '%s'", employee_filter)
            return [], {
                "dipendenti": total_employees,
                "dipendenti_inclusi": 0,
                "file_totali": 0,
                "file_mancanti": 0,
                "file_errori": 0,
                "file_colonne_mancanti": 0,
                "file_senza_turni": 0,
            }, []

    rows: list[dict[str, Any]] = []
    years_list = list(years) if years else []
    auto_years = not years_list
    file_totali = 0
    file_mancanti = 0
    file_errori = 0
    file_colonne_mancanti = 0
    file_senza_turni = 0
    min_year: int | None = None
    max_year: int | None = None

    employee_rollups: list[dict[str, Any]] = []

    for emp in employees:
        emp_name = emp.get("employee", "unknown")
        emp_norm = _normalize_employee(emp_name)
        total_shifts = 0

        counts_f: dict[int, int] = {}
        counts_p: dict[int, int] = {}
        counts_n: dict[int, int] = {}

        for inc in emp.get("files", []):
            file_totali += 1
            pairs_rel = None
            outputs = getattr(inc, "outputs", None)
            if outputs:
                pairs_rel = getattr(outputs, "pairs_csv", None)
            expected_path = None
            if pairs_rel:
                expected_path = os.path.abspath(
                    os.path.join(os.path.dirname(os.path.abspath(index_path)), pairs_rel)
                )
            if not expected_path:
                expected_path = _expected_pairs_path(
                    index_path,
                    emp_name,
                    getattr(inc, "file_name", None),
                    getattr(inc, "file_id", None),
                )
            if not os.path.exists(expected_path):
                file_mancanti += 1
                continue

            try:
                df = pd.read_csv(expected_path)
            except Exception:
                file_errori += 1
                continue

            if "entry_ts" not in df.columns:
                file_colonne_mancanti += 1
                continue

            closed_df = _close_incomplete_pairs(df, max_gap_hours=close_gap_hours)
            if closed_df.empty:
                file_senza_turni += 1
                continue

            closed_df["entry_ts"] = _to_datetime_series(closed_df["entry_ts"])
            closed_df["exit_ts"] = _to_datetime_series(closed_df["exit_ts"])
            closed_df["duration"] = closed_df["exit_ts"] - closed_df["entry_ts"]

            valid = closed_df.dropna(subset=["entry_ts", "exit_ts", "duration"])
            valid = valid[valid["duration"] >= pd.Timedelta(0)]
            if valid.empty:
                file_senza_turni += 1
                continue

            valid = valid.copy()
            valid["anno"] = valid["entry_ts"].dt.year
            anno_series = valid["anno"].dropna()
            if not anno_series.empty:
                local_min = int(anno_series.min())
                local_max = int(anno_series.max())
                min_year = local_min if min_year is None else min(min_year, local_min)
                max_year = local_max if max_year is None else max(max_year, local_max)

            total_shifts += len(valid)

            years_in_file = sorted({int(y) for y in valid["anno"].dropna().unique()})
            holiday_dates = _italian_holidays_for_years(years_in_file)

            turno_norm = (
                valid.get("turno", pd.Series(index=valid.index, dtype="object"))
                .fillna("")
                .astype(str)
                .str.strip()
                .str.lower()
            )
            mask_n = turno_norm.str.contains("notte", na=False)
            mask_p = turno_norm.str.contains("pomeriggio", na=False)
            mask_f = (valid["entry_ts"].dt.dayofweek == 6) | (
                valid["entry_ts"].dt.date.isin(holiday_dates)
            )

            for year, value in valid.loc[mask_f].groupby("anno").size().items():
                year_i = int(year)
                counts_f[year_i] = counts_f.get(year_i, 0) + int(value)
            for year, value in valid.loc[mask_p].groupby("anno").size().items():
                year_i = int(year)
                counts_p[year_i] = counts_p.get(year_i, 0) + int(value)
            for year, value in valid.loc[mask_n].groupby("anno").size().items():
                year_i = int(year)
                counts_n[year_i] = counts_n.get(year_i, 0) + int(value)

        employee_rollups.append(
            {
                "nome_ricorrente": emp_norm,
                "totale": int(total_shifts),
                "counts_f": counts_f,
                "counts_p": counts_p,
                "counts_n": counts_n,
            }
        )

    if auto_years:
        if min_year is None or max_year is None:
            years_list = []
        else:
            years_list = list(range(min_year, max_year + 1))

    def build_row(label: str, counts: dict[int, int], totale: int, nome: str) -> dict[str, Any]:
        row: dict[str, Any] = {
            "nome_ricorrente": nome,
            "turno": label,
        }
        for year in years_list:
            row[str(year)] = counts.get(year, 0)
        row["tot_x_turno"] = int(sum(row[str(year)] for year in years_list))
        row["totale"] = int(totale)
        return row

    for rollup in employee_rollups:
        rows.append(
            build_row("F", rollup["counts_f"], rollup["totale"], rollup["nome_ricorrente"])
        )
        rows.append(
            build_row("P", rollup["counts_p"], rollup["totale"], rollup["nome_ricorrente"])
        )
        rows.append(
            build_row("N", rollup["counts_n"], rollup["totale"], rollup["nome_ricorrente"])
        )

    stats = {
        "dipendenti": total_employees,
        "dipendenti_inclusi": len(employees),
        "file_totali": file_totali,
        "file_mancanti": file_mancanti,
        "file_errori": file_errori,
        "file_colonne_mancanti": file_colonne_mancanti,
        "file_senza_turni": file_senza_turni,
    }
    return rows, stats, years_list


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Genera un riepilogo turni per dipendente (formato Excel-like)."
    )
    parser.add_argument(
        "--index",
        required=True,
        help="Path to included.index.json (MapIndex)",
    )
    parser.add_argument(
        "--output",
        default="output/turni_summary.csv",
        help="Output CSV (default: output/turni_summary.csv)",
    )
    parser.add_argument(
        "--years",
        default="2016:2025",
        help="Anni da includere (es. 2016:2025 o 2016,2017,2018)",
    )
    parser.add_argument(
        "--close-gap-hours",
        type=float,
        default=16.0,
        help="Max ore tra entrata/uscita per chiudere coppie incomplete (default: 16.0)",
    )
    parser.add_argument("--employee", help="Filtra per dipendente (normalizzato)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    ensure_parent_dir(args.output)

    years = _parse_years(args.years)
    if not years:
        raise SystemExit("Specificare almeno un anno valido con --years")

    anni_log = ",".join(str(y) for y in years) if years else "auto"
    logger.info(
        "Avvio riepilogo turni: index=%s anni=%s close_gap_hours=%s",
        args.index,
        anni_log,
        args.close_gap_hours,
    )

    rows, stats, years_used = build_turni_summary(
        args.index,
        years,
        close_gap_hours=args.close_gap_hours,
        employee_filter=args.employee,
    )
    columns = ["nome_ricorrente", "turno"] + [str(y) for y in years_used] + [
        "tot_x_turno",
        "totale",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(args.output, index=False)
    logger.info("Riepilogo turni salvato in %s", args.output)
    anni_out = ",".join(str(y) for y in years_used) if years_used else "nessun_anno"
    logger.info("Anni usati: %s", anni_out)
    logger.info(
        "Statistiche: dipendenti=%s inclusi=%s file_totali=%s mancanti=%s errori=%s colonne_mancanti=%s senza_turni=%s righe=%s",
        stats["dipendenti"],
        stats["dipendenti_inclusi"],
        stats["file_totali"],
        stats["file_mancanti"],
        stats["file_errori"],
        stats["file_colonne_mancanti"],
        stats["file_senza_turni"],
        len(rows),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
