from __future__ import annotations

import argparse
import os
from typing import Any, Iterable

import pandas as pd

from drive_scripts.fs_utils import ensure_parent_dir
from drive_scripts.logging_utils import get_logger, setup_logging
from drive_scripts.map_index_service import MapIndex
from shift_services import (
    EmployeeGrouper,
    ItalianHolidayCalendar,
    PairsCloser,
    PairsLoader,
    PairsPathResolver,
    ShiftClassifier,
    TurnoMatchPolicy,
)

logger = get_logger()


def _normalize_employee(name: str | None) -> str:
    return " ".join((name or "").strip().lower().split()) or "unknown"


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
    hours_threshold: float = 6.0,
    employee_filter: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int], list[int]]:
    report = MapIndex.load_index(index_path, strict=True, allow_legacy=True)
    grouper = EmployeeGrouper(_normalize_employee)
    employees = grouper.group(list(report.files.values()))
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

    resolver = PairsPathResolver(index_path)
    loader = PairsLoader(resolver)
    closer = PairsCloser(max_gap_hours=close_gap_hours)
    classifier = ShiftClassifier(
        calendar=ItalianHolidayCalendar(),
        include_holidays=True,
        match_policy=TurnoMatchPolicy(mode="contains"),
    )

    total_files_expected = sum(len(emp.get("files", [])) for emp in employees)
    files_seen = 0
    log_every = 25

    for emp_index, emp in enumerate(employees, start=1):
        emp_name = emp.get("employee", "unknown")
        emp_norm = _normalize_employee(emp_name)
        total_shifts = 0
        emp_files = emp.get("files", [])
        logger.info(
            "Dipendente %s/%s: %s (file=%s)",
            emp_index,
            len(employees),
            emp_name,
            len(emp_files),
        )

        counts_f: dict[int, int] = {}
        counts_p: dict[int, int] = {}
        counts_n: dict[int, int] = {}

        for inc in emp_files:
            file_totali += 1
            files_seen += 1
            if files_seen % log_every == 0 or files_seen == total_files_expected:
                logger.info("Progresso: %s/%s file", files_seen, total_files_expected)
            pairs_rel = None
            outputs = getattr(inc, "outputs", None)
            if outputs:
                pairs_rel = getattr(outputs, "pairs_csv", None)
            expected_path = resolver.resolve_pairs_path(
                emp_name,
                getattr(inc, "file_name", None),
                getattr(inc, "file_id", None),
                pairs_rel,
            )
            if not os.path.exists(expected_path):
                file_mancanti += 1
                continue

            try:
                df = loader.load_pairs(expected_path)
            except Exception:
                file_errori += 1
                continue

            if "entry_ts" not in df.columns:
                file_colonne_mancanti += 1
                continue

            closed_df = closer.close(df)
            if closed_df.empty:
                file_senza_turni += 1
                continue

            classified = classifier.classify(closed_df)
            valid = classified.dropna(subset=["entry_ts", "exit_ts", "duration"])
            valid = valid[valid["duration"] >= pd.Timedelta(0)]
            overtime_mask = valid["duration"] > pd.Timedelta(hours=hours_threshold)
            valid = valid.loc[overtime_mask]
            if valid.empty:
                file_senza_turni += 1
                continue

            valid = valid.copy()
            anno_series = valid["anno"].dropna()
            if not anno_series.empty:
                local_min = int(anno_series.min())
                local_max = int(anno_series.max())
                min_year = local_min if min_year is None else min(min_year, local_min)
                max_year = local_max if max_year is None else max(max_year, local_max)

            total_shifts += len(valid)

            mask_n = valid["is_night"]
            mask_p = valid["is_afternoon"]
            mask_f = valid["is_holiday"]

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
    parser.add_argument(
        "--hours",
        type=float,
        default=6.0,
        help="Soglia ore straordinario (default: 6.0)",
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
        hours_threshold=args.hours,
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
