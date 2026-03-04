param(
  [string]$SamplePath = "samples\tipo-2"
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
$SamplePath = $SamplePath.TrimEnd("\", "/")
$sampleRoot = if ([System.IO.Path]::IsPathRooted($SamplePath)) {
  $SamplePath
} else {
  Join-Path $repoRoot $SamplePath
}
$sampleName = Split-Path -Path $sampleRoot -Leaf

$pdfFolder = Join-Path $sampleRoot "pdfs"
$indexPath = Join-Path $pdfFolder "included.index.json"
$documentsDir = Join-Path $sampleRoot "documents"
$eventsDir = Join-Path $sampleRoot "events"
$employeeShiftsDir = Join-Path $sampleRoot "employee_shifts_from_raw"
$enrichedDir = Join-Path $sampleRoot "enriched"
$aggregatesDir = Join-Path $sampleRoot "aggregates"

Push-Location $repoRoot
try {
  & $python -m "src.drive_service.index.build_local_pdf_index" `
    --folder $pdfFolder `
    --name "included.index.json" `
    --identity $sampleName

  & $python -m "src.extract_documents_from_index" `
    --index $indexPath `
    --out $documentsDir `
    --verbose

  & $python -m "src.extract_events_from_documents" `
    --input-dir $documentsDir `
    --output-dir $eventsDir `
    --report-json (Join-Path $eventsDir "extract_events.report.json") `
    --verbose

  & $python -m "src.filter_midnight_events" `
    --input-dir $eventsDir `
    --events-name "events.csv" `
    --out-name "events.cleaned.csv" `
    --report-json (Join-Path $eventsDir "events.clean_midnight.report.json") `
    --removed-csv (Join-Path $eventsDir "events.midnight_removed.csv") `
    --verbose

  & $python -m "src.pair_employee_events" `
    --input-dir $eventsDir `
    --output-dir $employeeShiftsDir `
    --events-name "events.cleaned.csv" `
    --report-json (Join-Path $employeeShiftsDir "pair_employee_events.report.json") `
    --max-gap-hours 16 `
    --verbose

  & $python -m "src.turni_enrichment" `
    --input-dir $employeeShiftsDir `
    --out-dir $enrichedDir `
    --stats-json (Join-Path $enrichedDir "turni_enrichment.stats.json") `
    --verbose

  & $python -m "src.turni_employee_summary" `
    --enriched-dir $enrichedDir `
    --out (Join-Path $aggregatesDir "turni_employee_summary.csv") `
    --report-json (Join-Path $aggregatesDir "turni_employee_summary.report.json") `
    --format "csv" `
    --verbose
}
finally {
  Pop-Location
}
