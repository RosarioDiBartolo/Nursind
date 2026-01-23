param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("scan", "filter", "test", "help")]
    [string]$Task,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = "Stop"

function Show-Help {
    Write-Host "Usage:"
    Write-Host "  .\\tasks.ps1 scan   --root <FOLDER_ID> --out output -v"
    Write-Host "  .\\tasks.ps1 filter --manifest manifest.json --out output --report report.json -v"
    Write-Host "  .\\tasks.ps1 test"
}

switch ($Task) {
    "scan" {
        python -m drive_scanner.scan_directory @Args
    }
    "filter" {
        python -m drive_scanner.filter_scan @Args
    }
    "test" {
        pytest @Args
    }
    default {
        Show-Help
        exit 1
    }
}
