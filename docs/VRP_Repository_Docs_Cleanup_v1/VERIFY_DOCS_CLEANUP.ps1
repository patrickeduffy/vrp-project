param(
    [string]$ProjectRoot = "C:\Users\patri\vrp_project"
)

$ErrorActionPreference = "Stop"
$DocsRoot = Join-Path $ProjectRoot "docs"

$ExpectedDocs = @(
    "CURRENT_STATUS_AND_ROADMAP.md",
    "DOCUMENTATION_INDEX.md",
    "MODEL_LOCK_AND_OPERATIONAL_SUPERSESSION.md",
    "PRODUCTION_OPERATIONS.md",
    "VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx",
    "VRP_Corsi_Intraday_Hybrid_v2_Release_Notes.txt"
)

$ObsoleteDocs = @(
    "EOD_DASHBOARD_INTEGRATION_ADDENDUM.txt",
    "pip_freeze_current.txt",
    "README.md",
    "requirements.txt",
    "thetadata_vix_style_variance_process_v0_1.docx",
    "VRP_Corsi_Intraday_Hybrid_v2_Production_Runbook.docx",
    "VRP_Corsi_Intraday_Hybrid_v2_README.txt",
    "VRP_Corsi_Intraday_Hybrid_v2_Reproduction_Package.zip",
    "VRP_DOCS_REVIEW_REPORT.md",
    "VRP_Hybrid_v2_EOD_Dashboard_README.txt",
    "vrp_corsi_intraday_hybrid_v2_lock.json",
    "vrp_corsi_intraday_hybrid_v2_production_config.json"
)

$Failures = @()
foreach ($Name in $ExpectedDocs) {
    if (-not (Test-Path (Join-Path $DocsRoot $Name))) {
        $Failures += "Missing expected document: $Name"
    }
}
foreach ($Name in $ObsoleteDocs) {
    if (Test-Path (Join-Path $DocsRoot $Name)) {
        $Failures += "Obsolete document still present: $Name"
    }
}

$LockHash = (Get-FileHash (Join-Path $DocsRoot "VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx") -Algorithm SHA256).Hash.ToLowerInvariant()
$ReleaseHash = (Get-FileHash (Join-Path $DocsRoot "VRP_Corsi_Intraday_Hybrid_v2_Release_Notes.txt") -Algorithm SHA256).Hash.ToLowerInvariant()
if ($LockHash -ne "af6397fc97ee0759cfc36b93c061b869a70ad4516d1e9f7ccb2222c02a6d22fe") {
    $Failures += "Immutable model-lock DOCX hash mismatch."
}
if ($ReleaseHash -ne "e0b2f7c6bcf6c309bdda9a04c6808a0f820155d1c2043e3a4872cbbdfee00b37") {
    $Failures += "Immutable release-notes hash mismatch."
}

if ($Failures.Count -gt 0) {
    $Failures | ForEach-Object { Write-Error $_ }
    exit 1
}

Write-Host "DOCS CLEANUP VERIFICATION: PASS"
Write-Host "Expected active documents: $($ExpectedDocs.Count)"
Write-Host "Immutable model-lock artifacts: unchanged"
Write-Host "Obsolete documents: absent"
