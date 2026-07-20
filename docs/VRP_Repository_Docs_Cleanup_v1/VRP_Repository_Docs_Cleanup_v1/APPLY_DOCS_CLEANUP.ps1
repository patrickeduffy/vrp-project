param(
    [string]$ProjectRoot = "C:\Users\patri\vrp_project"
)

$ErrorActionPreference = "Stop"
$PackageRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$ReplacementRoot = Join-Path $PackageRoot "replacement"
$DocsRoot = Join-Path $ProjectRoot "docs"

Write-Host ("=" * 100)
Write-Host "VRP repository documentation cleanup"
Write-Host ("=" * 100)
Write-Host "Project root: $ProjectRoot"
Write-Host "Code/config/data changes: NONE"
Write-Host "Git commit: NONE"
Write-Host ""

if (-not (Test-Path (Join-Path $ProjectRoot ".git"))) {
    throw "Not a Git repository: $ProjectRoot"
}

Push-Location $ProjectRoot
try {
    $StatusBefore = git status --short --untracked-files=all
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to read Git status."
    }
    if ($StatusBefore) {
        throw "Working tree is not clean. Commit, stash, or remove current changes before applying cleanup.`n$StatusBefore"
    }

    $RequiredPaths = @(
        "config\vrp_corsi_intraday_hybrid_v2_lock.json",
        "config\vrp_corsi_intraday_hybrid_v2_production_config.json",
        "config\vrp_hybrid_v2_eod_runtime_config.json",
        "notebooks\vrp_hybrid_v2_eod_pipeline.py",
        "notebooks\vrp_hybrid_v2_signal_publish.py",
        "notebooks\vrp_hybrid_v2_health_check.py",
        "notebooks\streamlit_vrp_hybrid_v2_eod.py",
        "tests\test_eod_audit_phase1.py",
        "tests\test_eod_audit_phase2.py"
    )
    foreach ($Relative in $RequiredPaths) {
        $Path = Join-Path $ProjectRoot $Relative
        if (-not (Test-Path $Path)) {
            throw "Required active repository file is missing: $Relative"
        }
    }

    $LockPath = Join-Path $DocsRoot "VRP_Corsi_Intraday_Hybrid_v2_Model_Lock.docx"
    $ReleasePath = Join-Path $DocsRoot "VRP_Corsi_Intraday_Hybrid_v2_Release_Notes.txt"
    if (-not (Test-Path $LockPath)) { throw "Immutable model-lock DOCX is missing." }
    if (-not (Test-Path $ReleasePath)) { throw "Immutable release notes are missing." }

    $ExpectedLockHash = "af6397fc97ee0759cfc36b93c061b869a70ad4516d1e9f7ccb2222c02a6d22fe"
    $ExpectedReleaseHash = "e0b2f7c6bcf6c309bdda9a04c6808a0f820155d1c2043e3a4872cbbdfee00b37"
    $ObservedLockHash = (Get-FileHash $LockPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $ObservedReleaseHash = (Get-FileHash $ReleasePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($ObservedLockHash -ne $ExpectedLockHash) {
        throw "Model-lock DOCX hash differs from the reviewed immutable artifact. Cleanup aborted."
    }
    if ($ObservedReleaseHash -ne $ExpectedReleaseHash) {
        throw "Release-notes hash differs from the reviewed immutable artifact. Cleanup aborted."
    }

    $Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $BackupRoot = Join-Path $env:USERPROFILE "vrp_docs_cleanup_backup_$Timestamp"
    $BackupZip = "$BackupRoot.zip"
    New-Item -ItemType Directory -Path $BackupRoot | Out-Null
    Copy-Item (Join-Path $ProjectRoot "README.md") (Join-Path $BackupRoot "README.md") -Force
    Copy-Item $DocsRoot (Join-Path $BackupRoot "docs") -Recurse -Force
    Compress-Archive -Path (Join-Path $BackupRoot "*") -DestinationPath $BackupZip -Force
    Remove-Item $BackupRoot -Recurse -Force
    Write-Host "Backup created: $BackupZip"

    $RemoveFiles = @(
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

    foreach ($Name in $RemoveFiles) {
        $Path = Join-Path $DocsRoot $Name
        if (Test-Path $Path) {
            Remove-Item $Path -Force
            Write-Host "Removed: docs\$Name"
        }
    }

    Copy-Item (Join-Path $ReplacementRoot "README.md") (Join-Path $ProjectRoot "README.md") -Force
    foreach ($Name in @(
        "DOCUMENTATION_INDEX.md",
        "PRODUCTION_OPERATIONS.md",
        "CURRENT_STATUS_AND_ROADMAP.md",
        "MODEL_LOCK_AND_OPERATIONAL_SUPERSESSION.md"
    )) {
        Copy-Item (Join-Path $ReplacementRoot "docs\$Name") (Join-Path $DocsRoot $Name) -Force
        Write-Host "Installed: docs\$Name"
    }

    $FinalLockHash = (Get-FileHash $LockPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $FinalReleaseHash = (Get-FileHash $ReleasePath -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($FinalLockHash -ne $ExpectedLockHash -or $FinalReleaseHash -ne $ExpectedReleaseHash) {
        throw "Immutable lock artifacts changed unexpectedly after cleanup. Restore from $BackupZip."
    }

    Write-Host ""
    Write-Host ("=" * 100)
    Write-Host "CLEANUP APPLIED - REVIEW BEFORE COMMIT"
    Write-Host ("=" * 100)
    git status --short
    Write-Host ""
    Write-Host "Backup: $BackupZip"
    Write-Host "No commit was created."
}
finally {
    Pop-Location
}
