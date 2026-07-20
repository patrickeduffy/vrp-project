VRP Repository Documentation Cleanup v1
=======================================

This package cleans the uploaded docs folder and updates the root README.
It does not modify production code, model parameters, config, data, or Git history.

Apply
-----
1. Extract this package.
2. Open PowerShell in the extracted directory.
3. Run:

   Set-ExecutionPolicy -Scope Process Bypass
   .\APPLY_DOCS_CLEANUP.ps1

The script requires a clean Git working tree, validates the immutable model-lock
files, creates a timestamped backup ZIP in C:\Users\patri, applies the cleanup,
and leaves the changes uncommitted.

Verify
------

   .\VERIFY_DOCS_CLEANUP.ps1

Then inspect:

   cd C:\Users\patri\vrp_project
   git status
   git diff -- README.md docs

Suggested commit after review
-----------------------------

   git add README.md docs
   git commit -m "Clean and consolidate VRP documentation"
   git push origin main
