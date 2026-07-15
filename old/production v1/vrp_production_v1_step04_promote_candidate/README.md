# VRP Production v1 — Step 04 Promote Candidate

This step promotes a previously QA'd Step 03 candidate appended term-structure history into the canonical repaired history file.

It is intentionally conservative:

- Requires `--confirm-promote` to overwrite the canonical files.
- Creates timestamped backups before overwrite.
- Re-validates row counts, duplicate keys, tenor coverage, and positive variance fields.
- Writes audit reports to `data/audit/production_v1/`.

## Example

```powershell
cd "C:\Users\patri\vrp_project\production v1\vrp_production_v1_step04_promote_candidate"
py -m pip install -r requirements.txt
py run_step04_promote_candidate.py --project-root "C:\Users\patri\vrp_project" --start-date 20260626 --end-date 20260702 --confirm-promote
```
