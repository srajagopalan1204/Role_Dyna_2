# Roles Dyna – Phase 1 Processor

**Goal:** Implement Phase 1 of the Roles report creation pipeline.

## What Phase 1 Does
- Reads the RAW input workbook and detects the header row.
- Normalizes titles using `Title_Normalization`.
- Maps Primary/Secondary Specializations and Primary Functional Group (short form).
- Uses group-specific handler tabs (`OPS_Hndl`, `FIN_Hndl`, etc.) to derive `ROLE_Nm`.
- Enriches with location size (if `Loc_Sz` present) and operator id from `Oper_list_saso` (if present).
- Writes an Excel file with tabs:
  - `Master_Roles_by_User` (core output)
  - `Summary` (starter counts for Phase 1)
  - `Run_Rep` (run metadata + timing)
  - `Excp_Rep` (rows that could not be processed)
- Highlights cells containing the word `Error`.
- Appends to a rolling log file in `/workspaces/Roles_Dyna/Logs`.

## Not in Phase 1
- Final/complete Summary definitions (placeholders included).
- ADDL_Keys extraction logic (columns created when the tab exists, values left blank for now).
- ROLE_Ordering application (to be added post-cleanup/specs).

## Usage (GitHub Codespaces)
```bash
pip install -r requirements.txt
python roles_phase1.py --version v_1_0
# Optional overrides:
# python roles_phase1.py --raw "/path/to/raw.xlsx" --vars "/path/to/Var_Trns_Mstr.xlsx" --fgp "OPS" --errors-only
```

## CLI Options
- `--raw` Path to RAW input workbook (default from config.json).
- `--vars` Path to Translation Master workbook (default from config.json).
- `--fgp` Process only a specific Primary Functional Group (short or long name ok).
- `--errors-only` Emit only rows that contain errors to `Master_Roles_by_User` (always write `Excp_Rep`).
- `--version` Version label to bake into filenames and the Run_Rep.
- `--outputs-dir` Override outputs directory (default from config.json).
- `--logs-dir` Override logs directory (default from config.json).

## Output Files
- Excel workbook: `Roles_Output_{version}_{DDMMYY_HH_MM}.xlsx`
- Log file: `/workspaces/Roles_Dyna/Logs/Roles_Dyn_cons_Log.txt`

## Notes
- The script detects the correct header row by searching for these headers (case-insensitive):
  `Location`, `Status`, `Postion/Title` or `Position/Title`, `Primary Functional Group`, `Secondary Functional Group`.
- Rows that *begin with* (case-insensitive) `Planning on Retiring`, `Termed`, or `New Hires` are skipped.
- Missing mappings are annotated as `Error: <field> from <tab> not found` and highlighted in Excel.
