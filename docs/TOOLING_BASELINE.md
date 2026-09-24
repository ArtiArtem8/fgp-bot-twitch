# TOOLING BASELINE — Stage 1 (2026-09-24)

Снимок до исправлений исходного Python-кода. Числа ниже относятся к строгим правилам из `pyproject.toml` после настройки инструментов.

| Проверка | Результат |
|---|---|
| uv | 0.11.18; `uv lock --check` и `uv sync --locked` проходят; `uv.lock` содержит 36 пакетов |
| Ruff 0.16.8 | 20 файлов требуют форматирования; 2300 lint diagnostics |
| ty 0.0.83 | 95 diagnostics |
| deptry 0.25.1 | 0 issues |
| Tach 0.35.1 | 0 cycles, 0 boundary issues |
| unittest | 117 passed, 0 failed |
| coverage.py 7.16.1 | lines 88.2%, branches 75.0%, combined 85.4%; thresholds combined 85.0%, branch 75.0% |
| uv audit --frozen | 35 packages audited, 0 vulnerabilities |
| Gitleaks | 25 commits scanned, 0 findings (redaction enabled) |
| actionlint | 0 issues |
| zizmor --pedantic | 2 findings: anonymous job name, missing concurrency limit |

Другие standalone версии: prek 0.5.3, just 1.48.1, Gitleaks 8.30.1, actionlint 1.7.12, zizmor 1.30.1.
В этой миграции установлены prek, actionlint и zizmor; uv, just и Gitleaks уже были доступны.

## Ruff: breakdown by rule

| Rule | Count |
|---|---:|
| ANN001 | 32 |
| ANN002 | 1 |
| ANN003 | 5 |
| ANN201 | 145 |
| ANN202 | 9 |
| ANN204 | 2 |
| ANN401 | 10 |
| ARG001 | 5 |
| ASYNC109 | 2 |
| ASYNC110 | 1 |
| BLE001 | 2 |
| C901 | 12 |
| CPY001 | 22 |
| D100 | 18 |
| D101 | 35 |
| D102 | 197 |
| D103 | 28 |
| D105 | 2 |
| D107 | 15 |
| DOC201 | 2 |
| DOC501 | 7 |
| E225 | 216 |
| E226 | 43 |
| E227 | 1 |
| E231 | 664 |
| E261 | 4 |
| E306 | 9 |
| E501 | 150 |
| EM101 | 45 |
| EM102 | 16 |
| FBT001 | 6 |
| FBT002 | 2 |
| FBT003 | 6 |
| FURB162 | 1 |
| I001 | 6 |
| LOG001 | 1 |
| N818 | 1 |
| PLC0415 | 14 |
| PLC2801 | 1 |
| PLR0911 | 5 |
| PLR0912 | 6 |
| PLR0913 | 1 |
| PLR0914 | 2 |
| PLR0915 | 7 |
| PLR0916 | 1 |
| PLR0917 | 1 |
| PLR1702 | 7 |
| PLR2004 | 32 |
| PLR6201 | 2 |
| PLR6301 | 3 |
| PLW0108 | 1 |
| PLW0717 | 12 |
| PT009 | 233 |
| PT018 | 1 |
| PT027 | 25 |
| PTH105 | 1 |
| PYI034 | 1 |
| RUF001 | 30 |
| RUF027 | 3 |
| RUF029 | 1 |
| RUF201 | 14 |
| S101 | 8 |
| S105 | 1 |
| S106 | 1 |
| S311 | 1 |
| SIM102 | 2 |
| SIM117 | 2 |
| SLF001 | 18 |
| T201 | 35 |
| TC001 | 17 |
| TC003 | 2 |
| TRY003 | 58 |
| TRY004 | 1 |
| TRY300 | 4 |
| TRY301 | 15 |
| TRY400 | 5 |
| UP035 | 3 |

## ty: categories

| Rule | Count |
|---|---:|
| missing-type-argument | 34 |
| not-subscriptable | 21 |
| invalid-argument-type | 13 |
| unresolved-attribute | 12 |
| invalid-assignment | 7 |
| unsound-return-statement | 5 |
| call-non-callable | 1 |
| unknown-argument | 1 |
| unsound-assignment | 1 |

## Current module graph

`fgpbot.cli` depends on app/auth and lower-level services; app/commands/eventsub depend on Twitch, token, network, health and configuration modules. Tach has explicit current edges without artificial layers. Circular dependencies are forbidden. The module list and edges are in `tach.toml`.

## Stage 1 file inventory

- Added: `.pre-commit-config.yaml`, `justfile`, `tach.toml`, `.github/workflows/quality.yml`, `.github/dependabot.yml`, this baseline report.
- Changed: `pyproject.toml`, `uv.lock`, `setup.bat`, `.gitignore`, `README.md`, `docs/DESIGN.md`, `docs/GIT-SECURITY.md`.
- Deleted: `requirements.txt` (duplicates project dependencies), tracked `.githooks/pre-commit` (replaced by prek), `docs/FILES.sha256` (obsolete archive checksum list).
- Dev dependencies added: `coverage[toml]`, `deptry`, `tach`. Retained: `ruff`, `ty`. Removed: `basedpyright`.
- Removed pip fallback from setup. Runtime entry points and `.env` / `tokens.db` formats were not changed.

## Stage 2 order

Lock/config consistency → Ruff format → Ruff lint → ty → deptry → Tach → tests → coverage → audit → secrets → actionlint → zizmor.
