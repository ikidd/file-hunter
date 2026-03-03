# Contributing to File Hunter

Thanks for your interest in contributing.

## Setting up

```bash
git clone https://github.com/zen-logic/file-hunter.git
cd file-hunter
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Run in demo mode to get sample data:

```bash
./filehunter --demo
```

## Code style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
pip install ruff
ruff check .
ruff format .
```

Please run both before submitting a PR.

## Architecture

- **Backend** — Python, Starlette, uvicorn, aiosqlite. No frameworks beyond Starlette.
- **Frontend** — vanilla HTML/CSS/JavaScript. No build step, no bundler, no npm.
- **Database** — SQLite in WAL mode. Schema migrations are additive only (`ALTER TABLE ADD COLUMN` in try/except).

Key directories:

| Directory | Contents |
|-----------|----------|
| `file_hunter/routes/` | HTTP route handlers |
| `file_hunter/services/` | Business logic |
| `file_hunter/db.py` | Database connection and schema |
| `static/js/` | Frontend JavaScript |
| `static/css/` | Stylesheets and themes |

## Guidelines

- Keep PRs focused — one feature or fix per PR.
- The UI must stay responsive during scans. Don't run expensive queries on the shared DB connection.
- Target scale is 10M+ files. Avoid per-row loops — use batch operations or single SQL statements.
- No new runtime dependencies without discussion first. The dependency footprint is intentionally small.
- Frontend changes: no frameworks, no build tools, no npm. Vanilla JS only.

## Reporting bugs

Use the [bug report template](https://github.com/zen-logic/file-hunter/issues/new?template=bug_report.yml). Include server logs if possible — most UI issues trace back to a 500 error.

## License

By contributing you agree that your contributions will be licensed under the MIT License.
