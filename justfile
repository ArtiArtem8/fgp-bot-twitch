set windows-shell := ["pwsh.exe", "-NoLogo", "-NoProfile", "-Command"]

default:
    @just --list

sync:
    uv sync --locked

hooks:
    uv run --locked prek install

hooks-check:
    uv run --locked prek validate-config prek.toml

fix:
    uv run --locked ruff check --fix .
    uv run --locked ruff format .

fix-unsafe:
    uv run --locked ruff check --fix --unsafe-fixes .
    uv run --locked ruff format .

format:
    uv run --locked ruff format --check .

lint:
    uv run --locked ruff check .

type:
    uv run --locked ty check

deps:
    uv run --locked deptry .

arch:
    uv run --locked tach check

test:
    uv run --locked nose2 --no-user-config --plugin nose2.plugins.mp -N 6 -s tests -t . -q

cov:
    uv run --locked coverage erase
    uv run --locked coverage run -m nose2 --no-user-config --plugin nose2.plugins.mp -N 6 -s tests -t . -q
    uv run --locked coverage combine
    uv run --locked coverage report
    uv run --locked coverage json -o - | uv run --locked python -c "import json,sys; v=json.load(sys.stdin)['totals']['percent_branches_covered']; print('Branch coverage: %.1f%% (minimum 76.0%%)' % v); sys.exit(v < 76.0)"

audit:
    uv audit --frozen

secrets:
    gitleaks git . --log-opts="--all --full-history" --redact=100 --no-banner --ignore-gitleaks-allow

actions:
    actionlint
    zizmor --pedantic .github/workflows

check: hooks-check format lint type deps arch test audit

ci: hooks-check format lint type deps arch cov audit secrets actions
