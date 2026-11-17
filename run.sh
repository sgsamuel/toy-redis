#!/bin/sh
set -eu
exec uv run --quiet -m app.main "$@"
