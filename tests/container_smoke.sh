#!/usr/bin/env bash
set -euo pipefail

image="${1:-embyx-web:ci}"

entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "$image")"
expected_entrypoint='["/app/.venv/bin/python","-m","embyx_web"]'
if [[ "$entrypoint" != "$expected_entrypoint" ]]; then
  echo "unexpected image entrypoint: $entrypoint" >&2
  exit 1
fi

docker run --rm -i --entrypoint /app/.venv/bin/python "$image" - <<'PY'
import asyncio
from importlib.util import find_spec
import os
from pathlib import Path
import sys

import embyx_web
import fastapi
import uvicorn
from embyx_web.runtime_adapters import load_runtime_adapters


def require_origin(module_name: str, root: Path) -> None:
    spec = find_spec(module_name)
    assert spec is not None, f'{module_name} is not importable'
    assert spec.origin is not None, f'{module_name} has no file origin'
    Path(spec.origin).resolve().relative_to(root)


web_root = Path('/opt/embyx-web')
runtime_root = Path('/app')

Path(embyx_web.__file__).resolve().relative_to(web_root)
Path(fastapi.__file__).resolve().relative_to(web_root)
Path(uvicorn.__file__).resolve().relative_to(web_root)
require_origin('src', runtime_root)
require_origin('tap', Path('/app/.venv'))

assert sys.prefix == '/app/.venv'
assert sys.path[0] == ''
assert '/opt/embyx-web' in sys.path
assert '/app' in sys.path
assert any(path.startswith('/app/.venv/lib/python3.13/site-packages') for path in sys.path)
assert os.environ['EMBYX_WEB_RUNTIME_ROOT'] == '/app'
assert os.environ['EMBYX_WEB_RUNTIME_MODULE'] == 'src.embyx_runtime.fill_actor_api'
assert Path('/opt/embyx-web/embyx_web/static/index.html').is_file()

runtime = load_runtime_adapters(
    runtime_root=Path(os.environ['EMBYX_WEB_RUNTIME_ROOT']),
    module_name=os.environ['EMBYX_WEB_RUNTIME_MODULE'],
)
asyncio.run(runtime.aclose())
PY
