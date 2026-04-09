from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader

router = APIRouter()

_TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"
_jinja_env = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)), autoescape=False)


@router.get("/", response_class=HTMLResponse)
async def ui(request: Request) -> HTMLResponse:
    base = request.scope.get("root_path", "").rstrip("/")
    template = _jinja_env.get_template("browser.html")
    return HTMLResponse(content=template.render(base_path=base))
