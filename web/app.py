"""Frontend ao vivo da POC Kafka.

Consome um topico e faz stream das mensagens para o browser via SSE.
Reusa build_consumer() de scripts/_common.py sem modifica-lo.
"""
from __future__ import annotations

import asyncio
import html
import json
import sys
import uuid
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from _common import TOPIC, build_consumer  # noqa: E402

app = FastAPI()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
app.mount(
    "/static",
    StaticFiles(directory=str(Path(__file__).parent / "static")),
    name="static",
)

NUM_PARTITIONS = 3


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "default_topic": TOPIC,
            "partitions": list(range(NUM_PARTITIONS)),
        },
    )


def _render_msg(partition: int, offset: int, key, value) -> str:
    key_txt = html.escape(str(key)) if key is not None else "<em>null</em>"
    value_txt = html.escape(json.dumps(value, ensure_ascii=False))
    return (
        f'<div class="msg" hx-swap-oob="afterbegin:#part-{partition}">'
        f'<span class="offset">#{offset}</span>'
        f'<span class="key">{key_txt}</span>'
        f'<span class="value">{value_txt}</span>'
        f"</div>"
    )


@app.get("/stream")
async def stream(topic: str = TOPIC, group: str | None = None):
    group_id = group or f"web-ui-{uuid.uuid4().hex[:8]}"
    consumer = build_consumer(group_id, topics=[topic], auto_offset_reset="latest")
    loop = asyncio.get_running_loop()

    async def event_generator():
        try:
            while True:
                batches = await loop.run_in_executor(
                    None, lambda: consumer.poll(timeout_ms=500)
                )
                for _tp, records in batches.items():
                    for msg in records:
                        yield {
                            "event": "message",
                            "data": _render_msg(
                                msg.partition, msg.offset, msg.key, msg.value
                            ),
                        }
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        finally:
            await loop.run_in_executor(None, consumer.close)

    return EventSourceResponse(event_generator())
