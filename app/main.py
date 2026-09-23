"""LinkDrop API: a small, self-hosted queue for permitted media downloads."""

from __future__ import annotations

import asyncio
import ipaddress
import mimetypes
import os
import socket
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import yt_dlp
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

DATA_DIR = Path(os.getenv("DOWNLOAD_DIR", "/data/downloads")).resolve()
MAX_BATCH = int(os.getenv("MAX_BATCH", "8"))
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "2"))
DOWNLOAD_TTL_SECONDS = int(os.getenv("DOWNLOAD_TTL_SECONDS", "86400"))


class AnalyzeRequest(BaseModel):
    url: str = Field(max_length=2_048)


class QueueItem(BaseModel):
    url: str = Field(max_length=2_048)
    format_id: str | None = Field(default=None, max_length=80)
    quality: Literal["best", "1080", "720", "480", "360"] = "best"


class QueueRequest(BaseModel):
    items: list[QueueItem] = Field(min_length=1, max_length=MAX_BATCH)


class Job(BaseModel):
    id: str
    source_url: str
    state: Literal["queued", "downloading", "complete", "failed"] = "queued"
    quality: str = "best"
    format_id: str | None = Field(default=None, exclude=True)
    progress: int = 0
    title: str | None = None
    filename: str | None = None
    error: str | None = None
    created_at: float = Field(default_factory=time.time)
    completed_at: float | None = None


jobs: dict[str, Job] = {}
work_queue: asyncio.Queue[str] = asyncio.Queue()
workers: list[asyncio.Task[None]] = []


def validate_public_url(value: str) -> str:
    """Reject malformed and obvious internal-network URLs before yt-dlp sees them."""
    parsed = urlparse(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise HTTPException(422, "Enter a valid http:// or https:// link.")
    if parsed.username or parsed.password:
        raise HTTPException(422, "Links with embedded credentials are not allowed.")
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(parsed.hostname, None)}
    except socket.gaierror as exc:
        raise HTTPException(422, "This hostname could not be resolved.") from exc
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if not ip.is_global:
            raise HTTPException(422, "Internal or private network addresses are not allowed.")
    return parsed.geturl()


def public_metadata(url: str) -> dict:
    options = {"quiet": True, "no_warnings": True, "noplaylist": True, "skip_download": True}
    with yt_dlp.YoutubeDL(options) as client:
        info = client.extract_info(url, download=False)
    formats: list[dict] = []
    seen: set[str] = set()
    for item in info.get("formats", []):
        height = item.get("height")
        format_id = item.get("format_id")
        if not height or not format_id or format_id in seen:
            continue
        seen.add(format_id)
        formats.append(
            {
                "id": str(format_id),
                "label": f"{height}p" + (" (MP4)" if item.get("ext") == "mp4" else ""),
                "height": height,
                "ext": item.get("ext"),
                "filesize": item.get("filesize") or item.get("filesize_approx"),
            }
        )
    formats.sort(key=lambda item: item["height"], reverse=True)
    return {
        "title": info.get("title") or "Untitled video",
        "thumbnail": info.get("thumbnail"),
        "duration": info.get("duration"),
        "formats": formats[:20],
    }


def run_download(job_id: str) -> None:
    job = jobs[job_id]
    job.state = "downloading"
    target_dir = DATA_DIR / job_id
    target_dir.mkdir(parents=True, exist_ok=True)

    def update_progress(event: dict) -> None:
        if event.get("status") == "downloading":
            total = event.get("total_bytes") or event.get("total_bytes_estimate")
            downloaded = event.get("downloaded_bytes", 0)
            if total:
                job.progress = min(99, int(downloaded * 100 / total))

    requested = job.format_id
    # Some hosts expose only a combined stream, so every quality must have a
    # combined-stream fallback instead of failing at the end of the job.
    format_selector = requested or (
        "bv*+ba/b" if job.quality == "best"
        else f"bv*[height<={job.quality}]+ba/b[height<={job.quality}]/b"
    )
    options = {
        "format": format_selector,
        "merge_output_format": "mp4",
        "outtmpl": str(target_dir / "%(title).120B [%(id)s].%(ext)s"),
        "noplaylist": True,
        "restrictfilenames": True,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [update_progress],
    }
    try:
        with yt_dlp.YoutubeDL(options) as client:
            info = client.extract_info(job.source_url, download=True)
            job.title = info.get("title") or job.title
        files = [path for path in target_dir.iterdir() if path.is_file() and not path.name.endswith(".part")]
        if not files:
            raise RuntimeError("The source did not return a downloadable file.")
        finished = max(files, key=lambda path: path.stat().st_mtime)
        job.filename = finished.name
        job.progress = 100
        job.state = "complete"
        job.completed_at = time.time()
    except Exception as exc:  # yt-dlp errors are made safe for the UI here.
        job.state = "failed"
        job.error = str(exc)[:300]


async def download_worker() -> None:
    while True:
        job_id = await work_queue.get()
        try:
            await asyncio.to_thread(run_download, job_id)
        finally:
            work_queue.task_done()


async def remove_expired_jobs() -> None:
    while True:
        await asyncio.sleep(3_600)
        cutoff = time.time() - DOWNLOAD_TTL_SECONDS
        for job_id, job in list(jobs.items()):
            if job.completed_at and job.completed_at < cutoff:
                directory = DATA_DIR / job_id
                for file in directory.glob("*"):
                    file.unlink(missing_ok=True)
                directory.rmdir()
                jobs.pop(job_id, None)


@asynccontextmanager
async def lifespan(_: FastAPI):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    workers.extend(asyncio.create_task(download_worker()) for _ in range(MAX_CONCURRENT))
    workers.append(asyncio.create_task(remove_expired_jobs()))
    yield
    for worker in workers:
        worker.cancel()


app = FastAPI(title="LinkDrop", lifespan=lifespan)


@app.post("/api/analyze")
async def analyze(request: AnalyzeRequest):
    url = validate_public_url(request.url)
    try:
        return await asyncio.to_thread(public_metadata, url)
    except Exception as exc:
        raise HTTPException(422, f"Could not read this link: {str(exc)[:180]}") from exc


@app.post("/api/jobs", status_code=202)
async def create_jobs(request: QueueRequest):
    created: list[Job] = []
    for item in request.items:
        job = Job(id=uuid.uuid4().hex, source_url=validate_public_url(item.url), quality=item.quality)
        job.format_id = item.format_id
        jobs[job.id] = job
        await work_queue.put(job.id)
        created.append(job)
    return {"jobs": created}


@app.get("/api/jobs")
async def list_jobs():
    return {"jobs": sorted(jobs.values(), key=lambda job: job.created_at, reverse=True)}


@app.get("/api/files/{job_id}")
async def get_file(job_id: str):
    job = jobs.get(job_id)
    if not job or job.state != "complete" or not job.filename:
        raise HTTPException(404, "This file is not available.")
    file = DATA_DIR / job_id / job.filename
    if not file.is_file():
        raise HTTPException(404, "This file is no longer available.")
    media_type, _ = mimetypes.guess_type(file.name)
    return FileResponse(
        file,
        filename=job.filename,
        media_type=media_type or "application/octet-stream",
        headers={
            "Cache-Control": "private, no-store, max-age=0",
            "X-Content-Type-Options": "nosniff",
        },
    )


app.mount("/", StaticFiles(directory="app/static", html=True), name="site")
