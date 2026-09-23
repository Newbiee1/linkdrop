# LinkDrop

A clean-room, mobile-first web app for downloading media that you own or are authorized to save. It queues small batches, offers practical quality choices (including 480p), and creates phone-friendly MP4 downloads where the source permits it.

It is intentionally a private, self-hosted tool. It does not bypass DRM, paywalls, authentication, or access controls. Do not expose it to the public internet without authentication and rate limiting.

## What it does

- Paste one link or up to 8 links at a time.
- Inspect a link before queuing it and choose best available, 1080p, 720p, 480p, or 360p.
- Runs two downloads at once by default and provides a file link when each job finishes.
- Removes finished files after 24 hours by default.
- Works in Safari on iPhone and Chrome on Android because the interface is responsive and the output uses MP4 when possible.

The download engine is [yt-dlp](https://github.com/yt-dlp/yt-dlp); its supported sources change regularly. Whether a particular source works, and which resolutions it exposes, depends on that source and its permission/terms.

## Run it

```bash
docker compose up -d --build
```

The app listens only on `127.0.0.1:8080`, so place it behind your existing HTTPS reverse proxy. A minimal Nginx route looks like this:

```nginx
location /linkdrop/ {
    proxy_pass http://127.0.0.1:8080/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

Protect `/linkdrop/` with your existing authentication layer before use. The service fetches user-provided URLs, so it must remain private and should run with egress controls in production.

## Configuration

| Setting | Default | Purpose |
| --- | --- | --- |
| `MAX_CONCURRENT_DOWNLOADS` | `2` | Number of active jobs. |
| `MAX_BATCH` | `8` | Maximum URLs accepted in one request. |
| `DOWNLOAD_TTL_SECONDS` | `86400` | How long completed files remain available. |
| `DOWNLOAD_DIR` | `/data/downloads` | Persistent download storage. |

## Project approach

This project is independently written; it is not a fork of MeTube. MeTube is an excellent AGPL-licensed reference for a larger self-hosted downloader, while LinkDrop is intentionally small, mobile-first, and designed for your private Greenpicks server.
