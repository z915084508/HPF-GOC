# HPF GOC

Hispafly Ground Operations Center is a continuously running Python worker. Every
20 seconds it reads VATSIM and CDM data and sends relevant Hoppie ACARS/TELEX
messages to HPF flights.

## Runtime model

This is a background worker, not a website. It only makes outbound HTTPS
connections and does not listen on a public port:

- `data.vatsim.net` for live flight data
- `viff-system.network` for CDM/TSAT data
- `www.hoppie.nl` for outgoing messages

The service writes deduplication state to `state.json`. In Docker this file is
stored in the persistent `goc-data` volume so a restart does not resend old
messages.

## Required configuration

Copy `.env.example` to `.env` and set:

- `HOPPIE_LOGON` (required): Hoppie logon code
- `GOC_STATION` (optional): sender station, default `HPFGOC`

Never commit `.env`. The repository previously contained a Hoppie credential;
replace that credential before deploying because deleting the file does not
remove it from Git history.

## Recommended 24/7 deployment: Docker Compose

Use a small Linux VPS with at least 1 shared vCPU, 512 MB RAM (1 GB preferred),
and 5 GB disk. Ubuntu 24.04 LTS or Debian 12 are suitable.

```bash
git clone https://github.com/z915084508/HPF-GOC.git
cd HPF-GOC
cp .env.example .env
# Edit .env and set a newly issued HOPPIE_LOGON.
docker compose up -d --build
docker compose ps
docker compose logs -f --tail=100 goc
```

No firewall port or reverse proxy is required. Keep inbound traffic closed apart
from the SSH port used for administration. Docker restarts the worker after a
crash or server reboot, rotates logs, and checks a heartbeat file every 30
seconds.

### Update release

```bash
git pull --ff-only
docker compose up -d --build
docker image prune -f
```

The named volume survives rebuilds. Back it up with the rest of the VPS if the
message history is operationally important.

## Direct systemd deployment

For a host without Docker, create a dedicated unprivileged `hpf-goc` user,
install the repository at `/opt/hpf-goc`, create a virtual environment, install
`requirements.txt`, and store secrets in `/etc/hpf-goc.env` with permissions
`0600`. The hardened unit template is in `deploy/hpf-goc.service`.

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hpf-goc
sudo systemctl status hpf-goc
sudo journalctl -u hpf-goc -f
```

## Local development

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
.venv/bin/python goc_auto.py
```

When attached to an interactive terminal, the manual `telex`, `ping`, and
`help` commands remain available. With no terminal, or with `GOC_HEADLESS=1`,
the watcher runs in the foreground as required by Docker and systemd.
