# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Téléinfo Linky reads the TIC (Télé-Information Client) data stream from an Enedis Linky electricity meter over a Raspberry Pi serial port (via a PITInfo module) and writes it to InfluxDB 2.x, Prometheus (remote write), or both. Grafana visualizes the data. All code comments, log messages, docs, the README and the CHANGELOG are in **French**: keep new text in French too.

The repo is public and other people run it with InfluxDB, so InfluxDB behaviour must not regress: the `config.yml` keys, the data model and the "never drop a frame" retry loop stay as they are.

Layout:
- `teleinfo_linky/`: the Python package. It needs Python **3.9** at minimum (Raspberry Pi OS 11), so don't use `match` statements or `X | Y` type syntax.
  - `__main__.py`: argument parsing (`--config`, `--version`), one output thread per enabled destination, then the serial loop.
  - `config.py`: loads and validates `config.yml` into frozen dataclasses.
  - `tic.py`: the serial port, plus pure parsing functions (`decouper_groupe`, `verifier_checksum`, `lire_trames`).
  - `envoi.py`: the generic retry loop.
  - `sorties/influxdb.py`, `sorties/prometheus.py`, `sorties/remote_write.py`: the two outputs and the remote-write encoder.
- `linky.py`: a compatibility launcher for 1.0.0 installs, whose systemd unit runs `/usr/bin/python3 ~/linky/linky.py`.
- `linky.service`: a systemd **user** unit using `%h/linky/.venv/bin/teleinfo-linky`.
- `install.sh`: the installer and upgrader. Users run it with `curl | bash`, so the body lives in `main()`.
  - It installs `python3-yaml` and `python3-serial` with apt and creates the venv with `--system-site-packages`, because pip would otherwise download, and sometimes compile, them on a Pi Zero (armv6), which takes very long.
  - `PYTHONNOUSERSITE=1`, set in the script and in `linky.service`, keeps the 1.0.0 pip packages in `~/.local` out of the venv.
  - It upgrades pip only when it is older than 22. Debian bullseye's pip 20.3, in a `--system-site-packages` venv, builds the package with Debian's setuptools 52 instead of the isolated setuptools ≥ 77, and fails with `AttributeError: install_layout`.
- `dashboard_grafana.json` (InfluxDB, Flux) and `dashboard_grafana_prometheus.json` (PromQL).
- `dashboard_grafana_hphc.json` and `dashboard_grafana_standard_hphc.json`: InfluxDB dashboards for the HP/HC option in historique and standard mode, taken from the Heat-Miser fork.
- `config.example.yml`: the template for the git-ignored `config.yml`.
- `CHANGELOG.md`: follows [Keep a Changelog, French](https://keepachangelog.com/fr/1.1.0/), and is written entirely in French.
- `Références/`: the Enedis TIC specifications. `Enedis-NOI-CPT_02E.pdf` covers historique mode and `Enedis-NOI-CPT_54E.pdf` covers standard mode.

## Development

```
uv venv -p 3.9 .venv && uv pip install -p .venv/bin/python -e '.[test,influxdb]'
.venv/bin/python -m pytest -q
shellcheck install.sh
```

- CI (`.github/workflows/ci.yml`) runs pytest on Python 3.9, 3.11 and 3.13, plus shellcheck.
- `python -m teleinfo_linky` (or `teleinfo-linky`) reads `config.yml` from the **current directory** unless `--config` is given.
- With `send_data: false` in both `influxdb` and `prometheus`, frames are parsed but not sent.
- With `debug: true`, every raw information group is logged. Messages emitted on every frame (frame received, writes) are at debug level too, so the journal stays quiet at info level.
- Log lines carry a timestamp unless stderr goes to journald, which already timestamps them. The check compares `JOURNAL_STREAM` with stderr's device:inode, because desktop sessions export `JOURNAL_STREAM` to every program they start.
- `lire_trames()` takes any object with `readline()`, so the parser is tested without serial hardware (see `tests/test_tic.py`).
- For an end-to-end run, use a `socat` pty pair with a script that writes frames.
  - A Linux pty rejects pyserial's 7E1 settings with `termios.error` (EINVAL), so wrap `termios.tcsetattr` to ignore that error during tests.
  - Run local `prom/prometheus:v3.16.0 --web.enable-remote-write-receiver` and `influxdb:2.7.8` containers.
- To check the serial link on the Pi: `screen /dev/ttyAMA0 1200,cs7` (historique mode) or `screen /dev/ttyAMA0 9600,cs7,parenb,-parodd,-cstopb` (standard mode).
- On the Pi: `systemctl --user start linky`, and logs with `journalctl --user-unit linky`.

## Architecture

- **Main thread** (`tic.lire_teleinfo`):
  - opens the serial port in 7E1, at 1200 baud if `linky.legacy_mode` is true (historique) and 9600 otherwise (standard);
  - waits for STX (`0x02`), then reads one information group per line;
  - strips only CR, LF, ETX and STX from each line. Never use a bare `rstrip()`: the checksum can be a space;
  - detects the separator (SP or HT) from the second-to-last character, and keeps the value of labels that are in `linky.keys` and whose checksum passes: a string with padding spaces stripped for labels in `ETIQUETTES_TEXTE`, `int()` otherwise;
  - in standard mode, a timestamped group (label HT date HT value HT checksum) is checksummed whole, then `separer_horodatage` drops the date. `DATE` has no value and is skipped;
  - on ETX, builds an immutable `Trame` (UTC datetime and values) and puts it on **each output's own queue**;
  - on a `SerialException` while reading, closes the port and reopens it after 10 s, so queued frames survive. An error while opening the port (permissions, `termios.error`) exits the program; `linky.service` restarts it after `RestartSec=10`.
- **Output threads** (`envoi.boucle_envoi`): one per enabled output.
  - An output's `ecrire(lot)` raises `ErreurTemporaire` (the batch is retried with backoff of 2^x s, capped at 2^10) or `ErreurDefinitive` (the batch is dropped).
  - Any unexpected exception is treated as temporary.
  - Each queue is an `envoi.FileBornee` whose size is the output's `taille_max_file` (0 means unbounded). When full, it drops the oldest frame, so `put()` never blocks the serial loop.
- **InfluxDB** (`SortieInfluxDB`):
  - one synchronous write per frame;
  - each TIC label is a *measurement*, with a single field `value` and a default tag `location`;
  - the time is formatted to the second;
  - every error is temporary, and its queue is unbounded (`taille_max_file = 0`), so frames pile up in memory while InfluxDB is down;
  - `influxdb_client` is imported lazily.
  - The Grafana dashboard queries `r._measurement == "<LABEL>" and r._field == "value"` against the bucket `teleinfo`. Renaming measurements or fields breaks it.
  - `tests/test_influxdb.py` pins this behaviour against a fake InfluxDB server: the exact line protocol, the write URL and token, and every error being temporary. If one of these tests fails, the change breaks existing users, so fix the code, not the test.
- **Prometheus** (`SortiePrometheus`):
  - every `push_interval` seconds, drains the queue (up to 2000 frames) into one remote-write request. Every frame keeps its own millisecond timestamp;
  - metric name `linky_<label lowercase>`, with characters other than `[a-zA-Z0-9_:]` replaced by `_` (`SMAXSN-1` gives `linky_smaxsn_1`), labels `job="linky"` and `location`;
  - a text label becomes an info metric `linky_<label>_info{value="<text>"}` with value 1;
  - `remote_write.py` encodes the protobuf by hand and uses a literal-only snappy stream, so there is no runtime dependency (Pi Zero, armv6). Labels must be sorted by name;
  - `location` is converted to a string (`None` becomes an empty label), because YAML reads a postcode as an integer;
  - redirects are not followed (urllib would turn the POST into a GET). Network errors, 3xx, 5xx, 429, 401 and 403 are temporary;
  - other 4xx responses are definitive (out-of-order or too-old samples). A single rejected sample can make Prometheus reject the whole request;
  - frames older than `AGE_MAX` (50 min, a 10 min margin under Prometheus' 1 h limit) are dropped before sending, and the queue holds at most `2 * AGE_MAX` frames.
- **Checksum** (`verifier_checksum`):
  - method 1 sums label + separator + value;
  - method 2 also includes the trailing separator;
  - the result is `(sum & 0x3F) + 0x20`;
  - see Enedis-NOI-CPT_02E, page 19.

**Parser limits:**
- `ETIQUETTES_TEXTE` (`tic.py`) is a fixed list. Any other label goes through `int()`, so an unlisted non-numeric label raises an exception in the generic handler instead of being stored. `STGE` is hexadecimal and fails whenever it contains a letter.
- Don't replace the list with "`int()`, falling back to a string": a label whose type changes between frames causes an InfluxDB field type conflict, and since every InfluxDB error is temporary, that frame would be retried forever.
- For the same reason, never move a label that `int()` already accepts (`ADCO`, `MOTDETAT`…) into `ETIQUETTES_TEXTE`: existing users store it as an integer field.
- The date of timestamped groups is dropped; the value is stored with the frame's reception time.

## Releases

Releases are `.tar.gz` archives built by `.github/workflows/release.yml` when a `vX.Y.Z` tag is pushed. The workflow:
1. runs `scripts/notes_version.py`, which fails if the tag doesn't match `teleinfo_linky.__version__` or if the CHANGELOG section is missing or undated;
2. runs the tests;
3. runs `git archive` (`.gitattributes` excludes the PDFs, screenshots, tests and CI files);
4. adds a SHA-256 file and a provenance attestation;
5. creates the GitHub release with `install.sh`, using the CHANGELOG section as release notes.

Asset names carry no version, so `releases/latest/download/<asset>` always works.

To release:
1. Move the `[Non publié]` entries into a new `## [X.Y.Z] - AAAA-MM-JJ` section, with a real date, and update the comparison links at the bottom.
2. Bump `__version__` in `teleinfo_linky/__init__.py`.
3. Merge, then tag `vX.Y.Z` and push the tag.
