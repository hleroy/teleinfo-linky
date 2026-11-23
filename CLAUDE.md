# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Téléinfo Linky reads the TIC (Télé-Information Client) data stream from an Enedis Linky electricity meter over a Raspberry Pi serial port (via a PITInfo module) and writes it to InfluxDB 2.x. Grafana visualizes the data. All code comments, log messages, docs and the README are in **French**: keep new code in French too.

The whole application is `linky.py`. The other files are:
- `linky.service`: a systemd **user** unit. It hardcodes `/home/pi/linky/`.
- `dashboard_grafana.json`: a Grafana 8.5 dashboard with Flux queries.
- `config.example.yml`: the template for the git-ignored `config.yml`.
- `Références/`: the Enedis TIC specifications. `Enedis-NOI-CPT_02E.pdf` covers historique mode and `Enedis-NOI-CPT_54E.pdf` covers standard mode. The checksum algorithm and frame format come from these PDFs.

## Running

There is no build step, test suite or linter. The dependencies are `pip install PyYAML pySerial influxdb-client`.

```
cp config.example.yml config.yml   # then edit it
python3 linky.py
```

- `linky.py` opens `config.yml` from the **current working directory**, not from the script's own directory, so run it from the directory that contains `config.yml`.
- With `influxdb.send_data: false`, the script reads and parses frames without connecting to InfluxDB. Use this for local testing.
- With `debug: true`, every raw information group is logged.
- The script needs real serial hardware. To test the parser without it, call `_checksum` directly or feed lines to the parsing logic by hand. Configuration values are module globals that are only set under `if __name__ == '__main__'`, so importing the module does not initialize them.
- To check the serial link by hand: `screen /dev/ttyAMA0 1200,cs7` (historique mode) or `screen /dev/ttyAMA0 9600,cs7,parenb,-parodd,-cstopb` (standard mode).
- On the Pi, the service runs with `systemctl --user start linky` and its logs are at `journalctl --user-unit linky`.

## Architecture

The script runs two threads that share a `queue.Queue`:

1. **Main thread (`linky()`)**: opens the serial port in 7E1. The baud rate is 1200 if `linky.legacy_mode` is true (historique) and 9600 otherwise (standard). It waits for STX (`0x02`) so it starts on a frame boundary, then reads one information group per line. For each line it detects the separator from the second-to-last character (SP or HT) and splits it into label, value and checksum. If the label is in `linky.keys` and the checksum passes, it stores `int(val)` in the current frame. On ETX (`0x03`), it timestamps the frame in UTC and pushes it onto the queue.
2. **Daemon thread (`_send_frames_to_influx`)**: pops frames and writes them synchronously. A failed write is retried forever with exponential backoff, capped at 2^10 s. A frame is never dropped. While InfluxDB is down, frames pile up in memory.

**InfluxDB data model:** each TIC label becomes its own *measurement* (for example `BASE` or `IINST`). Each has a single field `value` and a default tag `location` taken from `linky.location`. The Grafana dashboard queries `r._measurement == "<LABEL>" and r._field == "value"` against the bucket `teleinfo`. Renaming measurements or fields breaks the dashboard.

**Checksum (`_checksum`):** `checksum_method` 1 sums label + separator + value. Method 2 also includes the trailing separator. The result is `(sum & 0x3F) + 0x20`. See Enedis-NOI-CPT_02E, page 19.

**Parser limits to keep in mind:**
- Every selected value goes through `int()`, so a non-numeric label such as `PTEC`, `OPTARIF` or `ADCO` raises an exception in the generic handler instead of being stored.
- In standard mode, some groups carry a timestamp field (label, date, value, checksum). The parser cuts only at the first separator, so these groups do not parse correctly either.
