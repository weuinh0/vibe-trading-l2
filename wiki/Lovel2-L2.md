# Lovel2 A-share Level-2 integration

Vibe-Trading-L2 keeps all existing free public providers as the default. The
optional `get_lovel2_l2` tool adds read-only access to a locally delivered,
authorized A-share Level-2 stream.

## Trust boundary

```text
Lovel2 service -> official customer client -> local JSONL -> get_lovel2_l2
```

The official customer client owns authentication. Vibe-Trading-L2 does not
contain or implement the service login protocol, and the model cannot choose a
filesystem path. Only the operator can set `LOVEL2_JSONL_PATH`.

Never commit the client configuration, passwords, certificates, production
endpoints, device identifiers, entitlement receipts, or real market captures.

## Configure

Copy `agent/.env.example` to `agent/.env`, then set:

```dotenv
LOVEL2_JSONL_PATH=D:/market-data/realtime.jsonl
LOVEL2_SCAN_BYTES=33554432
LOVEL2_STALE_SECONDS=15
```

Restart the agent after changing the environment. The tool supports:

- `status`: configured file name, size and modification time, without exposing
  the full local path.
- `snapshot`: latest matching ten-level `market` snapshot.
- `orders`: recent matching exchange order events, newest first.
- `transactions`: recent matching transaction events, newest first.

Each call scans only a bounded tail window (32 MiB by default, 256 MiB maximum)
and ignores an incomplete final line while the customer client is writing it.
If an illiquid symbol has no event inside that window, the response says so
instead of silently returning old or unrelated data.

## Units and freshness

Prices are source integers divided by 10,000. Quantities remain provider raw
integers. Amount scale may differ by market and instrument class, so amount
fields remain explicitly named `*_raw` and must not be treated as CNY without a
formal unit rule for that product.

The response includes source/local timestamps when present, record age and an
`is_stale` flag. Strategies should fail closed when freshness is unknown or
stale.

## Data access

The integration does not bundle market data. Product coverage and usage rights
are governed by each customer's separate service agreement. See
[Lovel2](https://www.lovel2.site/) for service information.

