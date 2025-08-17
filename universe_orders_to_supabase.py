#!/usr/bin/env python3
import argparse, base64, os, sys, time, requests
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import psycopg2, psycopg2.extras

API_URL   = "https://www.universe.com/graphql"
TOKEN_URL = "https://www.universe.com/oauth/token"

PAGE_LIMIT_DEFAULT    = 10   # Universe allows up to 50
BACKFILL_DAYS_DEFAULT = 7    # backfill X daysbefore last_fetched_at

# -------------------------- GraphQL -----------------------------------------

ORDERS_QUERY = """
query OrdersPage($eventId: ID!, $limit: Int!, $offset: Int!, $updatedSince: Time) {
  event(id: $eventId) {
    id title state maxQuantity slug updatedAt calendarDates
    orders(updatedSince: $updatedSince) {
      totalCount
      nodes(limit: $limit, offset: $offset) {
        id state createdAt confirmed
        buyer { firstName lastName email }
        orderItems {
          nodes {
            id amount orderState qrCode firstName lastName
            rate { id name soldCount maxQuantity price }
          }
        }
      }
    }
  }
}
"""

# ---------------------------- SQL -------------------------------------------

SELECT_EVENTS_SQL = """
SELECT id, last_fetched_at
FROM event
WHERE fetch_state = 'active'
ORDER BY calendar_date NULLS LAST, id;
"""

GET_WATERMARK_SQL = "SELECT last_fetched_at FROM event WHERE id = %s;"

UPDATE_EVENT_META_SQL = """
UPDATE event
SET state = %s,
    max_quantity = %s,
    updated_at = %s,
    last_fetched_at = %s
WHERE id = %s;
"""

ORDER_UPSERT_SQL = """
INSERT INTO ticket_order (
  id, event_id, state, created_at, confirmed,
  buyer_first_name, buyer_last_name, buyer_email
) VALUES %s
ON CONFLICT (id) DO UPDATE SET
  state            = EXCLUDED.state,
  created_at       = EXCLUDED.created_at,
  confirmed        = EXCLUDED.confirmed,
  buyer_first_name = EXCLUDED.buyer_first_name,
  buyer_last_name  = EXCLUDED.buyer_last_name,
  buyer_email      = EXCLUDED.buyer_email;
"""

ITEM_UPSERT_SQL = """
INSERT INTO order_item (
  id, order_id, amount, order_state, qr_code,
  attendee_first_name, attendee_last_name,
  rate_id, rate_price
) VALUES %s
ON CONFLICT (id) DO UPDATE SET
  amount               = EXCLUDED.amount,
  order_state          = EXCLUDED.order_state,
  qr_code              = EXCLUDED.qr_code,
  attendee_first_name  = EXCLUDED.attendee_first_name,
  attendee_last_name   = EXCLUDED.attendee_last_name,
  rate_id              = EXCLUDED.rate_id,
   -- keep the first non-null price we ever saw
  rate_price           = COALESCE(order_item.rate_price, EXCLUDED.rate_price);
"""

# UPSERT: includes rate_category_slug and normalized_name, but keeps DB values if present
RATE_UPSERT_SQL = """
INSERT INTO rate (id, event_id, name, price, max_quantity, sold_count, rate_category_slug, normalized_name)
VALUES %s
ON CONFLICT (id) DO UPDATE SET
  event_id     = EXCLUDED.event_id,
  name         = EXCLUDED.name,
  price        = EXCLUDED.price,
  max_quantity = EXCLUDED.max_quantity,
  sold_count   = EXCLUDED.sold_count,
  -- sticky fields: do not clobber manual edits
  rate_category_slug = COALESCE(rate.rate_category_slug, EXCLUDED.rate_category_slug),
  normalized_name    = COALESCE(rate.normalized_name,    EXCLUDED.normalized_name),
  updated_at   = now();
"""


# -------------------------- Helpers -----------------------------------------

def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

def access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    r = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {basic}",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]

def gql(session: requests.Session, token: str, query: str, variables: dict, allow_partial: bool = True):
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    r = session.post(API_URL, json={"query": query, "variables": variables},
                     headers=headers, timeout=90)
    r.raise_for_status()
    js = r.json()
    errs = js.get("errors")
    if errs and not allow_partial:
        raise RuntimeError(errs)
    return js.get("data"), errs

def dec(x):
    return None if x in (None, "") else Decimal(str(x))

# ----------------------- DB operations --------------------------------------

def select_events_to_fetch(cur, include_closed: bool):
    if include_closed:
        cur.execute("SELECT id, last_fetched_at FROM event ORDER BY calendar_date NULLS LAST, id;")
    else:
        cur.execute(SELECT_EVENTS_SQL)
    return cur.fetchall()  # [(id, last_fetched_at), ...]

def get_watermark(cur, event_id: str):
    cur.execute(GET_WATERMARK_SQL, (event_id,))
    row = cur.fetchone()
    return row[0] if row else None

def update_event_meta(cur, event_id: str, state: str, max_qty, updated_at_iso: str):
    now_utc = datetime.now(timezone.utc)
    cur.execute(
        UPDATE_EVENT_META_SQL,
        (state, max_qty, updated_at_iso, now_utc, event_id)
    )
    return now_utc

def upsert_orders_items(cur, event_id: str, orders_nodes):
    order_rows, item_rows = [], []
    rates_map = {}  # rate_id -> (id, event_id, name, price, max_qty, sold_count)

    for o in orders_nodes:
        buyer = o.get("buyer") or {}
        order_rows.append((
            o["id"], event_id, o.get("state"), o.get("createdAt"), o.get("confirmed"),
            buyer.get("firstName"), buyer.get("lastName"), buyer.get("email"),
        ))

        for it in (o.get("orderItems") or {}).get("nodes", []):
            rate = it.get("rate") or {}

            # collect latest snapshot per rate.id (if present)
            if rate.get("id"):
                rates_map[rate["id"]] = (
                    rate["id"],
                    event_id,
                    rate.get("name"),
                    None if rate.get("price") is None else Decimal(str(rate.get("price"))),
                    rate.get("maxQuantity"),
                    rate.get("soldCount"),
                    None,  # rate_category_slug -> left NULL; you set it manually in DB
                    None   # normalized_name    -> initial NULL; you can fill/edit later
                )

            item_rows.append((
                it["id"], o["id"], it.get("amount"), it.get("orderState"), it.get("qrCode"),
                it.get("firstName"), it.get("lastName"),
                rate.get("id"),                      # <-- new: rate_id
                None if (rate.get("price") is None) else Decimal(str(rate.get("price")))
            ))


    # 1) orders/items
    if order_rows:
        psycopg2.extras.execute_values(cur, ORDER_UPSERT_SQL, order_rows, page_size=1000)
    if item_rows:
        psycopg2.extras.execute_values(cur, ITEM_UPSERT_SQL,  item_rows,  page_size=1000)

    # 2) rates (isolated savepoint so 1) isn't lost on failure)
    n_rates = 0
    if rates_map:
        rate_rows = list(rates_map.values())
        try:
            cur.execute("SAVEPOINT sp_rates")
            psycopg2.extras.execute_values(cur, RATE_UPSERT_SQL, rate_rows, page_size=500)
            cur.execute("RELEASE SAVEPOINT sp_rates")
            n_rates = len(rate_rows)
        except Exception as ex:
            cur.execute("ROLLBACK TO SAVEPOINT sp_rates")
            rate_ids = [r[0] for r in rate_rows][:5]
            log(f"⚠️ rate upsert failed for event {event_id}: {ex} (first rate_ids: {rate_ids} …)")

    return len(order_rows), len(item_rows), n_rates

# ----------------------- Fetch per Event ------------------------------------

def fetch_for_event(cur, session, token: str, event_id: str,
                    page_limit: int, backfill_days: int):
    # 1) Watermark -> updatedSince
    wm = get_watermark(cur, event_id)
    updated_since = None
    if wm:
        from_dt = (wm - timedelta(days=backfill_days)).astimezone(timezone.utc)
        updated_since = from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # 2) first call (totalCount + event meta)
    data, errs = gql(session, token, ORDERS_QUERY, {
        "eventId": event_id, "limit": 1, "offset": 0, "updatedSince": updated_since
    }, allow_partial=True)
    if errs:
        log(f"⚠️ Event {event_id} first call returned {len(errs)} GraphQL error(s); continuing with partial data.")
    if not data or not data.get("event"):
        log(f"✗ Event {event_id}: no event data returned; skipping.")
        return 0, 0, 0

    ev = data["event"]
    total = ev["orders"]["totalCount"]
    log(f"Event {event_id}: total={total} (updatedSince={updated_since or 'FULL'})")

    # 3) Paging
    fetched, offset = 0, 0
    total_order_rows, total_item_rows, total_rate_rows = 0, 0, 0
    page_idx = 0

    while offset < total:
        vars_ = {"eventId": event_id, "limit": page_limit, "offset": offset, "updatedSince": updated_since}
        data, errs = gql(session, token, ORDERS_QUERY, vars_, allow_partial=True)
        if errs:
            log(f"⚠️ GQL errors on event {event_id}, offset {offset}: {errs[0]}")

        if not data or not data.get("event"):
            log(f"✗ Missing data for event {event_id} at offset {offset}; skipping page.")
            offset += page_limit
            page_idx += 1
            continue

        nodes = data["event"]["orders"]["nodes"] or []

        # Log the order ids of this page for troubleshooting
        order_ids = [o.get("id") for o in nodes if o]
        log(f"Event {event_id} page offset {offset}: orders={order_ids}")

        # Per-page savepoint so a bad page doesn't roll back prior pages of this event
        sp_name = f"sp_page_{page_idx}"
        cur.execute(f"SAVEPOINT {sp_name}")
        try:
            n_orders, n_items, n_rates = upsert_orders_items(cur, event_id, nodes)
            cur.execute(f"RELEASE SAVEPOINT {sp_name}")
        except Exception as dbex:
            cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
            log(f"✗ DB upsert failed at offset {offset}: {dbex}. Skipping this page.")
            n_orders, n_items, n_rates = 0, 0, 0

        total_order_rows += n_orders
        total_item_rows  += n_items
        total_rate_rows  += n_rates
        fetched += len(nodes)
        offset  += page_limit
        page_idx += 1

        log(f"Event {event_id}: {fetched}/{total} orders processed "
            f"(rows upserted: orders={n_orders}, items={n_items}, rates={n_rates})")
        time.sleep(0.1)

    # 4) update event-metadata in DB (needed fields only)
    now_utc = update_event_meta(
        cur,
        event_id=event_id,
        state=ev.get("state"),
        max_qty=ev.get("maxQuantity"),
        updated_at_iso=ev.get("updatedAt"),
    )
    log(f"Event {event_id}: updated state/max_quantity/updated_at; "
        f"last_fetched_at = {now_utc.isoformat()}")

    return total_order_rows, total_item_rows, total_rate_rows

# ----------------------------- CLI ------------------------------------------

def parse_args():
    env = os.environ.get
    p = argparse.ArgumentParser(
        description="Universe → Supabase Loader: calls events from DB, updates orders and data incrementally, updates event- and rate-metadata"
    )
    p.add_argument("--client-id",      default=env("UNIVERSE_CLIENT_ID"), required=not env("UNIVERSE_CLIENT_ID"))
    p.add_argument("--client-secret",  default=env("UNIVERSE_CLIENT_SECRET"), required=not env("UNIVERSE_CLIENT_SECRET"))
    p.add_argument("--refresh-token",  default=env("UNIVERSE_REFRESH_TOKEN"), required=not env("UNIVERSE_REFRESH_TOKEN"))
    p.add_argument("--pg-dsn",         default=env("SUPABASE_PG_DSN"), required=not env("SUPABASE_PG_DSN"),
                   help="Postgres DSN, z.B. postgresql://user:pass@host:5432/db?sslmode=require")
    p.add_argument("--limit",          type=int, default=int(env("UNIVERSE_PAGE_LIMIT") or PAGE_LIMIT_DEFAULT))
    p.add_argument("--backfill-days",  type=int, default=int(env("WM_BACKFILL_DAYS") or BACKFILL_DAYS_DEFAULT))
    p.add_argument("--include-closed", action="store_true", help="include events with fetch_state <> 'active'")
    args = p.parse_args()
    args.limit = max(1, min(args.limit, 50))  # clamp
    return args

# ----------------------------- Main -----------------------------------------

def main():
    a = parse_args()
    token = access_token(a.client_id, a.client_secret, a.refresh_token)
    log("got access-token.")

    with psycopg2.connect(a.pg_dsn) as conn, requests.Session() as sess:
        conn.autocommit = False
        with conn.cursor() as cur:
            events = select_events_to_fetch(cur, include_closed=a.include_closed)
            if not events:
                log("no events found to load data for (checked for fetch_state='active').")
                return
            log(f"{len(events)} processing events: {[e[0] for e in events]}")

            total_orders, total_items, total_rates = 0, 0, 0
            for eid, _wm in events:
                try:
                    o_rows, i_rows, r_rows = fetch_for_event(cur, sess, token, eid, a.limit, a.backfill_days)
                    conn.commit()
                    total_orders += o_rows
                    total_items  += i_rows
                    total_rates  += r_rows
                    log(f"✓ Event {eid}: committed (orders_rows={o_rows}, item_rows={i_rows}, rate_rows={r_rows})")
                except Exception as ex:
                    conn.rollback()
                    log(f"✗ Event {eid}: Error, rollback. Details: {ex}")

            log(f"Done. Upserts total: orders_rows={total_orders}, item_rows={total_items}, rate_rows={total_rates}")

if __name__ == "__main__":
    main()
