#!/usr/bin/env python3
"""
---------------------------------
universe_orders_to_csv.py
---------------------------------
Downloads *all* orders of one Universe event
- uses LIMIT variable for paging -
and writes them to a CSV **with a running progress log**.

CLI flags fall back to ENV if omitted:
  UNIVERSE_EVENT_ID,
  UNIVERSE_CLIENT_ID,
  UNIVERSE_CLIENT_SECRET,
  UNIVERSE_REFRESH_TOKEN
"""

#!/usr/bin/env python3
import argparse, base64, csv, os, sys, time, requests
from pathlib import Path
from datetime import datetime, timedelta

API_URL   = "https://www.universe.com/graphql"
TOKEN_URL = "https://www.universe.com/oauth/token"
LIMIT     = 20

QUERY = """
query OrdersPage($eventId: ID!, $limit: Int!, $offset: Int!) {
  event(id: $eventId) {
    id
    title
    state
    maxQuantity
    slug
    updatedAt
    calendarDates
    orders {
      totalCount
      nodes(limit:$limit, offset:$offset) {
        id state createdAt confirmed
        buyer { firstName lastName email }
        orderItems {
          nodes {
            id amount orderState qrCode
            costBreakdown { currency fee discount price subtotal }
            rate { name soldCount maxQuantity price  }
          }
        }
      }
    }
  }
}
"""

def args_or_env() -> argparse.Namespace:
    env = os.getenv
    p   = argparse.ArgumentParser()
    p.add_argument("--event-id",      default=env("UNIVERSE_EVENT_ID"))
    p.add_argument("--client-id",     default=env("UNIVERSE_CLIENT_ID"))
    p.add_argument("--client-secret", default=env("UNIVERSE_CLIENT_SECRET"))
    p.add_argument("--refresh-token", default=env("UNIVERSE_REFRESH_TOKEN"))
    p.add_argument("--outfile",       default="orders.csv")
    a = p.parse_args()
    miss = [k for k in ("event_id","client_id","client_secret","refresh_token")
            if not getattr(a, k)]
    if miss:
        p.error("Missing flags or env vars: " + ", ".join(miss))
    return a

def access_token(cid, secret, rtoken):
    basic = base64.b64encode(f"{cid}:{secret}".encode()).decode()
    r = requests.post(
        TOKEN_URL,
        headers={"Authorization": f"Basic {basic}",
                 "Content-Type":  "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": rtoken},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)

def main():
    a      = args_or_env()
    token  = access_token(a.client_id, a.client_secret, a.refresh_token)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # === CSV columns adjusted to your new query ===
    cols = [
        # event-level
        "event_id","event_title","event_state","event_slug",
        "event_max_quantity","event_updated_at","event_calendar_dates",
        # order-level
        "order_id","order_state","order_created_at","order_confirmed",
        "buyer_first","buyer_last","buyer_email",
        # item-level
        "item_id","item_amount","item_order_state","item_qr_code",
        "rate_name","rate_price","rate_sold_count","rate_max_quantity",
        "currency","cb_price","cb_subtotal","cb_fee","cb_discount"
    ]

    with Path(a.outfile).open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=cols,
            quoting=csv.QUOTE_ALL,      # always quote → RFC-4180 safe
            lineterminator="\r\n",      # Excel-friendly
            extrasaction="ignore",
        )
        writer.writeheader()

        offset, fetched_orders, fetched_items = 0, 0, 0
        start = time.time()

        # First call to get totalCount and event meta (we keep the meta)
        first = requests.post(
            API_URL,
            json={"query": QUERY,
                  "variables": {"eventId": a.event_id, "limit": 1, "offset": 0}},
            headers=headers, timeout=60).json()
        if first.get("errors"):
            sys.exit(first["errors"])
        event = first["data"]["event"]
        total = event["orders"]["totalCount"]
        log(f"Event: {event['title']}  – total orders: {total}")

        # normalize calendarDates list → single string
        cal_dates = event.get("calendarDates") or []
        cal_dates_str = " | ".join(cal_dates) if isinstance(cal_dates, list) else str(cal_dates)

        event_base = {
            "event_id":           event["id"],
            "event_title":        event["title"],
            "event_state":        event["state"],
            "event_slug":         event["slug"],
            "event_max_quantity": event["maxQuantity"],
            "event_updated_at":   event["updatedAt"],
            "event_calendar_dates": cal_dates_str,
        }

        while offset < total:
            vars_ = {"eventId": a.event_id, "limit": LIMIT, "offset": offset}
            resp  = requests.post(API_URL,
                                  json={"query": QUERY, "variables": vars_},
                                  headers=headers, timeout=60).json()
            if resp.get("errors"):
                sys.exit(resp["errors"])

            nodes = resp["data"]["event"]["orders"]["nodes"]
            fetched_orders += len(nodes)

            for o in nodes:
                order_base = {
                    **event_base,
                    "order_id":          o["id"],
                    "order_state":       o["state"],
                    "order_created_at":  o["createdAt"],
                    "order_confirmed":   o.get("confirmed"),
                    "buyer_firstname":       o["buyer"]["firstName"],
                    "buyer_lastname":        o["buyer"]["lastName"],
                    "buyer_email":       o["buyer"]["email"],
                }

                for it in o["orderItems"]["nodes"]:
                    fetched_items += 1
                    row = {
                        **order_base,
                        "item_id":           it["id"],
                        "item_amount":       it["amount"],
                        "item_order_state":  it.get("orderState"),
                        "item_qr_code":      it.get("qrCode"),
                        "rate_name":         (it["rate"] or {}).get("name"),
                        "rate_price":        (it["rate"] or {}).get("price"),
                        "rate_sold_count":   (it["rate"] or {}).get("soldCount"),
                        "rate_max_quantity": (it["rate"] or {}).get("maxQuantity"),
                        "currency":          (it["costBreakdown"] or {}).get("currency"),
                        "cb_price":          (it["costBreakdown"] or {}).get("price"),
                        "cb_subtotal":       (it["costBreakdown"] or {}).get("subtotal"),
                        "cb_fee":            (it["costBreakdown"] or {}).get("fee"),
                        "cb_discount":       (it["costBreakdown"] or {}).get("discount"),
                    }
                    # None → ""
                    writer.writerow({k: ("" if v is None else v) for k, v in row.items()})

            offset += LIMIT
            pct     = (fetched_orders / total * 100) if total else 100.0
            elapsed = timedelta(seconds=int(time.time() - start))
            log(f"Page done – {fetched_orders}/{total} orders "
                f"({pct:5.1f} %) – {fetched_items} items – elapsed {elapsed}")
            time.sleep(0.1)

    log(f"Finished. CSV '{a.outfile}' ready – "
        f"{fetched_orders} orders, {fetched_items} items.")

if __name__ == "__main__":
    main()
