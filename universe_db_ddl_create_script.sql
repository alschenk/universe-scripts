-- =========================================================
--  ENUM for fetch_state
-- =========================================================
DO $$
BEGIN
  IF to_regtype('public.fetch_state') IS NULL THEN
    CREATE TYPE public.fetch_state AS ENUM ('active','closed');
  END IF;
END$$;

-- =========================================================
--  EVENT  (one row per Universe event)
--  Source: event { id, title, state, maxQuantity, slug, updatedAt, calendarDates[0] }
-- =========================================================
CREATE TABLE IF NOT EXISTS event (
  id               TEXT PRIMARY KEY,          -- event.id
  title            TEXT,
  state            TEXT,
  max_quantity     INTEGER,                   -- maxQuantity
  slug             TEXT,
  updated_at       TIMESTAMPTZ,               -- updatedAt
  calendar_date    TIMESTAMPTZ,               -- single date instead of array: calendarDates[0]
  fetch_state      fetch_state NOT NULL DEFAULT 'active'
);

-- Helper indexes
CREATE INDEX IF NOT EXISTS event_calendar_date_idx ON event (calendar_date);
CREATE INDEX IF NOT EXISTS event_fetch_state_active_idx
  ON event (id) WHERE fetch_state = 'active';

-- =========================================================
--  TICKET_ORDER  (one row per order)
--  Source: orders.nodes { id, state, createdAt, confirmed, buyer{firstName,lastName,email} }
-- =========================================================
CREATE TABLE IF NOT EXISTS ticket_order (
  id                TEXT PRIMARY KEY,         -- order.id
  event_id          TEXT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
  state             TEXT,                     -- order.state
  created_at        TIMESTAMPTZ,              -- order.createdAt
  confirmed         BOOLEAN,                  -- order.confirmed
  buyer_first_name  TEXT,                     -- buyer.firstName
  buyer_last_name   TEXT,                     -- buyer.lastName
  buyer_email       TEXT                      -- buyer.email
);

-- Common query paths
CREATE INDEX IF NOT EXISTS ticket_order_event_created_idx
  ON ticket_order (event_id, created_at);
CREATE INDEX IF NOT EXISTS ticket_order_buyer_email_idx
  ON ticket_order (buyer_email);

-- =========================================================
--  ORDER_ITEM  (one row per ticket/line item)
--  Source: orderItems.nodes {
--    id, amount, orderState, qrCode,
--    rate { name, price, soldCount, maxQuantity },
--    costBreakdown { currency, price, subtotal, fee, discount }
--  }
-- =========================================================
CREATE TABLE IF NOT EXISTS order_item (
  id                         TEXT PRIMARY KEY,       -- item.id
  order_id                   TEXT NOT NULL REFERENCES ticket_order(id) ON DELETE CASCADE,

  amount                     INTEGER,                -- amount
  order_state                TEXT,                   -- orderState
  qr_code                    TEXT,                   -- qrCode

  -- rate.*
  rate_name                  TEXT,                   -- rate.name
  rate_price                 NUMERIC(12,2),          -- rate.price
  rate_sold_count            INTEGER,                -- rate.soldCount
  rate_max_quantity          INTEGER,                -- rate.maxQuantity

  -- costBreakdown.* (removed later)
  cost_breakdown_currency    TEXT,                   -- costBreakdown.currency
  cost_breakdown_price       NUMERIC(12,2),          -- costBreakdown.price
  cost_breakdown_subtotal    NUMERIC(12,2),          -- costBreakdown.subtotal
  cost_breakdown_fee         NUMERIC(12,2),          -- costBreakdown.fee
  cost_breakdown_discount    NUMERIC(12,2)           -- costBreakdown.discount
);

CREATE INDEX IF NOT EXISTS order_item_order_idx ON order_item(order_id);

ALTER TABLE event ADD COLUMN IF NOT EXISTS last_fetched_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS event_last_fetched_idx ON event(last_fetched_at);

-- Add column 'url' if not present
ALTER TABLE event ADD COLUMN IF NOT EXISTS url text;

-- Ticket categories / price tiers per event
create table if not exists rate (
  id            text primary key,                  -- Universe rate.id (globally unique)
  event_id      text not null references event(id) on delete cascade,
  name          text not null,
  price         numeric(12,2),
  max_quantity  integer,
  sold_count    integer,
  updated_at    timestamptz not null default now()
);

-- Helpful for lookups and to spot renamed tiers
create unique index if not exists rate_event_name_uidx on rate(event_id, name);
create index if not exists rate_event_idx on rate(event_id);

ALTER TABLE order_item DROP COLUMN IF EXISTS cost_breakdown_currency;
ALTER TABLE order_item DROP COLUMN IF EXISTS cost_breakdown_price;
ALTER TABLE order_item DROP COLUMN IF EXISTS cost_breakdown_subtotal;
ALTER TABLE order_item DROP COLUMN IF EXISTS cost_breakdown_fee;
ALTER TABLE order_item DROP COLUMN IF EXISTS cost_breakdown_discount;

ALTER TABLE order_item
  ADD COLUMN IF NOT EXISTS attendee_first_name text,
  ADD COLUMN IF NOT EXISTS attendee_last_name  text;

-- 1. Add the new column (nullable for now)
ALTER TABLE public.order_item
  ADD COLUMN IF NOT EXISTS rate_id text;

-- 2. (Optional but recommended) index for joins
CREATE INDEX IF NOT EXISTS ix_order_item_rate_id
  ON public.order_item(rate_id);

-- 3. Add a NOT VALID FK so we can backfill first and validate later
ALTER TABLE public.order_item
  ADD CONSTRAINT order_item_rate_fk
  FOREIGN KEY (rate_id) REFERENCES public.rate(id)
  DEFERRABLE INITIALLY DEFERRED
  NOT VALID;

-- 1) Category master (slug as stable key)
CREATE TABLE IF NOT EXISTS public.rate_category (
  slug          text PRIMARY KEY,           -- e.g. 'weekend', 'camping'
  name          text NOT NULL,              -- display label, e.g. 'Weekend Ticket'
  display_order int  NOT NULL DEFAULT 100,
  is_active     boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

-- 2) Add columns on rate
ALTER TABLE public.rate
  ADD COLUMN IF NOT EXISTS rate_category_slug text,
  ADD COLUMN IF NOT EXISTS normalized_name   text;

-- 3) FK (defer validation so you can backfill first)
ALTER TABLE public.rate
  ADD CONSTRAINT rate_rate_category_fk
  FOREIGN KEY (rate_category_slug) REFERENCES public.rate_category(slug)
  NOT VALID;

-- 4) Helpful indexes
CREATE INDEX IF NOT EXISTS ix_rate_rate_category_slug ON public.rate(rate_category_slug);
CREATE INDEX IF NOT EXISTS ix_rate_normalized_name    ON public.rate(normalized_name);

ALTER TABLE public.order_item VALIDATE CONSTRAINT order_item_rate_fk;
ALTER TABLE public.rate       VALIDATE CONSTRAINT rate_rate_category_fk;

-- Replaceable view for Looker/Studio
DROP VIEW IF EXISTS v_event_order_items;
CREATE VIEW v_event_order_items AS
select 
  e.id as event_id, e.title as event_title, e.calendar_date as event_date, 
  to2.id as order_id, to2.state as order_state, to2.created_at as order_created_at, to2.confirmed as order_confirmed,
  oi.id as order_item_id, oi.order_state as order_item_state, 
  r.normalized_name as rate_name, r.price as rate_price, 
  r.rate_category_slug, rc.name as rate_category_name  
from ticket_order to2
  inner join event e on e.id = to2.event_id
  inner join order_item oi on to2.id = oi.order_id 
  inner join rate r on oi.rate_id = r.id
  inner join rate_category rc on r.rate_category_slug = rc.slug 
where to2.state in ('PAID','ENDED','CLOSED')
order by e.calendar_date, to2.created_at;
