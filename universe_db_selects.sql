-- select all rates / ticket categories
select e.title, r.*
from rate r
  inner join "event" e on e.id=r.event_id
order by e.title, r.name

-- select for view v_event_order_items
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