-- BROKEN MODEL: joins to a column that doesn't exist (product_sku vs product_id)
-- This simulates a schema change upstream breaking a downstream model
-- The Healer Agent will detect this and open a PR with the fix
select
    o.order_date,
    p.product_name,
    p.category,
    p.unit_price,
    count(distinct o.order_id) as total_orders,
    sum(o.amount)              as total_revenue
from {{ ref('stg_orders') }} o
inner join {{ ref('stg_products') }} p
    on o.product_sku = p.product_id   -- BUG: stg_orders has no product_sku column
group by 1, 2, 3, 4
