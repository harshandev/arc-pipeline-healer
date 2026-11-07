-- Fixed: changed o.product_sku to o.product_id (correct column name)
select
    o.order_date,
    p.product_name,
    p.category,
    p.unit_price,
    count(distinct o.order_id) as total_orders,
    sum(o.amount)              as total_revenue
from {{ ref("stg_orders") }} o
inner join {{ ref("stg_products") }} p
    on o.product_id = p.product_id
group by 1, 2, 3, 4
