select
    o.order_date,
    p.product_name,
    p.category,
    p.unit_price,
    count(distinct o.order_id) as total_orders,
    sum(o.amount)              as total_revenue
from {{ ref('stg_orders') }} o
inner join {{ ref('stg_products') }} p
    on o.product_category = p.category
where o.order_date is not null
group by o.order_date, p.product_name, p.category, p.unit_price