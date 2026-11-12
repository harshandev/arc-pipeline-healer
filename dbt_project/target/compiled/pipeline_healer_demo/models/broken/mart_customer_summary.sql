select
    c.segment,
    c.region,
    count(distinct o.order_id)  as total_orders,
    sum(o.amount)               as total_revenue,
    avg(o.amount)               as avg_order_value
from {{ ref('stg_orders') }} o
inner join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
group by c.segment, c.region