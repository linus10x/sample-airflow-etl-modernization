select * from {{ ref('stg_vendor_a_products') }}
union all
select * from {{ ref('stg_vendor_b_products') }}
union all
select * from {{ ref('stg_vendor_c_products') }}
