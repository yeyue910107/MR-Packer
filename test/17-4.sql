select
    l_partkey, l_quantity, l_extendedprice
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#34'
    and p_container = 'MED PACK';
