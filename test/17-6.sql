select sum(l_quantity) as sum_lq ,sum(l_extendedprice) / 7.0 as sum_le
from (
select
                        l_partkey, l_quantity, l_extendedprice
                from
                        lineitem,
                        part
                where
                        p_partkey = l_partkey
                        and p_brand = 'Brand#34'
                        and p_container = 'MED PACK'
) p
group by l_partkey;
