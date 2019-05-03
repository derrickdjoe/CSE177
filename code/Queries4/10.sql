SELECT SUM(l_extendedprice * l_discount * (1.0-l_tax))
FROM lineitem
WHERE
l_suppkey > 0 AND
l_suppkey < 999