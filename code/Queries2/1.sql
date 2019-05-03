SELECT SUM(ps_supplycost), ps_suppkey
FROM partsupp
WHERE ps_partkey > -1 AND ps_partkey < 1000
GROUP BY ps_suppkey
