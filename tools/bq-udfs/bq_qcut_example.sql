DECLARE
  N_BUCKETS,
  N_PARTITIONS,
  N_COUNT INT64;
SET
  N_BUCKETS = 20;
SET
  N_PARTITIONS = 200;
SET
  N_COUNT = (
  SELECT
    COUNT(*)
  FROM
    `anand-bq-test-2.Anand_BQ_Test_1.scores100m`);
  #--------------------------------------------------------------
  # User defined function to do calculation like pandas qcut
  # the ranking is 0 based index e.g. 20 buckets starts from 0
  # and runs till 19
  #--------------------------------------------------------------
CREATE TEMP FUNCTION
  qcut(buckets INT64,
    countA INT64,
    rank FLOAT64)
  RETURNS INT64 AS ( CAST(FLOOR(rank * (buckets / countA) - (buckets / countA)) AS int64) );
SELECT
  HH_ID,
  Scores,
  qcut(N_BUCKETS,
    N_COUNT,
    RANK() OVER(PARTITION BY CAST(FLOOR(Scores * 200) AS int64)
    ORDER BY
      Scores) + b.rank_part) qcut
FROM
  `anand-bq-test-2.Anand_BQ_Test_1.scores100m` a
INNER JOIN (
  SELECT
    part,
    SUM(count_part) OVER (ORDER BY part) - count_part rank_part
  FROM (
    SELECT
      CAST(FLOOR(Scores * 200) AS INT64) part,
      COUNT(Scores) count_part
    FROM
      `anand-bq-test-2.Anand_BQ_Test_1.scores100m`
    GROUP BY
      CAST(FLOOR(Scores * 200) AS INT64) ) ) b
ON
  CAST(FLOOR(Scores * 200) AS INT64) = b.part