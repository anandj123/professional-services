#--------------------------------------------------------------
# User defined function to do calculation like pandas qcut
# the ranking is 0 based index e.g. 20 buckets starts from 0
# and runs till 19
#--------------------------------------------------------------
CREATE TEMP FUNCTION qcut(buckets INT64, countA INT64, val FLOAT64) RETURNS INT64 AS 
(
  cast(floor(val * (buckets / countA) - (buckets / countA)) as int64)
);

#--------------------------------------------------------------
# Analytics function rank() over() throws out of memory errors when ran
# on a large dataset. So here the total dataset is divided into
# 200 buckets and each one is ranked individually. Then they are
# combined by adding the sum of the records in all the previous
# buckets.
#--------------------------------------------------------------

select 
  HH_ID
  , Scores
  ,qcut(20, 
        (select count(*) from `anand-bq-test-2.Anand_BQ_Test_1.scores100m`), 
        rank() over(partition by cast(floor(Scores * 200) as int64) order by Scores) + b.rank_part)  qcut
from `anand-bq-test-2.Anand_BQ_Test_1.scores100m` a
inner join 
(
  select 
    part
    , sum(count_part) over (order by part) - count_part rank_part
  from (
          select 
              cast(floor(Scores * 200) as int64) part
              ,count(Scores) count_part
          from `anand-bq-test-2.Anand_BQ_Test_1.scores100m`
          group by cast(floor(Scores * 200) as int64)
       )
 ) b
ON cast(floor(Scores * 200) as int64) = b.part