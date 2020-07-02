create or replace schema "testinsert";
set schema '"testinsert"';

create or replace stream "testinsert"."insertstream"
( thread integer
, batch integer
, rec integer
, d1 double
, d2 double
, vc1 varchar(100)
, vc2 varchar(100)
);

create or replace view "testinsert"."monitorview1"
AS
select stream *
, count(*) over (partition by thread rows unbounded preceding) as rowcount
from "testinsert"."insertstream"
;

create or replace view "testinsert"."monitorview"
AS
select stream step(s.rowtime by interval '30' second) as rt
, thread
, count(*) as period_count
, max(batch) as latest_batch
, max(rowcount) as cumulative_count
from "testinsert"."monitorview1" s
group by step(s.rowtime by interval '30' second)
, thread
;
