create table test1(id bigint, name string) ;

create table test2(id bigint, name string) ;

insert overwrite table test1
select 1, "name1"
union all
select 2, "name2"
union all
select 3, "name3";


insert overwrite table test2
select 11, "name11"
union all
select 22, "name22"
union all
select 33, "name33"
union all
select 1, "name1";


1. 构建一条SQL，同时apply下面三条优化规则:
CombineFilters
CollapseProject
BooleanSimplification

select
id, id1,
id1+1 as id2
from (
select
id,
id +1 as id1
from test1 where id>2)a
where a.id>1
 and (1=1 or 1=2);


2. 构建一条SQL，同时apply下面五条优化规则:
ConstantFolding
PushDownPredicates
ReplaceDistinctWithAggregate
ReplaceExceptWithAntiJoin
FoldablePropagation

select
id,
name
from (
select
id,
name,
1.0 x, 'abc' y, Now() z
from test1
ORDER BY x, y, 3) a
where a.id=1
and x=1.0
except DISTINCT select id, name from test2;

