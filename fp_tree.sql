--FP table with window functions - very fast!
create materialized view fp as
with t as (
	select tid, 
		item, 
		lag(item,1) over (partition by tid order by item_order) as previous_item, 
		item_order 
	from t_2
),
efp as (
	select item, 
		string_agg(coalesce(previous_item,'null'), ':') over (partition by tid order by item_order) path
	from t	select item, string_agg(coalesce(previous_item,'null'), ':') over (partition by tid order by item_order) path
	from t
)
select item, 
	path, 
	count(1) as cnt
from efp
group by item, path
order by 3 desc;

--PL-PGSQL alternative - slow
create or replace procedure build_fp() language plpgsql as $$
declare
	curpath text;
	curtid integer;
	curitem text;
	curitemorder integer;
	firstitem text;
begin
	for curtid in (select distinct tid from t_2 order by tid)
	loop
		curpath := null;
		if curtid = 1 then
			for curitem in (select item from t_2 where tid = curtid order by item_order)
			loop
				insert into fp values (curitem, 1, curpath);
				commit;
				if curpath is null then
					curpath := 'null' || ':' || curitem;
				else
					curpath := curpath || ':' || curitem;
				end if;
			end loop;
		else
			for curitem, curitemorder in (select item, item_order from t_2 where tid = curtid order by item_order)
			loop
				if exists (select 1 from fp where item = curitem and path is null and curitemorder = 1) then
					update fp set cnt = cnt+1 where item = curitem and path is null;
					commit;
					if curpath is null then
						curpath := 'null' || ':' || curitem;
					else
						curpath := curpath || ':' || curitem;
					end if;
				elsif exists (select 1 from fp where item = curitem and path = curpath) then
					update fp set cnt = cnt+1 where item = curitem and path = curpath;
					commit;
					if curpath is null then
						curpath := 'null' || ':' || curitem;
					else
						curpath := curpath || ':' || curitem;
					end if;
				else
					insert into fp values (curitem, 1, curpath);
					commit;
					if curpath is null then
						curpath := 'null' || ':' || curitem;
					else
						curpath := curpath || ':' || curitem;
					end if;
				end if;	
			end loop;
		end if;
	end loop;
end; $$	
