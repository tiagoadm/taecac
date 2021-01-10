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
				else if exists (select 1 from fp where item = curitem and path = curpath) then
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
				end if;		
			end loop;
		end if;
	end loop;
end; $$	
