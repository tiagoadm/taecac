create or replace procedure build_fp_tree()
language plpgsql 
as $$
declare
	curcnt integer;
	curpath text;
	curtid integer;
	curitem text;
	firstitem text;
begin
	for curtid in (select distinct tid from t_2 order by 1 asc)
	loop
		curpath := null;
		curcnt := 1;
		if curtid = 1 then
			for curitem in (select item from t_2 where tid = curtid order by item_order desc)
			loop
				insert into fp values (curitem, curcnt, curpath);
				commit;
				if curpath is null then
					curpath := 'null' || ':' || curitem;
				else
					curpath := curpath || ':' || curitem;
				end if;
			end loop;
		else
			firstitem := (select item from t_2 where tid = curtid and item_order = 1);
			if exists (select 1 from fp where item = firstitem and path is null) then
				for curitem in (select item from t_2 where tid = curtid)
				loop
					insert into fp values (curitem, curcnt, curpath);
					commit;
					if curpath is null then
						curpath := 'null' || ':' || curitem;
					else
						curpath := curpath || ':' || curitem;
					end if;
				end loop;
			else
				for curitem in (select item from t_2 where tid = curtid)
				loop
					if exists (select 1 from fp where item = curitem and path = curpath) then
						curcnt = (select cnt+1 from fp where item = curitem and path = curpath);
						update fp set cnt = curcnt where item = curitem and path = curpath;
						commit;
					else 
						insert into fp values (curitem, curcnt, curpath);
						commit;
					end if;
				end loop;
				if curpath is null then
					curpath := 'null' || ':' || curitem;
				else
					curpath := curpath || ':' || curitem;
				end if;
			end if;
		end if;
	end loop;
end; $$	
