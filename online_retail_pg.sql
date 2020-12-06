--Índice para melhorar a performance das queries
create index idx_uci_database_lkp on uci_database("InvoiceNo", "StockCode", "InvoiceDate");

--Corrigir o problema das minúsculas na coluna StockCode
update public.uci_database
set "StockCode" = UPPER("StockCode")

/********* INICIO NORMALIZAÇÃO **********/
--Tabelas
create table sales ("InvoiceNo" text, "StockCode" text, "CustomerID" float8, "InvoiceDate" timestamp, "UnitPrice" float8, "Quantity" int8,
primary key ("InvoiceNo", "StockCode", "CustomerID"),
CONSTRAINT fk_customer foreign key ("CustomerID") references customers("CustomerID"),
CONSTRAINT fk_product foreign key ("StockCode") references products("StockCode")
);
create table products ("StockCode" text primary key, "Description" text, "Created" timestamp, "LastUpdate" timestamp);
create table customers ("CustomerID" float8 primary key, "Country" text, "Created" timestamp, "LastUpdate" timestamp);

--Inserção de dados Customers
insert into customers
select distinct "CustomerID", 
	first_value("Country") over (partition by "CustomerID" order by "InvoiceDate" desc), 
	min("InvoiceDate") over (partition by "CustomerID"), 
	max("InvoiceDate") over (partition by "CustomerID")
from public.uci_database
where "CustomerID" is not null;
--Inserção de dados Products
insert into products
select distinct "StockCode", 
	first_value("Description") over (partition by "StockCode" order by "InvoiceDate" desc), 
	min("InvoiceDate") over (partition by "StockCode"), 
	max("InvoiceDate") over (partition by "StockCode")
from public.uci_database;
--Inserção de dados Sales
insert into sales
select "InvoiceNo" , "StockCode" , "CustomerID" , max("InvoiceDate") , max("UnitPrice") "UnitPrice", sum("Quantity") "Quantity"
FROM public.uci_database
where "CustomerID" is not null
group by "InvoiceNo" , "StockCode" , "CustomerID";

/********* FIM NORMALIZAÇÃO **********/