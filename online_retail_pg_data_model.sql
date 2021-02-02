--Cleaned dataset
create materialized view cleaned_dataset as 
select distinct * --2.1
from uci_database ud
where left(upper("InvoiceNo"),1) not in ('A','C') --2.5
and not "Description" ~ ('
	wrong.*?|test|SAMPLES|Sale error|returned|on cargo order|Manual|mailout|incorrect.*?|
	check|damage.*?|\?+|printing smudges/thrown away|crushed boxes|[Aa]djust.*?|[add|allocate] stock.*?|
	[[Aa]mazon|AMAZON].*?|broken|Breakages|CARRIAGE|.*?[Dd]otcom.*?|[Ff]ound.*?|FOUND.*?|[Hh]ad been put aside.*?
') --2.4
and upper("StockCode") not like 'GIFT%' --2.4
and "Quantity" > 0 --2.3
and "UnitPrice" > 0 --2.3
and "Description" is not null --2.2
and "CustomerID" is not null --2.2
and "InvoiceNo" is not null --2.2
and "StockCode" is not null; --2.2
--Transactions
create table transactions as
select distinct "InvoiceNo"
    "StockCode",
    "CustomerID",
    "InvoiceDate",
    "UnitPrice",
    "Quantity"
from cleaned_dataset;
--Customers
create table customers as
with customers_intermediate as (
    select distinct "CustomerID",
        "Country",
    "InvoiceDate",
    max("InvoiceDate") over (partition by "CustomerID") as max_date
    from cleaned_dataset
)
select distinct "CustomerID",
    "Country",
    "InvoiceDate" as "LastUpdate"
from customers_intermediate
where "InvoiceDate" = max_date; --3.2
--Products
create table products as
with products_intermediate as (
    select distinct "StockCode",
        "Description",
        "InvoiceDate",
        max("InvoiceDate") over (partition by "StockCode") as max_date
    from cleaned_dataset
)
select distinct "StockCode",
    "Description",
    "InvoiceDate" as "LastUpdate"
from products_intermediate
where "InvoiceDate" = max_date; --3.3
--Primary keys
ALTER TABLE transactions ADD PRIMARY KEY ("InvoiceNo", "StockCode", "CustomerID");
ALTER TABLE customers ADD PRIMARY KEY ("CustomerID");
ALTER TABLE products ADD PRIMARY KEY ("StockCode");
--Foreign keys
ALTER TABLE transactions ADD CONSTRAINT fk_prod FOREIGN KEY ("StockCode") REFERENCES products("StockCode");
ALTER TABLE transactions ADD CONSTRAINT fk_cust FOREIGN KEY ("CustomerID") REFERENCES customers("CustomerID");
