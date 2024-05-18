CREATE SCHEMA mi_kiosko;

CREATE TABLE mi_kiosko.dim_date (
    sk_date SERIAL PRIMARY KEY,
    "date" date,
    "year" VARCHAR(4),
    "month" VARCHAR(2),
    "day" VARCHAR(2)
);

CREATE TABLE mi_kiosko.dim_stock (
    sk_stock SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    company_name VARCHAR(100),
    sector VARCHAR(50),
    country_iso VARCHAR(3),
    currency VARCHAR(3)
);

CREATE TABLE mi_kiosko.fact_price_history (
    sk_price_id SERIAL PRIMARY KEY,
    fk_stock INT,
    fk_date INT,
    open_price decimal(10,2),
    high_price decimal(10,2),
    low_price decimal(10,2),
    close_price decimal(10,2),
    volume bigint,
    FOREIGN KEY (fk_date) REFERENCES mi_kiosko.dim_date(sk_date),
    FOREIGN KEY (fk_stock) REFERENCES mi_kiosko.dim_stock(sk_stock)
);

CREATE VIEW mi_kiosko.view_daily_volatility AS
with stock_price_history as(
	select dd."date", ds.symbol as stock, fph.open_price, fph.high_price, fph.low_price, fph.close_price, fph.volume 
	from mi_kiosko.fact_price_history fph 
	left join mi_kiosko.dim_date dd 
	on fph.fk_date = dd.sk_date 
	left join mi_kiosko.dim_stock ds 
	on fph.fk_stock = ds.sk_stock 
	where ds.symbol = 'AAPL'
)
SELECT 
    date,
    stock,
    high_price,
    low_price,
    (high_price - low_price) AS daily_volatility,
    1 - (low_price / high_price ) AS daily_volatility_perc
FROM 
    stock_price_history
ORDER BY 
    stock, date;

----Cambio de precio entre el cierre del día anterior y el actual
CREATE VIEW mi_kiosko.view_pricing_change AS
with total_data as(
	select dd."date", ds.symbol, fph.open_price, fph.high_price, fph.low_price, fph.close_price, fph.volume 
	from mi_kiosko.fact_price_history fph 
	left join mi_kiosko.dim_date dd 
	on fph.fk_date = dd.sk_date 
	left join mi_kiosko.dim_stock ds 
	on fph.fk_stock = ds.sk_stock 
	where ds.symbol = 'AAPL'
)
SELECT 
    date,
    symbol,
    close_price,
    LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY date) AS previous_close_price,
    close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY date) AS price_change,
    1 - (LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY date) /close_price) AS percentage_change
FROM 
    total_data
ORDER BY 
    symbol, date;