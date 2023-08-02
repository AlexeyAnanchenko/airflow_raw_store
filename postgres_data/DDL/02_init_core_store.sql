CREATE TABLE IF NOT EXISTS public.raw_store (
    "Row ID" INT PRIMARY KEY,
    "Order ID" VARCHAR(20),
    "Order Date" DATE,
    "Ship Date" DATE,
    "Ship Mode" VARCHAR(100),
    "Customer ID" VARCHAR(20),
    "Customer Name" VARCHAR(100),
    "Segment" VARCHAR(20),
    "Country" VARCHAR(30),
    "City" VARCHAR(30),
    "State" VARCHAR(30),
    "Postal Code" INT,
    "Region" VARCHAR(20),
    "Product ID" VARCHAR(20),
    "Category" VARCHAR(40),
    "Sub-Category" VARCHAR(40),
    "Product Name" VARCHAR(200),
    "Sales" NUMERIC(10, 2),
    "Quantity" INT,
    "Discount" NUMERIC(2, 2),
    "Profit" NUMERIC(10, 4)
);

CREATE TABLE IF NOT EXISTS public.date_conversion (
    date_id SERIAL PRIMARY KEY,
    date_origin DATE,
    "day" INT,
    "month" INT,
    "year" INT,
    "quarter" INT,
    day_of_week VARCHAR(15),
    "week" INT
);

CREATE TABLE IF NOT EXISTS public.order_store (
    order_id SERIAL PRIMARY KEY,
    order_id_from_raw VARCHAR(20),
    date_id INT REFERENCES public.date_conversion(date_id)
);

CREATE TABLE IF NOT EXISTS public.ship_mode (
    mode_id SERIAL PRIMARY KEY,
    mode_title VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS public.shipment (
    shipment_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES public.date_conversion(date_id),
    mode_id INT REFERENCES public.ship_mode(mode_id)
);

CREATE TABLE IF NOT EXISTS public.segment (
    segment_id SERIAL PRIMARY KEY,
    segment_title VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS public.customer (
    customer_id SERIAL PRIMARY KEY,
    customer_id_from_raw VARCHAR(20),
    customer_name VARCHAR(100),
    segment_id INT REFERENCES public.segment(segment_id)
);

CREATE TABLE IF NOT EXISTS public.country (
    country_id SERIAL PRIMARY KEY,
    country_title VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS public.region (
    region_id SERIAL PRIMARY KEY,
    region_title VARCHAR(20),
    country_id INT REFERENCES public.country(country_id)
);

CREATE TABLE IF NOT EXISTS public.state (
    state_id SERIAL PRIMARY KEY,
    state_title VARCHAR(30),
    region_id INT REFERENCES public.region(region_id)
);

CREATE TABLE IF NOT EXISTS public.city (
    city_id SERIAL PRIMARY KEY,
    city_title VARCHAR(30),
    state_id INT REFERENCES public.state(state_id)
);

CREATE TABLE IF NOT EXISTS public.postal_code (
    postal_code_id SERIAL PRIMARY KEY,
    postal_code INT,
    city_id INT REFERENCES public.city(city_id)
);

CREATE TABLE IF NOT EXISTS public.category (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(30)
);

CREATE TABLE IF NOT EXISTS public.sub_category (
    sub_category_id SERIAL PRIMARY KEY,
    sub_category_title VARCHAR(40),
    category_id INT REFERENCES public.category(category_id)
);

CREATE TABLE IF NOT EXISTS public.product (
    product_id SERIAL PRIMARY KEY,
    product_id_from_raw VARCHAR(20),
    product_name VARCHAR(150),
    price NUMERIC(8, 2),
    sub_category_id INT REFERENCES public.sub_category(sub_category_id)
);

CREATE TABLE IF NOT EXISTS public.sales_store (
    order_id INT REFERENCES public.order_store(order_id),
    shipment_id INT REFERENCES public.shipment(shipment_id),
    customer_id INT REFERENCES public.customer(customer_id),
    postal_code_id INT REFERENCES public.postal_code(postal_code_id),
    product_id INT REFERENCES public.product(product_id),
    sum_of_sale NUMERIC(10, 2),
    quantity INT,
    discount NUMERIC(2, 2),
    profit NUMERIC(10, 4),
    PRIMARY KEY (order_id, shipment_id, customer_id, postal_code_id, product_id)
);

/*
-- таблица с датами
INSERT INTO date_conversion (date_origin, "day", "month", "year", "quarter", day_of_week, "week")
SELECT d.date_origin,
	   EXTRACT(DAY FROM d.date_origin) AS "day",
	   EXTRACT(MONTH FROM d.date_origin) AS "month",
	   EXTRACT(YEAR FROM d.date_origin) AS "year",
	   EXTRACT(QUARTER FROM d.date_origin) AS "quarter",
	   EXTRACT(ISODOW FROM d.date_origin) AS day_of_week,
       EXTRACT(WEEK FROM d.date_origin) AS "week"
FROM (
	SELECT DISTINCT "Order Date" AS date_origin FROM raw_store
	UNION
	SELECT DISTINCT "Ship Date" AS date_origin FROM raw_store
) AS d
ORDER BY d.date_origin;


-- таблица order_store
INSERT INTO order_store (order_id_from_raw, date_id) 
SELECT DISTINCT rs."Order ID" AS order_id_from_raw,
				dc.date_id
FROM raw_store AS rs
JOIN date_conversion AS dc ON rs."Order Date" = dc.date_origin
ORDER BY order_id_from_raw;


-- таблица ship_mode
INSERT INTO ship_mode (mode_title)
SELECT DISTINCT "Ship Mode"
FROM raw_store;


-- таблица shipment
INSERT INTO shipment (date_id, mode_id)
SELECT DISTINCT dc.date_id, sm.mode_id
FROM raw_store AS rs
JOIN date_conversion AS dc ON rs."Ship Date" = dc.date_origin
JOIN ship_mode AS sm ON rs."Ship Mode" = sm.mode_title
ORDER BY dc.date_id DESC;


-- таблица segment
INSERT INTO segment (segment_title)
SELECT DISTINCT rs."Segment" AS segment_title
FROM raw_store AS rs;


-- таблица customer
INSERT INTO customer (customer_id_from_raw, customer_name, segment_id)
SELECT DISTINCT rs."Customer ID" AS customer_id_from_raw,
	   			rs."Customer Name" AS customer_name,
	   			s.segment_id
FROM raw_store AS rs
JOIN segment AS s ON s.segment_title = rs."Segment"
ORDER BY customer_id_from_raw;


-- таблица category
INSERT INTO category (category_name)
SELECT DISTINCT rs."Category" AS category_name
FROM raw_store AS rs;


-- таблица sub_category
INSERT INTO sub_category (sub_category_title, category_id)
SELECT DISTINCT rs."Sub-Category" AS sub_category_title, c.category_id
FROM raw_store AS rs
JOIN category AS c ON rs."Category" = c.category_name;


-- таблица product (дубликаты)
INSERT INTO product (product_id_from_raw, product_name, sub_category_id, price)

WITH
price AS (
	SELECT rs."Product ID",
	   	   rs."Product Name",
	       ROUND(AVG(rs.price), 2) AS price
	FROM (
		SELECT rs."Product ID",
			   rs."Product Name",
			   ROUND((rs."Sales" / (1.00 - rs."Discount")) / rs."Quantity", 2) AS price
		FROM raw_store AS rs
	) AS rs
	GROUP BY rs."Product ID", rs."Product Name"
)

SELECT DISTINCT rs."Product ID" AS product_id_from_raw,
	   			rs."Product Name" AS product_name,
	   			sc.sub_category_id,
	   			pr.price
FROM raw_store AS rs
JOIN sub_category AS sc ON rs."Sub-Category" = sc.sub_category_title
JOIN price AS pr ON rs."Product ID" = pr."Product ID" AND rs."Product Name" = pr."Product Name";


-- таблица country
INSERT INTO country (country_title)
SELECT DISTINCT rs."Country"
FROM raw_store rs;


-- таблица region
INSERT INTO region (region_title, country_id)
SELECT DISTINCT rs."Region" AS region_title, c.country_id 
FROM raw_store AS rs
JOIN country AS c ON rs."Country" = c.country_title;


-- таблица state
INSERT INTO state (state_title, region_id)
SELECT DISTINCT rs."State", r.region_id
FROM raw_store AS rs
JOIN region AS r ON rs."Region" = r.region_title
ORDER BY 1;


-- таблица city (дубликаты)
INSERT INTO city (city_title, state_id)
SELECT DISTINCT rs."City" AS city_title, s.state_id
FROM raw_store AS rs
JOIN state AS s ON rs."State" = s.state_title
ORDER BY 1;


-- таблица postal_code
INSERT INTO postal_code (postal_code, city_id)
SELECT DISTINCT rs."Postal Code" AS postal_code, c.city_id
FROM raw_store AS rs
JOIN (
	SELECT c.city_id, c.city_title, c.state_id, s.state_title
	FROM city AS c
	JOIN state AS s ON c.state_id = s.state_id	
) AS c ON rs."City" = c.city_title AND c.state_title = rs."State"
ORDER BY 1;

-- таблица sales_store
INSERT INTO sales_store (order_id, shipment_id, customer_id, postal_code_id,
						 product_id, sum_of_sale, quantity, discount, profit)
WITH
sales AS (
	SELECT os.order_id, sh.shipment_id, cu.customer_id, ps.postal_code_id,
		   pr.product_id, rs."Sales" AS sum_of_sale, rs."Quantity" AS quantity,
		   rs."Discount" AS discount, rs."Profit" AS profit,
		   pr.price * rs."Quantity" AS sum_by_price
	FROM raw_store AS rs
	JOIN order_store AS os ON rs."Order ID" = os.order_id_from_raw
	JOIN (
		SELECT s.shipment_id, s.date_id, dc.date_origin, sm.mode_title
		FROM shipment AS s
		JOIN date_conversion AS dc ON s.date_id = dc.date_id
		JOIN ship_mode AS sm ON s.mode_id = sm.mode_id
	) AS sh ON rs."Ship Date" = sh.date_origin AND rs."Ship Mode" = sh.mode_title
	JOIN customer AS cu ON rs."Customer ID" = cu.customer_id_from_raw
	JOIN postal_code AS ps ON rs."Postal Code" = ps.postal_code
	JOIN product AS pr ON rs."Product ID" = pr.product_id_from_raw
						  AND rs."Product Name" = pr.product_name
)

SELECT order_id, shipment_id, customer_id, postal_code_id, product_id,
	   SUM(sum_of_sale) AS sum_of_sale,
	   SUM(quantity) AS quantity,
	   ROUND((SUM(sum_by_price) - SUM(sum_of_sale)) / SUM(sum_by_price), 2) AS discount,
	   SUM(profit) AS profit
FROM sales
GROUP BY order_id, shipment_id, customer_id, postal_code_id, product_id
ORDER BY order_id;
*/