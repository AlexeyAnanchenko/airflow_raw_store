CREATE TABLE IF NOT EXISTS public.sales_by_year (
    "year" INT,
    category_name VARCHAR(20),
    sum_of_sale NUMERIC
);

CREATE TABLE IF NOT EXISTS public.sub_category_sales (
    sub_category_title VARCHAR(30),
    total_sales_in_2015 NUMERIC
);