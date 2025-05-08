CREATE TABLE `me-sb-dgcp-dpoc-data-pr.CICD_Management.orders` (
    order_id INT64 NOT NULL,
    customer_id INT64 NOT NULL,
    order_date TIMESTAMP,
    order_status STRING,
    total_amount FLOAT64,
    shipping_address STRUCT<
        address STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        country STRING
    >,
    billing_address STRUCT<
        address STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        country STRING
    >,
    order_details ARRAY<STRUCT<
        product_id INT64,
        product_name STRING,
        quantity INT64,
        price FLOAT64
    >>
)
;
