CREATE OR REPLACE PROPERTY GRAPH tpcds_retail_model
  NODE TABLES (
    `tpcds.public.store_sales` AS store_sales
      KEY(ss_item_sk, ss_ticket_number)
      DEFAULT LABEL OPTIONS(description="Store sales fact table", synonyms=["sales transactions", "POS data"])
      PROPERTIES(
        ss_item_sk OPTIONS(description="Foreign key to item"),
        ss_customer_sk,
        ss_ext_sales_price OPTIONS(description="Extended sales price"),
        ss_net_profit,
        MEASURE(SUM(ss_ext_sales_price)) AS total_sales OPTIONS(description="Total sales revenue", synonyms=["gross sales", "total revenue"]),
        MEASURE(SUM(ss_net_profit)) AS total_profit
      ),
    `tpcds.public.customer` AS customer
      KEY(c_customer_sk)
      DEFAULT LABEL OPTIONS(description="Customer dimension")
      PROPERTIES(
        c_customer_sk,
        c_first_name,
        c_last_name,
        c_first_name || ' ' || c_last_name AS customer_full_name OPTIONS(description="Customer full name (computed)", synonyms=["full name"])
      ),
    `tpcds.public.item` AS item
      KEY(i_item_sk)
      PROPERTIES(
        i_item_sk,
        i_brand OPTIONS(description="Brand name")
      )
  )
  EDGE TABLES (
    `tpcds.public.store_sales` AS store_sales_to_customer
      KEY(ss_item_sk, ss_ticket_number)
      SOURCE KEY (ss_item_sk, ss_ticket_number) REFERENCES store_sales (ss_item_sk, ss_ticket_number)
      DESTINATION KEY (ss_customer_sk) REFERENCES customer (c_customer_sk)
      DEFAULT LABEL OPTIONS(synonyms=["who bought"]),
    `tpcds.public.store_sales` AS store_sales_to_item
      KEY(ss_item_sk, ss_ticket_number)
      SOURCE KEY (ss_item_sk, ss_ticket_number) REFERENCES store_sales (ss_item_sk, ss_ticket_number)
      DESTINATION KEY (ss_item_sk) REFERENCES item (i_item_sk)
  )
  OPTIONS(description="TPC-DS retail semantic model", synonyms=["retail analytics"]);
