CREATE OR REPLACE PROPERTY GRAPH tpcds_retail_model
  NODE TABLES (
    `tpcds.public.store_sales` AS store_sales
      KEY(ss_item_sk, ss_ticket_number)
      DEFAULT LABEL OPTIONS(description="Store sales fact table", synonyms=["sales transactions", "POS data"])
      PROPERTIES(
        ss_item_sk OPTIONS(description="Foreign key to item", synonyms=["item key", "product key"]),
        ss_customer_sk OPTIONS(description="Foreign key to customer", synonyms=["customer key", "buyer key"]),
        ss_ext_sales_price OPTIONS(description="Extended sales price", synonyms=["line revenue", "extended price"]),
        ss_net_profit OPTIONS(description="Net profit for the line item", synonyms=["line margin", "profit"]),
        MEASURE(SUM(ss_ext_sales_price)) AS total_sales OPTIONS(description="Total sales revenue", synonyms=["gross sales", "total revenue"]),
        MEASURE(SUM(ss_net_profit)) AS total_profit OPTIONS(description="Total net profit", synonyms=["total margin", "net profit"])
      ),
    `tpcds.public.customer` AS customer
      KEY(c_customer_sk)
      DEFAULT LABEL OPTIONS(description="Customer dimension", synonyms=["customers", "shoppers"])
      PROPERTIES(
        c_customer_sk OPTIONS(description="Customer surrogate key", synonyms=["customer id"]),
        c_first_name OPTIONS(description="Customer first name", synonyms=["given name"]),
        c_last_name OPTIONS(description="Customer last name", synonyms=["surname", "family name"]),
        c_first_name || ' ' || c_last_name AS customer_full_name OPTIONS(description="Customer full name (computed)", synonyms=["full name"])
      ),
    `tpcds.public.item` AS item
      KEY(i_item_sk)
      DEFAULT LABEL OPTIONS(description="Item dimension", synonyms=["products", "catalog"])
      PROPERTIES(
        i_item_sk OPTIONS(description="Item surrogate key", synonyms=["item id", "product id"]),
        i_brand OPTIONS(description="Brand name", synonyms=["manufacturer brand", "label"])
      )
  )
  EDGE TABLES (
    `tpcds.public.store_sales` AS store_sales_to_customer
      KEY(ss_item_sk, ss_ticket_number)
      SOURCE KEY (ss_item_sk, ss_ticket_number) REFERENCES store_sales (ss_item_sk, ss_ticket_number)
      DESTINATION KEY (ss_customer_sk) REFERENCES customer (c_customer_sk)
      DEFAULT LABEL OPTIONS(synonyms=["who bought", "purchased by"]),
    `tpcds.public.store_sales` AS store_sales_to_item
      KEY(ss_item_sk, ss_ticket_number)
      SOURCE KEY (ss_item_sk, ss_ticket_number) REFERENCES store_sales (ss_item_sk, ss_ticket_number)
      DESTINATION KEY (ss_item_sk) REFERENCES item (i_item_sk)
      DEFAULT LABEL OPTIONS(synonyms=["what was sold", "product sold"])
  )
  OPTIONS(description="TPC-DS retail semantic model", synonyms=["retail analytics", "store sales model"]);
