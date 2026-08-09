-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Expected export of tests/fixtures/tpcds_ossie.yaml (golden fixture).

CREATE OR REPLACE PROPERTY GRAPH tpcds_retail_model
  NODE TABLES (
    `tpcds.public.store_sales` AS store_sales
      KEY(ss_item_sk, ss_ticket_number)
      OPTIONS(description="Store sales fact table\n\nSynonyms: sales transactions, POS data")
      PROPERTIES(
        ss_item_sk OPTIONS(description="Foreign key to item"),
        ss_customer_sk,
        ss_ext_sales_price OPTIONS(description="Extended sales price"),
        ss_net_profit,
        MEASURE(SUM(ss_ext_sales_price)) AS total_sales OPTIONS(description="Total sales revenue\n\nSynonyms: gross sales, total revenue"),
        MEASURE(SUM(ss_net_profit)) AS total_profit
      ),
    `tpcds.public.customer` AS customer
      KEY(c_customer_sk)
      OPTIONS(description="Customer dimension")
      PROPERTIES(
        c_customer_sk,
        c_first_name,
        c_last_name,
        c_first_name || ' ' || c_last_name AS customer_full_name OPTIONS(description="Customer full name (computed)\n\nSynonyms: full name")
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
      OPTIONS(description="Synonyms: who bought"),
    `tpcds.public.store_sales` AS store_sales_to_item
      KEY(ss_item_sk, ss_ticket_number)
      SOURCE KEY (ss_item_sk, ss_ticket_number) REFERENCES store_sales (ss_item_sk, ss_ticket_number)
      DESTINATION KEY (ss_item_sk) REFERENCES item (i_item_sk)
  )
  OPTIONS(description="TPC-DS retail semantic model\n\nSynonyms: retail analytics");
