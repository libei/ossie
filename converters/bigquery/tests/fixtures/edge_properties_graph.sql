CREATE OR REPLACE PROPERTY GRAPH orders_graph
  NODE TABLES (
    `shop.public.orders` AS orders
      KEY(order_id)
      DEFAULT LABEL OPTIONS(description="Order fact table")
      PROPERTIES(
        order_id,
        customer_id
      ),
    `shop.public.customer` AS customer
      KEY(customer_id)
      DEFAULT LABEL OPTIONS(description="Customer dimension")
      PROPERTIES(
        customer_id,
        customer_name
      )
  )
  EDGE TABLES (
    `shop.public.orders` AS placed_by
      KEY(order_id)
      SOURCE KEY (order_id) REFERENCES orders (order_id)
      DESTINATION KEY (customer_id) REFERENCES customer (customer_id)
      DEFAULT LABEL OPTIONS(synonyms=["ordered by"])
      PROPERTIES(
        order_date OPTIONS(description="When the order was placed"),
        order_total
      )
  )
  OPTIONS(description="Orders placed by customers");
