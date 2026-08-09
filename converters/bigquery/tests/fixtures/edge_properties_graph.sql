CREATE OR REPLACE PROPERTY GRAPH orders_graph
  NODE TABLES (
    `shop.public.orders` AS orders
      KEY(order_id)
      DEFAULT LABEL OPTIONS(description="Order fact table", synonyms=["orders", "sales orders"])
      PROPERTIES(
        order_id OPTIONS(description="Order surrogate key", synonyms=["order number"]),
        customer_id OPTIONS(description="Foreign key to customer", synonyms=["buyer id"])
      ),
    `shop.public.customer` AS customer
      KEY(customer_id)
      DEFAULT LABEL OPTIONS(description="Customer dimension", synonyms=["customers", "buyers"])
      PROPERTIES(
        customer_id OPTIONS(description="Customer surrogate key", synonyms=["customer number"]),
        customer_name OPTIONS(description="Customer full name", synonyms=["name"])
      )
  )
  EDGE TABLES (
    `shop.public.orders` AS placed_by
      KEY(order_id)
      SOURCE KEY (order_id) REFERENCES orders (order_id)
      DESTINATION KEY (customer_id) REFERENCES customer (customer_id)
      DEFAULT LABEL OPTIONS(synonyms=["ordered by", "purchased by"])
      PROPERTIES(
        order_date OPTIONS(description="When the order was placed", synonyms=["purchase date", "ordered on"]),
        order_total OPTIONS(description="Total amount of the order", synonyms=["order amount", "basket value"])
      )
  )
  OPTIONS(description="Orders placed by customers", synonyms=["order graph", "purchases"]);
