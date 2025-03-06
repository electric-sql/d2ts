# D2QL Examples

## Kitchen Sink Example

```ts
const query: Query = {
  select: [
    "@order_id",
    { customer_name: "@customers.name" },
    { total_amount: { SUM: "@orders.amount" } },
    "@orders.status",
    { order_date: { DATE: "@orders.date" } },
    { metadata: { JSON_EXTRACT: ["@orders.details", "$.meta"] } }
  ],
  from: "orders",
  join: [
    {
      type: "inner",
      from: "customers",
      as: "customers",
      on: [
        "@orders.customer_id",
        "=",
        { col: "customers.id" }
      ],
      where: [
        [
          "@customers.status",
          "=",
          "active"
        ],
        "and",
        [
          "@customers.region",
          "=",
          "north"
        ]
      ]
    }
  ],
  where: [
    [
      "@orders.date",
      ">=",
      new Date("2021-01-01")
    ],
    "and",
    [
      "@orders.date",
      "<=",
      new Date("2021-12-31")
    ],
    "and",
    [
      "@orders.notes",
      "like",
      { value: "@VIP%" }
    ],
    "and",
    [
      "@orders.details",
      "is not",
      { value: { key: "value", extra: [1, 2, 3] } }
    ]
  ],
  groupBy: ["@orders.customer_id", "@orders.status"],
  having: [
    { col: "total_amount" },
    ">",
    1000
  ],
  orderBy: [
    "@orders.date",
    { total_amount: "desc" }
  ],
  limit: 50,
  offset: 10
};
```

## Common Table Expressions (CTEs) Example

```ts
const query: Query = {
  with: [
    // First CTE: Get active customers
    {
      select: ["@id", "@name", "@email", "@region"],
      from: "customers",
      where: ["@status", "=", "active"],
      as: "active_customers"
    },
    // Second CTE: Get recent orders (references the first CTE)
    {
      select: [
        "@order_id", 
        "@customer_id", 
        "@amount", 
        "@date",
        { customer_name: "@active_customers.name" },
        { customer_region: "@active_customers.region" }
      ],
      from: "orders",
      join: [
        {
          type: "inner",
          from: "active_customers",
          on: ["@orders.customer_id", "=", "@active_customers.id"]
        }
      ],
      where: ["@date", ">=", new Date("2023-01-01")],
      as: "recent_orders"
    }
  ],
  // Main query uses the second CTE
  select: [
    "@customer_region",
    { total_orders: { COUNT: "@order_id" } },
    { total_amount: { SUM: "@amount" } },
    { avg_order_value: { AVG: "@amount" } }
  ],
  from: "recent_orders",
  groupBy: ["@customer_region"],
  orderBy: [{ total_amount: "desc" }]
};
```

This example demonstrates:
1. Creating a CTE for active customers
2. Creating a second CTE that joins with the first CTE
3. Using the second CTE in the main query for aggregation
