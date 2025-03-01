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
