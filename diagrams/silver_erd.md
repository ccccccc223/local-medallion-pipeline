# Silver ERD

Typed, deduplicated-by-label, and validated. Every table carries a surrogate
primary key, because **no natural key in this source is unique**. Collision
rows are retained rather than dropped: there is no basis for deciding which
row of a colliding pair is correct, and dropping either loses a real record.

Two conformed dimensions carry the relationships that actually hold. The
fact-to-fact relationships are shown with their measured match rates, so the
diagram documents the gap rather than implying a join that works.

```mermaid
erDiagram
    silver_dim_geography ||--o{ silver_location : "conforms"
    silver_dim_geography ||--o{ silver_agent : "conforms"
    silver_dim_product_category ||--o{ silver_orders : "conforms"
    silver_dim_product_category ||--o{ silver_agent_commission : "conforms"

    silver_orders |o..o{ silver_sales : "order_id - 0.13% resolve"
    silver_orders |o..o{ silver_location : "order_id - 0.17% resolve"
    silver_agent |o..o{ silver_sales : "agent_id - 1.55% resolve"
    silver_agent |o..o{ silver_agent_commission : "agent_id - 1.47% resolve"

    silver_dim_geography {
        integer geography_key PK "surrogate"
        text city "76 values, shared by location and agent"
        text state
        text country "23 values, shared by location and agent"
        text region
    }

    silver_dim_product_category {
        integer product_category_key PK "surrogate"
        text product_category "4 values, identical in both sources"
    }

    silver_orders {
        integer order_line_key PK "surrogate"
        text order_id "natural key part - collides on 3 values"
        date order_date "natural key part - disambiguates collisions"
        text product "natural key part - grain is order line"
        date shipping_date
        integer aging "reconciles to date difference on every row"
        text ship_mode
        text product_category FK "conforms"
        text record_type "single / multi_product / split_shipment / collision"
        text dq_status "valid, warning or quarantine"
        text source_file "lineage"
        integer source_row "lineage"
        text batch_timestamp "lineage"
    }

    silver_sales {
        integer sales_key PK "surrogate"
        text order_id "natural key - 23 collisions, no product column"
        text agent_id FK "1.55% resolve to agent"
        integer sales "cast from 8-char text, zero-padding verified lossless"
        integer quantity
        integer discount "0-9, meaning undocumented, not rescaled"
        integer profit "correlates 0.00 with sales"
        real shipping_cost
        text order_priority
        text customer_id "not a usable customer identity"
        boolean is_id_collision "46 rows"
        boolean is_zero_quantity "271 rows, quarantined"
        text dq_status
    }

    silver_location {
        integer location_key PK "surrogate"
        text order_id "natural key - 16 collisions"
        text location_id "attribute only, not a place key"
        text city FK "conforms"
        text state
        text country
        text region
        text segment "Consumer / Corporate / Home Office"
        text customer_name "400 values from a shared name pool"
        boolean is_id_collision "32 rows"
        text dq_status
    }

    silver_agent {
        integer agent_key PK "surrogate"
        text agent_id "natural key - 274 rows across 137 collisions"
        text agent_name "400 values, all collisions differ on it"
        text country FK "conforms"
        text state
        text city
        boolean is_id_collision
        text dq_status
    }

    silver_agent_commission {
        integer commission_key PK "surrogate - mandatory, no natural key exists"
        text agent_id "1.47% resolve to agent"
        text product_category FK "conforms"
        real commission_percentage "1.00-99.99, uniform"
        boolean is_conflicting_rate "304 rows, quarantined"
        text dq_status
    }

    silver_data_quality {
        text table_name
        text check_name
        integer value
        text note
    }

    silver_data_quality_issues {
        text silver_table "which table the record is in"
        integer silver_record_id "the surrogate key of the flagged row"
        text issue_code
        text severity "warning or error"
        text issue_detail
    }
```

## Reading the notation

| Notation | Meaning |
|---|---|
| `\|\|--o{` solid | The relationship holds. Both conformed dimensions match exactly across their source tables. |
| `\|o..o{` dashed | The relationship is intended by the source but does not resolve. The match rate is on the label. |

## Keys

| Table | Grain | Natural key | Unique? | Primary key |
|---|---|---|---|---|
| `silver_agent` | one agent | `agent_id` | no — 137 collisions | `agent_key` |
| `silver_agent_commission` | agent × category | `(agent_id, product_category)` | no — 152 conflicts | `commission_key` |
| `silver_orders` | order line | `(order_id, order_date, product)` | no — 2 split shipments | `order_line_key` |
| `silver_sales` | one order | `order_id` | no — 23 collisions | `sales_key` |
| `silver_location` | one order | `order_id` | no — 16 collisions | `location_key` |

Six alternative composite keys were tested and rejected. `(order_id,
shipping_date)`, `(agent_id, city)` and `(order_id, location_id)` are unique
only by accident — they include a mutable measure or a meaningless
identifier. `(order_id, customer_id)` and `(order_id, customer_name)` violate
the functional dependency `order_id → customer_id`, and are unique only
*because* the collisions break that dependency, which would mean using the
defect to conceal the defect.

`silver_data_quality_issues` references the other tables polymorphically —
`silver_table` plus `silver_record_id` — so it is not drawn with relationship
lines. It keys on the surrogate rather than the natural key precisely because
the ambiguity of the natural key is what it exists to record.
