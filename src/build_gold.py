"""
Gold layer: business-ready marts built from Silver.

The shape of this layer is dictated by one finding: the fact tables do not
join. Order IDs reconcile between sales and orders on 42 of 31,115 rows, and
a three-way join with location returns zero. So there is no wide fact table
here. Each business process is aggregated from the one Silver table that
actually holds its data, and only dimensions that genuinely conform - product
category and geography - are used across marts.

What each mart may contain is therefore constrained by what its source table
holds. sales carries no date, so no sales mart is reported over time.
Category lives in orders and revenue lives in sales, so no category revenue
figure is published. These are absences by design, and the coverage table
records them.

One mart, gold_matched_sales_orders, does join sales to orders. It exists to
make the integrity gap visible rather than to be analysed: it carries 42 rows
against 31,115, and its coverage is displayed alongside it.

Run:  python src/build_gold.py   (requires build_silver.py to have run)
"""

import os
import sqlite3

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "database", "analytics.db")

coverage_rows = []


def record_coverage(business_process, source_table, silver_rows, gold_rows,
                    rows_used, note=""):
    """Log how much of a Silver table reached its mart, and why."""
    coverage_rows.append({
        "business_process": business_process,
        "source_table": source_table,
        "silver_rows": silver_rows,
        "gold_rows": gold_rows,
        "source_rows_aggregated": rows_used,
        "coverage_percentage": round(100 * rows_used / silver_rows, 2),
        "note": note,
    })


# ---------------------------------------------------------------
# 1. Sales performance
#
# silver_sales holds no date and no product category - both live in tables
# that do not join to it. Its only true dimensions are order priority and
# discount level, so that is the whole grain of this mart.
#
# Rows graded quarantine are excluded: those are the 271 zero-quantity rows,
# whose amounts cannot be trusted. Collisions are graded warning and are
# kept, because their amounts are valid even though their order ID is not.
# ---------------------------------------------------------------

def build_sales_summary(sales):
    usable = sales[sales.dq_status != "quarantine"]

    mart = (usable.groupby(["order_priority", "discount"], as_index=False)
                  .agg(sales_record_count=("sales_key", "count"),
                       total_sales=("sales", "sum"),
                       total_quantity=("quantity", "sum"),
                       total_profit=("profit", "sum"),
                       total_shipping_cost=("shipping_cost", "sum"),
                       collision_affected_rows=("is_id_collision", "sum")))

    # "discount level", not "discount band": the source does not document
    # whether 0-9 is a percentage, a decile or a banding, so the values are
    # carried through unscaled and unlabelled.
    mart = mart.rename(columns={"discount": "discount_level"})
    mart["average_sales_per_record"] = (mart.total_sales
                                        / mart.sales_record_count).round(2)

    record_coverage("sales_performance", "silver_sales", len(sales),
                    len(mart), len(usable),
                    "excludes rows quarantined for zero quantity against a "
                    "non-zero amount; no date or category dimension exists "
                    "in this table")
    return mart


# ---------------------------------------------------------------
# 2. Order fulfilment
#
# The count is named fulfilment_record_count rather than order_count: the
# grain of silver_orders is the order line, and order IDs collide, so a row
# count is not an order count.
# ---------------------------------------------------------------

def build_order_fulfilment(orders):
    usable = orders[orders.dq_status != "quarantine"].copy()
    usable["order_month"] = usable.order_date.str.slice(0, 7)

    mart = (usable.groupby(["order_month", "product_category", "ship_mode"],
                           as_index=False)
                  .agg(fulfilment_record_count=("order_line_key", "count"),
                       distinct_order_ids=("order_id", "nunique"),
                       average_aging_days=("aging", "mean"),
                       maximum_aging_days=("aging", "max")))
    mart["average_aging_days"] = mart.average_aging_days.round(2)

    # Anomalies are the rows graded warning - ID collisions and possible
    # split shipments. multi_product_order is deliberately not counted here:
    # those are legitimate line items on one order, and counting them as
    # anomalies would report 36 problem rows where there are 10.
    flagged = (usable[usable.dq_status == "warning"]
               .groupby(["order_month", "product_category", "ship_mode"],
                        as_index=False)
               .agg(flagged_record_count=("order_line_key", "count")))
    mart = mart.merge(flagged, how="left",
                      on=["order_month", "product_category", "ship_mode"])
    mart["flagged_record_count"] = mart.flagged_record_count.fillna(0).astype(int)

    record_coverage("order_fulfilment", "silver_orders", len(orders),
                    len(mart), len(usable),
                    "distinct_order_ids is lower than the record count "
                    "because the grain is order line, not order")
    return mart


# ---------------------------------------------------------------
# 3. Geography and customer segment
#
# location_record_count, not order_count: order IDs collide here too, so
# these are delivery records rather than distinct orders.
# ---------------------------------------------------------------

def build_geography_segment(location, geography):
    usable = location[location.dq_status != "quarantine"]

    mart = (usable.groupby(["region", "country", "state", "city", "segment"],
                           as_index=False)
                  .agg(location_record_count=("location_key", "count"),
                       distinct_source_order_ids=("order_id", "nunique")))

    # Attach the conformed geography key on the full hierarchy, not just city
    # and country. validate="many_to_one" makes the join fail loudly if the
    # dimension ever stops being unique at that grain, rather than silently
    # multiplying rows and inflating every count downstream.
    mart = mart.merge(geography[["geography_key", "city", "state", "country"]],
                      how="left", on=["city", "state", "country"],
                      validate="many_to_one")

    record_coverage("geography_segment", "silver_location", len(location),
                    len(mart), len(usable),
                    "counts delivery records, not distinct orders")
    return mart


# ---------------------------------------------------------------
# 4. Agent commission
#
# Reported by product category only. Agent geography is deliberately absent:
# commission resolves to the agent master on roughly 1.5% of rows, so a
# geography breakdown would describe that 1.5% while appearing to describe
# the whole commission book.
# ---------------------------------------------------------------

def build_agent_commission(commission):
    # Silver quarantines conflicting rates, so filtering on dq_status excludes
    # them here. That exclusion is deliberate rather than incidental: where one
    # agent and category carry two rates, averaging them would publish a figure
    # that is not either agent's actual commission. The cost is that those 152
    # agents contribute nothing to the average; at 0.5% of rows that bias is
    # smaller than the bias from averaging contradictory values.
    usable = commission[commission.dq_status != "quarantine"]

    mart = (usable.groupby("product_category", as_index=False)
                  .agg(commission_record_count=("commission_key", "count"),
                       distinct_agents=("agent_id", "nunique"),
                       average_commission_percentage=("commission_percentage", "mean"),
                       minimum_commission_percentage=("commission_percentage", "min"),
                       maximum_commission_percentage=("commission_percentage", "max"),
                       ))

    # Excluded rows are counted per category, not carried as a table-wide
    # total: repeating one figure on every row would read as a per-category
    # count and would quadruple if anyone summed the column.
    # SQLite has no boolean type, so flags written by Silver come back as
    # integers. The comparison is explicit rather than relying on truthiness,
    # which would treat the column as a list of column labels.
    excluded_by_category = (commission[commission.is_conflicting_rate == 1]
                            .groupby("product_category").size())
    mart["excluded_conflicting_rows"] = (mart.product_category
                                         .map(excluded_by_category)
                                         .fillna(0).astype(int))

    for column in ["average_commission_percentage",
                   "minimum_commission_percentage",
                   "maximum_commission_percentage"]:
        mart[column] = mart[column].round(2)

    record_coverage("agent_commission", "silver_agent_commission",
                    len(commission), len(mart), len(usable),
                    "category level only; agent geography omitted because "
                    "commission resolves to the agent master on ~1.5% of rows")
    return mart


# ---------------------------------------------------------------
# 5. Matched sales and orders
#
# This mart exists to show the integrity gap, not to be analysed. It is the
# only place the two facts are joined, and it covers 0.13% of sales. Its
# coverage is recorded so the dashboard can display the denominator next to
# any figure drawn from it.
#
# A three-way join adding location returns zero rows, so geography cannot be
# attached to matched sales at all.
# ---------------------------------------------------------------

def build_matched_sales_orders(sales, orders, location):
    matched = sales.merge(
        orders[["order_line_key", "order_id", "order_date", "product_category",
                "product", "ship_mode", "aging", "dq_status"]],
        how="inner", on="order_id", suffixes=("", "_order"))

    mart = matched.rename(columns={"dq_status": "sales_dq_status",
                                   "dq_status_order": "order_dq_status"})
    mart = mart[["sales_key", "order_line_key", "order_id", "order_date",
                 "product_category", "product", "ship_mode", "aging",
                 "sales", "quantity", "discount", "profit", "shipping_cost",
                 "order_priority", "sales_dq_status", "order_dq_status"]].copy()

    three_way = mart.merge(location[["order_id"]], how="inner", on="order_id")

    # Coverage is measured in distinct sales rows, not joined rows. One sales
    # row matching several order lines would produce several joined rows and
    # overstate coverage; the two happen to be equal here, but the join is
    # not guaranteed to stay one-to-one.
    matched_sales_rows = mart["sales_key"].nunique()

    record_coverage("matched_sales_orders", "silver_sales", len(sales),
                    len(mart), matched_sales_rows,
                    f"inner join on order_id, {matched_sales_rows} distinct "
                    f"sales rows matched; {len(three_way)} rows survive a "
                    f"further join to location, so geography cannot be attached")
    return mart


# ---------------------------------------------------------------
# 6. Coverage and quality summary
# ---------------------------------------------------------------

def build_quality_summary(connection):
    """One row per business process, plus the Silver quality grades."""
    coverage = pd.DataFrame(coverage_rows)

    grades = pd.read_sql("""
        SELECT table_name AS source_table,
               SUM(CASE WHEN check_name = 'dq_status_valid' THEN value END) AS valid_rows,
               SUM(CASE WHEN check_name = 'dq_status_warning' THEN value END) AS warning_rows,
               SUM(CASE WHEN check_name = 'dq_status_quarantine' THEN value END) AS quarantined_rows
        FROM silver_data_quality
        WHERE check_name LIKE 'dq_status_%'
        GROUP BY table_name
    """, connection)

    summary = coverage.merge(grades, how="left", on="source_table")
    for column in ["valid_rows", "warning_rows", "quarantined_rows"]:
        summary[column] = summary[column].fillna(0).astype(int)
    return summary


# ---------------------------------------------------------------

# Each mart's declared grain. Enforced as a unique index rather than left as
# a comment: these tables are far too small for an index to matter for speed,
# so the constraint exists purely to make the grain claim testable. If an
# aggregation ever stopped collapsing to the stated grain, the build fails
# here instead of quietly publishing duplicated rows.
GOLD_GRAIN = {
    "gold_sales_summary": ["order_priority", "discount_level"],
    "gold_order_fulfilment": ["order_month", "product_category", "ship_mode"],
    "gold_geography_segment": ["region", "country", "state", "city", "segment"],
    "gold_agent_commission": ["product_category"],
    "gold_matched_sales_orders": ["sales_key", "order_line_key"],
    "gold_data_quality_summary": ["business_process"],
}


def main():
    # Module-level accumulator, so a second call inside one process would
    # otherwise append to the first run's coverage rows.
    coverage_rows.clear()

    connection = sqlite3.connect(DB_PATH)
    try:
        def read_silver(name):
            return pd.read_sql(f"SELECT * FROM silver_{name}", connection)

        sales = read_silver("sales")
        orders = read_silver("orders")
        location = read_silver("location")
        commission = read_silver("agent_commission")
        geography = read_silver("dim_geography")

        gold_tables = {
            "gold_sales_summary": build_sales_summary(sales),
            "gold_order_fulfilment": build_order_fulfilment(orders),
            "gold_geography_segment": build_geography_segment(location, geography),
            "gold_agent_commission": build_agent_commission(commission),
            "gold_matched_sales_orders": build_matched_sales_orders(
                sales, orders, location),
        }
        gold_tables["gold_data_quality_summary"] = build_quality_summary(connection)

        for table_name in gold_tables:
            frame = gold_tables[table_name]
            frame.to_sql(table_name, connection, if_exists="replace", index=False)
            print(f"{table_name}: {len(frame)} rows")
        print()

        # Every Gold measure reconciles to its Silver source. An aggregation
        # that silently drops rows produces a plausible-looking wrong number,
        # so each mart's row count is checked against the rows it aggregated,
        # and the sales total against the sum it was built from.
        checks = [
            ("gold_sales_summary row count",
             "SELECT SUM(sales_record_count) FROM gold_sales_summary",
             "SELECT COUNT(*) FROM silver_sales WHERE dq_status != 'quarantine'"),
            ("gold_sales_summary total",
             "SELECT SUM(total_sales) FROM gold_sales_summary",
             "SELECT SUM(sales) FROM silver_sales WHERE dq_status != 'quarantine'"),
            ("gold_order_fulfilment row count",
             "SELECT SUM(fulfilment_record_count) FROM gold_order_fulfilment",
             "SELECT COUNT(*) FROM silver_orders WHERE dq_status != 'quarantine'"),
            ("gold_geography_segment row count",
             "SELECT SUM(location_record_count) FROM gold_geography_segment",
             "SELECT COUNT(*) FROM silver_location WHERE dq_status != 'quarantine'"),
            ("gold_agent_commission row count",
             "SELECT SUM(commission_record_count) FROM gold_agent_commission",
             "SELECT COUNT(*) FROM silver_agent_commission "
             "WHERE dq_status != 'quarantine' AND is_conflicting_rate = 0"),
            ("matched coverage",
             "SELECT COUNT(DISTINCT sales_key) FROM gold_matched_sales_orders",
             "SELECT source_rows_aggregated FROM gold_data_quality_summary "
             "WHERE business_process = 'matched_sales_orders'"),
        ]

        for label, gold_query, silver_query in checks:
            gold_value = connection.execute(gold_query).fetchone()[0]
            silver_value = connection.execute(silver_query).fetchone()[0]
            if gold_value != silver_value:
                raise AssertionError(
                    f"{label}: gold {gold_value} != source {silver_value}")
            print(f"  reconciled - {label}: {gold_value:,}")

        # The geography key must attach to every row; a null would mean the
        # conformed dimension does not cover a city the facts refer to.
        unmatched = connection.execute(
            "SELECT COUNT(*) FROM gold_geography_segment "
            "WHERE geography_key IS NULL").fetchone()[0]
        if unmatched:
            raise AssertionError(
                f"{unmatched} geography rows have no dimension key")

        for table_name in GOLD_GRAIN:
            columns = ", ".join(GOLD_GRAIN[table_name])
            connection.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS grain_{table_name} "
                f"ON {table_name} ({columns})")

        connection.commit()
        print("\nAll Gold measures reconcile to Silver.")
        print("Grain constraints applied to all six marts.")
    except Exception:
        # A failure partway through would otherwise leave some marts rebuilt
        # and others stale, which is worse than none being rebuilt.
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()