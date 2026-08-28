"""
Local dashboard over the Gold layer.

Reads only from gold_* tables. No transformation or business logic happens
here - every figure shown was computed and reconciled in build_gold.py, so
the dashboard cannot disagree with the pipeline.

Data coverage is the first tab, before any business chart. The integrity gap
in this source is large enough that coverage is not a footnote: several
obvious questions cannot be answered at all, and a reader needs to know that
before reading anything else.

Run:  streamlit run dashboard/app.py

The database is built on first run if it does not exist.
Set ANALYTICS_DB_PATH to point at a database elsewhere.
"""

import os
import sqlite3
import subprocess
import sys

import pandas as pd
import streamlit as st

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB_PATH = os.path.join(PROJECT_ROOT, "database", "analytics.db")
DB_PATH = os.path.expanduser(os.environ.get("ANALYTICS_DB_PATH", DEFAULT_DB_PATH))

GOLD_TABLES = [
    "gold_data_quality_summary",
    "gold_sales_summary",
    "gold_order_fulfilment",
    "gold_geography_segment",
    "gold_agent_commission",
    "gold_matched_sales_orders",
]

st.set_page_config(page_title="Medallion Analytics", layout="wide")

BUILD_SCRIPTS = ("load_bronze.py", "build_silver.py", "build_gold.py")


@st.cache_resource(show_spinner=False)
def ensure_database():
    """Build the warehouse from the source CSVs if it isn't there yet.

    The pipeline is idempotent and the database is a build artefact, not
    source, so a fresh deployment reconstructs it rather than shipping it.
    Runs against DB_PATH, including a custom ANALYTICS_DB_PATH.
    """
    if os.path.exists(DB_PATH):
        return

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    progress = st.progress(0.0, text="Building the warehouse from source CSVs…")

    for i, script in enumerate(BUILD_SCRIPTS):
        progress.progress(i / len(BUILD_SCRIPTS), text=f"Running {script}…")
        result = subprocess.run(
            [sys.executable, os.path.join(PROJECT_ROOT, "src", script)],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            progress.empty()
            st.error(f"{script} failed")
            st.code(result.stderr or result.stdout)
            st.stop()

    progress.empty()


ensure_database()


@st.cache_data(show_spinner=False)
def load_gold(db_path, modified_at):
    """Load every Gold table in one read-only connection.

    modified_at is unused inside the function but is part of the cache key,
    so re-running the pipeline invalidates the cache automatically instead of
    leaving the dashboard showing stale figures.

    The connection is opened read-only: a dashboard has no business writing
    to the warehouse, and mode=ro makes that structural rather than a
    convention.
    """
    del modified_at
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as connection:
        present = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")}
        missing = sorted(set(GOLD_TABLES) - present)
        if missing:
            raise RuntimeError("Missing Gold tables: " + ", ".join(missing))
        return {name: pd.read_sql(f"SELECT * FROM {name}", connection)
                for name in GOLD_TABLES}


def compact(value):
    """Render large magnitudes readably. Sales values run to 12 digits."""
    magnitude = abs(value)
    for divisor, suffix in [(1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")]:
        if magnitude >= divisor:
            return f"{value / divisor:,.2f}{suffix}"
    return f"{value:,.0f}"


def weighted_mean(frame, value_column, weight_column):
    """Weight an average by row count.

    The marts hold one average per group, and groups carry very different
    volumes, so a plain mean of means would overweight the quiet ones.
    """
    weights = frame[weight_column]
    if frame.empty or weights.sum() == 0:
        return 0.0
    return float((frame[value_column] * weights).sum() / weights.sum())


try:
    tables = load_gold(DB_PATH, os.path.getmtime(DB_PATH))
except (sqlite3.Error, RuntimeError) as error:
    st.error(str(error))
    st.stop()

coverage = tables["gold_data_quality_summary"]
sales_summary = tables["gold_sales_summary"]
fulfilment = tables["gold_order_fulfilment"]
geography = tables["gold_geography_segment"]
commission = tables["gold_agent_commission"]
matched = tables["gold_matched_sales_orders"]

st.title("Medallion Analytics")
st.caption("Bronze → Silver → Gold, built locally in SQLite. "
           "Every figure is read from the Gold layer and reconciled to Silver.")

coverage_tab, fulfilment_tab, geography_tab, commercial_tab, findings_tab = st.tabs(
    ["Data coverage", "Order fulfilment", "Geography & segment",
     "Sales & commission", "Key findings"])


# ---------------------------------------------------------------
# Coverage is the first tab, deliberately
# ---------------------------------------------------------------
with coverage_tab:
    st.markdown(
        "**The source datasets do not join.** Order IDs reconcile between "
        "sales and orders on 42 of 31,115 rows, and a three-way join adding "
        "location returns zero. Each business process is therefore aggregated "
        "from the one table that holds its data. The marts covering a single "
        "table reach 99–100% of their source; the one mart that joins two "
        "tables reaches 0.13%."
    )

    single_source = coverage[coverage.business_process != "matched_sales_orders"]
    # silver_sales feeds two business processes, so the quarantine column has
    # to be deduplicated on the table name before it is summed. Deduplicating
    # on the value would be wrong: two tables could share a count legitimately.
    quarantined = coverage.drop_duplicates(subset="source_table").quarantined_rows.sum()

    one, two, three, four = st.columns(4)
    one.metric("Source rows landed", f"{int(coverage.silver_rows.sum()):,}")
    two.metric("Single-table coverage",
               f"{single_source.coverage_percentage.min():.1f}–"
               f"{single_source.coverage_percentage.max():.1f}%")
    three.metric("Cross-table join coverage", "0.13%",
                 delta="42 of 31,115 rows", delta_color="inverse")
    four.metric("Rows quarantined", f"{int(quarantined):,}")

    st.bar_chart(coverage.set_index("business_process")["coverage_percentage"],
                 height=240)

    st.subheader("Quality status")
    quality_status = coverage.set_index("business_process")[
        ["valid_rows", "warning_rows", "quarantined_rows"]].rename(
            columns={"valid_rows": "Valid",
                     "warning_rows": "Warning",
                     "quarantined_rows": "Quarantined"})
    st.bar_chart(quality_status, height=280, stack=True)

    st.subheader("Matched sales and orders")
    st.warning(
        f"**{len(matched)} of 31,115 sales rows (0.13%).** This is the only "
        "place revenue and product category coexist, so it is the only "
        "possible source of a revenue-by-category figure. Electronics leads "
        "on 18 orders against roughly 7,800 in the full dataset, which makes "
        "the ranking sampling noise. Shown to document the gap, not to "
        "support analysis."
    )
    with st.expander(f"Inspect all {len(matched)} matched rows"):
        st.dataframe(matched, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------
with fulfilment_tab:
    st.caption("From silver_orders. Counts are fulfilment records, not "
               "orders: the grain is the order line, and order IDs collide.")

    categories = sorted(fulfilment.product_category.unique())
    modes = sorted(fulfilment.ship_mode.unique())
    left_filter, right_filter = st.columns(2)
    chosen_categories = left_filter.multiselect(
        "Product category", categories, default=categories)
    chosen_modes = right_filter.multiselect(
        "Ship mode", modes, default=modes)

    filtered = fulfilment[fulfilment.product_category.isin(chosen_categories)
                          & fulfilment.ship_mode.isin(chosen_modes)]

    if filtered.empty:
        st.info("Select at least one category and one ship mode.")
    else:
        one, two, three, four = st.columns(4)
        one.metric("Fulfilment records",
                   f"{int(filtered.fulfilment_record_count.sum()):,}")
        two.metric("Distinct source order IDs",
                   f"{int(filtered.distinct_order_ids.sum()):,}",
                   help="Lower than the record count where an order has "
                        "several product lines.")
        three.metric("Average aging (days)",
                     f"{weighted_mean(filtered, 'average_aging_days', 'fulfilment_record_count'):.1f}")
        four.metric("Records flagged",
                    f"{int(filtered.flagged_record_count.sum()):,}",
                    help="ID collisions and possible split shipments. "
                         "Legitimate multi-line orders are not counted here.")

        st.subheader("Fulfilment records by month")
        monthly = (filtered.groupby("order_month", as_index=False)
                           .agg(records=("fulfilment_record_count", "sum")))
        st.line_chart(monthly.set_index("order_month")["records"], height=260)

        st.subheader("Average aging by category and ship mode")
        weighted = filtered.copy()
        weighted["aging_total"] = (weighted.average_aging_days
                                   * weighted.fulfilment_record_count)
        by_mode = (weighted.groupby(["product_category", "ship_mode"],
                                    as_index=False)
                           .agg(aging_total=("aging_total", "sum"),
                                records=("fulfilment_record_count", "sum")))
        by_mode["average_aging_days"] = (by_mode.aging_total
                                         / by_mode.records).round(2)
        st.bar_chart(by_mode.pivot(index="product_category", columns="ship_mode",
                                   values="average_aging_days"), height=260)
        st.caption("First Class and Standard Class deliver in the same average "
                   "time. That is not a plausible service difference, and it "
                   "is one reason these figures should not be read as "
                   "operational performance.")


# ---------------------------------------------------------------
with geography_tab:
    st.caption("From silver_location. Shows where orders are delivered. "
               "Revenue by region is not available: revenue lives in sales, "
               "which joins to location on 51 of 25,504 rows.")

    regions = ["All regions"] + sorted(geography.region.unique())
    chosen_region = st.selectbox("Drill into a region", regions)

    scoped = (geography if chosen_region == "All regions"
              else geography[geography.region == chosen_region])
    level = "region" if chosen_region == "All regions" else "country"

    if scoped.empty:
        st.info("No delivery records for that region.")
    else:
        one, two, three = st.columns(3)
        one.metric("Delivery records",
                   f"{int(scoped.location_record_count.sum()):,}")
        two.metric("Countries", f"{scoped.country.nunique():,}")
        three.metric("Cities", f"{scoped.city.nunique():,}")

        st.subheader(f"Delivery records by {level}")
        distribution = (scoped.groupby(level, as_index=False)
                              .agg(records=("location_record_count", "sum"))
                              .sort_values("records", ascending=False))
        st.bar_chart(distribution.set_index(level)["records"], height=260)

        st.subheader("Customer segment mix")
        segment_mix = (scoped.groupby("segment", as_index=False)
                             .agg(records=("location_record_count", "sum")))
        segment_mix["share_percent"] = (100 * segment_mix.records
                                        / segment_mix.records.sum()).round(1)
        st.dataframe(segment_mix, use_container_width=True, hide_index=True)
        st.caption("The three segments split almost exactly in thirds, which "
                   "no real retailer does. Treat the mix as an artefact of "
                   "how this data was generated.")


# ---------------------------------------------------------------
with commercial_tab:
    st.caption("From silver_sales and silver_agent_commission. Neither table "
               "has a date or a joinable category, so each is reported on the "
               "dimensions it actually holds.")

    priorities = sorted(sales_summary.order_priority.unique())
    chosen_priorities = st.multiselect("Order priority", priorities,
                                       default=priorities)
    scoped_sales = sales_summary[
        sales_summary.order_priority.isin(chosen_priorities)]

    if scoped_sales.empty:
        st.info("Select at least one order priority.")
    else:
        one, two, three = st.columns(3)
        one.metric("Sales value", compact(float(scoped_sales.total_sales.sum())))
        two.metric("Sales records",
                   f"{int(scoped_sales.sales_record_count.sum()):,}")
        three.metric("Rows affected by collisions",
                     f"{int(scoped_sales.collision_affected_rows.sum()):,}")

        left, right = st.columns(2)
        with left:
            st.subheader("Sales value by priority")
            by_priority = (scoped_sales.groupby("order_priority", as_index=False)
                                       .agg(total_sales=("total_sales", "sum")))
            st.bar_chart(by_priority.set_index("order_priority")["total_sales"],
                         height=240)
            st.caption("Amounts are unscaled. Sales is a fixed-width 8-digit "
                       "value with a median of 50.2 million, so these are "
                       "relative magnitudes, not currency.")

        with right:
            st.subheader("Sales value by discount level")
            by_discount = (scoped_sales.groupby("discount_level", as_index=False)
                                       .agg(total_sales=("total_sales", "sum")))
            st.bar_chart(by_discount.set_index("discount_level")["total_sales"],
                         height=240)
            st.caption("Discount is an integer 0–9. The source does not "
                       "document whether that is a percentage, a decile or a "
                       "band, so it is carried through unscaled.")

        st.subheader("Commission rate by category")
        commission_rates = commission.set_index("product_category")[
            ["minimum_commission_percentage",
             "average_commission_percentage",
             "maximum_commission_percentage"]].rename(
                columns={"minimum_commission_percentage": "Minimum",
                         "average_commission_percentage": "Average",
                         "maximum_commission_percentage": "Maximum"})
        st.bar_chart(commission_rates, height=280)
        st.caption("Rates run from 1.00 to 99.99 with the four category "
                   "averages within half a point of each other. Agent "
                   "geography is omitted: commission resolves to the agent "
                   "master on 1.47% of rows.")


# ---------------------------------------------------------------
with findings_tab:
    st.markdown(
        """
**1. Referential integrity has failed at the source.** Sales to orders
resolves on 0.13% of rows, location to orders on 0.17%, and agent references
on roughly 1.5%. Whitespace, casing and format were ruled out — the keys are
generated independently per file. This is an upstream key-generation defect
and would need escalating to the source system owners before anything is
built on top of it.

**2. Conformed dimensions are intact, and that is what makes a model
possible.** Product category matches exactly across orders and commission,
and all 23 countries and 76 cities are shared between location and agent.
Each source is modelled as its own fact against those dimensions.

**3. Duplicate keys are three different problems, not one.** In orders, 15 of
18 repeated IDs share an order date and category and differ only in product —
those are line items, and the grain is finer than the table name implies. In
sales, location and agent, no duplicate pair shares a customer, a city or a
name, so those are ID collisions. Every collision row is retained with a
surrogate key: there is no basis for deciding which row is correct.

**4. The measures are independently generated and cannot be combined.** Sales
correlates with profit at −0.001 and with quantity at 0.005. No margin or
unit-price figure is published. Order value is a fixed-width 8-digit number
with a median of 50.2 million, so amounts are relative magnitudes only.

**5. Several operational signals are not credible.** First Class and Standard
Class share the same average aging. Customer segments split 33.4 / 33.3 /
33.3. Commission runs uniformly from 1% to 99.99%, with 6,205 agents above
90%. These are artefacts of generation rather than findings, and would be
flagged to a stakeholder before any conclusion was drawn.

**6. What cannot be answered.** Revenue over time, revenue by category,
revenue by region, profit margin, unit price, and any customer-level
analysis.

---

**What a business could act on today.** Fulfilment volume and delivery time
by category and ship mode, order distribution across 23 countries and three
segments, and commission cost by category — all covering 99–100% of their
source. What they cannot get is anything combining sales with another table.
That is a broken key upstream, not a modelling limitation, and fixing order
ID generation would unlock the entire revenue analytics surface. Until then,
anyone building a category revenue report on this data is reporting on 42
orders without knowing it.
"""
    )

st.divider()
st.caption(
    f"Source: Gold tables in {os.path.basename(DB_PATH)} · "
    f"database last built "
    f"{pd.Timestamp(os.path.getmtime(DB_PATH), unit='s').strftime('%Y-%m-%d %H:%M')}"
)
