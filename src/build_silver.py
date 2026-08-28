"""
Silver layer: clean, type, standardise and validate the Bronze tables.

Design decisions, each driven by a profiling finding:

1. No row is ever deleted. Duplicate keys in this source are ID collisions
   between unrelated records, not redundant copies, so there is no basis for
   choosing which row is correct. Every table gets a surrogate key so the
   rows can coexist, and the collisions are flagged in a boolean column.

2. Surrogate keys are assigned after a deterministic sort, so the same
   input always produces the same keys. Relying on row order would make the
   keys change between runs.

3. Types are cast explicitly here rather than inferred at read time. Values
   that fail to cast are counted into the data quality table instead of
   raising, because the count is itself a measurement.

4. Column names are standardised to snake_case.

5. Referential integrity is measured and recorded, not enforced. Enforcing
   it would discard almost the entire dataset - match rates are between
   0.13% and 1.55%.

6. Quality is expressed two ways. Every row carries a dq_status of valid,
   warning or quarantine so downstream layers can filter with one predicate.
   Separately, silver_data_quality_issues holds one row per problem found,
   so any flagged record can be joined back and inspected individually.

   ID collisions are graded 'warning' rather than 'quarantine': the rows
   themselves are complete and internally consistent, it is the identifier
   that is ambiguous. Excluding them would delete real orders. Values that
   are internally contradictory - a failed cast, zero units for a non-zero
   amount - are graded 'quarantine' because the record cannot be trusted.

Run:  python src/build_silver.py   (requires load_bronze.py to have run)
"""

import os
import sqlite3

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "database", "analytics.db")

# One declaration of each table's natural key - the columns that identify a
# row at that table's grain. Every surrogate key is ordered by its table's
# natural key so the two cannot drift apart.
#
# None of these is unique in this source; that is the finding, not an
# oversight. They are recorded because they are what the key *should* be,
# and because ordering by them keeps related rows adjacent.
#
# orders carries order_date as well as order_id and product. Strictly the
# grain is order line, which order_id plus product expresses - order_date is
# there because order IDs collide across unrelated orders and the date is
# what separates them. It is included so this declaration matches the grain
# conclusion documented in build_orders rather than contradicting it.
#
# Columns rejected as key components elsewhere in the analysis - customer_id
# (functionally dependent on order_id), location_id (not a place reference),
# shipping_date (a mutable fact attribute) - are deliberately absent here
# too, so the ordering never implies a key the analysis rejected.
NATURAL_KEYS = {
    "agent": ["agent_id"],
    "agent_commission": ["agent_id", "product_category"],
    "orders": ["order_id", "order_date", "product"],
    "sales": ["order_id"],
    "location": ["order_id"],
}

# Collected as the pipeline runs, then written to silver_data_quality.
quality_checks = []


def record(table, check, value, note=""):
    """Add one data quality measurement to the run log."""
    quality_checks.append({
        "table_name": table,
        "check_name": check,
        "value": value,
        "note": note,
    })


# One row per problem found, so a flagged record can be joined back to the
# table it came from and inspected. The aggregate counts in
# silver_data_quality answer "how much"; this table answers "which rows".
quality_issues = []


def log_issues(frame, mask, table_name, key_column, issue_code, severity, detail):
    """Record one issue row for every row matching `mask`."""
    flagged = frame.loc[mask, key_column]
    for record_id in flagged:
        quality_issues.append({
            "silver_table": table_name,
            "silver_record_id": int(record_id),
            "issue_code": issue_code,
            "severity": severity,
            "issue_detail": detail,
        })


def grade(frame, quarantine_mask, warning_mask):
    """Combine masks into a single dq_status column.

    quarantine wins over warning: a record that is both ambiguous and
    internally contradictory is the less trustworthy of the two.
    """
    status = pd.Series("valid", index=frame.index)
    status[warning_mask] = "warning"
    status[quarantine_mask] = "quarantine"
    return status


def standardise_columns(frame):
    """Lower-case the column names and strip whitespace from text values.

    Source columns are already underscore-separated (Agent_ID,
    Product_Category), so lower-casing is all that snake_case requires.
    A regex that splits on capitals would turn Agent_ID into agent_i_d.
    """
    frame = frame.rename(columns={c: c.lower() for c in frame.columns})
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = frame[column].str.strip()
    return frame


def add_surrogate_key(frame, key_name, dataset):
    """Add a stable surrogate key, ordered by the table's natural key.

    Sorting before numbering makes the key deterministic: the same source
    files always produce the same key for the same row, regardless of the
    order the batch files happened to be read in.

    batch_timestamp breaks ties between rows sharing a natural key, and
    source_file plus source_row break any remaining tie - 88 of the
    commission conflicts occur inside one batch, so the timestamp alone is
    not enough. Ordering on the source position rather than relying on the
    order SQLite returns rows in makes the key deterministic by construction:
    SQL does not guarantee row order without an ORDER BY.
    """
    sort_columns = (NATURAL_KEYS[dataset]
                    + ["batch_timestamp", "source_file", "source_row"])
    frame = frame.sort_values(sort_columns, kind="mergesort").reset_index(drop=True)
    frame.insert(0, key_name, range(1, len(frame) + 1))
    return frame


def flag_collisions(frame, key_columns, identity_columns, flag_name):
    """Mark rows whose key repeats across records that are not the same entity.

    Rows sharing a key are the same entity only if they agree on the
    identity columns - a single order has one order date, a single agent has
    one name. Where they disagree, the key has collided.
    """
    repeated = frame.duplicated(subset=key_columns, keep=False)
    frame[flag_name] = False
    if not repeated.any():
        return frame

    groups = frame[repeated].groupby(key_columns)
    agrees = None
    for column in identity_columns:
        matches = groups[column].nunique() == 1
        agrees = matches if agrees is None else (agrees & matches)

    colliding_keys = agrees[~agrees].index
    if len(colliding_keys) > 0:
        if len(key_columns) == 1:
            is_collision = frame[key_columns[0]].isin(colliding_keys)
        else:
            index = pd.MultiIndex.from_frame(frame[key_columns])
            is_collision = index.isin(colliding_keys)
        frame.loc[is_collision, flag_name] = True
    return frame


def cast_numeric(frame, column, table_name):
    """Cast a text column to numeric, recording any failures."""
    values = pd.to_numeric(frame[column], errors="coerce")
    failed = int(values.isna().sum())
    if failed:
        record(table_name, f"uncastable_{column}", failed,
               "values that could not be parsed as numeric")
    return values


def cast_date(frame, column, table_name):
    """Cast a text column to date, recording any failures."""
    values = pd.to_datetime(frame[column], errors="coerce")
    failed = int(values.isna().sum())
    if failed:
        record(table_name, f"unparseable_{column}", failed,
               "values that could not be parsed as a date")
    return values.dt.strftime("%Y-%m-%d")


# ---------------------------------------------------------------
# Table builders
# ---------------------------------------------------------------

def build_agent(bronze):
    """Agent master. Grain: one agent. Natural key: agent_id."""
    frame = standardise_columns(bronze)
    frame = flag_collisions(frame, ["agent_id"], ["agent_name"], "is_id_collision")
    frame = add_surrogate_key(frame, "agent_key", "agent")

    frame["dq_status"] = grade(frame,
                               quarantine_mask=pd.Series(False, index=frame.index),
                               warning_mask=frame.is_id_collision)
    log_issues(frame, frame.is_id_collision, "silver_agent", "agent_key",
               "agent_id_collision", "warning",
               "Agent_ID identifies two different people; both rows retained.")

    record("silver_agent", "row_count", len(frame))
    record("silver_agent", "id_collisions", int(frame.is_id_collision.sum()),
           "same agent_id, different person - both rows retained")
    return frame


def build_agent_commission(bronze):
    """Commission rates. Grain: one agent x product category.

    This is the only table with no valid natural key: agent_id plus
    product_category still repeats, with conflicting rates, and 88 of those
    conflicts occur inside a single batch file - so they cannot be resolved
    by taking the most recent batch. The surrogate key is mandatory here.
    """
    frame = standardise_columns(bronze)
    frame["commission_percentage"] = cast_numeric(
        frame, "commission_percentage", "silver_agent_commission")

    frame = flag_collisions(frame, ["agent_id", "product_category"],
                            ["commission_percentage"], "is_conflicting_rate")
    frame = add_surrogate_key(frame, "commission_key", "agent_commission")

    # A conflicting rate is quarantined, not merely flagged. The dividing
    # line used throughout Silver is whether the record can be trusted, not
    # whether its key is clean: an ID collision leaves a valid record with an
    # ambiguous identifier, but two rates for one agent and category means
    # the measure itself is contradictory. Grading it warning would let both
    # values into a published average, which is the one outcome to avoid.
    uncastable = frame.commission_percentage.isna()
    frame["dq_status"] = grade(
        frame,
        quarantine_mask=uncastable | frame.is_conflicting_rate,
        warning_mask=pd.Series(False, index=frame.index))
    log_issues(frame, frame.is_conflicting_rate, "silver_agent_commission",
               "commission_key", "conflicting_commission_rate", "error",
               "Agent and category resolve to more than one rate; no natural key exists.")
    log_issues(frame, uncastable, "silver_agent_commission", "commission_key",
               "invalid_commission_percentage", "error",
               "Commission_Percentage could not be cast to a number.")

    record("silver_agent_commission", "row_count", len(frame))
    record("silver_agent_commission", "conflicting_rates",
           int(frame.is_conflicting_rate.sum()),
           "same agent and category, different rate - no natural key exists")
    return frame


def build_orders(bronze):
    """Order fulfilment. Grain: one product line on one order.

    The table name implies one row per order, but 15 order IDs carry two
    rows that share an order date and category and differ only in product.
    Those are line items, so the grain is finer than the name suggests and
    the key needs order_date (to separate collided orders) and product.
    """
    frame = standardise_columns(bronze)
    frame["order_date"] = cast_date(frame, "order_date", "silver_orders")
    frame["shipping_date"] = cast_date(frame, "shipping_date", "silver_orders")
    frame["aging"] = cast_numeric(frame, "aging", "silver_orders")

    # A single order has one order date; a differing date means the ID
    # collided across two unrelated orders.
    frame = flag_collisions(frame, ["order_id"], ["order_date"],
                            "is_id_collision")

    # Same order, same date, same product appearing twice is only a split
    # shipment if the shipment itself differs. If the shipping columns also
    # match, the rows are an exact duplicate of one another and mean
    # something else entirely, so the two are separated rather than assumed.
    order_line = ["order_id", "order_date", "product"]
    shipment = ["shipping_date", "ship_mode", "aging"]

    repeats_order_line = frame.duplicated(subset=order_line, keep=False)
    repeats_whole_row = frame.duplicated(subset=order_line + shipment,
                                         keep=False)

    is_split = repeats_order_line & ~repeats_whole_row
    is_exact_duplicate = repeats_whole_row
    repeats_order_id = frame.duplicated(subset=["order_id"], keep=False)

    # A single label rather than several booleans, because the four cases are
    # mutually exclusive. multi_product_order is named explicitly: those rows
    # are legitimate line items, and leaving them unlabelled would make the
    # single most consequential finding in this table invisible.
    #
    # Each condition excludes the collisions rather than relying on a later
    # assignment to overwrite them. "order_id repeats" is not by itself
    # evidence of a multi-product order - a collided ID repeats too - so the
    # exclusion is part of the test, not a side effect of ordering. With
    # every duplicate in this source being a pair the two formulations agree,
    # but an order ID shared by two multi-line orders would silently lose its
    # split-shipment label under the overwrite-only version.
    frame["record_type"] = "single_product_order"
    frame.loc[repeats_order_id & ~frame.is_id_collision,
              "record_type"] = "multi_product_order"
    frame.loc[is_split & ~frame.is_id_collision,
              "record_type"] = "possible_split_shipment"
    frame.loc[is_exact_duplicate & ~frame.is_id_collision,
              "record_type"] = "exact_duplicate"
    frame.loc[frame.is_id_collision, "record_type"] = "id_collision"

    frame = add_surrogate_key(frame, "order_line_key", "orders")

    # Aging is derived from the two dates, so it can be reconciled against
    # them. A mismatch means one of the three fields is wrong.
    order_dt = pd.to_datetime(frame.order_date)
    ship_dt = pd.to_datetime(frame.shipping_date)
    inconsistent = ((ship_dt - order_dt).dt.days != frame.aging) | (ship_dt < order_dt)

    frame["dq_status"] = grade(
        frame,
        quarantine_mask=inconsistent | (frame.record_type == "exact_duplicate"),
        warning_mask=frame.record_type.isin(["id_collision",
                                             "possible_split_shipment"]))

    log_issues(frame, frame.record_type == "id_collision", "silver_orders",
               "order_line_key", "order_id_collision", "warning",
               "Order_ID reused across orders placed years apart.")
    log_issues(frame, frame.record_type == "possible_split_shipment",
               "silver_orders", "order_line_key", "possible_split_shipment",
               "warning",
               "Order, date and product repeat with different ship modes.")
    log_issues(frame, frame.record_type == "exact_duplicate", "silver_orders",
               "order_line_key", "exact_duplicate_row", "error",
               "Order line repeats with identical shipment details.")
    log_issues(frame, inconsistent, "silver_orders", "order_line_key",
               "aging_inconsistent_with_dates", "error",
               "Aging does not reconcile with order and shipping dates.")

    record("silver_orders", "row_count", len(frame))
    for label, count in frame.record_type.value_counts().items():
        record("silver_orders", f"record_type_{label}", int(count))
    return frame


def build_sales(bronze):
    """Order financials. Grain: one order.

    sales has no product column, so it cannot be line level even in
    principle - there would be nothing to tell one line from another.
    All 23 duplicate order IDs are collisions.
    """
    frame = standardise_columns(bronze)

    # Casting sales to a number discards the leading zeros that Bronze
    # preserved, so first establish that they carry no information. Every
    # value is exactly 8 characters, and zero-padding the cast result back
    # to 8 reproduces the original string on every row - the zeros are
    # fixed-width padding, and the cast is lossless. If either check failed,
    # the zeros would mean something and the column would have to stay text.
    original_sales = frame["sales"]
    fixed_width = int((original_sales.str.len() == 8).all())
    record("silver_sales", "sales_fixed_width_8", fixed_width,
           "1 if every raw Sales value is exactly 8 characters")

    # discount is an integer 0-9, not a 0-1 fraction. It is cast but not
    # rescaled, because what it denotes (percent, decile, band) is not
    # documented in the source.
    for column in ["sales", "quantity", "discount", "profit", "shipping_cost"]:
        frame[column] = cast_numeric(frame, column, "silver_sales")

    recoverable = int((frame["sales"].astype("Int64").astype(str)
                       .str.zfill(8) == original_sales).all())
    record("silver_sales", "sales_leading_zeros_lossless", recoverable,
           "1 if zero-padding the cast value reproduces the source string")

    frame = flag_collisions(frame, ["order_id"],
                            ["customer_id", "agent_id"], "is_id_collision")

    # Zero units against a non-zero amount is internally inconsistent. It
    # could be a return, a cancellation or bad data; the schema does not
    # distinguish them, so the rows are flagged, not interpreted.
    frame["is_zero_quantity"] = (frame["quantity"] == 0) & (frame["sales"] != 0)

    frame = add_surrogate_key(frame, "sales_key", "sales")

    uncastable = frame[["sales", "quantity", "discount", "profit",
                        "shipping_cost"]].isna().any(axis=1)
    frame["dq_status"] = grade(
        frame,
        quarantine_mask=uncastable | frame.is_zero_quantity,
        warning_mask=frame.is_id_collision)

    log_issues(frame, frame.is_id_collision, "silver_sales", "sales_key",
               "order_id_collision", "warning",
               "Order_ID resolves to two records with different customers and agents.")
    log_issues(frame, frame.is_zero_quantity, "silver_sales", "sales_key",
               "zero_quantity_nonzero_sales", "error",
               "Zero units recorded against a non-zero sales amount.")
    log_issues(frame, uncastable, "silver_sales", "sales_key",
               "invalid_numeric_value", "error",
               "At least one measure could not be cast to a number.")

    record("silver_sales", "row_count", len(frame))
    record("silver_sales", "id_collisions", int(frame.is_id_collision.sum()),
           "same order_id, different customer and agent")
    record("silver_sales", "zero_quantity_rows",
           int(frame.is_zero_quantity.sum()),
           "zero units with a non-zero sales amount")
    return frame


def build_location(bronze):
    """Delivery geography and customer segment. Grain: one order.

    location_id is retained as an attribute but is not a foreign key to a
    place: 330 values map to more than one city, and each city carries
    several hundred distinct values.
    """
    frame = standardise_columns(bronze)
    frame = flag_collisions(frame, ["order_id"],
                            ["city", "customer_name"], "is_id_collision")
    frame = add_surrogate_key(frame, "location_key", "location")

    # location_id is not a place reference: 330 values resolve to more than
    # one city. Rows carrying such a value are marked so the dashboard does
    # not treat it as a geography key.
    ambiguous_ids = (frame.groupby("location_id")["city"].transform("nunique") > 1)

    frame["dq_status"] = grade(frame,
                               quarantine_mask=pd.Series(False, index=frame.index),
                               warning_mask=frame.is_id_collision | ambiguous_ids)
    log_issues(frame, frame.is_id_collision, "silver_location", "location_key",
               "order_id_collision", "warning",
               "Order_ID resolves to two different cities and customers.")
    log_issues(frame, ambiguous_ids, "silver_location", "location_key",
               "location_id_not_a_place_key", "warning",
               "This location_id resolves to more than one city.")

    record("silver_location", "row_count", len(frame))
    record("silver_location", "id_collisions", int(frame.is_id_collision.sum()),
           "same order_id, different city and customer")
    return frame


# ---------------------------------------------------------------
# Conformed dimensions
#
# The fact tables do not join to each other - order and agent IDs match at
# between 0.13% and 1.55%. The descriptive attributes do conform exactly:
# the same 4 product categories, the same 23 countries, the same 76 cities.
# These two dimensions are what allows the facts to be analysed together.
# ---------------------------------------------------------------

def build_product_category_dimension(orders, commission):
    categories = sorted(set(orders["product_category"])
                        | set(commission["product_category"]))
    frame = pd.DataFrame({"product_category": categories})
    frame.insert(0, "product_category_key", range(1, len(frame) + 1))

    shared = set(orders["product_category"]) == set(commission["product_category"])
    record("silver_dim_product_category", "row_count", len(frame))
    record("silver_dim_product_category", "categories_conform_across_sources",
           int(shared), "orders and agent_commission use an identical set")
    return frame


def build_geography_dimension(location, agent):
    columns = ["city", "state", "country"]
    from_location = location[columns + ["region"]].copy()
    from_agent = agent[columns].copy()
    from_agent["region"] = None

    frame = pd.concat([from_location, from_agent], ignore_index=True)
    # Region is only present in location; take the non-null value per city.
    frame = (frame.sort_values("region")
                  .drop_duplicates(subset=columns, keep="first")
                  .sort_values(columns)
                  .reset_index(drop=True))
    frame.insert(0, "geography_key", range(1, len(frame) + 1))

    shared_cities = set(location["city"]) & set(agent["city"])
    record("silver_dim_geography", "row_count", len(frame))
    record("silver_dim_geography", "cities_shared_by_location_and_agent",
           len(shared_cities))
    return frame


# ---------------------------------------------------------------
# Referential integrity: measured, not enforced
# ---------------------------------------------------------------

def measure_referential_integrity(sales, location, orders, agent, commission):
    def measure(child, child_column, parent, parent_column, label):
        matched = int(child[child_column].isin(set(parent[parent_column])).sum())
        record("silver_data_quality", label, matched,
               f"of {len(child)} rows "
               f"({100 * matched / len(child):.2f}% resolve to a parent)")

    measure(sales, "order_id", orders, "order_id", "fk_sales_to_orders")
    measure(location, "order_id", orders, "order_id", "fk_location_to_orders")
    measure(sales, "agent_id", agent, "agent_id", "fk_sales_to_agent")
    measure(commission, "agent_id", agent, "agent_id", "fk_commission_to_agent")


# ---------------------------------------------------------------

def main():
    # Module-level accumulators, so a second call inside one process would
    # otherwise append to the first run's results.
    quality_checks.clear()
    quality_issues.clear()

    connection = sqlite3.connect(DB_PATH)
    try:
        def read_bronze(name):
            return pd.read_sql(f"SELECT * FROM bronze_{name}", connection)

        agent = build_agent(read_bronze("agent"))
        commission = build_agent_commission(read_bronze("agent_commission"))
        orders = build_orders(read_bronze("orders"))
        sales = build_sales(read_bronze("sales"))
        location = build_location(read_bronze("location"))

        product_category = build_product_category_dimension(orders, commission)
        geography = build_geography_dimension(location, agent)

        measure_referential_integrity(sales, location, orders, agent, commission)

        # Aggregate counts of dq_status, so the two quality tables can be
        # cross-checked against each other.
        for table_name, frame in [("silver_agent", agent),
                                  ("silver_agent_commission", commission),
                                  ("silver_orders", orders),
                                  ("silver_sales", sales),
                                  ("silver_location", location)]:
            for status, count in frame.dq_status.value_counts().items():
                record(table_name, f"dq_status_{status}", int(count))

        silver_tables = {
            "silver_agent": agent,
            "silver_agent_commission": commission,
            "silver_orders": orders,
            "silver_sales": sales,
            "silver_location": location,
            "silver_dim_product_category": product_category,
            "silver_dim_geography": geography,
            "silver_data_quality": pd.DataFrame(quality_checks),
            "silver_data_quality_issues": pd.DataFrame(quality_issues),
        }

        for table_name in silver_tables:
            frame = silver_tables[table_name]
            frame.to_sql(table_name, connection, if_exists="replace", index=False)
            print(f"{table_name}: {len(frame)} rows")

        # Silver must not lose rows: collisions are flagged, never dropped.
        for name in ["agent", "agent_commission", "orders", "sales", "location"]:
            bronze_rows = pd.read_sql(
                f"SELECT COUNT(*) AS n FROM bronze_{name}", connection)["n"][0]
            silver_rows = pd.read_sql(
                f"SELECT COUNT(*) AS n FROM silver_{name}", connection)["n"][0]
            if bronze_rows != silver_rows:
                raise AssertionError(
                    f"silver_{name}: {silver_rows} rows, bronze has {bronze_rows}")

        # Indexes on the columns the Gold layer and dashboard join and
        # filter on. Cheap to create, and the dashboard is interactive.
        for statement in [
            "CREATE INDEX IF NOT EXISTS idx_silver_sales_order ON silver_sales (order_id)",
            "CREATE INDEX IF NOT EXISTS idx_silver_sales_agent ON silver_sales (agent_id)",
            "CREATE INDEX IF NOT EXISTS idx_silver_orders_order ON silver_orders (order_id)",
            "CREATE INDEX IF NOT EXISTS idx_silver_location_order ON silver_location (order_id)",
            "CREATE INDEX IF NOT EXISTS idx_silver_location_geo ON silver_location (city, state, country, region)",
            "CREATE INDEX IF NOT EXISTS idx_silver_commission_agent ON silver_agent_commission (agent_id, product_category)",
            "CREATE INDEX IF NOT EXISTS idx_silver_dq_issue ON silver_data_quality_issues (silver_table, silver_record_id)",
            # Unique indexes stand in for a declared PRIMARY KEY: pandas
            # to_sql creates plain columns, so without these the surrogate
            # keys are unique by construction but unenforced by the schema.
            "CREATE UNIQUE INDEX IF NOT EXISTS pk_silver_agent ON silver_agent (agent_key)",
            "CREATE UNIQUE INDEX IF NOT EXISTS pk_silver_agent_commission ON silver_agent_commission (commission_key)",
            "CREATE UNIQUE INDEX IF NOT EXISTS pk_silver_orders ON silver_orders (order_line_key)",
            "CREATE UNIQUE INDEX IF NOT EXISTS pk_silver_sales ON silver_sales (sales_key)",
            "CREATE UNIQUE INDEX IF NOT EXISTS pk_silver_location ON silver_location (location_key)",
        ]:
            connection.execute(statement)

        connection.commit()
        print("\nRow counts match Bronze - no rows dropped.")
    except Exception:
        # Without this, a failure partway through leaves some Silver tables
        # rebuilt and others stale. pandas to_sql issues its own DDL, so the
        # rollback is best-effort; a production build would write to staging
        # tables and swap them in only after every check passed.
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()