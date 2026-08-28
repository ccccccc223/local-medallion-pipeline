"""
Raw data profiling for the medallion pipeline.

Five checks, run in order:
  1. Shape, nulls, full-row duplicates
  2. Grain and keys
  3. Referential integrity between tables
  4. Whether dimension values conform across tables
  5. Value ranges and date validity

Run:  python src/profile_raw.py
"""

import glob
import os
import pandas as pd

# Resolve paths from the script's own location rather than the working
# directory, so the script behaves identically whether it is run from the
# terminal, from VS Code, or imported as a module.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE = os.path.join(PROJECT_ROOT, "data", "raw")


def load(dataset_name):
    """Read every batch file for one dataset and stack them into one DataFrame.

    Files are named {timestamp}-{dataset}.csv, so matching on the suffix
    picks up every batch for a dataset. The leading hyphen in the pattern
    matters: "*-agent.csv" selects only the agent files and correctly
    excludes "*-agent-commission.csv". A looser "*agent*.csv" would silently
    merge the two datasets together.

    Everything is read as text so that values with leading zeros
    (e.g. Sales = '04793526') survive ingestion unchanged. Type casting
    happens later, in the Silver layer.
    """
    paths = sorted(glob.glob(os.path.join(BASE, f"*-{dataset_name}.csv")))
    if not paths:
        raise FileNotFoundError(
            f"No files matching *-{dataset_name}.csv in {BASE}"
        )
    batches = []
    for path in paths:
        batch = pd.read_csv(path, dtype=str)
        batch["source_file"] = os.path.basename(path)
        batches.append(batch)
    return pd.concat(batches, ignore_index=True)


agent = load("agent")
commission = load("agent-commission")
location = load("location")
orders = load("orders")
sales = load("sales")


# ---------------------------------------------------------------
# 1. Shape, nulls, full-row duplicates
# ---------------------------------------------------------------
print("\n--- 1. SHAPE, NULLS, DUPLICATES ---")

tables = {
    "agent": agent,
    "agent_commission": commission,
    "location": location,
    "orders": orders,
    "sales": sales,
}

for name in tables:
    table = tables[name]
    null_count = table.isna().sum().sum()
    # source_file is added by this script, so it is excluded from the
    # comparison - otherwise identical rows from two batches would look
    # distinct purely because they came from different files.
    duplicate_count = table.drop(columns="source_file").duplicated().sum()
    print(f"{name}: {len(table)} rows, {null_count} nulls, "
          f"{duplicate_count} full-row duplicates")


# ---------------------------------------------------------------
# 2. Grain and keys
#
# No schema is declared, so keys are inferred. A column is treated as a
# candidate key when it semantically identifies the record - not when it
# merely happens to be unique. In `sales`, pairs such as (Shipping_Cost,
# Customer_ID) are unique in this sample, but shipping cost is a measure,
# not an identity.
#
# Where a candidate key repeats, the repetition is diagnosed rather than
# assumed to be a defect, by asking: do the repeated rows share the
# attributes they would have to share if they described the same entity?
#
#   they share    -> one entity spread over several rows, so the real grain
#                    is finer and the key needs another column
#   they differ   -> two unrelated records were issued the same ID
#
# The two cases need opposite treatment, so they are separated here.
# ---------------------------------------------------------------
print("\n--- 2. GRAIN AND KEYS ---")


def examine_grain(table, label, key, identity_columns, refinements=None,
                  divergence="ID collision - one key, two different entities"):
    """Test a candidate key and, if it repeats, classify the repetitions.

    Counts are reported as both keys and rows, because they are not
    interchangeable: 18 repeated order IDs affect 36 rows, and the data
    quality tables downstream count rows.

    Groups that agree on the identity columns are one entity spread over
    several rows, so the grain is finer than the key assumes. Groups that
    disagree are a defect - though not always the same defect, hence the
    `divergence` label, which the caller sets per table.

    `refinements` are additional columns added to the key one step at a
    time. They are diagnostics, not a proposed primary key: Silver uses a
    surrogate. Each step shows how many rows the added column accounts for.
    """
    repeated = table[table.duplicated(subset=key, keep=False)]
    repeated_keys = table.duplicated(subset=key).sum()
    print(f"\n{label}, key {key}: {repeated_keys} keys repeat, "
          f"{len(repeated)} rows affected")
    if repeated_keys == 0:
        return

    groups = repeated.groupby(key)

    shares_identity = None
    for column in identity_columns:
        matches = groups[column].nunique() == 1
        if shares_identity is None:
            shares_identity = matches
        else:
            shares_identity = shares_identity & matches

    same_entity = int(shares_identity.sum())
    divergent = groups.ngroups - same_entity
    print(f"    {same_entity} keys agree on {identity_columns} "
          f"-> one entity, finer grain")
    print(f"    {divergent} keys disagree -> {divergence}")

    if refinements:
        widened = list(key)
        for column in refinements:
            widened = widened + [column]
            remaining = table.duplicated(subset=widened, keep=False).sum()
            print(f"    diagnostic: + {column:<15} -> {remaining} rows still repeat")


# A single order has one order date, so a shared date means these rows are
# lines on one order rather than two unrelated orders sharing an ID.
#
# The refinement steps separate three different causes. Order_Date accounts
# for the rows whose ID collided across unrelated orders (a defect). Product
# accounts for the rows that are genuine line items on one order (a grain
# correction). Ship_Mode accounts for the last pair, an order line shipped
# twice by different methods. None of this is the primary key - Silver uses
# a surrogate - but the sequence shows which column answers which question.
examine_grain(orders, "orders", ["Order_ID"], ["Order_Date"],
              refinements=["Order_Date", "Product", "Ship_Mode"])

# A single order has one customer and one selling agent.
examine_grain(sales, "sales", ["Order_ID"], ["Customer_ID", "Agent_ID"])

# A single order ships to one place for one customer.
examine_grain(location, "location", ["Order_ID"], ["City", "Customer_Name"])

# A single agent is one named person.
examine_grain(agent, "agent", ["Agent_ID"], ["Agent_Name"])

# A single agent-category pair has one commission rate. Where two rates
# appear, the entity has not split - the agent and category are the same
# and only the value differs - so this is a conflicting value rather than
# an ID collision. The two need different fixes upstream: a collision is a
# key generation defect, a conflict is a master data problem. 88 of the 152
# occur inside one batch file, so taking the latest batch cannot resolve them.
examine_grain(commission, "commission", ["Agent_ID", "Product_Category"],
              ["Commission_Percentage"],
              divergence="conflicting value - same entity, two rates")


# ---------------------------------------------------------------
# 3. Referential integrity
# ---------------------------------------------------------------
print("\n--- 3. REFERENTIAL INTEGRITY ---")


def match_rate(child_keys, parent_keys, label):
    """What share of child rows have a matching key in the parent table.

    The parent keys are converted to a set first so that each lookup is a
    hash lookup rather than a scan of the parent column.
    """
    parent_set = set(parent_keys)
    matched = child_keys.isin(parent_set).sum()
    percent = 100 * matched / len(child_keys)
    print(f"  {label}: {matched} of {len(child_keys)} matched ({percent:.2f}%)")


match_rate(sales["Order_ID"], orders["Order_ID"], "sales.Order_ID -> orders")
match_rate(location["Order_ID"], orders["Order_ID"], "location.Order_ID -> orders")
match_rate(sales["Agent_ID"], agent["Agent_ID"], "sales.Agent_ID -> agent")
match_rate(commission["Agent_ID"], agent["Agent_ID"], "commission.Agent_ID -> agent")

# Location_ID looks like a foreign key to a place dimension, but there is no
# place table to test it against, so it is tested for internal consistency
# instead: a genuine place reference resolves to exactly one city, and each
# city is reached by roughly one identifier.
cities_per_location_id = location.groupby("Location_ID")["City"].nunique()
location_ids_per_city = location.groupby("City")["Location_ID"].nunique()

print(f"  Location_ID: {location['Location_ID'].nunique()} distinct "
      f"across {len(location)} rows")
print(f"    resolving to more than one city: {(cities_per_location_id > 1).sum()}")
print(f"    distinct Location_IDs per city: "
      f"min {location_ids_per_city.min()}, max {location_ids_per_city.max()}")

# The geography columns, by contrast, form a strict hierarchy. This is what
# makes a conformed geography dimension possible despite the broken keys.
print(f"  cities resolving to more than one state: "
      f"{(location.groupby('City')['State'].nunique() > 1).sum()}")
print(f"  states resolving to more than one country: "
      f"{(location.groupby('State')['Country'].nunique() > 1).sum()}")
print(f"  countries resolving to more than one region: "
      f"{(location.groupby('Country')['Region'].nunique() > 1).sum()}")


# ---------------------------------------------------------------
# 4. Conformed dimensions
# ---------------------------------------------------------------
print("\n--- 4. CONFORMED DIMENSIONS ---")

order_categories = set(orders["Product_Category"].dropna().str.strip())
commission_categories = set(
    commission["Product_Category"].dropna().str.strip()
)

print("  product categories identical:",
      order_categories == commission_categories)
print("  categories:", sorted(order_categories & commission_categories))

geography_columns = ["City", "State", "Country"]
location_geographies = set(
    location[geography_columns]
    .drop_duplicates()
    .itertuples(index=False, name=None)
)
agent_geographies = set(
    agent[geography_columns]
    .drop_duplicates()
    .itertuples(index=False, name=None)
)
shared_geographies = location_geographies & agent_geographies

print(f"  geographies: location {len(location_geographies)}, "
      f"agent {len(agent_geographies)}, shared {len(shared_geographies)}")
print(f"  agent geographies missing from location: "
      f"{len(agent_geographies - location_geographies)}")


# ---------------------------------------------------------------
# 5. Value ranges and date validity
# ---------------------------------------------------------------
print("\n--- 5. VALUES AND DATES ---")

print("Sales values starting with zero:", sales["Sales"].str.startswith("0").sum())

numeric_columns = ["Sales", "Quantity", "Discount", "Profit", "Shipping_Cost"]
for column in numeric_columns:
    # errors="coerce" turns unparseable values into NaN instead of raising,
    # so the count of failures is itself a data quality measurement.
    values = pd.to_numeric(sales[column], errors="coerce")
    print(f"{column}: min {values.min()}, max {values.max()}, "
          f"unparseable {values.isna().sum()}")

# Commission is the only measure outside `sales`, and the only rate in the
# dataset. Values run 1 to 99.99 with two decimal places, so this is a
# percentage, not a 0-1 fraction - reading it as a fraction would understate
# every commission by a factor of 100.
commission_rate = pd.to_numeric(commission["Commission_Percentage"],
                                errors="coerce")
print(f"Commission_Percentage: min {commission_rate.min()}, "
      f"max {commission_rate.max()}, "
      f"unparseable {commission_rate.isna().sum()}")

# Cross-column consistency: every check above tests one column in isolation.
# A sale of zero units for a non-zero amount is internally inconsistent. It
# could be a return, a cancellation, or bad data - nothing in the schema
# distinguishes them, so these rows are flagged rather than interpreted.
sales_amount = pd.to_numeric(sales["Sales"], errors="coerce")
quantity = pd.to_numeric(sales["Quantity"], errors="coerce")
print("Zero quantity with non-zero Sales:",
      ((quantity == 0) & (sales_amount != 0)).sum())

order_date = pd.to_datetime(orders["Order_Date"], errors="coerce")
shipping_date = pd.to_datetime(orders["Shipping_Date"], errors="coerce")
aging = pd.to_numeric(orders["Aging"], errors="coerce")

print("date range:", order_date.min().date(), "to", order_date.max().date())
print("shipped before ordered:", (shipping_date < order_date).sum())
print("Aging disagrees with date difference:",
      ((shipping_date - order_date).dt.days != aging).sum())
