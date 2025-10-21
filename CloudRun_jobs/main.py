import argparse
from google.cloud import bigquery
from mlxtend.frequent_patterns import fpgrowth, association_rules
from loguru import logger
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
import hashlib
from datetime import date


def build_query(fact_table: str, territory_id: int, date_filter: int) -> str:
    conditions = list()
    if territory_id is not None:
        conditions.append(f"TerritoryID = {territory_id}")
    if date_filter is not None:
        conditions.append(f"OrderDateKey < {date_filter}")

    condition_clause = "WHERE " + \
        " AND ".join(conditions) if len(conditions) > 0 else ""

    query = f"""
    SELECT SalesOrderID, ARRAY_AGG(ProductID) AS products
    FROM {fact_table}
    {condition_clause}
    GROUP BY SalesOrderID
    """

    return query


def hash_rule(row):
    ants = ', '.join(map(str, row["antecedents"]))
    cons = ', '.join(map(str, row["consequents"]))
    return hashlib.sha256((ants+cons).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(
        description="Run Association Rules mining on BigQuery sales data."
    )
    parser.add_argument(
        "project_id", help="Google Cloud project ID."
    )
    parser.add_argument(
        "dataset", help="BigQuery dataset name containing fact_sales and dim_territory tables"
    )
    parser.add_argument(
        "--territory_id",
        help="Territory id of the territory to process", type=int
    )
    parser.add_argument(
        "--date_filter",
        help="Max datekey for transactions, serve for demo only", type=int
    )
    args = parser.parse_args()

    date_filter = None
    if args.date_filter is not None:
        try:
            assert args.date_filter*1e-7 > 1
        except AssertionError:
            logger.error("Invalid date format. Use YYYYMMDD.")
            return
        date_filter = args.date_filter

    project_id = args.project_id
    dataset = args.dataset
    territory_id = args.territory_id
    fact_table = f"`{project_id}.{dataset}.fact_sales`"

    logger.info(f"Running Association Rules mining job for fact_table: \
    {fact_table}, territory_id={territory_id}, date_filter={date_filter}")

    bq = bigquery.Client(project=project_id)
    query = build_query(fact_table, territory_id, date_filter)

    df = bq.query(query).to_dataframe()

    if df.empty:
        logger.warning("No data returned for this territory_id. Exiting.")
        return
    else:
        logger.info(f"Retrieved {len(df)} transactions")

    # should discuss more about thresholds
    min_sup = 0.015 if len(df) >= 1000 else 0.15

    te = TransactionEncoder()
    te_ary = te.fit(df["products"]).transform(df["products"])
    basket = pd.DataFrame(te_ary, columns=te.columns_)

    frequent_itemsets = fpgrowth(
        basket, min_support=min_sup, use_colnames=True)
    rules = association_rules(
        frequent_itemsets, metric="confidence", min_threshold=0.7
    )

    if rules.empty:
        logger.error("No association rules found for this territory_id. \
            Try lower the min_sup or min_conf.")
        return

    result = rules.iloc[:, :7].copy()

    if len(result) > 400:
        result = result.sort_values(by=["support", "lift"], ascending=[
                                    False, False]).head(400)

    result["territory_id"] = territory_id
    result["territory_id"] = result["territory_id"].astype("Int64")
    result["update_at"] = int(date.today().strftime("%Y%m%d"))
    result["antecedents"] = result["antecedents"].apply(
        lambda x: sorted(list(x)))
    result["consequents"] = result["consequents"].apply(
        lambda x: sorted(list(x)))
    result["rule_hash"] = result.apply(hash_rule, axis=1)

    table_id = f"{project_id}.{dataset}_analytics.AssociationRules"
    logger.info(f"Writing results to {table_id}")

    job = bq.load_table_from_dataframe(
        result,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    logger.info("Upload completed")


if __name__ == "__main__":
    main()
