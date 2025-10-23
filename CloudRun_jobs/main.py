import argparse
import math
from google.cloud import bigquery
from mlxtend.frequent_patterns import fpgrowth, association_rules
import numpy as np
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


def initialize_min_sup(transactions: list):
    distinct_products = np.unique(np.concatenate(transactions))
    one_hot_transactions = np.zeros((len(transactions), max(distinct_products)+1),
                                    dtype=np.int8)
    for ti, t in enumerate(transactions):
        one_hot_transactions[ti, t] = 1

    # source: https://www.jurnal.yoctobrain.org/index.php/ijodas/article/view/134
    unique_product_num = len(distinct_products)
    N_avg = np.sum(one_hot_transactions) / unique_product_num
    max_sup = np.max(np.mean(one_hot_transactions, axis=0))
    R = math.pow(2, unique_product_num) - 1
    min_sup = max_sup * (1 - math.pow(1/math.sqrt(R), 1/N_avg))

    return min_sup


def hash_rule(row):
    ants = ', '.join(map(str, row["antecedents"]))
    cons = ', '.join(map(str, row["consequents"]))
    return hashlib.sha256((ants+cons).encode()).hexdigest()


def mine_rules(basket, single_itemset_flag=True,
               min_sup=0.02, max_rules=35):
    logger.info(f"Running with parameters: min_sup={min_sup},\
    max_rules={max_rules}, single-item set={single_itemset_flag}.")

    while min_sup >= 0.01:
        frequent_itemsets = fpgrowth(
            basket, min_support=min_sup, use_colnames=True)
        rules = association_rules(
            frequent_itemsets, metric="confidence", min_threshold=0.65
        )

        result = rules.iloc[:, :7].copy()

        # get only single item set
        if single_itemset_flag:
            single_condition = (
                result["antecedents"].apply(len) == 1
            ) & (
                result["consequents"].apply(len) == 1
            )
            result = result[single_condition]

        result = result.sort_values(by=["support", "lift"], ascending=[
            False, False]).head(max_rules)

        # ensure at least a total of 0.8*max_rules rules found
        if len(result) > int(0.8*max_rules):
            break
        else:
            logger.info(
                f"Too few rules, need at least {int(0.8*max_rules)} \
                rules, got {len(result)} instead. Lowering min_sup to {min_sup/2}"
            )
            min_sup /= 2

    return result


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
        logger.info(f"Retrieved {len(df)} transactions.")

    # Run algorithm iteratively with multiple min_sup thresholds
    te = TransactionEncoder()
    te_ary = te.fit(df["products"]).transform(df["products"])
    basket = pd.DataFrame(te_ary, columns=te.columns_)
    min_sup = initialize_min_sup(df["products"].to_list())
    result = mine_rules(basket, min_sup=min_sup)

    result["territory_id"] = territory_id
    result["territory_id"] = result["territory_id"].astype("Int64")
    result["update_at"] = int(date.today().strftime("%Y%m%d"))
    result["antecedents"] = result["antecedents"].apply(
        lambda x: sorted(list(x)))
    result["consequents"] = result["consequents"].apply(
        lambda x: sorted(list(x)))
    result["rule_hash"] = result.apply(hash_rule, axis=1)

    table_id = f"{project_id}.{dataset}_analytics.AssociationRules"
    logger.info(f"Writing results to {table_id}, {len(result)} rules.")

    job = bq.load_table_from_dataframe(
        result,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )

    try:
        job.result()
        logger.info("Upload completed")
    except Exception as e:
        logger.error(f"Upload failed with error: {e}")
        raise e


if __name__ == "__main__":
    main()
