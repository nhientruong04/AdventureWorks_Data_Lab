import argparse
import math
import uuid
from google.cloud import bigquery
from mlxtend.frequent_patterns import fpgrowth, association_rules
import numpy as np
from loguru import logger
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder
import hashlib
import time
from datetime import date, datetime, timedelta
from google.api_core.exceptions import TooManyRequests, ServiceUnavailable, InternalServerError


MAX_RETRIES = 3
INITIAL_DELAY = 15


def build_query(fact_table: str, territory_id: int, date_filter: int,
                window: int = None) -> str:
    # window is the length of the window in days
    conditions = list()
    if territory_id is not None:
        conditions.append(f"TerritoryID = {territory_id}")

    if date_filter is not None:
        conditions.append(f"OrderDateKey < {date_filter}")

        if window is not None:
            max_date = datetime.strptime(str(date_filter), "%Y%m%d")
            window_span = timedelta(window)
            min_date = (max_date - window_span).strftime("%Y%m%d")
            logger.info(
                f"Recieved window {window} days, min_date is {min_date}"
            )

            conditions.append(f"OrderDateKey > {int(min_date)}")

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
    max_retries = 5

    while min_sup >= 0.01 and max_retries > 0:
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
                rules, got {len(result)} instead. Lowering min_sup to {min_sup*0.75}"
            )
            min_sup *= 0.75
            max_retries -= 1

    return result


def write_with_retry(bq_client, result_df, table_id):
    logger.info(f"Writing results to {table_id}, {len(result_df)} rules.")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            job = bq_client.load_table_from_dataframe(
                result_df, table_id,
                job_config=bigquery.LoadJobConfig(
                    write_disposition="WRITE_APPEND")
            )
            job.result()
            logger.info("Upload completed.")
            return job

        except (TooManyRequests, ServiceUnavailable, InternalServerError) as e:
            if attempt < MAX_RETRIES:
                wait_time = INITIAL_DELAY * \
                    (2 ** (attempt - 1))  # exponential backoff
                logger.warning(
                    f"Attempt {attempt}/{MAX_RETRIES} failed with error: {e}. "
                    f"Retrying in {wait_time} seconds..."
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    "Max retries reached. Upload failed.")
                raise

        except Exception as e:
            logger.error(f"Upload failed with non-retryable error: {e}")
            raise


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
    parser.add_argument(
        "--window",
        help="Window determines the time window for transactions in days,\
        with date_filter - window.", type=int
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
    window = args.window
    fact_table = f"`{project_id}.{dataset}.fact_sales`"

    logger.info(f"Running Association Rules mining job for fact_table: \
    {fact_table}, territory_id={territory_id}, date_filter={date_filter}, \
    window={window}")

    bq = bigquery.Client(project=project_id)
    query = build_query(fact_table, territory_id, date_filter, window)

    df = bq.query(query).to_dataframe()

    if df.empty:
        logger.warning(
            "No data found for the query:\n"
            f"{query}\n"
            "Exiting.")
        return
    else:
        logger.info(f"Retrieved {len(df)} transactions.")

    # Run algorithm iteratively with multiple min_sup thresholds
    te = TransactionEncoder()
    te_ary = te.fit(df["products"]).transform(df["products"])
    basket = pd.DataFrame(te_ary, columns=te.columns_)
    min_sup = initialize_min_sup(df["products"].to_list())
    max_rules = 20 if window is not None else 35
    result = mine_rules(basket, min_sup=min_sup, max_rules=max_rules)

    if len(result) == 0:
        logger.info(f"No rules found for this territory {territory_id},"
                    f"date_filter {date_filter} and window {window}.")
        return

    result["row_id"] = [str(uuid.uuid4()) for _ in range(len(result))]
    result["territory_id"] = territory_id
    result["territory_id"] = result["territory_id"].astype("Int64")
    result["window"] = window
    result["window"] = result["window"].astype("Int64")

    if date_filter is None:
        result["update_at"] = int(date.today().strftime("%Y%m%d"))
    else:
        result["update_at"] = date_filter

    result["antecedents"] = result["antecedents"].apply(
        lambda x: sorted(list(x)))
    result["consequents"] = result["consequents"].apply(
        lambda x: sorted(list(x)))
    result["rule_hash"] = result.apply(hash_rule, axis=1)

    table_id = f"{project_id}.{dataset}_analytics.AssociationRulesScheduled"
    write_with_retry(bq, result, table_id)


if __name__ == "__main__":
    main()
