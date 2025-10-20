import argparse
from google.cloud import bigquery
from mlxtend.frequent_patterns import fpgrowth, association_rules
from loguru import logger
import pandas as pd
from mlxtend.preprocessing import TransactionEncoder


def main():
    parser = argparse.ArgumentParser(
        description="Run Association Rules mining on BigQuery sales data."
    )
    parser.add_argument(
        "project_id", help="Google Cloud project ID containing the dataset"
    )
    parser.add_argument(
        "dataset", help="BigQuery dataset name containing fact_sales and dim_territory tables"
    )
    parser.add_argument(
        "--region",
        default="total",
        help="Region (territory) to process, or 'total' for all regions",
    )
    args = parser.parse_args()

    project_id = args.project_id
    dataset = args.dataset
    region = args.region

    logger.info(f"Running Association Rules mining job for project={
                project_id}, dataset={dataset}, region={region}")

    bq = bigquery.Client(project=project_id)

    query = str()
    if region.lower() == "total":
        query = f"""
        SELECT SalesOrderID, ARRAY_AGG(ProductID) AS products
        FROM `{project_id}.{dataset}.fact_sales`
        GROUP BY SalesOrderID
        """
    else:
        query = f"""
        SELECT SalesOrderID, ARRAY_AGG(ProductID) AS products
        FROM `{project_id}.{dataset}.fact_sales` as fs
        JOIN `{project_id}.{dataset}.dim_territory` as dt
            ON fs.TerritoryID = dt.TerritoryID
        WHERE TerritoryName = '{region}'
        GROUP BY SalesOrderID
        """

    df = bq.query(query).to_dataframe()

    if df.empty:
        logger.warn("No data returned for this region. Exiting.")
        return
    else:
        logger.info(f"Retrieved {len(df)} transactions")

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
        logger.err("No association rules found for this region. \
            Try lower the min_sup or min_conf.")
        return

    result = rules.iloc[:, :7].copy()

    if len(result) > 400:
        result = result.sort_values(by=["support", "lift"], ascending=[
                                    False, False]).head(400)

    result["antecedents"] = result["antecedents"].apply(
        lambda x: sorted(list(x)))
    result["consequents"] = result["consequents"].apply(
        lambda x: sorted(list(x)))

    suffix = "Total" if region.lower() == "total" else region
    table_id = f"{project_id}.analytics.AssociationRules_{suffix}"
    logger.info(f"Writing results to {table_id}")

    job = bq.load_table_from_dataframe(
        result,
        table_id,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    job.result()
    logger.info("Upload completed")


if __name__ == "__main__":
    main()
