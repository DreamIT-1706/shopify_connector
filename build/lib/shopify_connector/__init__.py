from pyspark.sql import SparkSession
from .config_shopify import *
from .br_ingestion_shopify import *
from .br_to_sil_shopify import *
import time
from datetime import datetime

__version__ = "1.0.0"
__all__ = ["config_shopify", "br_ingestion_shopify", "br_to_sil_shopify", "run_pipeline"]


def run_pipeline(config, spark):
    start_time = time.time()

    print("\n" + "=" * 60)
    print(" SHOPIFY ETL PIPELINE STARTED")
    print("=" * 60)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        if not isinstance(config, dict):
            raise ValueError("run_pipeline expected config context as dict input")

        workspace_id = str(config.get("workspace_id", "")).strip()
        fabric_tenant_id = str(config.get("fabric_tenant_id", "")).strip()
        source_name = str(config.get("source_name", "")).strip().lower()

        if not workspace_id:
            raise ValueError("workspace_id is required in pipeline config")
        if not fabric_tenant_id:
            raise ValueError("fabric_tenant_id is required in pipeline config")
        if source_name != "shopify":
            raise ValueError(f"Invalid source_name for Shopify pipeline: '{source_name}'")

        config_shopify(config, spark)
        br_ingestion_shopify(spark)
        br_to_sil_shopify(spark)

        result = {
            "status": "success",
            "source": "shopify",
        }

        print("\n" + "=" * 60)
        print(" PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duration: {round(time.time() - start_time, 2)} seconds")

        return result

    except Exception as e:
        error_result = {
            "status": "failed",
            "error": str(e),
            "source": "shopify",
        }

        print("\n" + "=" * 60)
        print(" PIPELINE FAILED")
        print("=" * 60)
        print(f"Error: {str(e)}")
        print(f"Duration: {round(time.time() - start_time, 2)} seconds")

        return error_result
