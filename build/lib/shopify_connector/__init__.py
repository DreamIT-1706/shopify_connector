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
        source_info_id = str(config).strip()
        if not source_info_id:
            raise ValueError("run_pipeline expected resolved source_info_id as config input")

        config_shopify(source_info_id, spark)
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

        return error_result
