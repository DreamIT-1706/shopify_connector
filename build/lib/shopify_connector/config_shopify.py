#!/usr/bin/env python
# coding: utf-8

import os
import json
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from cryptography.fernet import Fernet
from azure.identity import ClientSecretCredential
import notebookutils
from notebookutils import mssparkutils, lakehouse

from master_connector.configCred import get_credentials


def create_lakehouses(names):
    for name in names:
        try:
            notebookutils.lakehouse.create(name=name)
            print(f"Created: {name}")
        except Exception as e:
            error_str = str(e).lower()
            if (
                "itemdisplaynamealreadyinuse" in error_str or
                "already in use" in error_str or
                "already exists" in error_str
            ):
                print(f"Already exists: {name}")
            else:
                print(f"Unexpected error for {name}: {e}")
                raise


def get_lakehouse_path(lakehouse_name: str) -> str:
    try:
        lakehouse_details = notebookutils.lakehouse.get(lakehouse_name)
    except Exception:
        raise Exception(f"A LAKEHOUSE WITH NAME '{lakehouse_name}' DOES NOT EXIST IN THE WORKSPACE")

    return lakehouse_details.get("properties", {}).get("abfsPath")


def _get_db_connection_details():
    client_secret = os.getenv("CLIENT_SECRET", "").strip()
    if not client_secret:
        raise ValueError(
            "CLIENT_SECRET env variable is not set. "
            "It must be injected as a notebook parameter by deploy_to_fabric.py"
        )

    creds = get_credentials(client_secret=client_secret)

    credential = ClientSecretCredential(
        tenant_id=creds["TENANT_ID"],
        client_id=creds["CLIENT_ID"],
        client_secret=creds["CLIENT_SECRET"]
    )

    db_access_token = credential.get_token("https://database.windows.net/.default").token

    jdbc_url = (
        f"jdbc:sqlserver://{creds['DB_SERVER']}:1433;"
        f"databaseName={creds['DB_NAME']};"
        f"encrypt=true;"
        f"trustServerCertificate=false;"
        f"hostNameInCertificate=*.database.windows.net;"
        f"loginTimeout=30"
    )

    return jdbc_url, db_access_token


def _load_shopify_rows(source_info_id, spark):
    source_info_id = str(source_info_id).strip()
    if not source_info_id:
        raise ValueError("source_info_id is required")

    jdbc_url, db_access_token = _get_db_connection_details()

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option(
            "query",
            f"""
            SELECT
                source_info_id,
                store_domain,
                access_token,
                prefix,
                active_flag
            FROM dbo.shopify
            WHERE source_info_id = '{source_info_id}'
            """
        )
        .option("accessToken", db_access_token)
        .load()
    )

    rows = df.collect()
    if not rows:
        raise ValueError(f"No Shopify rows found for source_info_id='{source_info_id}'")

    return rows


def _build_shopify_config(rows):
    config = {"stores": {}}

    for row in rows:
        store_domain = row["store_domain"]
        access_token = row["access_token"]
        prefix = row["prefix"]
        active_flag = row["active_flag"]

        if not store_domain:
            raise ValueError("store_domain is missing in shopify table row")
        if not access_token:
            raise ValueError(f"access_token is missing for store_domain '{store_domain}'")
        if prefix is None:
            raise ValueError(f"prefix is missing for store_domain '{store_domain}'")

        store_domain = str(store_domain).strip()

        if store_domain not in config["stores"]:
            config["stores"][store_domain] = {
                "access_token": access_token,
                "prefix": prefix,
                "sources": {
                    "orders": {"active_flag": bool(active_flag) if active_flag is not None else False}
                }
            }
        else:
            config["stores"][store_domain]["sources"]["orders"]["active_flag"] = (
                bool(active_flag) if active_flag is not None else False
            )

    return config


def bronze_config(config, fernet, spark, BRONZE_LAKEHOUSE_PATH):
    schema = StructType([
        StructField("store", StringType(), True),
        StructField("access_token", StringType(), True),
        StructField("table", StringType(), True),
        StructField("source", StringType(), True),
        StructField("prefix", StringType(), True),
        StructField("last_sync", TimestampType(), True),
        StructField("isActive", BooleanType(), True)
    ])

    last_sync = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")

    data = []
    for store_name, store_config in config["stores"].items():
        access_token = store_config["access_token"]
        prefix = store_config["prefix"]
        sources = store_config["sources"]

        for source_name, source_config in sources.items():
            active_flag = source_config["active_flag"]
            table_name = f"br_shopify_{source_name}"
            full_table_name = table_name + prefix
            encrypted_token = fernet.encrypt(access_token.encode()).decode()

            data.append((
                store_name,
                encrypted_token,
                full_table_name,
                source_name,
                prefix,
                last_sync,
                active_flag
            ))

    new_df = spark.createDataFrame(data, schema)
    table_path = f"{BRONZE_LAKEHOUSE_PATH}/Tables/br_shopify_config"

    try:
        spark.read.format("delta").load(table_path)
    except Exception:
        print("Table doesn't exist yet -> creating...")
        (
            new_df.write
            .format("delta")
            .mode("overwrite")
            .save(table_path)
        )

    delta_table = DeltaTable.forPath(spark, table_path)

    (
        delta_table.alias("target")
        .merge(
            new_df.alias("source"),
            "target.store = source.store AND target.table = source.table AND target.source = source.source"
        )
        .whenMatchedUpdate(set={
            "isActive": "source.isActive"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )


def staging_config(STAGING_LAKEHOUSE_PATH, BRONZE_LAKEHOUSE_PATH, spark):
    BRONZE_CONFIG_PATH = f"{BRONZE_LAKEHOUSE_PATH}/Tables/br_shopify_config"
    bronze_config_df = spark.read.format("delta").load(BRONZE_CONFIG_PATH)

    default_sync_date = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")

    new_processing_rows = bronze_config_df.select(
        col("table").alias("source"),
        col("isActive").alias("isActive"),
        col("source").alias("bronze_source")
    ).distinct()

    new_processing_rows = (
        new_processing_rows
        .withColumn("table", concat(lit("sil.shopify."), col("bronze_source")))
        .withColumn("last_sync", lit(default_sync_date))
        .withColumn("key", lit("").cast(StringType()))
    )

    new_processing_rows = new_processing_rows.select("table", "last_sync", "source", "isActive", "key")
    new_processing_rows = new_processing_rows.filter(col("table").isNotNull())

    PROCESSING_CONFIG_PATH = f"{STAGING_LAKEHOUSE_PATH}/Tables/Staging_config"

    try:
        existing_df = spark.read.format("delta").load(PROCESSING_CONFIG_PATH)
        print("Existing Staging_config found; will preserve last_sync and upsert isActive")
    except Exception:
        existing_df = None
        print("No existing Staging_config; creating from scratch")

    if existing_df is not None:
        delta_table = DeltaTable.forPath(spark, PROCESSING_CONFIG_PATH)

        (
            delta_table.alias("target")
            .merge(
                new_processing_rows.alias("source"),
                "target.table = source.table AND target.source = source.source"
            )
            .whenMatchedUpdate(set={
                "isActive": "source.isActive"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("MERGE completed: isActive updated, new sources added, last_sync preserved")
    else:
        (
            new_processing_rows.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(PROCESSING_CONFIG_PATH)
        )
        print("Staging_config created for the first time")


def silver_config(SILVER_LAKEHOUSE_PATH, spark):
    schema1 = StructType([
        StructField("table", StringType(), True),
        StructField("primary_key", StringType(), True),
        StructField("last_sync", TimestampType(), True)
    ])

    data1 = []
    df = spark.createDataFrame(data1, schema1)

    sil_lakehouse_path = f"{SILVER_LAKEHOUSE_PATH}/Tables/Sil_config"

    try:
        spark.read.format("delta").load(sil_lakehouse_path)
    except Exception:
        df.write.format("delta").mode("overwrite").save(sil_lakehouse_path)


def config_shopify(source_info_id, spark):
    rows = _load_shopify_rows(source_info_id, spark)
    config = _build_shopify_config(rows)

    print("Configuration loaded successfully")
    print(json.dumps(config, indent=2))

    lakehouse_names = ["Bronze_Lakehouse", "Staging_Lakehouse", "Silver_Lakehouse", "Gold_Lakehouse"]
    create_lakehouses(lakehouse_names)

    workspace_info = notebookutils.lakehouse.list()
    if not workspace_info:
        raise Exception("No lakehouses found in the workspace.")

    STAGING_LAKEHOUSE_PATH = get_lakehouse_path("Staging_Lakehouse")
    BRONZE_LAKEHOUSE_PATH = get_lakehouse_path("Bronze_Lakehouse")
    SILVER_LAKEHOUSE_PATH = get_lakehouse_path("Silver_Lakehouse")

    key_path = f"{BRONZE_LAKEHOUSE_PATH}/Files/fernet_key.txt"

    try:
        key_df = spark.read.text(key_path)
        print("Loading existing key...")
        private_key = key_df.first()[0].encode("utf-8")
        print("Existing key loaded")
    except Exception:
        print("Generating new key...")
        private_key = Fernet.generate_key()
        key_df = spark.createDataFrame([private_key.decode("utf-8")], "string")
        key_df.write.mode("overwrite").text(key_path)
        print("New key generated and saved")

    fernet = Fernet(private_key)
    print(f"Fernet key ready: {private_key.decode('utf-8')[:20]}...")

    bronze_config(config, fernet, spark, BRONZE_LAKEHOUSE_PATH)
    staging_config(STAGING_LAKEHOUSE_PATH, BRONZE_LAKEHOUSE_PATH, spark)
    silver_config(SILVER_LAKEHOUSE_PATH, spark)

    return config
