#!/usr/bin/env python
# coding: utf-8

# ## br_to_sil_shopify
# 
# New notebook
# In[1]:
import time
import os
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import *
from notebookutils import mssparkutils, lakehouse
from pyspark.sql.types import *
from pyspark.sql.functions import *
import notebookutils
from collections import deque
# In[2]:

def get_lakehouse_path(lakehouse_name: str) -> str:
    try:
        lakehouse_details = notebookutils.lakehouse.get(lakehouse_name)
    except:
        raise Exception(f"A LAKEHOUSE WITH NAME '{lakehouse_name}' DOES NOT EXIST IN THE WORKSPACE")
    
    return lakehouse_details.get('properties', {}).get('abfsPath')

# In[3]:
def paths():
    workspace_info = notebookutils.lakehouse.list() 
    if not workspace_info:
        raise Exception("No lakehouses found in the workspace.")

    STAGING_LK_PATH = get_lakehouse_path('Staging_Lakehouse')
    BRONZE_LK_PATH = get_lakehouse_path('Bronze_Lakehouse')
    SILVER_LK_PATH = get_lakehouse_path ('Silver_Lakehouse')

    return STAGING_LK_PATH,BRONZE_LK_PATH,SILVER_LK_PATH

# In[4]:

def list_tables(spark,lk_path: str)->str:
    
    table_paths = notebookutils.fs.ls(f'{lk_path}/Tables/')
    table_names = []
    for table in table_paths:
        table_path = table.path
        if DeltaTable.isDeltaTable(spark, table_path):
            table_name = os.path.basename(table_path.rstrip('/'))
            if table_name.startswith(('sil.shopify', 'dim.shopify','shopify')):
                table_names.append(os.path.basename(table_path.rstrip('/')))
    return table_names
# In[5]:

def get_name_from_path(path):
    return path.split('/')[-1]

# In[6]:

def replace_null_equivalents(df):
    
    null_equivalents = ["null", "NULL", "None", "none", "NaN", "nan", "", " ", "NA", "N/A", "n/a", "?", "-", "--"]
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            df = df.withColumn(
                col_name,
                when(col(f"`{col_name}`").isin(null_equivalents), None).otherwise(col(f"`{col_name}`"))
            )
    return df

# In[7]:

def update_config_date(table, source, new_last_sync, path,spark):
    
    delta_table = DeltaTable.forName(spark, f"delta.`{path}`")

    if source:
        condition = f"table = '{table}' AND source = '{source}'"
    else:
        condition = f"table = '{table}'"

    delta_table.update(
        condition=condition,
        set={"last_sync": lit(new_last_sync)} 
    )
    print(f"Config table updated for table {table} with source {source} with date {new_last_sync}")
# In[8]:

def parse_the_json_like_column(df, col_name):
    
    col_name = col_name.strip('`')
    df = df.withColumn(col_name, regexp_replace(col(f"`{col_name}`"), r"^\s*\{\s*", "{"))
    df = df.withColumn(col_name, regexp_replace(col(f"`{col_name}`"), r"\s*\}\s*$", "}"))

    def parse_func(s):
        if s is None or s == '':
            return None
        
        s = s.strip()
        if not (s.startswith('{') and s.endswith('}')):
            return None
        
        result = {}
        i = 1 

        while i < len(s) - 1:
            while i < len(s) - 1 and s[i] in ' \t\n':
                i += 1
            
            if i >= len(s) - 1:
                break
            
            key_start = i
            while i < len(s) and s[i] != '=':
                i += 1
            
            if i >= len(s):
                break
            
            key = s[key_start:i].strip().rstrip(',')
            i += 1
            
            while i < len(s) and s[i] in ' \t\n':
                i += 1
            
            value_start = i
            depth_curly = 0
            depth_square = 0
            
            while i < len(s):
                char = s[i]
                
                if char == '{':
                    depth_curly += 1
                elif char == '}':
                    if depth_curly > 0:
                        depth_curly -= 1
                    else:
                        break
                elif char == '[':
                    depth_square += 1
                elif char == ']':
                    depth_square -= 1
                elif char == ',' and depth_curly == 0 and depth_square == 0:
                    j = i + 1
                    while j < len(s) and s[j] in ' \t\n':
                        j += 1
                    
                    found_key_pattern = False
                    if j < len(s):
                        k = j
                        while k < len(s) and s[k] not in '=,{}[] \t\n':
                            k += 1
                        if k < len(s) and s[k] == '=':
                            found_key_pattern = True
                    
                    if found_key_pattern:
                        break
                
                i += 1
            
            value = s[value_start:i].strip()
            if key:
                result[key] = value
            
            if i < len(s) and s[i] == ',':
                i += 1
        
        return result if result else None

    parse_to_map_udf = udf(parse_func, MapType(StringType(), StringType()))
    df = df.withColumn(col_name, parse_to_map_udf(col(f"`{col_name}`")))
    return df

# In[9]:

def extract_the_map_column_into_new_columns(df, col_name):
    
    col_name = col_name.strip('`')
    sample_row = df.filter(col(f"`{col_name}`").isNotNull()).select(f"`{col_name}`").limit(1).collect()
    
    if sample_row:
        sample_row = sample_row[0][0]
    else: return df
    for key in sample_row.keys():
        df = df.withColumn(f"{col_name}.{key}", col(f"`{col_name}`")[key])
    return df

# In[10]:

def detect_is_json_like_column(df, col_name):
    
    col_name = col_name.strip('`')
    col_type = df.schema[col_name].dataType
    if col_type != StringType():
        return False

    json_like_expr = (
        (col(f"`{col_name}`").isNotNull()) &
        (trim(f"`{col_name}`") != lit(""))  &
        (substring(f"`{col_name}`", 1, 1) == lit("{"))  &
        (substring(f"`{col_name}`", -1, 1) == lit("}")) &
        (substring(f"`{col_name}`", 1, 1) != lit("["))
    )

    sample = df.select(f"`{col_name}`") \
               .sample(withReplacement=False, fraction=1.0) \
               .limit(200)

    agg = sample.agg(max(when(json_like_expr, lit(1)).otherwise(lit(0))).alias("has_json"))
    row = agg.collect()
    if row:
        return row[0]["has_json"] == 1
    else:
        return False

# In[11]:

def parse_json_for_table(target,spark,STAGING_LK_PATH):

    try:
        spark.sql(f"""
            ALTER TABLE delta.`{target}` 
            SET TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5'
            )
        """)
        print("column mapping enabled")
    except Exception as e:
        print(f"column mapping already enabled or error: {e}")

    spark.sql(f"OPTIMIZE delta.`{target}`")
    main_df = spark.read.format("delta").load(target)

    current_partitions = main_df.rdd.getNumPartitions()
    target_partitions = current_partitions * 4
    print(f"Repartitioning from {current_partitions} to {target_partitions} partitions")
    main_df = main_df.repartition(target_partitions)        

    id_cols = []
    for i in main_df.schema.names:
        if "dim" in get_name_from_path(target):
            if '.id' in i:
                id_cols.append(i)
        if 'id' == i:
            id_cols.append(i)
    id_cols_escaped = [f"`{x}`" for x in id_cols]

    queue = deque(main_df.schema.names)
    drop_json_cols = []
    log_print_count = 0
    first_json_col = ''
    reload_target_table = False
    table_id_found = False

    while queue:
        log_print_count += 1
        if log_print_count == 20:
            print(queue)
            log_print_count = 0

        col_name =  queue.popleft()
        print('trying for', col_name)

        if reload_target_table or col_name not in main_df.schema.names:
            print(f"column {col_name} not in df, reading it again from target")
            spark.sql(f"OPTIMIZE delta.`{target}`")
            main_df = spark.read.format("delta").load(target)
            main_df = main_df.repartition(target_partitions)
            reload_target_table = False

        if detect_is_json_like_column(main_df, col_name):
            start_time = time.time()

            if not first_json_col:
                first_json_col = col_name

            print(f"extracting json column {col_name}")
            drop_json_cols.append(col_name)

            if first_json_col != col_name:
                temp = main_df.select("store", *id_cols_escaped, f"`{first_json_col}`", f"`{col_name}`")
                temp = temp.withColumnRenamed(first_json_col, f"{first_json_col}_str")
            
            else:
                temp = main_df.select("store", *id_cols_escaped, f"`{col_name}`")
                temp = temp.withColumn(f"{col_name}_str", col(f"`{col_name}`"))

            temp = parse_the_json_like_column(temp, col_name)
            temp = extract_the_map_column_into_new_columns(temp, col_name)
            # temp = replace_null_equivalents(temp)

            if id_cols == ['id'] or not table_id_found:
                main_id = 'id_merged' if 'id_merged' in main_df.schema.names else 'id'
                if main_id == 'id_merged':
                    temp = temp.withColumn("rownum", monotonically_increasing_id())
                    window_spec = Window.partitionBy("id", "store", f"`{first_json_col}_str`").orderBy("rownum")
                    temp = temp.withColumn("ranking", row_number().over(window_spec))
                    temp = temp.withColumn("id_merged", concat(col("id"), lit("#"), col("store"), lit("#"), col("ranking"), lit("#"), col(f"`{first_json_col}_str`")))
                    temp = temp.drop("rownum", "ranking", f"`{first_json_col}_str`")

                exclude_cols = set(id_cols + ["store", "id", "id_merged", col_name, f"{first_json_col}_str"])
                update_cols = {f"`{c}`": f"source.`{c}`" for c in temp.schema.names if c not in exclude_cols}
                insert_cols = {f"`{c}`": f"source.`{c}`" for c in temp.schema.names if c not in exclude_cols}

                delta_table = DeltaTable.forPath(spark, target)
                delta_table.alias("target").merge(
                    temp.alias("source"),
                    f"target.`{main_id}` = source.`{main_id}` AND target.`store` = source.`store`"
                ).whenMatchedUpdate(set=update_cols).whenNotMatchedInsert(values=insert_cols).execute()  
            
            elif table_id_found:
                exclude_cols = set(id_cols + ["store", "id", "id_merged", col_name, f"{first_json_col}_str"])
                update_cols = {f"`{c}`": f"source.`{c}`" for c in temp.schema.names if c not in exclude_cols}
                insert_cols = {f"`{c}`": f"source.`{c}`" for c in temp.schema.names if c not in exclude_cols}

                merge_conditions = [f"target.`{x}` = source.`{x}`" for x in id_cols]
                merge_conditions.append("target.`store` = source.`store`")
                merge_condition = " AND ".join(merge_conditions)

                delta_table = DeltaTable.forPath(spark, target)
                delta_table.alias("target").merge(
                    temp.alias("source"),
                    merge_condition
                ).whenMatchedUpdate(set=update_cols).whenNotMatchedInsert(values=insert_cols).execute()

            else: print(f"strange case detected, \nid_cols are\n{id_cols}\n\nmain_df cols are\n{main_df.schema.names}\n\ntemp cols are\n{temp.schema.cols}\n")

            if "dim" in get_name_from_path(target):
                # does any row in temp have the key id inside the map column?
                sample = (
                    temp.select(col(f"`{col_name}`"))
                        .filter(col(f"`{col_name}`").isNotNull())
                        .sample(withReplacement=False, fraction=1.0)
                        .limit(200)
                )
                check = (
                    sample.withColumn("has_id", 
                        array_contains(map_keys(col(f"`{col_name}`")), "id"))
                        .select("has_id").persist()
                )
                has_id_any = check.filter(col("has_id")).limit(1).count() > 0
                check.unpersist()
                if has_id_any and not table_id_found:
                    print('table id found, it is', f"{col_name}.id")
                    id_cols.append(f"{col_name}.id")
                    id_cols_escaped = [f"`{x}`" for x in id_cols]
                    table_id_found = True
                    reload_target_table = True

            exclude_from_queue = set(id_cols + ['store', 'id_merged', f"{first_json_col}_str", col_name])
            new_cols = [i for i in temp.schema.names if i not in exclude_from_queue]
            print("new cols are \n", new_cols, "\n\n")
            queue.extend(new_cols)

    # drop json columns
    if drop_json_cols:
        cols_str = ", ".join([f"`{c}`" for c in drop_json_cols])
        print(f'dropping json columns from {get_name_from_path(target)}')
        print(f"columns are {', '.join(drop_json_cols)}")
        spark.sql(f"ALTER TABLE delta.`{target}` DROP COLUMNS ({cols_str})")

    # replace null values
    main_df = spark.read.format("delta").load(target)
    main_df = replace_null_equivalents(main_df)
    main_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target)

    # maintain the table unique ids
    if not table_id_found and 'id_merged' in main_df.schema.names:
        table_name = get_name_from_path(target)
        key_table_path = f"{STAGING_LK_PATH}/Tables/shopify_unique_keys"
        new_row_df = spark.createDataFrame([(table_name, "id_merged")], ["table", "key"])
        if DeltaTable.isDeltaTable(spark, key_table_path):
            delta_table = DeltaTable.forPath(spark, key_table_path)
            delta_table.alias("t").merge(
                new_row_df.alias("s"),
                "t.table = s.table"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            new_row_df.write.format("delta").mode("overwrite").save(key_table_path)

    else:
        table_name = get_name_from_path(target)
        key_table_path = f"{STAGING_LK_PATH}/Tables/shopify_unique_keys"
        new_key = ",".join(id_cols)
        new_row_df = spark.createDataFrame([(table_name, new_key)], ["table", "key"])
        if DeltaTable.isDeltaTable(spark, key_table_path):
            delta_table = DeltaTable.forPath(spark, key_table_path)
            delta_table.alias("t").merge(
                new_row_df.alias("s"),
                "t.table = s.table"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            new_row_df.write.format("delta").mode("overwrite").save(key_table_path)

# In[12]:

def detect_is_array_like_column(df, col_name):
    
    col_type = df.schema[col_name].dataType
    if col_type != StringType():
        return False
    
    array_like_expr = (
        (col(f"`{col_name}`").isNotNull()) &
        (trim(f"`{col_name}`") != lit("")) &
        (substring(f"`{col_name}`", 1, 1) == lit("[")) &
        (substring(f"`{col_name}`", -1, 1) == lit("]")) &
        (length(trim(f"`{col_name}`")) > lit(1))
    )
    
    sample = df.select(f"`{col_name}`") \
            .filter(col(f"`{col_name}`").isNotNull())\
            .sample(withReplacement=False, fraction=1.0) \
            .limit(200)
    
    if sample.limit(1).count() < 0:
        return False
    
    agg = sample.agg(max(when(array_like_expr, lit(1)).otherwise(lit(0))).alias("has_array"))
    row = agg.collect()
    if row:
        row = row[0]
        return row["has_array"] == 1
    else: 
        return False

# In[13]:

def parse_the_array_like_column(df, col_name):
    
    candidate_id_cols = [i for i in df.columns if i == "id" or ".id" in i]
    null_counts = df.agg(*[
        sum(when(col(f"`{c}`").isNull(), 1).otherwise(0)).alias(c)
        for c in candidate_id_cols
    ]).collect()[0].asDict()
    non_null_id_cols = [f"`{c}`" for c, cnt in null_counts.items() if cnt == 0]
    print(f"persisting id columns: {non_null_id_cols}")
    df = df.select('store', *non_null_id_cols, f"`{col_name}`")

    def parse_func(s):
        if s is None or s == '[]':
            return []
        
        s = s.strip()
        if s.startswith('[') and s.endswith(']'):
            s = s[1:-1].strip()
        
        if not s:
            return []
        
        elements = []
        i = 0
        start = 0
        depth_curly = 0
        depth_square = 0 
        
        while i < len(s):
            char = s[i]
            
            if char == '{':
                depth_curly += 1
            elif char == '}':
                depth_curly -= 1
            elif char == '[':
                depth_square += 1
            elif char == ']':
                depth_square -= 1
            elif char == ',' and depth_curly == 0 and depth_square == 0:
                element = s[start:i].strip()
                if element:
                    elements.append(element)
                start = i + 1
            
            i += 1
        
        element = s[start:].strip()
        if element:
            elements.append(element)
        
        return elements

    parse_func_udf = udf(parse_func, ArrayType(StringType()))
    df = df.withColumn(col_name, parse_func_udf(col(f"`{col_name}`")))
    return df

# In[14]:

def extract_the_array_column_into_new_columns(df, col_name):
    
    candidate_id_cols = [i for i in df.columns if i == "id" or ".id" in i]
    null_counts = df.agg(*[
        sum(when(col(f"`{c}`").isNull(), 1).otherwise(0)).alias(c)
        for c in candidate_id_cols
    ]).collect()[0].asDict()
    non_null_id_cols = [f"`{c}`" for c, cnt in null_counts.items() if cnt == 0]

    temp = df.select("store", *non_null_id_cols, explode(col(f"`{col_name}`")).alias(col_name))
    return temp

# In[15]:

def create_dim_tables_from_table(source,spark,STAGING_LK_PATH):

    main_df = spark.read.format("delta").load(source)
    if not main_df.limit(1).count():
        print(f"during array parsing no data found in {get_name_from_path(source)}")
        return []
    print(f"processing source {get_name_from_path(source)} ")
    cols = main_df.columns
    source_name = get_name_from_path(source).split('.', 1)[1]

    new_dim_tables = []
    for i in sorted(cols):
        print(f'trying for {i}')
        if detect_is_array_like_column(main_df, i):
            print(f"extracting array column {i}")
            start_time = time.time()

            temp = main_df.filter(col(f"`{i}`").isNotNull())
            temp = parse_the_array_like_column(main_df, i)
            temp = extract_the_array_column_into_new_columns(temp, i)
            temp = replace_null_equivalents(temp)

            dim_table_name = f'dim.{source_name}.{i}'

            if temp.limit(1).count() == 0:
                print(f"dimension table {dim_table_name} was empty so it was not stored.")
                continue

            temp = temp.withColumn("rownum", monotonically_increasing_id())
            window_spec = Window.partitionBy("id", "store", f"`{i}`").orderBy("rownum")
            temp = temp.withColumn("ranking", row_number().over(window_spec))
            temp = temp.withColumn("id_merged", concat(col("id"), lit("#"), col("store"), lit("#"), col("ranking"), lit("#"), col(f"`{i}`")))
            temp = temp.drop("rownum", "ranking")
            temp = temp.withColumn("record_timestamp", current_timestamp())
            

            temp.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("delta.columnMapping.mode","name").save(f'{STAGING_LK_PATH}/Tables/{dim_table_name}')
            spark.sql(f"OPTIMIZE delta.`{STAGING_LK_PATH}/Tables/{dim_table_name}`")

            new_dim_tables.append(dim_table_name)
            print(f"saved dimension table {dim_table_name}")
            main_df = main_df.drop(i)

            end_time = time.time()
            print(f"total time taken {end_time - start_time:2f}s \n")

    main_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("delta.columnMapping.mode","name").save(source)
    print(f"updated {get_name_from_path(source)} table after dropping array columns")

    spark.sql(f"OPTIMIZE delta.`{source}`")
    return new_dim_tables

# In[16]:

def infer_column_type_spark(df, col_name, sample_size=500):
    
    _TYPE_INFO = [
    # (BooleanType(),   "boolean"),
    (DoubleType(),    "double"),
    (ShortType(),     "short"),
    (LongType(),      "long"),
    (TimestampType(), "timestamp"),
    (DateType(),      "date"),
    # (DecimalType(38, 10), "decimal")
    ]

    if col_name not in df.columns:
        raise ValueError(f"Column '{col_name}' not found in DataFrame.")

    # Get total non-null count for the column
    total_count = df.select(col(f"`{col_name}`")).filter(
        col(f"`{col_name}`").isNotNull()
    ).count()
    
    if total_count == 0:
        return ("string", False, 0)

    # Take sample
    sample = (
        df.select(col(f"`{col_name}`").cast("string").alias("_raw"))
          .filter(col("_raw").isNotNull())
          .sample(withReplacement=False, fraction=1.0)
          .limit(sample_size)
    )

    if sample.rdd.isEmpty():
        return ("string", False, 0)

    sample_count = sample.count()

    # Try each type
    for spark_type_cls, cast_name in _TYPE_INFO:
        nulls_in_sample = (
            sample
            .select(col("_raw").cast(spark_type_cls).alias(f"_cast_{cast_name}"))
            .filter(col(f"_cast_{cast_name}").isNull())
            .count()
        )
        
        if nulls_in_sample == 0:
            # Validate on full dataset to detect mixed types
            full_null_count = (
                df.select(col(f"`{col_name}`").cast("string").alias("_raw"))
                  .filter(col("_raw").isNotNull())
                  .select(col("_raw").cast(spark_type_cls).alias(f"_cast_{cast_name}"))
                  .filter(col(f"_cast_{cast_name}").isNull())
                  .count()
            )
            
            if full_null_count > 0:
                # Mixed type detected!
                percentage = (full_null_count / total_count) * 100
                print(f"WARNING: Column '{col_name}' has mixed types!")
                print(f"   - Sample suggested: {cast_name}")
                print(f"   - But {full_null_count}/{total_count} ({percentage:.2f}%) values cannot be cast to {cast_name}")
                print(f"   - Falling back to STRING to preserve all data")
                return ("string", True, full_null_count)
            
            return (cast_name, False, 0)

    # Try decimal
    decimal_info = _try_decimal(sample, "_raw")
    if decimal_info is not None:
        # Validate decimal on full dataset
        full_null_count = (
            df.select(col(f"`{col_name}`").cast("string").alias("_raw"))
              .filter(col("_raw").isNotNull())
              .select(col("_raw").cast(decimal_info).alias("_cast_decimal"))
              .filter(col("_cast_decimal").isNull())
              .count()
        )
        
        if full_null_count > 0:
            percentage = (full_null_count / total_count) * 100
            print(f" WARNING: Column '{col_name}' has mixed types!")
            print(f"   - Sample suggested: {decimal_info}")
            print(f"   - But {full_null_count}/{total_count} ({percentage:.2f}%) values cannot be cast to {decimal_info}")
            print(f"   - Falling back to STRING to preserve all data")
            return ("string", True, full_null_count)
        
        return (decimal_info, False, 0)

    return ("string", False, 0)


def _try_decimal(sample, col_name):
    double_cast = sample.select(col(f"`{col_name}`").cast("double").alias("_d"))
    if double_cast.filter(col("_d").isNull()).count() > 0:
        return None

    int_frac = sample.select(
        when(
            col(f"`{col_name}`").contains("."),
            struct(
                length(split(col(f"`{col_name}`"), "[.]").getItem(0).cast("string")).alias("int"),
                length(split(col(f"`{col_name}`"), "[.]").getItem(1).cast("string")).alias("frac")
            )
        ).otherwise(
            struct(
                length(col(f"`{col_name}`").cast("string")).alias("int"),
                lit(0).alias("frac")
            )
        ).alias("parts")
    ).agg(
        max(col("parts.int")).alias("max_int"),
        max(col("parts.frac")).alias("max_frac")
    ).collect()[0]

    max_int  = int_frac["max_int"]  or 0
    max_frac = int_frac["max_frac"] or 0

    if any("-" in str(v[col_name]) for v in sample.select(f"`{col_name}`").head(5)):
        max_int += 1

    precision = max_int + max_frac
    if precision == 0 or precision > 38:
        return None

    return f"decimal({precision},{max_frac})"


def infer_schema_spark(df, sample_size=500):

    schema_map = {}
    mixed_columns = []
    
    for c in df.columns:
        data_type, is_mixed, null_count = infer_column_type_spark(df, c, sample_size)
        schema_map[c] = data_type
        if is_mixed:
            mixed_columns.append((c, null_count))
    
    return schema_map, mixed_columns


def infer_and_cast_table(spark,source, sample_size=500):

    main_df = spark.read.format("delta").load(source)
    main_df = replace_null_equivalents(main_df)
    table_name = get_name_from_path(source)
    
    if not main_df.limit(1).count():
        print(f'During infer casting no data found in {table_name}')
        return
    
    print(f'Inferring schema for {table_name} (sample_size={sample_size})...')
    schema_map, mixed_columns = infer_schema_spark(main_df, sample_size)
    
    # Report findings
    if mixed_columns:
        print(f'\nMIXED TYPE COLUMNS DETECTED IN {table_name}:')
        for col_name, null_count in mixed_columns:
            print(f'   - {col_name}: {null_count} values would become NULL on cast → kept as STRING')
        print()
    
    # Cast columns
    df_casted = main_df.select('*')
    for col_name, data_type in schema_map.items():
        df_casted = df_casted.withColumn(col_name, col(f"`{col_name}`").cast(data_type))

    print(f'Saving table {table_name} after inferring schema')
    df_casted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("delta.columnMapping.mode","name").save(source)
    
    spark.sql(f"OPTIMIZE delta.`{source}`")

    # Final summary
    type_counts = {}
    for dt in schema_map.values():
        type_counts[dt] = type_counts.get(dt, 0) + 1
    
    print(f'{table_name} schema inference complete:')
    for dt, count in sorted(type_counts.items()):
        print(f'   - {dt}: {count} columns')
    print()

# In[17]:
def del_staging_tables(spark,STAGING_LK_PATH):
    tables = list_tables(spark,STAGING_LK_PATH)
    if not tables:
        print(f"No tables found")
    else:
        print(f"Found {len(tables)} table(s)")
        for table_name in tables:
            
            full_table_path = f"{STAGING_LK_PATH}/Tables/{table_name}"
            notebookutils.fs.rm(full_table_path, recurse=True)
            print(f"Deleted {table_name}")

# In[18]:
def br_to_staging(df_bronze_config,df_staging_config,BRONZE_LK_PATH,STAGING_LK_PATH,STAGING_CONFIG_PATH,spark):
    rows =(df_staging_config
        .filter(col("source").contains("shopify"))
        .filter(col("isActive") == True)
        .collect())
    print(rows)
    for row in rows:
        source_table_name = row['source']
        table_name = row['table']
        last_sync = row['last_sync']
        new_last_sync = datetime.now()

        bronze_table_path = f"{BRONZE_LK_PATH}/Tables/{source_table_name}"
        staging_table_path = f"{STAGING_LK_PATH}/Tables/{table_name}"

        try:
            main_df = spark.read.format("delta").load(bronze_table_path).filter(col("record_timestamp") >= lit(last_sync))
        except Exception as e:
            if "PATH_NOT_FOUND" in str(e):
                print(f"Skipping {bronze_table_path} - does not exist")
                continue
            else:
                raise
        if not main_df.limit(1).count():
            print(f"during table movement no data found in {source_table_name}")
            continue

        print(f"moving source {source_table_name} to target {table_name}")
        config = (
            df_staging_config.alias("proc")
            .join(
                df_bronze_config.select(col("table").alias("br_table"), "store").alias("br"),
                col("proc.source") == col("br_table"),
                how="inner"
            )
        )

        rows = config.filter(col("br_table") == lit(source_table_name)).collect()
        store_name = rows[0]['store']
        main_df = main_df.withColumn("store", lit(store_name))

        if DeltaTable.isDeltaTable(spark, staging_table_path):
            delta_table = DeltaTable.forPath(spark, staging_table_path)
            delta_table.alias("target").merge(
                main_df.alias("source"),
                f"target.id = source.id AND target.store = source.store"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            main_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(staging_table_path)
        
        update_config_date(table_name, source_table_name, new_last_sync, STAGING_CONFIG_PATH,spark)

# In[19]:
def staging_to_silver(spark,STAGING_LK_PATH,SILVER_LK_PATH,df_silver_config,SILVER_CONFIG_PATH):
    staging_tables = list_tables(spark,STAGING_LK_PATH)
    for table in staging_tables:
        source_table = f"{STAGING_LK_PATH}/Tables/{table}"
        target_table = f"{SILVER_LK_PATH}/Tables/{table}"
        key_table = f"{STAGING_LK_PATH}/Tables/shopify_unique_keys"



        print(f"moving processing.{table} to silver.{table}")
        new_last_sync = datetime.now()

        df = spark.read.format("delta").load(source_table)
        df = df.withColumn("record_timestamp", current_timestamp())

        primary_key = df_silver_config.filter(col("table") == table).collect()
        unique_key = spark.read.format("delta").load(key_table).filter(col("table") == table).collect()

        if not len(primary_key):
            last_sync = datetime.strptime("1900-01-01 00:00:00.00000", "%Y-%m-%d %H:%M:%S.%f")
            if not len(unique_key): primary_key = "id" if "dim" not in table else "id_merged"
            else: primary_key = unique_key[0]['key']
            new_row = Row(table=table, last_sync=last_sync, primary_key=primary_key)
            new_df = spark.createDataFrame([new_row])
            new_df.write.mode("append").save(SILVER_CONFIG_PATH)
        else:
            primary_key = primary_key[0]['primary_key']
        primary_key_vals = [val.strip() for val in primary_key.split(',')]
        primary_key = [f"target.{val} = source.{val}" for val in primary_key_vals]
        primary_key  = ' AND '.join(primary_key)

        if DeltaTable.isDeltaTable(spark, target_table):
            delta_table = DeltaTable.forPath(spark, target_table)
            delta_table.alias("target").merge(
                df.alias("source"),
                f"{primary_key} AND target.store = source.store"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print(f"Moved table {table} to silver")
        else:
            # df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_table)
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
            .option("delta.columnMapping.mode", "name") \
            .option("delta.minReaderVersion", "2") \
            .option("delta.minWriterVersion", "5") \
            .save(target_table)
            print(f'overwriting the table silver.{table}')

        update_config_date(table, None, new_last_sync, SILVER_CONFIG_PATH,spark)
# In[20]:

def br_to_sil_shopify(spark):
    (
    STAGING_LK_PATH,
    BRONZE_LK_PATH,
    SILVER_LK_PATH) = paths()

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

    BRONZE_CONFIG_PATH = f"{BRONZE_LK_PATH}/Tables/br_shopify_config"
    STAGING_CONFIG_PATH = f"{STAGING_LK_PATH}/Tables/Staging_config"
    SILVER_CONFIG_PATH = f"{SILVER_LK_PATH}/Tables/Sil_config"

    df_bronze_config = spark.read.format("delta").load(BRONZE_CONFIG_PATH)
    df_staging_config = spark.read.format("delta").load(STAGING_CONFIG_PATH)
    df_silver_config = spark.read.format("delta").load(SILVER_CONFIG_PATH)

    del_staging_tables(spark,STAGING_LK_PATH)

    br_to_staging(df_bronze_config,df_staging_config,BRONZE_LK_PATH,STAGING_LK_PATH,STAGING_CONFIG_PATH,spark)

    # Unmapping json and array
    tables_parse_json_queue = deque()
    tables_parse_array_queue = deque()
    tables_parse_json_queue.extend(list_tables(spark,STAGING_LK_PATH))

    while tables_parse_json_queue or tables_parse_array_queue:
        while tables_parse_json_queue:
            table = tables_parse_json_queue.popleft()
            print(f"processing json for table {table}")
            table_path = f'{STAGING_LK_PATH}/Tables/{table}'
            parse_json_for_table(table_path,spark,STAGING_LK_PATH)
            tables_parse_array_queue.append(table)
        
        while tables_parse_array_queue:
            table = tables_parse_array_queue.popleft()
            print(f"processing array for table {table}")
            table_path = f'{STAGING_LK_PATH}/Tables/{table}'
            dim_tables = create_dim_tables_from_table(table_path,spark,STAGING_LK_PATH)
            tables_parse_json_queue.extend(dim_tables)

    # Change data types
    existing_tables_in_schema = list_tables(spark,STAGING_LK_PATH)
    for table in existing_tables_in_schema:
        infer_and_cast_table(spark,f'{STAGING_LK_PATH}/Tables/{table}')

    staging_to_silver(spark,STAGING_LK_PATH,SILVER_LK_PATH,df_silver_config,SILVER_CONFIG_PATH)

    
