# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bda1512e-7828-4622-9e75-46f282c23084",
# META       "default_lakehouse_name": "news_db",
# META       "default_lakehouse_workspace_id": "17378dde-0dcd-423e-8803-6acd4a382d80"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Read the JSON file as a Dataframe

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/latest_news.json")
# df now is a Spark DataFrame containing JSON data from "Files/latest_news.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Selecting just the article column from the dataframe

# CELL ********************

df = df.select("articles")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Explode the JSON column

# CELL ********************

from pyspark.sql.functions import explode
df_exploded = df.select(explode(df["articles"]).alias("json_object"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_exploded)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Converting the Exploded JSON Dataframe to a single JSON string list

# CELL ********************

json_list = df_exploded.toJSON().collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Testing the JSON string list

# CELL ********************

print(json_list[1])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

news_json = json.loads(json_list[1]) #Converting the JSON string to a JSON dictionary
#print(news_json)
#print(news_json["json_object"]["name"])
#print(news_json["json_object"]["category"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(news_json)
print(news_json["json_object"]["author"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Processing the JSON property to List

# CELL ********************

title = []
description = []
author = []
url =[]
image = []
provider = []
datePublished =[]


# Process each JSON object in the list
for json_str in json_list:
    try:
        # Parse the JSON string into a dictionary
        article = json.loads(json_str)

        if article["json_object"].get("description") and article["json_object"].get("title") and article["json_object"].get("author") and article["json_object"].get("url") and article["json_object"].get("urlToImage") and article["json_object"].get("publishedAt") and article["json_object"].get("source", {}).get("name", {}): 
        #Extract information from the dictionary
            title.append(article["json_object"]["title"])
            description.append(article["json_object"]["description"])
            author.append(article["json_object"]["author"])
            url.append(article["json_object"]["url"])
            image.append(article["json_object"]["urlToImage"])
            provider.append(article["json_object"]["source"]["name"])
            datePublished.append(article["json_object"]["publishedAt"])

    except Exception as e:
        print(f"Error processing JSON object: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(news_json["json_object"]["title"])
print(news_json["json_object"]["description"])
print(news_json["json_object"]["url"])
print(news_json["json_object"]["urlToImage"])
print(news_json["json_object"]["source"]["name"])
print(news_json["json_object"]["publishedAt"])
print(news_json["json_object"]["author"])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

author

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Converting the List to a Dataframe

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType


# Combine the lists
data = list(zip(title,description,author,url,image,provider,datePublished))

# Define schema
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("author", StringType(), True),
    StructField("url", StringType(), True),
    StructField("image", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("datePublished", StringType(), True)
])

# Create DataFrame
df_cleaned = spark.createDataFrame(data, schema=schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_cleaned)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Processing the Date column

# CELL ********************

from pyspark.sql.functions import to_date, date_format

df_cleaned_final = df_cleaned.withColumn("datePublished", date_format(to_date("datePublished"), "dd-MMM-yyyy"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_cleaned_final.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.sql("ALTER TABLE news_db.tbl_news ADD COLUMNS (author STRING)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# table_name = 'news_db.tbl_news'

# # Overwrite the Delta table with only the latest new data (Run this once)
# df_cleaned_final.write.format("delta").mode("overwrite").saveAsTable(table_name)

# print("✅ Delta table has been reset with only new data, including 'author'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Writing the Final Dataframe to the Lakehouse DB in a Delta format

# CELL ********************

from pyspark.sql.utils import AnalysisException
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

try:

    table_name = 'news_db.tbl_news'

    df_cleaned_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:

    print("Table Already Exists")

    df_cleaned_final.createOrReplaceTempView("vw_df_cleaned_final")

    spark.sql(f"""  MERGE INTO {table_name} target_table
                    USING vw_df_cleaned_final source_view

                    ON source_view.url = target_table.url
                    
                    WHEN MATCHED AND 
                    source_view.title <> target_table.title OR
                    source_view.description <> target_table.description OR
                    source_view.author <> target_table.author OR
                    source_view.image <> target_table.image OR
                    source_view.provider <> target_table.provider OR
                    source_view.datePublished <> target_table.datePublished   

                    THEN UPDATE SET *

                    WHEN NOT MATCHED THEN INSERT *

                """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from news_db.tbl_news

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.read.format("delta").table("news_db.tbl_news")
# df.show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_cleaned_final.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
