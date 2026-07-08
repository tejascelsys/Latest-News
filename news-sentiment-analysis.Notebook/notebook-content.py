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

# ### Read the News Table from the Lakehouse DB

# CELL ********************

df = spark.sql("SELECT * FROM news_db.tbl_news")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Import Synapse ML

# CELL ********************

import synapse.ml.core
from synapse.ml.services import AnalyzeText

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Configure Synapse ML

# CELL ********************

#Import the model and configure the input and output columns
model = (AnalyzeText()
        .setTextCol("description")
        .setKind("SentimentAnalysis")
        .setOutputCol("response")
        .setErrorCol("error"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Apply the model

# CELL ********************

#Apply the model to our dataframe
result = model.transform(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Grab sentiment from the response

# CELL ********************

#Create Sentiment Column
from pyspark.sql.functions import col

sentiment_df = result.withColumn("sentiment", col("response.documents.sentiment"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sentiment_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Remove unwanted columns

# CELL ********************

sentiment_df_final = sentiment_df.drop("error","response")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sentiment_df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date

sentiment_df_final = sentiment_df_final.withColumn("datePublished", to_date(col("datePublished"), "dd-MMM-yyyy"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.sql("ALTER TABLE news_db.tbl_sentiment_analysis ADD COLUMNS (author STRING)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# table_name = 'news_db.tbl_sentiment_analysis'

# # Overwrite the Delta table with only the latest new data (Run this once)
# sentiment_df_final.write.format("delta").mode("overwrite").saveAsTable(table_name)

# print("✅ Delta table has been reset with only new data, including 'author'.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Type 1 Merge

# CELL ********************

from pyspark.sql.utils import AnalysisException

try:

    table_name = 'news_db.tbl_sentiment_analysis'

    sentiment_df_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:

    print("Table Already Exists")

    sentiment_df_final.createOrReplaceTempView("vw_sentiment_df_final")

    spark.sql(f"""  MERGE INTO {table_name} target_table
                    USING vw_sentiment_df_final source_view

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

display(sentiment_df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
