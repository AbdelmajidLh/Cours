# Databricks notebook source
# MAGIC %md
# MAGIC # Flight data using pyspark
# MAGIC >_Abdelmajid EL HOU
# MAGIC
# MAGIC Data source : [Ici](https://raw.githubusercontent.com/AbdelmajidLh/datasets/main/flight_data.csv)

# COMMAND ----------

# MAGIC %md
# MAGIC > ## 1. Initialisation de la SparkSession et Chargement des Données

# COMMAND ----------

# MAGIC %md
# MAGIC ```Python
# MAGIC # Initialisation de la SparkSession (nécessaire si en dehors de Databricks)
# MAGIC from pyspark.sql import sparkSession
# MAGIC
# MAGIC # initialiser sparkSession
# MAGIC spark = SparkSession.builder() \
# MAGIC   .appName("Flight Data Analysys") \
# MAGIC   .getOrCreate()
# MAGIC ```
# MAGIC

# COMMAND ----------

# Charger le fichier csv
file_path = "/FileStore/tables/flight_data.csv"
flight_df = spark.read.format("csv") \
  .option("header", "true") \
  .option("separator", ",") \
  .option("inferSchema", "true") \
  .option("treatEmptyValuesAsNulls","true") \
  .option("nullValue", None) \
  .option("emptyValue", None) \
  .load(file_path)

# COMMAND ----------

# afficher un apercu 
flight_df.show(5)

# COMMAND ----------

# afficher le schema
flight_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 2 : Analyse des Données

# COMMAND ----------

# nombre de lignes 
print(f"Nombre de lignes : {flight_df.count()}")


# COMMAND ----------

# colonnes
print(f"Colonnes : {flight_df.columns}")

# COMMAND ----------

# resume statistique
flight_df.select(flight_df.columns[:5]).describe().show()

# COMMAND ----------

# afficher certains colonnes uniquement
flight_df.select("Year", "Month", "DayofMonth", "DayOfWeek").show(10)

# COMMAND ----------

# selectionner quelques colonnes
flight_df.select(flight_df.columns[:4]).show(10)

# COMMAND ----------

# afficher les lignes où une colonne specifique est nulle
from pyspark.sql.functions import *
flight_df.filter(col("CarrierDelay").isNull()).show(5) # isNull isnan

# COMMAND ----------

# afficher les lignes où une colonne contient "NA" en tant que texte
# resultats quelques colonnes
flight_df.filter(col("CarrierDelay") == "NA").select("Year", "Month", "DayofMonth", "DayOfWeek", "CarrierDelay").show(10)

# COMMAND ----------

# remplacer les NA dans la colonne CarrierDelay par des NULL
newDf = flight_df.withColumn('CarrierDelay', regexp_replace('CarrierDelay', 'NA', None))
# remplacer tous les na du df
# flight_df = flight_df.na.replace('NA', None)

# COMMAND ----------

# check
newDf.filter(col("CarrierDelay") == "NA").select("Year", "Month", "DayofMonth", "DayOfWeek", "CarrierDelay").show(10)

# COMMAND ----------

# Afficher les vols entre 20 et 31 janvier 2008
newDf.filter((newDf.Year == '2008') & (newDf.Month == '1') & (newDf.DayofMonth >= 20)).select("Year", "Month", "DayofMonth", "DayOfWeek").show(6)

# COMMAND ----------

# filtrer les vols valides (non annulés et non détournés) : "Cancelled" = 0 et Diverted = 0
valid_flight = newDf.filter((col("Cancelled") == 0)  & (col("Diverted") == 0  ))
valid_flight.show(5)

# COMMAND ----------

# check
valid_flight.select(col("Diverted")).distinct().show()

# COMMAND ----------

# ajouter une colonne TotalDelay
flight_df = newDf.withColumn("TotalDelay", col("ArrDelay") + col("DepDelay"))


# COMMAND ----------

# ajouter une colonne pour identifier les vols long-distance (> 1000)
flight_df = flight_df.withColumn("LongHaul", when(col("Distance") > 1000, True).otherwise(False))

# COMMAND ----------

# trier les vols valides par retard total decroissant
flight_df.orderBy(col("TotalDelay").desc()).select("UniqueCarrier", "TotalDelay").show(10)

# COMMAND ----------

# autre methode
flight_df.orderBy(col("TotalDelay"), ascending=False).select("UniqueCarrier", "TotalDelay").show(10)

# COMMAND ----------

flight_df.filter(col("LongHaul") == 'True').select("Year", "DepDelay", "ArrDelay", "TotalDelay", "Distance", "LongHaul").show(5)

# COMMAND ----------

print(valid_flight.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Étape 3 : Agrégations et Analyses

# COMMAND ----------

# retard moyen à l'arrivee par compagnie aérienne
retard_moyen_par_compagnie = flight_df.groupBy("UniqueCarrier") \
  .agg(avg("ArrDelay").alias("AvgArrDelay")) \
    .orderBy("AvgArrDelay", ascending = False)
  #.orderBy(col("AvgArrDelay").desc()).show(5)\
retard_moyen_par_compagnie.show(5)
  

# COMMAND ----------

display(retard_moyen_par_compagnie)

# COMMAND ----------

# Nombre de vols par jour
nbVols_jour = flight_df.groupBy("DayofMonth") \
  .agg(count("*").alias("Nbr")) \
    .orderBy("Nbr", ascending = False)
nbVols_jour.show(5)

# COMMAND ----------

display(nbVols_jour)

# COMMAND ----------

# statistiques sur les distances par aéroport d'origine
df_stats = flight_df.groupBy("Origin") \
  .agg(min("Distance").alias("MinDistance"),
       max("Distance").alias("MaxDistance"),
       avg("Distance").alias("AvgDistance")
       ).orderBy("AvgDistance", ascending = False) \

stats.show(10)

# COMMAND ----------

# visualiser les resultats
display(df_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exporter les résultats

# COMMAND ----------

# MAGIC %md
# MAGIC Le format Parquet est un format de stockage colonnaire, compressé et optimisé pour les systèmes Big Data, permettant des analyses rapides et efficaces tout en réduisant la taille des fichiers.

# COMMAND ----------

output_parquet = "/FileStore/tables/df_stats"
df_stats.write \
    .format("parquet") \
    .option("compression", "snappy") \
    .mode("overwrite") \
    .partitionBy("Origin") \
    .save(output_parquet)
print(f"Les données sauvegardées à : {output_parquet}")

# COMMAND ----------

# Lister les fichiers dans le dossier
display(dbutils.fs.ls("/FileStore/tables/df_stats/"))


# COMMAND ----------

df_pq = spark.read.format("parquet")\
  .load(output_parquet)
df_pq.show()
