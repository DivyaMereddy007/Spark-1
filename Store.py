# Databricks notebook source
retail_df = spark.read.csv("/FileStore/tables/train.csv", header = True, inferSchema = True)

# COMMAND ----------

retail_df.count()

# COMMAND ----------

retail_df.columns

# COMMAND ----------

sales_custs_df = retail_df.select( "Store", "Sales", "Customers" )

# COMMAND ----------

retails_open_df = retail_df.where( retail_df.Open > 0  )

# COMMAND ----------

holidays_df = retail_df.where( ( retail_df.StateHoliday == 1 ) & ( retail_df.SchoolHoliday == 1 ) )

# COMMAND ----------

store_ids = retail_df.select( retail_df.Store ).distinct()

# COMMAND ----------

store_ids.show()

# COMMAND ----------

weekday_promos = retail_df.stat.crosstab( "DayOfWeek" , "Promo" )

# COMMAND ----------

weekday_promos = weekday_promos.withColumnRenamed( "DayOfWeek_Promo", "DayOfWeek" )  \
                             .withColumnRenamed( "0", "NoPromo" )                  \
                             .withColumnRenamed( "1","Promo" )

# COMMAND ----------

weekday_promos.show()

# COMMAND ----------

weekday_promos.sort( "Promo" ).show()

# COMMAND ----------

weekday_promos.sort( "Promo", ascending = False ).show()

# COMMAND ----------

from pyspark.sql.functions import month, year

# COMMAND ----------

retail_df = retail_df.withColumn( 'month', month( retail_df.Date ) )

# COMMAND ----------

retail_df = retail_df.withColumn( 'year', year( retail_df.Date ) )

# COMMAND ----------

retail_df.printSchema()

# COMMAND ----------

sales_by_stores = retail_df.groupBy( "Store" ).sum( "Sales" )
sales_by_stores

# COMMAND ----------

sales_by_stores_custs = retail_df.groupBy( "Store" ).agg( { "Sales" :"sum", "Customers": "sum" } )

# COMMAND ----------

sales_by_stores_custs.show()

# COMMAND ----------

sales_by_stores_custs.sort( "sum(Sales)", ascending = False ).show( 10 )

# COMMAND ----------

from pyspark.sql.functions import col, round

# COMMAND ----------

sales_by_stores_custs = sales_by_stores_custs.withColumn(
  "avg_purchase_val",
  round( col( "sum(Sales)" ) / col( "sum(Customers)" ), 2) )

# COMMAND ----------

stores_df = spark.read.csv("/FileStore/tables/store.csv", header = True, inferSchema = True)

# COMMAND ----------

stores_df.show(5)

# COMMAND ----------

stores_df.printSchema()

# COMMAND ----------

store_by_types = stores_df.groupBy( "StoreType" ).count()

# COMMAND ----------

total = stores_df.count()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

store_by_types.withColumn( "percentage", round( col( "count" ) / lit( total ), 2) ).show()

# COMMAND ----------

#Write a function to calculate the total months passed since competition is opened
stores_df = stores_df.withColumn(
  "CompetitionOpenSinceMonth",
  stores_df["CompetitionOpenSinceMonth"].cast( 'float' ) )

stores_df = stores_df.withColumn(
  "CompetitionOpenSinceYear",
  stores_df["CompetitionOpenSinceYear"].cast( 'float' ) )

# COMMAND ----------

stores_df = stores_df.fillna( 0.0 )

# COMMAND ----------

stores_df.where( col( "CompetitionOpenSinceYear" ).isNull() ).show()

# COMMAND ----------

from dateutil.relativedelta import relativedelta
from datetime import datetime
import math

def diff_in_months( fromYear, fromMonth ):
  if (fromYear == 0.0) or (fromMonth == 0.0):
      return 0.0
  else:
      return ( 2015.0 - fromYear ) * 12.0 + ( 12.0 - fromMonth )



# COMMAND ----------

diff_in_months( 2012, 3 )

# COMMAND ----------

from pyspark.sql.functions import udf, array
from pyspark.sql.types import FloatType

# COMMAND ----------

comp_months_udf = udf( diff_in_months, FloatType() )

# COMMAND ----------

stores_df.printSchema()


# COMMAND ----------

stores_df = stores_df.withColumn( "comp_months",
                   comp_months_udf( stores_df.CompetitionOpenSinceYear,
                                stores_df.CompetitionOpenSinceMonth ) )

# COMMAND ----------

stores_df.show(2)

# COMMAND ----------

stores_limited_df = stores_df.select( 'Store',
                                    'StoreType',
                                    'Assortment',
                                    'CompetitionDistance',
                                    'comp_months')

# COMMAND ----------

stores_limited_df.show(10)

# COMMAND ----------


