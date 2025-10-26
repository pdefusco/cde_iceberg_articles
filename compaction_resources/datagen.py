#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

import os, sys
import numpy as np
import pandas as pd
from datetime import datetime
import dbldatagen as dg
import dbldatagen.distributions as dist
from dbldatagen import DataGenerator
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                              DoubleType, BooleanType, ShortType, \
                              TimestampType, DateType, DecimalType, \
                              ByteType, BinaryType, ArrayType, MapType, \
                              StructType, StructField


class DataGen:

    '''Class to Generate Banking Data'''

    def __init__(self, spark):
        self.spark = spark

    def dataGen(self, shuffle_partitions_requested, partitions_requested, data_rows):

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        dataSpec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("unique_id", "int", random=True)
                    .withColumn("code", "string", values=["a", "b", "c", "d", "f", "g", "h", "e", "i", "f", "g", "h"], random=True)
                    )

        for i in range(1, 100):
            col_n = f"col{i}"
            dataSpec = dataSpec.withColumn(col_n, "float", minValue=1, maxValue=10000000, random=True)

        df = dataSpec.build()

        return df

## CDE PROPERTIES
shuffle_partitions_requested=int(sys.argv[1])
partitions_requested=int(sys.argv[2])
data_rows=int(sys.argv[3])
db_name=sys.argv[4]
src_tbl=sys.argv[5]

print("Shuffle partitions requested: ", shuffle_partitions_requested)
print("Partitions requested: ", partitions_requested)
print("Data rows: ", data_rows)
print("DB Name: ", db_name)
print("SRC Table: ", src_tbl)

#---------------------------------------------------
#               CREATE SPARK SESSION WITH ICEBERG
#---------------------------------------------------

spark = SparkSession \
    .builder \
    .appName("DATAGEN LOAD") \
    .getOrCreate()

myDG = DataGen(spark)

df = myDG.dataGen(shuffle_partitions_requested, partitions_requested, data_rows)

spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(db_name))
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))

df.write.mode("overwrite").saveAsTable("{0}.{1}".format(db_name, src_tbl))

#df=df.select("*").orderBy(F.rand())

#df.write.mode("overwrite").saveAsTable("{0}.{1}_random".format(db_name, src_tbl))

spark.sql("DESCRIBE EXTENDED {}.{}".format(db_name, src_tbl)).show(n=100)
#spark.sql("DESCRIBE EXTENDED {}.{}_random".format(db_name, src_tbl)).show(n=100)
