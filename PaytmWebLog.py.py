# This script has been created to solve the Paytm Weblog challenge using Python and Spark (PySpark).
# I understand that using Scala would probably have been a preferable option, but due to my unfamiliarity with Scala,
# I have opted to use PySpark instead.
# I'm also using a Distributed File System Architecture for the first time.
# Hence I'm including all the steps I've undertaken to come up with this solution,
# with as much research I could do about this on the internet in the given timeframe.
# I'd also appreciate your comments on the mistakes I might have made during this.

#####ASSUMPTIONS MADE:

## Every distinct client ip represents a distinct user

## Session time = 60 minutes (For faster runtimes)

## Interval has been defined with respect to the clock time, ex - 8PM-9PM. This has been done to maximize the number of requests falling in an interval.
## A serious shortcoming of this can be:- If two requests are made at 7:59PM and 8:01PM, they fall in different intervals.
## However, such cases should not occur in a large number of cases, hence to simplify the solution, the intervals have been created by the clock.

##### Installing Spark for Windows ######

## Downloaded Apache Spark, extracted the files and set the SPARK_HOME and HADOOP_HOME System Environment Variables.
## Installed findspark and pyspark modules for Python.
## Used the following commands to import and initialize a Spark Session.

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName('appName').setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark import SQLContext
sqlContext = SQLContext(sc)

## Reading the input file
# df = sc.textFile("Paytmsample.txt")
# df.count()
#
#
# ## mapping the column names of the RDD object
# newdf = df.map(lambda x: Row(timestamp=str(x[0]), elb=x[1], client_port=x[2], backend_port=x[3], req_processing_time=x[4],
#                               backend_processing_time=x[5], res_processing_time=x[6], elb_status_code=x[7], backend_status_code=x[8],
#                               received_bytes=x[9], sent_bytes=x[10], request=x[11], user_agent=x[12],
#                               ssl_cipher=x[13], ssl_protocol=x[14]))
#


## Reading the input file. The file has been converted to csv because there were a few issues in reading txt.

df = spark.read.csv("Paytmsample.csv",sep=' ',ignoreLeadingWhiteSpace=True)
df.count()


## This code is used to map an RDD object and convert it to a dataframe. Not required since we already have a dataframe now(Through read_csv function).
# newdf = df.map(lambda x: Row(timestamp=str(x[0]), elb=x[1], client_port=x[2], backend_port=x[3], req_processing_time=x[4],
#                               backend_processing_time=x[5], res_processing_time=x[6], elb_status_code=x[7], backend_status_code=x[8],
#                               received_bytes=x[9], sent_bytes=x[10], request=x[11], user_agent=x[12],
#                               ssl_cipher=x[13], ssl_protocol=x[14]))

## check dataframe schema
df.printSchema()
df.head()

## Use custom column names
df = df.selectExpr("_c0 as timestamp", "_c1 as elb", "_c2 as client_port", "_c3 as backend_port", "_c4 as req_processing_time",
                   "_c5 as backend_processing_time", "_c6 as res_processing_time", "_c7 as elb_status_code", "_c8 as backend_status_code",
                   "_c9 as received_bytes", "_c10 as sent_bytes", "_c11 as request", "_c12 as user_agent",
                   "_c13 as ssl_cipher", "_c14 as ssl_protocol")

from pyspark.sql.functions import substring, length, col, expr
from pyspark.sql.functions import regexp_replace, col


## removing whitespaces
df.select(regexp_replace(col("timestamp"), " ", ""))

## Removing extra 'Z' at the end of the timestamp column
df = df.withColumn("timestamp",expr("substring(timestamp, 1, length(timestamp)-1)"))

## grouping data with the client ip
grpdata = df.groupBy("client_port")
type(grpdata)
# grpdata.head(10)

## show the ips with maximum hits
df.groupBy("client_port").count().sort(col("count").desc()).show()

## Adding columns of segregated date and time
import pyspark.sql.functions as F
split_col = F.split(df['timestamp'], 'T')
df_date = df.withColumn('Date', split_col.getItem(0))
df_time = df_date.withColumn('Time', F.concat(split_col.getItem(1),F.lit(' ')))
df_time.show()
## Aggregating using a time window(1 hour)
## NOTE: FOR TESTING MY CODE I HAVE INITIALLY USED A TIME WINDOW OF 1 HOUR. THIS CAN BE CHANGED TO ANY NUMBER OF MINUTES.
## This piece of code returns the IP, Date and Time window of an hour

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

## Helper functions
def getIntervalUdf(time):
    start = int(time.split(":")[0])
    return str(start)+"-"+str(start+1)

getIntervalUdf = udf(getIntervalUdf,StringType())

df_time = df_time.withColumn("Interval",getIntervalUdf("Time"))
df_time.show()
# df_time = df_time.groupby("Date","Interval","client_port","request")
# df_time = df_time.groupby("Date","Interval","client_port","request")
# df_time.show(10000)


########### SOLUTION TO PART 1 #############
## This returns an aggregated dataframe with the client_port and all the activities, in the same time interval
df_time.orderBy("client_port","Date","Interval").show()


########### SOLUTION TO PART 2###############
## Session time = In every session interval, for every IP, difference between the maximum and minimum Time of request

from pyspark.sql.functions import col, max as max_
from pyspark.sql.functions import col, min as min_

## find maximum timestamp for every ip in every timestamp
dfmaxtime = df_time.withColumn("timestamp", col("timestamp").cast("timestamp")).groupBy("client_port", "Date","Interval").agg(max_("Time"))

## find minimum timestamp for every ip in every timestamp
dfmintime = df_time.withColumn("timestamp", col("timestamp").cast("timestamp")).groupBy("client_port", "Date","Interval").agg(min_("Time"))

## merge the two dataframes and calculate the difference between the timestamps
dftime = dfmaxtime.join(dfmintime, ['client_port','Date','Interval'])

## changing column names
dftime = dftime.select(col("client_port").alias("client_port"), col("Date").alias("Date"), col("Interval").alias("Interval"), col("max(Time)").alias("Time1"), col("min(Time)").alias("Time2"))

## Concatinating Date and Time so that can be parsed as a timestamp
dftime = dftime.withColumn('TimeCon1', F.concat(F.col('Date'),F.lit('T'), F.col('Time1')))
dftime = dftime.withColumn('TimeCon2', F.concat(F.col('Date'),F.lit('T'), F.col('Time2')))

## removing extra whitespace
dftime = dftime.withColumn("TimeCon1",expr("substring(TimeCon1, 1, length(TimeCon1)-1)"))
dftime = dftime.withColumn("TimeCon2",expr("substring(TimeCon2, 1, length(TimeCon2)-1)"))

## converting to timestamp
dftime = dftime.withColumn('TimeCon1', col('TimeCon1').cast('timestamp'))
dftime = dftime.withColumn('TimeCon2', col('TimeCon2').cast('timestamp'))

## calculating sessiontime for every interval for every clientip address
from pyspark.sql import functions as F
timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSSSS"
timeDiff = (F.unix_timestamp('TimeCon1', format=timeFmt)
            - F.unix_timestamp('TimeCon2', format=timeFmt))
dftime = dftime.withColumn("Duration", timeDiff)

## calculating average session time for every user
dftime.groupBy("client_port", "Interval").agg({"Duration":"avg"}).show()

## overall average session time
dftime.agg({"Duration":"avg"}).show()


######## SOLUTION TO PART 3 #############

## Prepare aggregated dataframe with Date and Interval
split_col = F.split(df['timestamp'], 'T')
df_date = df.withColumn('Date', split_col.getItem(0))
df_time = df_date.withColumn('Time', F.concat(split_col.getItem(1),F.lit(' ')))

df_time = df_time.withColumn("Interval",getIntervalUdf("Time"))
df_time.show()

## Grouping by every session and URL request
dfUnique = df_time.groupBy("Date", "Interval", "request").count()

## Number of unique URL visits per session
dfUniqueURLVisits = dfUnique.groupBy("Date", "Interval").count()



######## SOLUTION TO PART 4 #############

## To find out the IPs with longest session times, I'll use the dataframe from solution to Part2, where we calculated average session time per IP

dfLongestSessTimes = dftime.groupBy("client_port").agg({"Duration":"avg"})
dfLongestSessTimes.sort(col("avg(Duration)").desc()).show()



################## PREDICTION PROBLEMS #####################

######### Solution to Problem 1 ###########

## To predict number of requests, we can simply use average of a few last minutes (Since the data given is only for a day and using time
## series or any other predictive algorithm will be highly biased on a day's data)

df_date = df.withColumn('Date', split_col.getItem(0))
df_time = df_date.withColumn('Time', F.concat(split_col.getItem(1),F.lit(' ')))

df_time = df_time.sort(col("Time").desc())

## Find out the timestamp for the last request made
lastrequesttime  = df_time.select('timestamp').first()[0]


## Calculating time interval from the last request made
timeFmt = "yyyy-MM-dd'T'HH:mm:ss.SSSSS"
df_time = df_time.withColumn("TimeSinceLastRequest", F.lit(lastrequesttime))

df_time = df_time.withColumn('TimeSinceLastRequest', col('TimeSinceLastRequest').cast('timestamp'))
df_time = df_time.withColumn('timestamp', col('TimeSinceLastRequest').cast('timestamp'))

timeDiff = (F.unix_timestamp('TimeSinceLastRequest', format=timeFmt)
            - F.unix_timestamp('timestamp', format=timeFmt))

df_time = df_time.withColumn("RequestDiff", timeDiff)

## The number of requests made in last 30 minutes = 1800 seconds (can be any number of minutes)
LoadLast30Minutes = df_time.filter(df_time['RequestDiff'] > 1800).count()

## The number of requests in the next minute
LoadNextMinute = int(LoadLast30Minutes/30)


######### Solution to Problem 2 ###########

## With only a day's data, it is difficult to predict the behaviour of an ip. However with enough days' data,
## we can predict the behaviour by knowing in what time interval the request has been made.
## This problem can be addressed in two ways:
## 1) We can simply aggregate the data to know the user's behaviour in the particular time interval.
## 2) A linear regression model can be used to train a model with ip addresses, time intervals and session lengths.
## Although both the solutions are again highly biased due to the lack of some additional important features.

## Using dataframe from solution 2:

newdf = dftime.groupBy("client_port", "Interval").agg({"Duration":"avg"})
type(newdf)

## Solution using Linear regression:-
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ['client_port','Interval'], outputCol = 'Duration')
vdf = vectorAssembler.transform(newdf)
vdf = vdf.select(['features', 'Duration'])

## Dividing Dataframe into two parts
splits = newdf.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

## Building the model
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol='Duration', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

## Predictions
lr_predictions = lr_model.transform(test_df)


######### Solution to Problem 3 ###########
## This problem can be addressed similarly to how we predicted session length for an IP
## Here we will need to calculate unique URL visits instead so session duration, then a similar Linear Regression model can be used.

#### Aggregating data to Client_port, Interval and number of unique URL visits level.
df_date = df.withColumn('Date', split_col.getItem(0))
df_time = df_date.withColumn('Time', F.concat(split_col.getItem(1),F.lit(' ')))

df_time = df_time.withColumn("Interval",getIntervalUdf("Time"))
df_time.show()

## Grouping by every client_port and URL request
dfUnique = df_time.groupBy("client_port", "Interval", "request").count()
newdf = dfUnique.groupBy("client_port", "Interval").count()


## Solution using Linear regression:-
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ['client_port','Interval'], outputCol = 'count')
vdf = vectorAssembler.transform(newdf)
vdf = vdf.select(['features', 'count'])

## Dividing Dataframe into two parts
splits = newdf.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

## Building the model
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol = 'features', labelCol='count', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

## Predictions
lr_predictions = lr_model.transform(test_df)
