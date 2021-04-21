import pandas as pd
from datetime import timedelta
from pyspark.sql import SparkSession
from pyspark import SparkContext 
from pyspark.sql.types import StructField, StructType, IntegerType, StringType,DoubleType,DecimalType,LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, col, mean
from  Elapsed_Time import count_elapsed_time


@count_elapsed_time
def sessions_python():
    sessions = open("Sessions.csv", "r")

    row_sessions= sessions.read().split('\n')

    list_sessions=[]
    for r in row_sessions:
        list_sessions.append(r.split(','))


    dict_sessions= dict( )
    for l in list_sessions:
        k= l[0]+'-'+l[1]
        v=l[2]
        dict_sessions[k]=v

    ordered_dict_sessions= {k:v for k,v in sorted(dict_sessions.items(), key=lambda item:item[0])}

    list_sessions_order = []
    for k,v in ordered_dict_sessions.items():
        list_sessions_order.append([k,v])
 
    durations=[]
    counter_sessions=0
    for i in range(0,len(list_sessions)-1):
        if ((list_sessions_order[i+1][0].split('-')[0] == list_sessions_order[i][0].split('-')[0]) & ((list_sessions_order[i+1][1]=='Close') & (list_sessions_order[i][1]=='Open'))):
            durations.append(int(list_sessions_order[i+1][0].split('-')[1]) - int(list_sessions_order[i][0].split('-')[1]))
            counter_sessions+=1
    
    print(list_sessions_order)        
    print(durations)
    print(f'The average time of all session is:', sum(durations)/counter_sessions)

@count_elapsed_time
def sessions_pandas():

    sessions= pd.read_csv('Sessions.csv', names=['User','Time','Status'])
    sessions.sort_values(by=['User','Time'], inplace=True)
    sessions['Time']= pd.to_numeric(sessions.Time, downcast='integer')
    sessions.loc[(sessions.User == sessions.User.shift(1)) & (sessions.Status=='Close') & (sessions.Status.shift(1)=='Open'),'duration'] = (sessions.Time - sessions.Time.shift(1))
    sessions.where(sessions.Status=='Close', inplace = True)
    sessions.dropna(thresh=2, inplace=True)
    print(sessions)
    print(f'The average time of all session is:',   )

@count_elapsed_time
def sessions_spark():

    spark = SparkSession.builder.appName('Sessions').master('local[*]').getOrCreate()
    custom_schema= StructType([StructField('User',StringType(),True), StructField('Time',IntegerType(),True),StructField('Status',StringType(),True)])
    sessions= spark.read.format('csv').schema(custom_schema).load('Sessions.csv')

    sessions= sessions.sort('User','Time')

    sessions_diff = sessions.withColumn("Session", lag('Time').over(Window.partitionBy('User').orderBy('Time'))).where(sessions.Status=='Close')
    sessions_diff= sessions_diff.withColumn("Duration", col("Time").cast(LongType()) - col('Session').cast(LongType())) .filter(sessions_diff.Status=='Close')
    sessions_diff.select('User','Time','Duration').show()
    print(f'The average time of all session is:')
    sessions_diff.select(mean(col("Duration"))).show()


if __name__ == "__main__":
    sessions_python()
    sessions_pandas()
    sessions_spark()
