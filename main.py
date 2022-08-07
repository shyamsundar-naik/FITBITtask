import logging
import pandas as pd
import json
import Utils.dbrelated
import csvkit
logging.basicConfig(filename="Tasks.log",level=logging.DEBUG ,format='%(levelname)s %(asctime)s %(name)s  %(message)s')

logging.info('Task1'.center(100,'*'))
logging.info("1. Read this dataset in pandas , mysql and mongodb ")

try:
    df = pd.read_csv('FitBitdata.csv')
    attr_json = df.to_json()
    attr_json = "[" + attr_json + "]"
    f = json.loads(attr_json)
    mgw = Utils.dbrelated.mongoWorks()
    mgw.addValues(f)
    logging.info("We have successfully read the dataset in pandas")
except Exception as e:
    logging.error(e)

logging.info('Task2'.center(100, '*'))
logging.info("2.  while creting a table in mysql dont use manual approach to create it  ,always use a automation to create a table in mysql ## hint - use csvkit library to automate this task and to load a data in bulk in you mysql ")

try:
    mysqlwork = Utils.dbrelated.mysqlWorks()
    mysqlwork.executeQuery('create database sqltaskworks')
    mysqlwork.executeQuery('use sqltaskworks')

    #csvsql --db "mysql://root:shyam@localhost:3306/sqltaskworks" --tables tableName --insert 'FitBotdata.csv'

    logging.info("We have successfully read the dataset in pandas")
except Exception as e:
    logging.error(e)



logging.info('Task3'.center(100,'*'))
logging.info("3. convert all the dates avaible in dataset to timestamp format in pandas and in sql you to convert it in date format")

try:

    df['ActivityDate'] = pd.to_datetime(df['ActivityDate'], infer_datetime_format=True)
    logging.info("We have successfully dates available in dataset to timestamp")
    logging.info(df.info())
except Exception as e:
    logging.error(e)


logging.info('Task4'.center(100,'*'))
logging.info("4.  Find out in this data that how many unique id's we have")

try:
    logging.info("please find the unique ids")
    logging.info(df['Id'].unique())
except Exception as e:
    logging.error(e)


logging.info('Task5'.center(100,'*'))
logging.info("5.  which id is one of the active id that you have in whole dataset")

try:
    logging.info("please find the ids")
    logging.info(list(df[(df['VeryActiveDistance'] != 0) & (df['LightActiveDistance'] != 0) &  (df['VeryActiveMinutes'] != 0) & (df['ModeratelyActiveDistance'] !=0) & (df['FairlyActiveMinutes'] != 0) & (df['LightlyActiveMinutes']!=0) & (df['SedentaryActiveDistance'] != 0)]['Id'].unique()))
except Exception as e:
    logging.error(e)

logging.info('Task6'.center(100,'*'))
logging.info("6.  how many of them have not logged there activity find out in terms of number of ids")

try:
    logging.info("please find the ids")
    logging.info(len(list(df[df['LoggedActivitiesDistance'] !=0]['Id'].unique())))
except Exception as e:
    logging.error(e)


logging.info('Task7'.center(100,'*'))
logging.info("7.  Find out who is the laziest person id that we have in dataset")

try:
    logging.info("please find the id")
    df1 = df[['Id', 'TotalSteps']].groupby('Id').sum().reset_index()
    logging.info(df1.sort_values(by=['TotalSteps'])['Id'][0])
except Exception as e:
    logging.error(e)

logging.info('Task8'.center(100,'*'))
logging.info("8.  Explore over an internet that how much calories burn is required for a healthy person and find out how many healthy person we have in our dataset")
logging.info("When explored on internet we can see that 2000 calories per day required to burn")
try:
    logging.info("please find the ids")
    df2 = df[['Id','ActivityDate','Calories']].groupby(['Id']).aggregate({'ActivityDate':'count','Calories':'sum'}).reset_index()
    df2['Calories Need to burn'] = df2['ActivityDate'].apply(lambda x: x * 2000)
    logging.info(list(df2[df2['Calories'] >= df2['Calories Need to burn']]['Id']))
except Exception as e:
    logging.error(e)

logging.info('Task9'.center(100,'*'))
logging.info("9.  how many person are not a regular person with respect to activity try to find out those")

try:
    logging.info("please find the ids")
    logging.info(df2[df2['ActivityDate'] != df2['ActivityDate'].max()]['Id'])
except Exception as e:
    logging.error(e)


logging.info('Task10'.center(100,'*'))
logging.info("10.  who is the thired most active person in this dataset find out those in pandas and in sql both .")

try:
    logging.info("please find the id")
    df3 = df[['Id', 'ActivityDate', 'TotalSteps', 'TotalDistance', 'Calories']].groupby(['Id']).aggregate({'ActivityDate': 'count', 'TotalSteps': 'sum', 'TotalDistance': 'sum', 'Calories': 'sum'}).reset_index()
    logging.info(df3.sort_values(by=['ActivityDate', 'TotalSteps', 'TotalDistance', 'Calories'])['Id'][df3.shape[0] - 1])
except Exception as e:
    logging.error(e)


logging.info('Task11'.center(100,'*'))
logging.info("11.  who is the 5th most laziest person avilable in dataset find it out")

try:
    logging.info("please find the id")
    logging.info(df3.sort_values(by=['ActivityDate', 'TotalSteps', 'TotalDistance', 'Calories'])['Id'][4])
except Exception as e:
    logging.error(e)


logging.info('Task12'.center(100,'*'))
logging.info("12. what is a totla acumulative calories burn for a person find out")

try:
    logging.info("please find the id")
    logging.info(df3[['Id','Calories']])
except Exception as e:
    logging.error(e)
