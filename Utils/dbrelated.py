import pymongo
import mysql.connector as conn

class mongoWorks:
    def __init__(self):
        self.__con = pymongo.MongoClient("mongodb+srv://shyam:shyam@cluster0.xzmbj.mongodb.net/?retryWrites=true&w=majority")
        self.__database = self.__con["mongoworks"]
        self._collections = self.__database['FitBit']

    def addValues(self,json):
        self._collections.insert_many(json)

class mysqlWorks:
    """Any mysql related works are accomplished here"""
    def __init__(self):
        self.__mydb = conn.connect(host="localhost", user="root", passwd="shyam")


    def executeQuery(self,query):
        cursor = self.__mydb.cursor()
        if self.__mydb.is_connected():
            cursor.execute(query)
        else:
            self.__mydb.cmd_reset_connection()
            cursor = self.__mydb.cursor()
            cursor.execute(query)


    def commit(self):
        self.__mydb.commit()

    def selExecuteQuery(self,query):
        cursor = self.__mydb.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def getColumnNames(self,databasename, tablename):
        query = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='"+ databasename +"' AND `TABLE_NAME`='"+tablename+"';"
        list1 = list(self.selExecuteQuery(query))
        return list1


