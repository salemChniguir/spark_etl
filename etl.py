"""
Created on Thu Oct 10 14:48:07 2019

@author: salemchniguir
@mail: salem.chniguir@etudiant-enit.utm.tn
@phone: +216 24 270 231
"""

class Extract(ABC):
    """
    An abstract Class

    Methods
    ----------
    extract():
        Abstract Method to extract Data to be implimented in children classes
    """

    @abstractmethod
    def extract(self):
        raise NotImplementedError("Subclass must implement abstract method")



class ExtractFromOracle(Extract):
    """
    A Child Class of Extract Class used to extract Data from DBMS Oracle

    Methods
    ----------
    extract():
         extracts from DBMS Oracle
    """
    def __init__(self,sqlContext,url,query_or_table,user,password):
        """
        Parameters
        ------------
        sqlContext: sqlContext
            the sqlContext of the Spark Application
        url: str
            the url to access to the DB in Oracle DBMS
        query_or_table: str
            the name of the table or the query
        user: str
            the user of the DB
        password: str
            the password to access to DB
        """

        self.url=url
        self.query_or_table=query_or_table
        self.user=user
        self.password=password
        self.sqlContext=sqlContext

    def extract(self):
        """
        gives as result data extracted from Oracle

        """
        result =self.sqlContext.read.format("jdbc").option("url",self.url)\
        .option("dbtable",self.query_or_table)\
        .option("user",self.user)\
        .option("password",self.password)\
        .load()
        return result


class ExtractFromFile(Extract):
    """
    A Child Class of Extract Class used to extract Data from csv File

    Methods
    ----------
    extract():
         extracts from csv File
    """

    def __init__(self,sch,sqlContext):
        """
        Parameters
        ------------
        sqlContext: sqlContext
            the sqlContext of the Spark Application
        sch: array
            the array contains the schema of the data in the file
        """
        self.sqlContext=sqlContext
        self.schema=s


    def extract(self):
        """
        gives as result data extracted from a csv file

        """
        result=self.sqlContext.read.load(self.schema,
        format="csv", sep=",", inferSchema="true", header="true")
        return result


class ExtractFromJson(Extract):
    """
    A Child Class of Extract Class used to extract Data from Json File

    Methods
    ----------
    extract():
         extracts from json File
    """

    def __init__(self,link):
        """
        Parameters
        ------------
        link: str
            the link of the file
        """
        self.link=link


    def extract(self):
        """
        gives as result data extracted from a json file

        """
        result=self.spark.read.load(self.link, format="json")
        return result



class Transform:
    """
    A Class used to transform data

    Methods
    ----------
    CastColumns():
        extracts from json File
    ReplaceAllNan():
        replace all Nan values in the dataFrame with a defined value
    ReplaceAllNanDict():
        take a dict of column:value to replace all Nan values for each column in the dataFrame

    """

    def __init__(self,sqlc,sc):
        """
        Parameters
        ------------
        sqlc: SQLContext
            the SQLContext of the Application
        sc: sparkContext
            the sparkContext of the Application
        """
        self.sqlContext=sqlc
        self.sparkContext=sc


    def CastColumns (self,df,listColumns):
        """
        Casts each column in listColumns to the choosen value
        It takes a list of tuples (x,y) where x is the name of the column
        and y the type of the cast

        Parameters
        ------------
        df: dataFrame
            the dataFrame to be casted
        listColumns: list
            it contains the name of columns to be casted
        """
        for c in listColumns:
            df=df.withColumn(c[0],df[c[0]].cast(c[1]))
        return df



    def ReplaceAllNan(self,df,d=0):
        """
        replaces all the NaN values with a chossen value (default 0)

        Parameters
        ------------
        df: dataFrame
            the dataFrame
        d: int
            the value that will replace all the NaN values
        """
        df=df.fillna(d)
        return df



    def ReplaceAllNanDict(self,df,dictio):
        """
        take a dict of (column,value) to replace all Nan values for each column in the dataFrame

        Parameters
        ------------
        df: dataFrame
            the dataFrame to be manipulated
        dictio: dict
            a dict contains (column,value) tuples
        """
        df=df.fillna(dictio)
        return df



class TransformCDR(Transform):
    """
    A Child Class of Transform Class used to transform the dataFrame to a cdrTable

    Methods
    ----------
    setTableCDR(df,month):
        transform the dataFrame df to a cdr(call detail record)
        dataFrame of the choosen month
    """


    def setTableCDR(self,df,month):
        """
        transform the dataFrame df to a dataFrame cdr(call datail record)
        of the choosen month and return df_cdr

        Parameters
        ------------
        df: dataFrame
            the dataFrame to be transformed
        dictio: str
            the choosen month of the cdr dataFrame
        """
        cdr= self.sqlContext.createDataFrame(self.sparkContext.emptyRDD(),df.schema)
        cdr=cdr.filter(cdr.DURATION>10)
        df1=df.select("FROM_SUBSCRIBER_ID","TO_SUBSCRIBER_ID").where(df.CALL_DATE==month)
        df2=df.select("TO_SUBSCRIBER_ID","FROM_SUBSCRIBER_ID").where(df.CALL_DATE==month)
        df3=df1.intersect(df2)
        df3.registerTempTable("cdrSelected")
        df.where(df.CALL_DATE==month).registerTempTable("cdr")
        df_cdr=self.sqlContext.sql("select CALL_DATE,cdr.FROM_SUBSCRIBER_ID,cdr.TO_SUBSCRIBER_ID,cdr.A_NUMBER_NETWORK,cdr.B_NUMBER_NETWORK,CALLS,SMS,DURATION,CALLING_DAYS \
        from cdr,cdrSelected where \
        (cdr.FROM_SUBSCRIBER_ID=cdrSelected.FROM_SUBSCRIBER_ID and cdr.TO_SUBSCRIBER_ID=cdrSelected.TO_SUBSCRIBER_ID)")
        return df_cdr

class TransformComportment(Transform):
    """
    A Child Class of Transform Class used to transform the dataFrame to a Comportment of the users

    Methods
    ----------
    setTableComportment(df1,df2,df3,df4,df5):
        transform the dataFrames df1,df2,df3,df4 and df5 to a dataFrame that
        contains  the Comportment of the users
    """
    def setTableComportment(self,df1,df2,df3,df4,df5):
        """
        transform the dataFrames df1,df2,df3,df4 and df5 to a dataFrame that
        contains  the Comportment of the users and returns result

        Parameters
        ------------

        df1: dataFrame
            the dataFrame to be transformed
        df2: dataFrame
            the dataFrame to be transformed
        df3: dataFrame
            the dataFrame to be transformed
        df4: dataFrame
            the dataFrame to be transformed
        df5: dataFrame
            the dataFrame to be transformed
        """
        d1=df1.select('CODE_CONTRAT','MONTH_DT','NB_APPEL','DUREE_APPEL','NB_APPEL_TT_GSM','DUREE_APPEL_TT_GSM','DUREE_APPEL_TT_FIXE','NB_APPEL_TT_FIXE',)
        d1 =d1.selectExpr("CODE_CONTRAT as CODE_CONTRAT", "NB_APPEL as NB_APPEL_out","DUREE_APPEL as DUREE_APPEL_out","NB_APPEL_TT_GSM as NB_APPEL_TT_GSM_out","DUREE_APPEL_TT_GSM as DUREE_APPEL_TT_GSM_out","DUREE_APPEL_TT_FIXE as DUREE_APPEL_TT_FIXE_out","NB_APPEL_TT_FIXE as NB_APPEL_TT_FIXE_out")
        d2=df2.select('CODE_CONTRAT','NB_APPEL','DUREE_APPEL','NB_APPEL_TT_GSM','DUREE_APPEL_TT_GSM','DUREE_APPEL_TT_FIXE','NB_APPEL_TT_FIXE',)
        d2=d2.selectExpr("CODE_CONTRAT as CODE_CONTRAT", "NB_APPEL as NB_APPEL_in","DUREE_APPEL as DUREE_APPEL_in","NB_APPEL_TT_GSM as NB_APPEL_TT_GSM_in","DUREE_APPEL_TT_GSM as DUREE_APPEL_TT_GSM_in","DUREE_APPEL_TT_FIXE as DUREE_APPEL_TT_FIXE_in","NB_APPEL_TT_FIXE as NB_APPEL_TT_FIXE_in")
        d3=df3.select('CODE_CONTRAT','ID_OFFRE','FLAG_3G','FLAG_4G','NB_CHANGEMENT_OFFRE','LAST_DATE_CHANGEMENT_OFFRE')
        d4=df4.select('CODE_CONTRAT','NB_JR_ACTIVITE_DATA','VOLUME_SESSION')
        d5=df5.select('CODE_CONTRAT','LAST_EVENT_DATE','DERNIERE_DATE_VOIX_SORTANT','DERNIERE_DATE_SMS_SORTANT','DERNIERE_DATE_DATA')

        result = d1.join(d2, on="CODE_CONTRAT").join(d3, on="CODE_CONTRAT").join(d4, on="CODE_CONTRAT").join(d5, on="CODE_CONTRAT")
        return result


class Load():
    """
    A Class used to load the dataFrames to the oracle DBMS

    Methods
    ----------
    loadDataFrame(df,tableName,user,password,mode="Append"):
        load the dataFrame to a table with a choosen tableName
    """
    def loadDataFrame(self,df,tableName,user,password,mode="Append"):
        """
        load the dataFrame df to a table with a choosen tableName
        Parameters
        ------------
        df: dataFrame
            the dataFrame that will be loaded to the Oracle DB
        tableName: str
            the name of the table where you will load the dataFrame
        user: str
            the name of the user of the Oracle DBMS
        password: str
            the password of the user account in the Oracle DBMS
        mode: str
            the mode of loading (default: Append)
        """
        df.write.mode(mode).format("jdbc")\
         .option("url","jdbc:oracle:thin:@localhost:1521:xe")\
         .option("dbtable", tableName)\
         .option("user", user)\
         .option("password", password)\
         .option("truncate", "true")\
         .save()


class ETL():
    """
    A Class used to do the ETL process (extract, transform, load)

    Methods
    ----------
    job(month):
        the job to be executed to do the etl process of the choosen month
    """

    def __init__(self,sqlc,sc,user,password):
        """
        Parameters
        ------------
        sqlc: SQLContext
            the SQLContext of the Application
        sc: sparkContext
            the sparkContext of the Application
        user: str
            the name of the user
        password: str
            the password of the user
        """
        self.sqlContext=sqlc
        self.sparkContext=sc
        self.DataFrames=[]
        self.user=user
        self.password=password
        self.Extracters=[]
        self.Extracters.append(ExtractFromOracle(self.sqlContext,"jdbc:oracle:thin:@localhost:1521:xe","dw_cla_monthly_trafic_msc",self.user,self.password))

        self.TransformCDR=TransformCDR(self.sqlContext,self.sparkContext)
        self.loder=Load()

    def job(self,month):
        """
        the job to be executed to do the etl process of the choosen month

        ------------
        month: str
            the month when the etl process will be executed
        """
        for e in self.Extracters:
            self.DataFrames.append(e.extract())
        for i in range(len(self.DataFrames)):
            self.DataFrames[i].cache()
        castColumns=[('CALLS','int'),('DURATION','int'),('SMS','int'),('CALLING_DAYS','int')]
        self.DataFrames[0]=self.TransformCDR.CastColumns(self.DataFrames[0],castColumns)
        cdrMonth=self.TransformCDR.setTableCDR(self.DataFrames[0],month)
        self.loder.loadDataFrame(cdrMonth,"CDR_FRIENDS",self.user,self.password,"Append")



from pyspark import SparkContext, SparkConf
from pyspark import SQLContext
from abc import ABC, abstractmethod


def main():
    spark_config = SparkConf().setMaster("local[4]")\
                .setAppName('etl')\
                .set("spark.ui.port", "4080")\
                .set("spark.sql.crossJoin.enabled", "true")\
                .set("spark.sql.shuffle.partitions","2")\
                .set("spark.cores.max",4)\
                .set("spark.executor.memory", "4g")\
                .set("spark.driver.memory","2g")\
                .set("spark.executor.cores", 4)\
                .set("spark.task.cpus",4)\
                .set("spark.shuffle.service.enabled","True")\
                .set("spark.dynamicAllocation.enabled","True")\
                .set("spark.dynamicAllocation.initialExecutors",4)\
                .set("spark.dynamicAllocation.minExecutors",3)\
                .set("spark.dynamicAllocation.maxExecutors",8)
    sc = SparkContext(conf=spark_config)

    sqlContext = SQLContext(sc)


    etl=ETL(sqlContext,sc,"user_name","your_password")
    etl.job("31MAY2019:00:00:00")
    sc.stop()
    print("end")



if __name__ == '__main__':
    main()
