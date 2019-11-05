#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 10 14:48:07 2019

@author: salemchniguir
@mail: salem.chniguir@etudiant-enit.utm.tn
@phone: +216 24 270 231 
"""

from pyspark import SparkContext, SparkConf
from pyspark import SQLContext

from abc import ABC, abstractmethod

#classe abstraite
class Extract(ABC):
    
    @abstractmethod
    def extract(self):
        raise NotImplementedError("Subclass must implement abstract method") 
        
        
#classe qui herite de la classe Extract et qui permet d'extraire des données de SGBD Oracle        
class ExtractFromOracle(Extract):
   
    def __init__(self,sqlContext,url,query_or_table,user,password):
        
        self.url=url
        self.query_or_table=query_or_table
        self.user=user
        self.password=password
        self.sqlContext=sqlContext
    
        
    def extract(self):
        result =self.sqlContext.read.format("jdbc").option("url",self.url).option("dbtable",self.query_or_table).option("user",self.user).option("password",self.password).load()
        return result
    
#classe qui herite de la classe Extract et qui permet d'extraire des données d'un fichier CSV    
class ExtractFromFile(Extract):
    def __init__(self,s,sqlContext):
        self.sqlContext=sqlContext
        self.schema=s
        
       
    def extract(self):
        result=self.sqlContext.read.load(self.schema,
        format="csv", sep=",", inferSchema="true", header="true")
        return result
    
#classe qui herite de la classe Extract et qui permet d'extraire des données d'un fichier Json   
class ExtractFromJson(Extract):
    def __init__(self,lien):
        self.lien=lien
       
    def extract(self):
        result=self.spark.read.load(self.lien, format="json")
        return result


#classe qui cotient quelques méthodes pour nettoyer les données  
class Transform:
  
    
    def __init__(self,sqlContext,sparkContext):
        self.sqlContext=sqlContext
        self.sparkContext=sparkContext
    # méthode permettant de faire le cast des colonnes sélectionnées 
    def CastColumns (self,dataFrame,listColumns):
        for c in listColumns:
            dataFrame=dataFrame.withColumn(c[0],dataFrame[c[0]].cast(c[1]))
        return dataFrame
   
    # méthode permettant de remplacer les valeurs nulles par une constante  
    def RemplacerAllNan(self,df,d=0):
        df=df.fillna(d)
        return df
    # méthode permettant de remplacer les valeurs nulles pour chaque colonne par des autres valeurs
    # attribut dictionnaire: contient les couples (colonne, nouvellesValeur)
    def RemplacerAllNanDict(self,df,dictionnaire):
        df=df.fillna(dictionnaire)
        return df

# classe permettant d'avoir une table CDR
class TransformCDR(Transform):
    
    
    def setTableCDR(self,df,month):
        
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
        
        #df=df.drop("AGREGATION_DAYS")
        #cdrMonth=df.where(df.CALL_DATE==month)
        #cdrMonth=cdrMonth.filter((cdrMonth.DURATION>10) | (cdrMonth.SMS>5)  | (cdrMonth.CALLING_DAYS>5)  | (cdrMonth.CALLS>5) )
        #cdrMonth=cdrMonth.distinct()
        return df_cdr           
    
    
    
    
# classe permettant d'avoir une table qui carecterise le comportement de chaque client        
class TransformComportement(Transform):
    def setTableComportement(self,df1,df2,df3,df4,df5):
        
        d1=df1.select('CODE_CONTRAT','MONTH_DT','NB_APPEL','DUREE_APPEL','NB_APPEL_TT_GSM','DUREE_APPEL_TT_GSM','DUREE_APPEL_TT_FIXE','NB_APPEL_TT_FIXE',)
        d1 =d1.selectExpr("CODE_CONTRAT as CODE_CONTRAT", "NB_APPEL as NB_APPEL_out","DUREE_APPEL as DUREE_APPEL_out","NB_APPEL_TT_GSM as NB_APPEL_TT_GSM_out","DUREE_APPEL_TT_GSM as DUREE_APPEL_TT_GSM_out","DUREE_APPEL_TT_FIXE as DUREE_APPEL_TT_FIXE_out","NB_APPEL_TT_FIXE as NB_APPEL_TT_FIXE_out")
        d2=df2.select('CODE_CONTRAT','NB_APPEL','DUREE_APPEL','NB_APPEL_TT_GSM','DUREE_APPEL_TT_GSM','DUREE_APPEL_TT_FIXE','NB_APPEL_TT_FIXE',)
        d2=d2.selectExpr("CODE_CONTRAT as CODE_CONTRAT", "NB_APPEL as NB_APPEL_in","DUREE_APPEL as DUREE_APPEL_in","NB_APPEL_TT_GSM as NB_APPEL_TT_GSM_in","DUREE_APPEL_TT_GSM as DUREE_APPEL_TT_GSM_in","DUREE_APPEL_TT_FIXE as DUREE_APPEL_TT_FIXE_in","NB_APPEL_TT_FIXE as NB_APPEL_TT_FIXE_in")
        d3=df3.select('CODE_CONTRAT','ID_OFFRE','FLAG_3G','FLAG_4G','NB_CHANGEMENT_OFFRE','LAST_DATE_CHANGEMENT_OFFRE')
        d4=df4.select('CODE_CONTRAT','NB_JR_ACTIVITE_DATA','VOLUME_SESSION')
        d5=df5.select('CODE_CONTRAT','LAST_EVENT_DATE','DERNIERE_DATE_VOIX_SORTANT','DERNIERE_DATE_SMS_SORTANT','DERNIERE_DATE_DATA')
        
        result = d1.join(d2, on="CODE_CONTRAT").join(d3, on="CODE_CONTRAT").join(d4, on="CODE_CONTRAT").join(d5, on="CODE_CONTRAT")
        return result
# classe permettant d'enregistrer les données dans l'SGBD Oracle
class Load():
    
    def loadDataFrame(self,dataFrame,tableName,user,password,mode="Append"):
        dataFrame.write.mode(mode).format("jdbc")\
         .option("url","jdbc:oracle:thin:@localhost:1521:xe")\
         .option("dbtable", tableName)\
         .option("user", user)\
         .option("password", password)\
         .option("truncate", "true")\
         .save()
         
         
# classe permettant de faire l'ETL
class ETL():
    def __init__(self,sqlContext,sparkContext,user,password):
        self.sqlContext=sqlContext
        self.sparkContext=sparkContext
        self.DataFrames=[]
        self.user=user
        self.password=password
        self.Extracters=[]
        self.Extracters.append(ExtractFromOracle(self.sqlContext,"jdbc:oracle:thin:@localhost:1521:xe","dw_cla_monthly_trafic_msc",self.user,self.password))
        #e1=ExtractFromOracle(self.sqlContext,"jdbc:oracle:thin:@localhost:1521:xe","FACT_USAGE_MONTHLY_SORTANT_B","telecom","97908631")
        #e2=ExtractFromOracle(self.sqlContext,"jdbc:oracle:thin:@localhost:1521:xe","FACT_USAGE_MONTHLY_ENTRANT_B","telecom","97908631")
        #e3=ExtractFromOracle(self.sqlContext,"jdbc:oracle:thin:@localhost:1521:xe","DIM_CONTRACT_D","telecom","97908631")
        #e4=ExtractFromOracle(self.sqlContext,"jdbc:oracle:thin:@localhost:1521:xe","FACT_USAGE_MOUNTHLY_DATA_B","telecom","97908631")
        #e5=ExtractFromOracle(self.sqlContext,"jdbc:oracle:thin:@localhost:1521:xe","DIM_PARC_RGS_D","telecom","97908631")
        
        self.TransformCDR=TransformCDR(self.sqlContext,self.sparkContext)
        #self.TransformComportement=TransformComportement(self.sqlContext,self.sparkContext)
        self.loder=Load()
    
    # job etl qui va ettre executé chaque mois 
    def job(self,month):
        for e in self.Extracters:
            self.DataFrames.append(e.extract())
        for i in range(len(self.DataFrames)):
            self.DataFrames[i].cache()
        castColumns=[('CALLS','int'),('DURATION','int'),('SMS','int'),('CALLING_DAYS','int')]
        self.DataFrames[0]=self.TransformCDR.CastColumns(self.DataFrames[0],castColumns)
        cdrMonth=self.TransformCDR.setTableCDR(self.DataFrames[0],month)
        #df2=self.TransformComportement.setTableComportement(self.DataFrames[1],self.DataFrames[2],self.DataFrames[3],self.DataFrames[4],self.DataFrames[5])
        self.loder.loadDataFrame(cdrMonth,"CDR_FRIENDS",self.user,self.password,"Append")
        #self.loder.loadDataFrame(df2,"comportement",self.user,self.password)
        
        
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
 
    
    etl=ETL(sqlContext,sc,"telecom","97908631")
    etl.job("31MAY2019:00:00:00")
    sc.stop()
    print("end")  
    
    
    
if __name__ == '__main__':
    main()