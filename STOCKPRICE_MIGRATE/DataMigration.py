import DbConnection
import Config as cp
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import csv

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
snowflake_logger=logging.getLogger('snowflake.connector')
snowflake_logger.setLevel(logging.ERROR)


class datamigration:
    def __init__(self,numThreads):
        self.numThreads = numThreads
        self.mysqldb=DbConnection.establishconnectionpool("mysqlDb",cp.num_mysql_pools,"TRUE",cp.mysqlconfig)
    def getMinMaxId(self):
        logger.info("getMinMaxId() process started")
        with DbConnection.mysqlconnection(obj.mysqldb) as cur:
            #cur=con.cursor()
            try:
                minquery = f"select maxId from STOCK_TEMP_TABLE order by id desc limit 1"
                logger.info("minquery: %s",minquery)
                resultdata=cur.execute(minquery)
                result = resultdata.fetchall()
                print(result)
                minId=int(result[0][0])
                maxId = minId+ cp.outboundRange
                logger.info("maxId: %s",maxId)
                return minId,maxId
            except Exception as e:
                logger.error("getMinMaxId() : Unknown Exception occured while fetching min and maxid",e)
    def getstockdata(self,minid,maxid):
        logger.info("getstockdata() Process started")
        with DbConnection.mysqlconnection(obj.mysqldb) as cur:
            #cur = con.cursor()
            try:
                minid=minid+1
                query=f"select DATE_FORMAT(STOCKDATE,'%Y-%m-%d') as STOCKDATE,OPEN,HIGH,LOW,CLOSE,VOLUME from {cp.targetmysqlTable} where id between {minid} and {maxid} and STOCKDATE not like '0000-00-00%'"
                logger.info("query : %s",query)
                resultdata=cur.execute(query)
                result=resultdata.fetchall()
                with open(cp.files_path+'/'+f"stockdata_{minid}_{maxid}.csv","w+",newline='') as csvfile:
                    csvwrite=csv.writer(csvfile,delimiter=',',quoting=csv.QUOTE_NONE,escapechar='\\')
                    csvwrite.writerows(result)
                return result
            except Exception as e:
                logger.error("getstockdata() : Unknown Error occured while fetching data",e)

    def loadDataToSnowflake(self,data,thread,minId,maxId):
        logger.info("loadDataToSnowflake() process started")
        with DbConnection.singlesfconnection() as sfcon:
            sfcur=sfcon.cursor()
            print(data[:5])
            minId=minId+1
            try:
                stagequery=f"create or replace stage migrate_stage FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"' EMPTY_FIELD_AS_NULL=TRUE SKIP_HEADER=1)"
                logger.info("stagequery :  %s",stagequery)
                sfcur.execute(stagequery)

                putfilequery=f"put file://{cp.files_path}/stockdata_{minId}_{maxId}.csv @migrate_stage"
                logger.info("putfilequery : %s",putfilequery)
                sfcur.execute(putfilequery)

                copyquery=f"copy into {cp.targetsnowflakeTable} from @migrate_stage/stockdata_{minId}_{maxId}.csv.gz"
                logger.info("copyquery : %s",copyquery)
                sfcur.execute(copyquery)
                '''
                tempquery=f"create or replace table STOCK_DATA_TEMP_{thread}(STOCKDATE DATE,OPEN FLOAT,HIGH FLOAT,LOW FLOAT,CLOSE FLOAT,VOLUME BIGINT)"
                logger.info("tempquery : %s",tempquery)
                sfcur.execute(tempquery)

                loadquery=f"insert into STOCK_DATA_TEMP_{thread}(STOCKDATE,OPEN,HIGH,LOW,CLOSE,VOLUME) values ( ?,?,?,?,?,?)"
                logger.info("loadquery : %s",loadquery)
                sfcur.executemany(loadquery,data)

                targetquery=f"insert into {cp.targetsnowflakeTable} select STOCKDATE,OPEN,HIGH,LOW,CLOSE,VOLUME from STOCK_DATA_TEMP_{thread}"
                logger.info("targetquery : %s",targetquery)
                sfcur.execute(targetquery)'''

                try:
                    with DbConnection.mysqlconnection(obj.mysqldb) as cur:
                        tempinsertquery = f"insert into STOCK_TEMP_TABLE(minid,maxid) values (%s,%s)"
                        logger.info("tempinsertquery : %s", tempinsertquery)
                        cur.execute(tempinsertquery, minId, maxId)
                except Exception as e:
                    logger.error("Unknown exception occured while updating min and maxid : ",e)
            except Exception as e:
                logger.error("loadDataToSnowflake() : Unknown Exception occured while moving data into Snowflake",e)

    def ThreadProcess(self):
        logger.debug("ThreadProcess(): Thread Process initialized")
        with ThreadPoolExecutor(max_workers=self.numThreads) as executor:
            try:
                for thread in range(self.numThreads):
                    minid,maxid=self.getMinMaxId()
                    data=self.getstockdata(minid,maxid)
                    #print(data)
                    if data is not None:
                        try:
                            executor.submit(self.loadDataToSnowflake,data,thread,minid,maxid)
                        except Exception as e:
                            logger.error("Error Occured ",e)
                    else:
                        logger.info("ThreadProcess(): No data found from the list")
                        time.sleep(5)
                    time.sleep(5)
            except Exception as e:
                logger.error("ThreadProcess() : Error Occured while migrating data to snowflake",e)



if __name__=='__main__':
    obj=datamigration(cp.numThreads)
    try:
        obj.ThreadProcess()
    except Exception as e:
        logger.error("Unknown error Occured: ",e)
