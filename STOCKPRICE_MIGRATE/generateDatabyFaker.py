from faker import Faker
from datetime import datetime
import csv
import random
import os
class fakerdata:
    def __init__(self,recordCount):
        self.recordCount=recordCount
        self.currenttime=datetime.now().strftime("%Y%m%d%H%M%S")
        self.currentdate=datetime.now().strftime("%Y%m%d%H")
        self.filePath='/home/spanchanapu/surya/STOCKPRICE_MIGRATE/generate_data'
    def generateData(self):
        fake=Faker()
        os.chdir(self.filePath)
        try:
            os.mkdir(self.currentdate)
        except:
            print("Directory already Exists.")
            with open(f'{self.filePath}/{self.currentdate}/stock_market_data_{self.currenttime}.csv','w') as csvfile:
                field_names=["STOCKDATE","OPEN","HIGH","LOW","CLOSE","VOLUME"]
                writer=csv.DictWriter(csvfile,fieldnames=field_names)
                writer.writeheader()
                for data in range(self.recordCount):
                    writer.writerow(
                        {
                            "STOCKDATE":fake.date(),
                            "OPEN":round(random.uniform(0,50),2),
                            "HIGH":round(random.uniform(50,100),2),
                            "LOW":round(random.uniform(0,50),2),
                            "CLOSE":round(random.uniform(0,50),2),
                            "VOLUME":random.randint(100000,1000000)
                        }
                    )

if __name__=="__main__":
    RECORD_COUNT=10000
    obj=fakerdata(RECORD_COUNT)
    obj.generateData()
