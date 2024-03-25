#!/bin/sh
progPath='/home/spanchanapu/surya/STOCKPRICE_MIGRATE'
pidfile=$progPath/load-pid.txt
date=`date +"%Y%M%D"`
dataPath='/home/spanchanapu/surya/STOCKPRICE_MIGRATE/USA_StockPrices'
cd $dataPath;echo "`pwd`"
ls  -d */ | awk -F ' ' '{printf "%s\n",$1}' > $progPath/directories.txt
if [ ! -f $pidfile ]; then
        touch $pidfile
        echo "Loading Started : `date`"
        while read -r directory; do
                cd $dataPath/${directory}
                ls *.csv > $dataPath/files.txt
                while IFS= read -r file; do
                        if [ -f $file ];then
                                #`head n 5 $file`

                                mysql --local-infile=1 -uroot -p'root' -hlocalhost -DSTOCK_PRICE_MIGRATION -ss -e "load data local infile '$file' into table STOCK_PRICES_CATEGORIES fields terminated by ',' optionally enclosed by '\"' lines terminated by '\n'(STOCKDATE,OPEN,HIGH,LOW,CLOSE,VOLUME);"
                                if [ $? -ne 0 ]; then
                                        rm $pidfile
                                        exit
                                fi
                        fi
                done<$dataPath/files.txt
        done<$progPath/directories.txt
rm $pidfile
else
        echo "Loading is still in progress.. Skipping the execution for now"
fi
spanchanapu@APT-SuryaP:~/surya/STOCKPRICE_MIGRATE$
