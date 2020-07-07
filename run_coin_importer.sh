#! /bin/bash

python_path="/Users/andy/.finance_bot/gekko/history/python"
latest_path="/Users/andy/.finance_bot/gekko/history/latest"
history_path="/Users/andy/.finance_bot/gekko/history"

exchange_db_file="binance_0.1.db"
start_date="2020-01-01"

COUNTER=1
RERUN=2
FILLRUN=10

while true; 
do
    cd $python_path
	cmd="exchange_coindata_importer.py"
	res=$(ps aux | grep -v grep | grep $cmd)

	if [ -z "$res" ] 
	then
		echo "Running the importer program again"
		time python3 $cmd #> log_coindata_importer.txt
		COUNTER=$[$COUNTER + 1]
        echo $COUNTER
	else
		echo "Already running"
	fi
 
    if [[ $(( $COUNTER % $RERUN )) -eq 0 ]]
    then
       echo  $COUNTER

       if [[ $(( $COUNTER % $FILLRUN )) -eq 0 ]]
       then
		  echo ""
          #echo "Filling db gaps"
          #time python3 $python_path/fill_db_gaps.py $latest_path/$exchange_db_file $start_date > log_fill_db_gaps.txt 
       fi

       #python3 history/python/update_binance_market.py > exchange/wrappers/binance-markets.json


       # copy to the history directory so that gekko have access to db
       #echo "Copying database to gekko history directory"
	   echo ""
       #time cp $latest_path/$exchange_db_file $history_path 
    fi

	sleep 15

	#sleep 1000
done
