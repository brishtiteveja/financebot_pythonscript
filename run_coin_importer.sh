#! /bin/bash

while true; 
do
	cmd="exchange_coindata_importer.py"
	res=$(ps aux | grep -v grep | grep $cmd)

	if [ -z "$res" ] 
	then
		echo "Running the importer program again"
		python3 $cmd > log_coindata_importer.txt &
	else
		echo "Already running"
	fi

	sleep 15

	#sleep 1000
done
