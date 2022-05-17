#!/bin/bash

echo " "
echo "Creating solution folder"
hadoop fs -mkdir project_files


echo " "
echo "Starting MapReduce task"
# TODO: proszę dostosować poniższe polecenie tak, aby uruchamiało ono zadanie MapReduce (2)
#przykład dla MapReduce Classic
hadoop jar SumActors.jar SumActors input/datasource1 output_mr


echo " "
echo "Starting Pig task"
pig -f genres.pig

echo " "
echo "Downloading results to local file system"
mkdir -p ./output
hadoop fs -copyToLocal output/* ./output

echo " "
echo " "
echo " "
echo " "
echo "Results:"
cat ./output/*