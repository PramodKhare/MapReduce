#!/usr/bin/env bash

if [ $# != 2 ]
then
	echo "Usage: ./run-all.sh <input> <output>"
	echo "For example: ./run-all.sh input/purchases4.txt output"
fi

# Compile & build the source code into jar  
ant

# Run the jar with given inputs
# java -Xms2500m -Xmx2500m -XX:ParallelGCThreads=6 -jar MedianPurchaseV1.jar $1 $2
java -jar MedianPurchaseV1.jar $1 $2