#!/usr/bin/env bash

if [ $# != 2 ]
then
	echo "Usage: ./run-program-only.sh <input> <output>"
	echo "For example: ./run-program-only.sh input/purchases4.txt output"
fi

# Run the jar with given inputs on hadoop
hadoop jar MedianPurchaseV2.jar $1 $2