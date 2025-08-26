#! /usr/bin/bash

echo
read -p "Enter Number: " n
echo ----------Sum of N Numbers----------
echo
sum=0
for (( i=1; i<=n; i++ )); do
	sum=$(( sum+i ))
done
echo
echo "The sum of $n numbers is $sum"
echo
