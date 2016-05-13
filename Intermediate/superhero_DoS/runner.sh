#!/bin/sh

echo "resultDir=""'"${PWD}"'" > ./src/result_dir.py

python ./src/data_formatter.py $1

for i in {0..9999};
  do 
    _file="./report/result.txt"
    if [ -s "$_file" ]
      then
        echo "Connection found!"
        rm ./resources/*
        exit 2;
      else
        echo "searching..."
        python ./src/iterator.py --target=$2 ./resources/bfs-$i >> ./resources/bfs-${i+1}
    fi
  done

