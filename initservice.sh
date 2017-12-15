#!/bin/bash

mkdir -p Database
mkdir -p DirectoryServerFiles/7001
mkdir -p DirectoryServerFiles/7002
mkdir -p DirectoryServerFiles/7003

python dictService.py 7333 &
python lock.py 7334 &
python fileService.py 7001 &
python fileService.py 7002 &
python fileService.py 7003 &
sleep 10 
python initdatabase.py
