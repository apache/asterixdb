#!/usr/bin/env bash

mkdir -p $1
echo "touch $1/$2" >> ~/file_create.log
touch $1/$2
