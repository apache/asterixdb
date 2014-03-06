#!/bin/bash

TMP=/tmp/verify_stage2_
d=ridpairs-000
f=part

if [ $# -ne "1" ]
then
  echo "Usage:   `basename $0` dataset"
  echo "Example: `basename $0` dblp.small"
  exit $E_BADARGS
fi

cat $1/$d/$f-????? > $TMP
./verify.sh $TMP $1.expected/$d/expected.txt -u
if [ "$?" -ne "0" ]
then
    exit 1
fi
rm $TMP
