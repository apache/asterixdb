#!/bin/bash

d=tokens-000
f=part

if [ $# -ne "1" ]
then
  echo "Usage:   `basename $0` dataset"
  echo "Example: `basename $0` dblp.small"
  exit $E_BADARGS
fi

./verify.sh $1/$d/$f-00000 $1.expected/$d/expected.txt
exit $?
