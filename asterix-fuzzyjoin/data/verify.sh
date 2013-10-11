#!/bin/bash

TMP=/tmp/verify_

if [ $# -lt "2" ]
then
  echo "Usage: `basename $0` result.txt result.T.txt [-u]"
  exit $E_BADARGS
fi

args=( $@ )
for i in 0 1
do
    f=${args[$i]}
    ### Assume second argument is alredy sorted and with no duplicates
    if [ "$i" -eq "0" ]
    then
        sort $3 $f > $TMP$i
    else
        rm $TMP$i 2> /dev/null
        ln -s $PWD/$f $TMP$i
    fi
    if [ "$?" -ne "0" ]
    then
        echo Fail -- preprocessing
        exit 1
    fi

    l[$i]=`wc --lines $TMP$i | cut --delimiter=" " --fields=1`
    if [ "$?" -ne "0" ]
    then
        echo Fail -- preprocessing
        exit 1
    fi
done


### Test 1
if [ "${l[0]}" -ne "${l[1]}" ]
then
    echo $1 ${l[0]}
    echo $2 ${l[1]}
    echo Fail -- different number of tokens
    exit 1
fi

### Test 2
diff --brief ${TMP}0 ${TMP}1
if [ "$?" -ne "0" ]
then
    echo Fail -- differnt tokens
    exit 1
fi
rm ${TMP}0 ${TMP}1

echo Pass