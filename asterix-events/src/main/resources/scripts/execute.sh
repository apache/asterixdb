#!/bin/bash
val=0
line=""
for x in $@
do
 if [[ $val == 0 ]]
 then
    line="$x="
    val=1
 else
    msg="$line$x"
    echo $line >> envr
    val=0
 fi
done
cat ./envr
