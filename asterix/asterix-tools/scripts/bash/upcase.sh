#!/bin/bash

while read line; do
    for i in $line; do U=`echo -n "${i:0:1}" | tr "[:lower:]" "[:upper:]"`; echo -n "${U}${i:1} "; done
    echo ""
done
