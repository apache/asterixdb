LOG=perflog/result.log
echo "">$LOG

for file in $1/*.hive
do
   sleep 10
   START=$(date +%s)
   echo $file  
   ../bin/hive -f $file > perflog/$file
   END=$(date +%s)
   DIFF=$(( $END - $START ))
   echo $file       $DIFF>>$LOG$2
done
