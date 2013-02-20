INSTANCE_NAME=$1
MASTER_NODE=$2
shift 2
numargs=$#
for ((i=1 ; i <= numargs ; i=i+2))
do
 host=$1
 nc_id=$2
 INFO=$(ssh $host "ps -ef | grep nc_join | grep -v grep | grep -v ssh| grep $nc_id" | head -n 1 )
 PARENT_ID=`echo  $INFO | cut -d " "  -f2`
 PID_INFO=$(ssh $host "ps -ef |  grep hyracks | grep -v grep | grep -v nc_join |  grep $PARENT_ID") 
 PID=`echo $PID_INFO | cut -d " " -f2`
 echo "NC:$host:$nc_id:$PID"
 shift 2
done

CC_PARENT_ID_INFO=$(ssh $MASTER_NODE "ps -ef  | grep hyracks | grep cc_start | grep -v ssh")
CC_PARENT_ID=`echo $CC_PARENT_ID_INFO | tr -s " " | cut -d " " -f2` 
CC_ID_INFO=$(ssh $MASTER_NODE "ps -ef | grep hyracks | grep $CC_PARENT_ID | grep -v bash")
CC_ID=`echo $CC_ID_INFO |  tr -s " " | cut -d " " -f2`
echo "CC:$MASTER_NODE:N/A:$CC_ID"
