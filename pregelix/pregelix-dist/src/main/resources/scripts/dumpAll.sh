. conf/cluster.properties
PREGELIX_PATH=`pwd`
LOG_PATH=$PREGELIX_PATH/logs/
rm -rf $LOG_PATH
mkdir $LOG_PATH
ccname=`hostname`

for i in `cat conf/slaves`
do
   ssh $i "cd ${PREGELIX_PATH}; bin/dumptrace.sh; bin/copylog.sh ${ccname} ${LOG_PATH}"
done

