hostname

MY_NAME=`hostname`
#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CURRENT_PATH=`pwd`
CCHOST=`ssh ${CCHOST_NAME} "cd ${CURRENT_PATH}; bin/getip.sh"`

#Import cluster properties
. conf/cluster.properties

#Clean up temp dir

rm -rf $NCTMP_DIR
mkdir $NCTMP_DIR

#Clean up log dir
rm -rf $NCLOGS_DIR
mkdir $NCLOGS_DIR


#Clean up I/O working dir
io_dirs=$(echo $IO_DIRS | tr "," "\n")
for io_dir in $io_dirs
do
	rm -rf $io_dir
	mkdir $io_dir
done

#Set JAVA_HOME
export JAVA_HOME=$JAVA_HOME

#Get OS
OS_NAME=`uname -a|awk '{print $1}'`
LINUX_OS='Linux'

IPADDR=`bin/getip.sh`
#echo $IPADDR

#Get node ID
NODEID=`hostname | cut -d '.' -f 1`

#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS

cd $HYRACKS_HOME
HYRACKS_HOME=`pwd`

#Enter the temp dir
cd $NCTMP_DIR

#Launch hyracks nc
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host $CCHOST -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -node-id $NODEID -iodevices "${IO_DIRS}" &> $NCLOGS_DIR/$NODEID.log &
