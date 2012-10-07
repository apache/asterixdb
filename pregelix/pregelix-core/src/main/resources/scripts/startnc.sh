hostname

#Get the IP address of the cc
CCHOST_NAME=`cat conf/master`
CCHOST=`dig +short $CCHOST_NAME`

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

#Get IP Address
IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
#HOSTNAME=`hostname`
#echo $HOSTNAME
#IPADDR=`dig +short ${HOSTNAME}`
NODEID=`hostname | cut -d '.' -f 1`

#Set JAVA_OPTS
export JAVA_OPTS=$NCJAVA_OPTS

#Enter the temp dir
cd $NCTMP_DIR

#Launch hyracks nc
chmod -R 755 $HYRACKS_HOME
echo $HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host $CCHOST -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -node-id $NODEID -iodevices "${IO_DIRS}" -frame-size $FRAME_SIZE
$HYRACKS_HOME/hyracks-server/target/appassembler/bin/hyracksnc -cc-host $CCHOST -cc-port $CC_CLUSTERPORT -cluster-net-ip-address $IPADDR  -data-ip-address $IPADDR -node-id $NODEID -iodevices "${IO_DIRS}" &> $NCLOGS_DIR/$NODEID.log &
