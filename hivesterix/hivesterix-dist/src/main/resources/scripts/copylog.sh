. conf/cluster.properties

NODEID=`hostname | cut -d '.' -f 1`
#echo $NODEID

#echo "rsync ${NCLOGS_DIR}/${NODEID}.log ${1}:${2}"
rsync ${NCLOGS_DIR}/${NODEID}.log ${1}:${2}
