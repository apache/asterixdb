#!/bin/bash

# Logging output for the CC and NCs is directed to their appropriately named
# files in LOG_HOME
#
# Example usage: ./runasterix.sh 4
# Loads 1 CC, 4 NCs (nc0, nc1, nc2, and nc3)
#

BASEDIR=`pwd`/$(dirname $0)
echo "Base dir: " ${BASEDIR}

ASTERIX_BIN=${BASEDIR}/asterix/asterix-app.zip
HYRACKS_SERVER_BIN=${BASEDIR}/hyracks-server/bin
HYRACKS_CLI_BIN=${BASEDIR}/hyracks-cli/bin

CONFIG_DIR=${BASEDIR}/config
LOG_DIR=${BASEDIR}/log

mkdir ${LOG_DIR} ${CONFIG_DIR}

CONFIG_NAME=local-autogen.properties	        # name of config file to generate

# check existence of directories
dirs=($HYRACKS_SERVER_BIN $HYRACKS_CLI_BIN $LOG_DIR $CONFIG_DIR)
for i in  "${dirs[@]}"
do
    if [ ! -d "$i" ]
    then
	    printf "Error: invalid directory layout -- can't access $i\n" >&2
	    exit 2
    fi
done

# set number of node controllers to load
if [ "$1" == "" ]
then
	numnc=1
else
	if echo $1 | egrep -q '^[0-9]+$'; then
		numnc=$1
	else
		printf "Error: $1 is not a number.\n" >&2
		printf "usage: %s [number_of_ncs]\n" $(basename $0) >&2
		exit 2
	fi
fi

# generate a suitable config file
echo "generating config file..."
printf "MetadataNode=nc1\nNewUniverse=true\n" > $CONFIG_DIR/$CONFIG_NAME
for ((i=1;i<=$numnc;i++)); do
    echo "nc$i.stores=/tmp/nc$i/" >> $CONFIG_DIR/$CONFIG_NAME
done
echo "OutputDir=/tmp/asterix_output/" >> $CONFIG_DIR/$CONFIG_NAME

# point to the config file and give java some extra memory
export CLASSPATH_PREFIX=$CONFIG_DIR
export JAVA_OPTS="-DAsterixConfigFileName=$CONFIG_NAME -Xms256m -Xmx512m"

echo "cluster controller starting..."
sh $HYRACKS_SERVER_BIN/hyrackscc -cc-root localhost -client-net-ip-address 127.0.0.1 -cluster-net-ip-address 127.0.0.1 &> $LOG_DIR/cc.log &

# for some reason this helps against getting a socket error
sleep 3

# start the node controllers
for ((i=1;i<=$numnc;i++)); do
	echo "node controller (nc$i) starting..."
	sh $HYRACKS_SERVER_BIN/hyracksnc -cc-host localhost -cluster-net-ip-address 127.0.0.1 -data-ip-address 127.0.0.1 -node-id "nc$i" \
		&> $LOG_DIR/nc$i.log &

    # avoid socket error
	sleep .5
done

# deploy the asterix application to hyracks
echo "connect to \"localhost\";create application asterix \"$ASTERIX_BIN\";" > $CONFIG_DIR/deploy.hcli
cat $CONFIG_DIR/deploy.hcli | sh $HYRACKS_CLI_BIN/hyrackscli
