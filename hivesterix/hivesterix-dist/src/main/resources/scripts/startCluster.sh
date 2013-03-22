bin/startcc.sh
sleep 5
bin/startAllNCs.sh

. conf/cluster.properties
# do we need to specify the version somewhere?
hyrackcmd=`ls ${HYRACKS_HOME}/hyracks-cli/target/hyracks-cli-*-binary-assembly/bin/hyrackscli`
# find zip file
appzip=`ls $PWD/../hivesterix-dist-*-binary-assembly.zip`

[ -f $hyrackcmd ] || { echo "Hyracks commandline is missing"; exit -1;}
[ -f $appzip ] || { echo "Genomix binary-assembly.zip is missing"; exit -1;}

CCHOST_NAME=`cat conf/master`

IPADDR=`bin/getip.sh`
echo "connect to \"${IPADDR}:${CC_CLIENTPORT}\"; create application hivesterix \"$appzip\";" | $hyrackcmd 
echo ""

