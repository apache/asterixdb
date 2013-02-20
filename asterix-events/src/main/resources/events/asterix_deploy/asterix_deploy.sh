MANAGIX_HOME=$1
HYRACKS_CLI=$MANAGIX_HOME/asterix/hyracks-cli/bin/hyrackscli
if  ! [ -x $HYRACKS_CLI ]
then
     chmod +x $HYRACKS_CLI
fi
ASTERIX_ZIP=$2
HOST=$3
echo "connect to \"$HOST\";" > temp
echo "create application asterix \"$ASTERIX_ZIP\";" >> temp 
($HYRACKS_CLI < temp)  
