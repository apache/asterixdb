#kill -9 `ps -ef  | grep hyracks | grep -v grep | cut -d "/" -f1 | tr -s " " | cut -d " " -f2`
CC_PARENT_ID_INFO=`ps -ef  | grep hyracks | grep cc_start | grep -v ssh`
CC_PARENT_ID=`echo $CC_PARENT_ID_INFO | tr -s " " | cut -d " " -f2`
CC_ID_INFO=`ps -ef | grep hyracks | grep $CC_PARENT_ID | grep -v bash`
CC_ID=`echo $CC_ID_INFO |  tr -s " " | cut -d " " -f2`
kill -9 $CC_ID
