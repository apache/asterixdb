#/bin/sh
export JAVA_OPTS="-DAsterixConfigFileName=asterix-rainbow.properties"; /home/onose/asterix-app-0.0.2-SNAPSHOT-binary-assembly/bin/asterix-cmd  -hyracks-port 2222 -execute true $*
