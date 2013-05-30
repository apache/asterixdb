#/bin/bash	

export JAVA_OPTS="-DAsterixConfigFileName=test.properties -DAsterixWebServerPort=20001 -Djava.util.logging.config.file=/home/nicnic/Work/Asterix/hyracks/logging.properties"
export HYRACKS_HOME="/home/nicnic/workspace/hyracks/tags/hyracks-0.1.5"
bash ${HYRACKS_HOME}/hyracks-server/target/appassembler/bin/hyrackscc 
#bash /home/nicnic/workspace/hyracks/trunk/hyracks/hyracks-server/target/appassembler/bin/hyrackscc
