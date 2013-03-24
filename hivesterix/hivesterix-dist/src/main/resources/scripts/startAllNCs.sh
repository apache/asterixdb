PREGELIX_PATH=`pwd`

for i in `cat conf/slaves`
do
   ssh $i "cd ${PREGELIX_PATH}; export JAVA_HOME=${JAVA_HOME}; bin/startnc.sh"
done
