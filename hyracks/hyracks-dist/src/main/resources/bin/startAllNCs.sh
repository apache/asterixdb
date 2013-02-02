PREGELIX_PATH=`pwd`

for i in `cat conf/slaves`
do
   ssh $i "cd ${PREGELIX_PATH}; bin/startnc.sh"
done
