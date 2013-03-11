JAVA_VERSION=`java -version 2>&1 |awk 'NR==1{ gsub(/"/,""); print $3 }'`
HOME_DIR=`echo ~`
echo JAVA_VERSION=$JAVA_VERSION
echo HOME_DIR=$HOME_DIR
