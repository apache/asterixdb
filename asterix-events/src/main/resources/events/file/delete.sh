PATH_TO_DELETE=$1
echo "rm -rf $PATH_TO_DELETE" >> ~/backup.log
rm -rf $PATH_TO_DELETE
