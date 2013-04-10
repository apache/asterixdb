USERNAME=$1
shift 1
numargs=$#
for ((i=1 ; i <= numargs ; i=i+2))
do
 host=$1
 ssh -l $USERNAME -oNumberOfPasswordPrompts=0 $host "echo $host"
 shift 1
done
