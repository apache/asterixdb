#get the OS
OS_NAME=`uname -a|awk '{print $1}'`
LINUX_OS='Linux'

if [ $OS_NAME = $LINUX_OS ];
then
        #Get IP Address
        IPADDR=`/sbin/ifconfig eth0 | grep "inet addr" | awk '{print $2}' | cut -f 2 -d ':'`
else
        IPADDR=`/sbin/ifconfig en1 | grep "inet " | awk '{print $2}' | cut -f 2 -d ':'`
fi
echo $IPADDR
