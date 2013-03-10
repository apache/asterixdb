for((i=1; i<=3; i++))
do
	./startcluster.sh
	./execute.sh tpch100 $i
done
