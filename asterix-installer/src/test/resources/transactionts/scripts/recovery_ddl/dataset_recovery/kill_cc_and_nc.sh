jps | awk '{if ($2 == "NCDriver" || $2 == "CCDriver") print $1;}' | xargs -n 1 kill -9
