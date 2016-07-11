$MANAGIX_HOME/bin/managix stop -n nc1 1>/dev/null 2>&1;
$MANAGIX_HOME/bin/managix delete -n nc1 1>/dev/null 2>&1;
$MANAGIX_HOME/bin/managix create -n nc1 -c $MANAGIX_HOME/clusters/local/local.xml;
$MANAGIX_HOME/bin/managix stop -n nc1;
cp $MANAGIX_HOME/../../../asterix-external-data/target/asterix-external-data-*-tests.jar \
    $MANAGIX_HOME/clusters/local/working_dir/asterix/repo/
$MANAGIX_HOME/bin/managix start -n nc1;
