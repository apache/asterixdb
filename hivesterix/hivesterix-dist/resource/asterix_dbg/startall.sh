HYRACKS_HOME=/home/yingyib/hyracks-0.1.5

ssh asterix-master ./hivesterix/target/appassembler/asterix_dbg/startcc.sh
sleep 20
ssh asterix-001 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-002 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-003 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-004 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-005 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-006 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-007 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-008 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-009 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh
ssh asterix-010 ./hivesterix/target/appassembler/asterix_dbg/startnc.sh

sleep 10
$HYRACKS_HOME/hyracks-cli/target/appassembler/bin/hyrackscli < ~/hivesterix/target/appassembler/asterix_dbg/hivedeploy.hcli
