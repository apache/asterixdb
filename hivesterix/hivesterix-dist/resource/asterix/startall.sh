#/*
# Copyright 2009-2013 by The Regents of the University of California
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# you may obtain a copy of the License from
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#*/
ssh asterix-master './hivesterix/target/appassembler/asterix/startcc.sh'&
sleep 20
ssh asterix-001 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-002 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-003 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-004 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-005 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-006 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-007 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-008 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-009 './hivesterix/target/appassembler/asterix/startnc.sh'&
ssh asterix-010 './hivesterix/target/appassembler/asterix/startnc.sh'&

sleep 30
export HYRACKS_HOME=/home/yingyib/hyracks_asterix_stabilization
$HYRACKS_HOME/hyracks-cli/target/appassembler/bin/hyrackscli < ~/hivesterix/target/appassembler/asterix/hivedeploy.hcli
