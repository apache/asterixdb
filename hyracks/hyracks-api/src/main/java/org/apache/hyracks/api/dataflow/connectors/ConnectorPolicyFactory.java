/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.api.dataflow.connectors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yingyib
 */
public class ConnectorPolicyFactory {
    public static ConnectorPolicyFactory INSTANCE = new ConnectorPolicyFactory();

    private ConnectorPolicyFactory() {

    }

    public IConnectorPolicy getConnectorPolicy(DataInput input) throws IOException {
        int kind = input.readInt();
        switch (kind) {
            case 0:
                return new PipeliningConnectorPolicy();
            case 1:
                return new SendSideMaterializedBlockingConnectorPolicy();
            case 2:
                return new SendSideMaterializedPipeliningConnectorPolicy();
            case 3:
                return new SendSideMaterializedReceiveSideMaterializedBlockingConnectorPolicy();
            case 4:
                return new SendSideMaterializedReceiveSideMaterializedPipeliningConnectorPolicy();
            case 5:
                return new SendSidePipeliningReceiveSideMaterializedBlockingConnectorPolicy();
        }
        return null;
    }

    public void writeConnectorPolicy(IConnectorPolicy policy, DataOutput output) throws IOException {
        if (policy instanceof PipeliningConnectorPolicy) {
            output.writeInt(0);
        } else if (policy instanceof SendSideMaterializedBlockingConnectorPolicy) {
            output.writeInt(1);
        } else if (policy instanceof SendSideMaterializedPipeliningConnectorPolicy) {
            output.writeInt(2);
        } else if (policy instanceof SendSideMaterializedReceiveSideMaterializedBlockingConnectorPolicy) {
            output.writeInt(3);
        } else if (policy instanceof SendSideMaterializedReceiveSideMaterializedPipeliningConnectorPolicy) {
            output.writeInt(4);
        } else if (policy instanceof SendSidePipeliningReceiveSideMaterializedBlockingConnectorPolicy) {
            output.writeInt(5);
        }
    }

}
