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
package org.apache.hyracks.dataflow.std.connectors;

public class GlobalHashingLocalityMap implements ILocalityMap {

    private static final long serialVersionUID = 1L;

    private int[] consumers;

    /* (non-Javadoc)
     * @see org.apache.hyracks.examples.text.client.aggregation.helpers.ILocalityMap#getConsumers(int)
     */
    @Override
    public int[] getConsumers(int senderID, int nConsumerPartitions) {
        if (consumers == null) {
            consumers = new int[nConsumerPartitions];
            for (int i = 0; i < consumers.length; i++) {
                consumers[i] = i;
            }
        }
        return consumers;
    }

    /* (non-Javadoc)
     * @see org.apache.hyracks.examples.text.client.aggregation.helpers.ILocalityMap#getConsumerPartitionCount()
     */
    @Override
    public int getConsumerPartitionCount(int nConsumerPartitions) {
        return nConsumerPartitions;
    }

    @Override
    public boolean isConnected(int senderID, int receiverID, int nConsumerPartitions) {
        return true;
    }

}
