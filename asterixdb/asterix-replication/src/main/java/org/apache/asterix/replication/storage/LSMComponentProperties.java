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
package org.apache.asterix.replication.storage;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.common.ioopcallbacks.AbstractLSMIOOperationCallback;
import org.apache.asterix.replication.logging.TxnLogUtil;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexOperationContext;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexReplicationJob;
import org.apache.hyracks.storage.am.lsm.common.api.LSMOperationType;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndex;
import org.apache.hyracks.storage.am.lsm.common.impls.AbstractLSMIndexFileManager;

public class LSMComponentProperties {

    private AtomicInteger numberOfFiles;
    private String componentId;
    private long lsnOffset;
    private long originalLSN;
    private String nodeId;
    private Long replicaLSN;
    private String maskPath = null;
    private String replicaPath = null;
    private LSMOperationType opType;

    public LSMComponentProperties(ILSMIndexReplicationJob job, String nodeId) {
        this.nodeId = nodeId;
        componentId = LSMComponentProperties.getLSMComponentID((String) job.getJobFiles().toArray()[0]);
        numberOfFiles = new AtomicInteger(job.getJobFiles().size());
        originalLSN = LSMComponentProperties.getLSMComponentLSN((AbstractLSMIndex) job.getLSMIndex(),
                job.getLSMIndexOperationContext());
        opType = job.getLSMOpType();
    }

    public LSMComponentProperties() {
    }

    public static long getLSMComponentLSN(AbstractLSMIndex lsmIndex, ILSMIndexOperationContext ctx) {
        long componentLSN = -1;
        try {
            componentLSN = ((AbstractLSMIOOperationCallback) lsmIndex.getIOOperationCallback())
                    .getComponentLSN(ctx.getComponentsToBeReplicated());
        } catch (HyracksDataException e) {
            e.printStackTrace();
        }
        if (componentLSN < 0) {
            componentLSN = 0;
        }
        return componentLSN;
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeUTF(componentId);
        dos.writeUTF(nodeId);
        dos.writeInt(numberOfFiles.get());
        dos.writeLong(originalLSN);
        dos.writeLong(lsnOffset);
        dos.writeInt(opType.ordinal());
    }

    public static LSMComponentProperties create(DataInput input) throws IOException {
        LSMComponentProperties lsmCompProp = new LSMComponentProperties();
        lsmCompProp.componentId = input.readUTF();
        lsmCompProp.nodeId = input.readUTF();
        lsmCompProp.numberOfFiles = new AtomicInteger(input.readInt());
        lsmCompProp.originalLSN = input.readLong();
        lsmCompProp.lsnOffset = input.readLong();
        lsmCompProp.opType = LSMOperationType.values()[input.readInt()];
        return lsmCompProp;
    }

    public String getMaskPath(ReplicaResourcesManager resourceManager) {
        if (maskPath == null) {
            LSMIndexFileProperties afp = new LSMIndexFileProperties(this);
            //split the index file path to get the LSM component file name
            afp.splitFileName();
            maskPath = getReplicaComponentPath(resourceManager) + File.separator + afp.getFileName()
                    + ReplicaResourcesManager.LSM_COMPONENT_MASK_SUFFIX;
        }
        return maskPath;
    }

    public String getReplicaComponentPath(ReplicaResourcesManager resourceManager) {
        if (replicaPath == null) {
            LSMIndexFileProperties afp = new LSMIndexFileProperties(this);
            replicaPath = resourceManager.getIndexPath(afp);
        }
        return replicaPath;
    }

    /***
     * @param filePath
     *            any file of the LSM component
     * @return a unique id based on the timestamp of the component
     */
    public static String getLSMComponentID(String filePath) {
        String[] tokens = filePath.split(File.separator);

        int arraySize = tokens.length;
        String fileName = tokens[arraySize - 1];
        String idxName = tokens[arraySize - 2];
        String dataverse = tokens[arraySize - 3];
        String partitionName = tokens[arraySize - 4];

        StringBuilder componentId = new StringBuilder();
        componentId.append(partitionName);
        componentId.append(File.separator);
        componentId.append(dataverse);
        componentId.append(File.separator);
        componentId.append(idxName);
        componentId.append(File.separator);
        componentId.append(fileName.substring(0, fileName.lastIndexOf(AbstractLSMIndexFileManager.DELIMITER)));
        return componentId.toString();
    }

    public String getComponentId() {
        return componentId;
    }

    public long getOriginalLSN() {
        return originalLSN;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getNumberOfFiles() {
        return numberOfFiles.get();
    }

    public int markFileComplete() {
        return numberOfFiles.decrementAndGet();
    }

    public void setNumberOfFiles(AtomicInteger numberOfFiles) {
        this.numberOfFiles = numberOfFiles;
    }

    public Long getReplicaLSN() {
        return replicaLSN;
    }

    public void setReplicaLSN(Long replicaLSN) {
        this.replicaLSN = replicaLSN;
    }

    public LSMOperationType getOpType() {
        return opType;
    }

    public void setOpType(LSMOperationType opType) {
        this.opType = opType;
    }

    public String getNodeUniqueLSN() {
        return TxnLogUtil.getNodeUniqueLSN(nodeId, originalLSN);
    }
}
