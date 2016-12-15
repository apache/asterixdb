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

import org.apache.asterix.common.utils.StoragePathUtil;
import org.apache.hyracks.storage.am.common.api.IMetadataPageManager;

public class LSMIndexFileProperties {

    private String fileName;
    private long fileSize;
    private String nodeId;
    private String dataverse;
    private String idxName;
    private boolean lsmComponentFile;
    private String filePath;
    private boolean requiresAck = false;
    private long LSNByteOffset;
    private int partition;

    public LSMIndexFileProperties() {
    }

    public LSMIndexFileProperties(String filePath, long fileSize, String nodeId, boolean lsmComponentFile,
            long LSNByteOffset, boolean requiresAck) {
        initialize(filePath, fileSize, nodeId, lsmComponentFile, LSNByteOffset, requiresAck);
    }

    public LSMIndexFileProperties(LSMComponentProperties lsmComponentProperties) {
        initialize(lsmComponentProperties.getComponentId(), -1, lsmComponentProperties.getNodeId(), false,
                IMetadataPageManager.Constants.INVALID_LSN_OFFSET, false);
    }

    public void initialize(String filePath, long fileSize, String nodeId, boolean lsmComponentFile, long LSNByteOffset,
            boolean requiresAck) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.nodeId = nodeId;
        this.lsmComponentFile = lsmComponentFile;
        this.LSNByteOffset = LSNByteOffset;
        this.requiresAck = requiresAck;
    }

    public void splitFileName() {
        String[] tokens = filePath.split(File.separator);
        int arraySize = tokens.length;
        this.fileName = tokens[arraySize - 1];
        this.idxName = tokens[arraySize - 2];
        this.dataverse = tokens[arraySize - 3];
        this.partition = StoragePathUtil.getPartitionNumFromName(tokens[arraySize - 4]);
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeUTF(nodeId);
        dos.writeUTF(filePath);
        dos.writeLong(fileSize);
        dos.writeBoolean(lsmComponentFile);
        dos.writeLong(LSNByteOffset);
        dos.writeBoolean(requiresAck);
    }

    public static LSMIndexFileProperties create(DataInput input) throws IOException {
        String nodeId = input.readUTF();
        String filePath = input.readUTF();
        long fileSize = input.readLong();
        boolean lsmComponentFile = input.readBoolean();
        long LSNByteOffset = input.readLong();
        boolean requiresAck = input.readBoolean();
        LSMIndexFileProperties fileProp = new LSMIndexFileProperties(filePath, fileSize, nodeId, lsmComponentFile,
                LSNByteOffset, requiresAck);
        return fileProp;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getDataverse() {
        return dataverse;
    }

    public void setDataverse(String dataverse) {
        this.dataverse = dataverse;
    }

    public String getIdxName() {
        return idxName;
    }

    public boolean isLSMComponentFile() {
        return lsmComponentFile;
    }

    public boolean requiresAck() {
        return requiresAck;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("File Name: " + fileName + "  ");
        sb.append("File Size: " + fileSize + "  ");
        sb.append("Node ID: " + nodeId + "  ");
        sb.append("Partition: " + partition + "  ");
        sb.append("IDX Name: " + idxName + "  ");
        sb.append("isLSMComponentFile : " + lsmComponentFile + "  ");
        sb.append("Dataverse: " + dataverse);
        sb.append("LSN Byte Offset: " + LSNByteOffset);
        return sb.toString();
    }

    public long getLSNByteOffset() {
        return LSNByteOffset;
    }

    public int getPartition() {
        return partition;
    }
}
