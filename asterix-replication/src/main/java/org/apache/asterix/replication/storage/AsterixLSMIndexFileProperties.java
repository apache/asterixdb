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

import org.apache.hyracks.storage.am.common.api.IMetaDataPageManager;
import org.apache.hyracks.storage.am.common.util.IndexFileNameUtil;

public class AsterixLSMIndexFileProperties {

    private String fileName;
    private long fileSize;
    private String nodeId;
    private String dataverse;
    private int ioDeviceNum;
    private String idxName;
    private boolean lsmComponentFile;
    private String filePath;
    private boolean requiresAck = false;
    private long LSNByteOffset;

    public AsterixLSMIndexFileProperties() {
    }

    public AsterixLSMIndexFileProperties(String filePath, long fileSize, String nodeId, boolean lsmComponentFile,
            long LSNByteOffset, boolean requiresAck) {
        initialize(filePath, fileSize, nodeId, lsmComponentFile, LSNByteOffset, requiresAck);
    }

    public AsterixLSMIndexFileProperties(LSMComponentProperties lsmComponentProperties) {
        initialize(lsmComponentProperties.getComponentId(), -1, lsmComponentProperties.getNodeId(), false,
                IMetaDataPageManager.INVALID_LSN_OFFSET, false);
    }

    public void initialize(String filePath, long fileSize, String nodeId, boolean lsmComponentFile, long LSNByteOffset,
            boolean requiresAck) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.nodeId = nodeId;
        String[] tokens = filePath.split(File.separator);
        int arraySize = tokens.length;
        this.fileName = tokens[arraySize - 1];
        this.ioDeviceNum = getDeviceIONumFromName(tokens[arraySize - 2]);
        this.idxName = tokens[arraySize - 3];
        this.dataverse = tokens[arraySize - 4];
        this.lsmComponentFile = lsmComponentFile;
        this.LSNByteOffset = LSNByteOffset;
        this.requiresAck = requiresAck;
    }

    public static int getDeviceIONumFromName(String name) {
        return Integer.parseInt(name.substring(IndexFileNameUtil.IO_DEVICE_NAME_PREFIX.length()));
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

    public static AsterixLSMIndexFileProperties create(DataInput input) throws IOException {
        String nodeId = input.readUTF();
        String filePath = input.readUTF();
        long fileSize = input.readLong();
        boolean lsmComponentFile = input.readBoolean();
        long LSNByteOffset = input.readLong();
        boolean requiresAck = input.readBoolean();
        AsterixLSMIndexFileProperties fileProp = new AsterixLSMIndexFileProperties();
        fileProp.initialize(filePath, fileSize, nodeId, lsmComponentFile, LSNByteOffset, requiresAck);
        return fileProp;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getDataverse() {
        return dataverse;
    }

    public void setDataverse(String dataverse) {
        this.dataverse = dataverse;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public int getIoDeviceNum() {
        return ioDeviceNum;
    }

    public void setIoDeviceNum(int ioDevoceNum) {
        this.ioDeviceNum = ioDevoceNum;
    }

    public String getIdxName() {
        return idxName;
    }

    public void setIdxName(String idxName) {
        this.idxName = idxName;
    }

    public boolean isLSMComponentFile() {
        return lsmComponentFile;
    }

    public void setLsmComponentFile(boolean lsmComponentFile) {
        this.lsmComponentFile = lsmComponentFile;
    }

    public boolean requiresAck() {
        return requiresAck;
    }

    public void setRequiresAck(boolean requiresAck) {
        this.requiresAck = requiresAck;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("File Name: " + fileName + "  ");
        sb.append("File Size: " + fileSize + "  ");
        sb.append("Node ID: " + nodeId + "  ");
        sb.append("I/O Device: " + ioDeviceNum + "  ");
        sb.append("IDX Name: " + idxName + "  ");
        sb.append("isLSMComponentFile : " + lsmComponentFile + "  ");
        sb.append("Dataverse: " + dataverse);
        sb.append("LSN Byte Offset: " + LSNByteOffset);
        return sb.toString();
    }

    public long getLSNByteOffset() {
        return LSNByteOffset;
    }

    public void setLSNByteOffset(long lSNByteOffset) {
        LSNByteOffset = lSNByteOffset;
    }
}