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
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;

public class LSMIndexFileProperties {

    private long fileSize;
    private String nodeId;
    private boolean lsmComponentFile;
    private String filePath;
    private boolean requiresAck = false;

    public LSMIndexFileProperties() {
    }

    public LSMIndexFileProperties(String filePath, long fileSize, String nodeId, boolean lsmComponentFile,
            boolean requiresAck) {
        initialize(filePath, fileSize, nodeId, lsmComponentFile, requiresAck);
    }

    public LSMIndexFileProperties(LSMComponentProperties lsmComponentProperties) {
        initialize(lsmComponentProperties.getComponentId(), -1, lsmComponentProperties.getNodeId(), false, false);
    }

    public void initialize(String filePath, long fileSize, String nodeId, boolean lsmComponentFile,
            boolean requiresAck) {
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.nodeId = nodeId;
        this.lsmComponentFile = lsmComponentFile;
        this.requiresAck = requiresAck;
    }

    public void serialize(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeUTF(nodeId);
        dos.writeUTF(filePath);
        dos.writeLong(fileSize);
        dos.writeBoolean(lsmComponentFile);
        dos.writeBoolean(requiresAck);
    }

    public static LSMIndexFileProperties create(DataInput input) throws IOException {
        String nodeId = input.readUTF();
        String filePath = input.readUTF();
        long fileSize = input.readLong();
        boolean lsmComponentFile = input.readBoolean();
        boolean requiresAck = input.readBoolean();
        LSMIndexFileProperties fileProp =
                new LSMIndexFileProperties(filePath, fileSize, nodeId, lsmComponentFile, requiresAck);
        return fileProp;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSize() {
        return fileSize;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isLSMComponentFile() {
        return lsmComponentFile;
    }

    public boolean requiresAck() {
        return requiresAck;
    }

    public String getFileName() {
        return Paths.get(filePath).toFile().getName();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("File Path: " + filePath + "  ");
        sb.append("File Size: " + fileSize + "  ");
        sb.append("Node ID: " + nodeId + "  ");
        sb.append("isLSMComponentFile : " + lsmComponentFile + "  ");
        return sb.toString();
    }
}
