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

package org.apache.asterix.external.indexing;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

import org.apache.asterix.common.config.DatasetConfig.ExternalFilePendingOp;

public class ExternalFile implements Serializable, Comparable<ExternalFile> {

    /**
     * A class for metadata entity externalFile
     * This class represents an external dataset file and is intended for use for saving external data snapshot
     */
    private static final long serialVersionUID = 1L;

    private String dataverseName;
    private String datasetName;
    private Date lastModefiedTime;
    private long size;
    private String fileName;
    private int fileNumber;
    private ExternalFilePendingOp pendingOp;

    public ExternalFile() {
        this.dataverseName = "";
        this.datasetName = "";
        this.fileNumber = -1;
        this.fileName = "";
        this.lastModefiedTime = new Date();
        this.size = 0;
        this.pendingOp = ExternalFilePendingOp.NO_OP;
    }

    public ExternalFile(String dataverseName, String datasetName, int fileNumber, String fileName,
            Date lastModefiedTime, long size, ExternalFilePendingOp pendingOp) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.fileNumber = fileNumber;
        this.fileName = fileName;
        this.lastModefiedTime = lastModefiedTime;
        this.size = size;
        this.setPendingOp(pendingOp);
    }

    public String getDataverseName() {
        return dataverseName;
    }

    public void setDataverseName(String dataverseName) {
        this.dataverseName = dataverseName;
    }

    public String getDatasetName() {
        return datasetName;
    }

    public void setDatasetName(String datasetName) {
        this.datasetName = datasetName;
    }

    public Date getLastModefiedTime() {
        return lastModefiedTime;
    }

    public void setLastModefiedTime(Date lastModefiedTime) {
        this.lastModefiedTime = lastModefiedTime;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getFileNumber() {
        return fileNumber;
    }

    public void setFileNumber(int fileNumber) {
        this.fileNumber = fileNumber;
    }

    public ExternalFilePendingOp getPendingOp() {
        return pendingOp;
    }

    public void setPendingOp(ExternalFilePendingOp pendingOp) {
        this.pendingOp = pendingOp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataverseName, datasetName, fileName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ExternalFile)) {
            return false;
        }
        ExternalFile anotherFile = (ExternalFile) obj;
        return fileNumber == anotherFile.fileNumber && Objects.equals(dataverseName, anotherFile.getDataverseName())
                && Objects.equals(datasetName, anotherFile.getDatasetName());
    }

    @Override
    public int compareTo(ExternalFile o) {
        return this.fileNumber - o.getFileNumber();
    }

}
