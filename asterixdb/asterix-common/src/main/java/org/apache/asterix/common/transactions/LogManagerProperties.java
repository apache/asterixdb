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
package org.apache.asterix.common.transactions;

import java.io.Serializable;

import org.apache.asterix.common.config.TransactionProperties;

public class LogManagerProperties implements Serializable {

    private static final long serialVersionUID = 2084227360840799662L;

    public static final String lineSeparator = System.getProperty("line.separator");
    private static final String DEFAULT_LOG_FILE_PREFIX = "transaction_log";

    // follow the naming convention <logFilePrefix>_<number> where number starts from 0
    private final String logFilePrefix;
    private final String logDir;

    // number of log pages in the log buffer
    private final int logPageSize;
    // number of log pages in the log buffer.
    private final int numLogPages;
    // maximum size of each log file
    private final long logPartitionSize;

    public LogManagerProperties(TransactionProperties txnProperties, String nodeId) {
        this.logPageSize = txnProperties.getLogBufferPageSize();
        this.numLogPages = txnProperties.getLogBufferNumPages();
        long logPartitionSize = txnProperties.getLogPartitionSize();
        this.logDir = txnProperties.getLogDirectory(nodeId);
        this.logFilePrefix = DEFAULT_LOG_FILE_PREFIX;
        int logBufferSize = logPageSize * numLogPages;
        //make sure that the log partition size is the multiple of log buffer size.
        this.logPartitionSize = (logPartitionSize / logBufferSize) * logBufferSize;
    }

    public long getLogPartitionSize() {
        return logPartitionSize;
    }

    public String getLogFilePrefix() {
        return logFilePrefix;
    }

    public String getLogDir() {
        return logDir;
    }

    public int getLogPageSize() {
        return logPageSize;
    }

    public int getNumLogPages() {
        return numLogPages;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("log_dir_ : " + logDir + lineSeparator);
        builder.append("log_file_prefix" + logFilePrefix + lineSeparator);
        builder.append("log_page_size : " + logPageSize + lineSeparator);
        builder.append("num_log_pages : " + numLogPages + lineSeparator);
        builder.append("log_partition_size : " + logPartitionSize + lineSeparator);
        return builder.toString();
    }
}
