/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.transactions;

import java.io.Serializable;

import edu.uci.ics.asterix.common.config.AsterixTransactionProperties;

public class LogManagerProperties implements Serializable {

    private static final long serialVersionUID = 2084227360840799662L;

    public static final String lineSeparator = System.getProperty("line.separator");
    public static final int LOG_MAGIC_NUMBER = 123456789;
    public static final String LOG_DIR_SUFFIX = ".txnLogDir";
    private static final String DEFAULT_LOG_FILE_PREFIX = "asterix_transaction_log";

    // follow the naming convention <logFilePrefix>_<number> where number starts from 0
    private final String logFilePrefix;
    private final String logDir;
    public String logDirKey;

    // number of log pages in the log buffer
    private final int logPageSize;
    // number of log pages in the log buffer.
    private final int numLogPages;
    // logBufferSize = logPageSize * numLogPages;
    private final int logBufferSize;
    // maximum size of each log file
    private final long logPartitionSize;

    public LogManagerProperties(AsterixTransactionProperties txnProperties, String nodeId) {
        this.logDirKey = new String(nodeId + LOG_DIR_SUFFIX);
        this.logPageSize = txnProperties.getLogBufferPageSize();
        this.numLogPages = txnProperties.getLogBufferNumPages();
        long logPartitionSize = txnProperties.getLogPartitionSize();
        this.logDir = txnProperties.getLogDirectory(nodeId);
        this.logFilePrefix = DEFAULT_LOG_FILE_PREFIX;
        this.logBufferSize = logPageSize * numLogPages;
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

    public int getLogBufferSize() {
        return logBufferSize;
    }

    public String getLogDirKey() {
        return logDirKey;
    }

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
