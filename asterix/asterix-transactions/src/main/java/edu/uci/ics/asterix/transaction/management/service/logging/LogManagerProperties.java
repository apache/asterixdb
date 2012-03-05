/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.asterix.transaction.management.service.logging;

import java.io.Serializable;
import java.util.Properties;

import edu.uci.ics.asterix.transaction.management.service.transaction.TransactionManagementConstants;

public class LogManagerProperties implements Serializable {

    /**
     * generated SerialVersionUID
     */
    private static final long serialVersionUID = 2084227360840799662L;

    private String logFilePrefix = "asterix_transaction_log"; // log files

    // follow the
    // naming
    // convention
    // <logFilePrefix>_<number>
    // where number
    // starts from 0

    private String logDir = "asterix_logs"; // the path where the LogManager
                                            // will create log files

    private int logPageSize = 128 * 1024; // 128 KB
    private int numLogPages = 8; // number of log pages in the log buffer.
    private long logPartitionSize = logPageSize * 250; // maximum size of each
                                                       // log file
    private long groupCommitWaitPeriod = 0; // time in milliseconds for which a
    // commit record will wait before
    // the housing page is marked for
    // flushing.
    private int logBufferSize = logPageSize * numLogPages;

    private final int logHeaderSize = 43; /*
                                           * ( magic number(4) + (length(4) +
                                           * type(1) + actionType(1) +
                                           * timestamp(8) + transacitonId(8) +
                                           * resourceMgrId(1) + pageId(8) +
                                           * prevLSN(8)
                                           */
    private int logTailSize = 8; /* checksum(8) */
    public int logMagicNumber = 123456789;

    public static final String LOG_PARTITION_SIZE_KEY = "log_partition_size";
    public static final String LOG_DIR_KEY = "log_dir";
    public static final String LOG_PAGE_SIZE_KEY = "log_page_size";
    public static final String NUM_LOG_PAGES_KEY = "num_log_pages";
    public static final String LOG_FILE_PREFIX_KEY = "log_file_prefix";
    public static final String GROUP_COMMIT_WAIT_PERIOD = "group_commit_wait_period";

    public LogManagerProperties(Properties properties) {
        if (properties.get(LOG_PAGE_SIZE_KEY) != null) {
            logPageSize = Integer.parseInt(properties.getProperty(LOG_PAGE_SIZE_KEY));
        }
        if (properties.get(NUM_LOG_PAGES_KEY) != null) {
            numLogPages = Integer.parseInt(properties.getProperty(NUM_LOG_PAGES_KEY));
        }

        if (properties.get(LOG_PARTITION_SIZE_KEY) != null) {
            logPartitionSize = Long.parseLong(properties.getProperty(LOG_PARTITION_SIZE_KEY));
        }

        groupCommitWaitPeriod = Long.parseLong(properties.getProperty("group_commit_wait_period", ""
                + groupCommitWaitPeriod));
        logFilePrefix = properties.getProperty(LOG_FILE_PREFIX_KEY, logFilePrefix);
        logDir = properties
                .getProperty(LOG_DIR_KEY, TransactionManagementConstants.LogManagerConstants.DEFAULT_LOG_DIR);
    }

    public long getLogPartitionSize() {
        return logPartitionSize;
    }

    public void setLogPartitionSize(long logPartitionSize) {
        this.logPartitionSize = logPartitionSize;
    }

    public String getLogFilePrefix() {
        return logFilePrefix;
    }

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }

    public String getLogDir() {
        return logDir;
    }

    public int getLogHeaderSize() {
        return logHeaderSize;
    }

    public int getLogChecksumSize() {
        return logTailSize;
    }

    public int getTotalLogRecordLength(int logContentSize) {
        return logContentSize + logHeaderSize + logTailSize;
    }

    public int getLogPageSize() {
        return logPageSize;
    }

    public void setLogPageSize(int logPageSize) {
        this.logPageSize = logPageSize;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("log_dir_ : " + logDir + FileUtil.lineSeparator);
        builder.append("log_file_prefix" + logFilePrefix + FileUtil.lineSeparator);
        builder.append("log_page_size : " + logPageSize + FileUtil.lineSeparator);
        builder.append("num_log_pages : " + numLogPages + FileUtil.lineSeparator);
        builder.append("log_partition_size : " + logPartitionSize + FileUtil.lineSeparator);
        builder.append("group_commit_wait_period : " + groupCommitWaitPeriod + FileUtil.lineSeparator);
        return builder.toString();
    }

    public void setNumLogPages(int numLogPages) {
        this.numLogPages = numLogPages;
    }

    public int getNumLogPages() {
        return numLogPages;
    }

    public void setLogBufferSize(int logBufferSize) {
        this.logBufferSize = logBufferSize;
    }

    public int getLogBufferSize() {
        return logBufferSize;
    }

    public long getGroupCommitWaitPeriod() {
        return groupCommitWaitPeriod;
    }

    public void setGroupCommitWaitPeriod(long groupCommitWaitPeriod) {
        this.groupCommitWaitPeriod = groupCommitWaitPeriod;
    }

}
