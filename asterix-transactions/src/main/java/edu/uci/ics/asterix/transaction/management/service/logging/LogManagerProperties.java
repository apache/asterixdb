/*
 * Copyright 2009-2012 by The Regents of the University of California
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

public class LogManagerProperties implements Serializable {

    private static final long serialVersionUID = 2084227360840799662L;

    public static final int LOG_MAGIC_NUMBER = 123456789;
    public static final String LOG_DIR_SUFFIX_KEY = ".txnLogDir";
    public static final String LOG_PAGE_SIZE_KEY = "log_page_size";
    public static final String LOG_PARTITION_SIZE_KEY = "log_partition_size";
    public static final String NUM_LOG_PAGES_KEY = "num_log_pages";
    public static final String LOG_FILE_PREFIX_KEY = "log_file_prefix";
    public static final String GROUP_COMMIT_WAIT_PERIOD_KEY = "group_commit_wait_period";

    private static final int DEFAULT_LOG_PAGE_SIZE = 128 * 1024; //128KB
    private static final int DEFAULT_NUM_LOG_PAGES = 8;
    private static final long DEFAULT_LOG_PARTITION_SIZE = (long) 1024 * 1024 * 1024 * 2; //2GB 
    private static final long DEFAULT_GROUP_COMMIT_WAIT_PERIOD = 1; // time in millisec.
    private static final String DEFAULT_LOG_FILE_PREFIX = "asterix_transaction_log";
    private static final String DEFAULT_LOG_DIRECTORY = "asterix_logs/";

    // follow the naming convention <logFilePrefix>_<number> where number starts from 0
    private final String logFilePrefix;
    private final String logDir;
    public String logDirKey;

    // number of log pages in the log buffer
    private final int logPageSize;
    // number of log pages in the log buffer.
    private final int numLogPages;
    // time in milliseconds
    private final long groupCommitWaitPeriod;
    // logBufferSize = logPageSize * numLogPages;
    private final int logBufferSize;
    // maximum size of each log file
    private final long logPartitionSize;

    public LogManagerProperties(Properties properties, String nodeId) {
        this.logDirKey = new String(nodeId + LOG_DIR_SUFFIX_KEY);
        this.logPageSize = Integer.parseInt(properties.getProperty(LOG_PAGE_SIZE_KEY, "" + DEFAULT_LOG_PAGE_SIZE));
        this.numLogPages = Integer.parseInt(properties.getProperty(NUM_LOG_PAGES_KEY, "" + DEFAULT_NUM_LOG_PAGES));
        long logPartitionSize = Long.parseLong(properties.getProperty(LOG_PARTITION_SIZE_KEY, ""
                + DEFAULT_LOG_PARTITION_SIZE));
        this.logDir = properties.getProperty(logDirKey, DEFAULT_LOG_DIRECTORY + nodeId);
        this.logFilePrefix = properties.getProperty(LOG_FILE_PREFIX_KEY, DEFAULT_LOG_FILE_PREFIX);
        this.groupCommitWaitPeriod = Long.parseLong(properties.getProperty(GROUP_COMMIT_WAIT_PERIOD_KEY, ""
                + DEFAULT_GROUP_COMMIT_WAIT_PERIOD));

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

    public long getGroupCommitWaitPeriod() {
        return groupCommitWaitPeriod;
    }

    public String getLogDirKey() {
        return logDirKey;
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
}
