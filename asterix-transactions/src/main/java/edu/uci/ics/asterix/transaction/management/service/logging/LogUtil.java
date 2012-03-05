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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.transaction.management.exception.ACIDException;

/**
 * A utility class providing helper methods for the {@link ILogManager}
 */
public class LogUtil {

    private static final Logger LOGGER = Logger.getLogger(LogUtil.class.getName());

    // read the log directory and initialize log anchor to point to the
    // current log partition file and the offset where the log manager shall
    // continue to insert log records.

    public static PhysicalLogLocator initializeLogAnchor(ILogManager logManager) throws ACIDException {
        int fileId = 0;
        long offset = 0;
        LogManagerProperties logManagerProperties = logManager.getLogManagerProperties();
        File logDir = new File(logManagerProperties.getLogDir());
        try {
            if (logDir.exists()) {
                List<String> logFiles = getLogFiles(logManagerProperties);
                if (logFiles == null || logFiles.size() == 0) {
                    FileUtil.createFileIfNotExists(getLogFilePath(logManagerProperties, 0));
                } else {
                    File logFile = new File(LogUtil.getLogFilePath(logManagerProperties,
                            Long.parseLong(logFiles.get(logFiles.size() - 1))));
                    fileId = logFiles.size() - 1;
                    offset = logFile.length();
                }
            } else {
                FileUtil.createNewDirectory(logManagerProperties.getLogDir());
                FileUtil.createFileIfNotExists(getLogFilePath(logManagerProperties, 0));
            }
        } catch (IOException ioe) {
            throw new ACIDException("Unable to initialize log anchor", ioe);
        }
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(" file id :" + fileId + " offset " + offset);
        }
        return new PhysicalLogLocator(fileId, offset, logManager);
    }

    public static List<String> getLogFiles(final LogManagerProperties logManagerProperties) {
        File logDir = new File(logManagerProperties.getLogDir());
        String[] logFiles = new String[0];
        List<String> logFileCollection = new ArrayList<String>();
        if (logDir.exists()) {
            logFiles = logDir.list(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    if (name.startsWith(logManagerProperties.getLogFilePrefix())) {
                        return true;
                    }
                    return false;
                }
            });
        }
        for (String logFile : logFiles) {
            logFileCollection.add(logFile.substring(logManagerProperties.getLogFilePrefix().length() + 1));
        }
        Collections.sort(logFileCollection, new Comparator<String>() {
            @Override
            public int compare(String arg0, String arg1) {
                return Integer.parseInt(arg0) - Integer.parseInt(arg1);
            }
        });
        return logFileCollection;
    }

    public static long getFileId(String logFilePath, LogManagerProperties logManagerProperties) {
        String logFileName = logFilePath;
        if (logFilePath.contains(File.separator)) {
            logFileName = logFilePath.substring(logFilePath.lastIndexOf(File.separator));
        }
        return Long.parseLong(logFileName.substring(logFileName.indexOf(logManagerProperties.getLogFilePrefix())));
    }

    public static String getLogFilePath(LogManagerProperties logManagerProperties, long fileId) {
        return logManagerProperties.getLogDir() + File.separator + logManagerProperties.getLogFilePrefix() + "_"
                + fileId;
    }

    public static LogicalLogLocator getDummyLogicalLogLocator(ILogManager logManager) {
        LogicalLogLocator logicalLogLocator = new LogicalLogLocator(-1, null, -1, logManager);
        return logicalLogLocator;
    }

    /*
     * given a lsn, get the offset within the log file where the corresponding
     * log record is (to be) placed.
     */
    public static long getFileOffset(ILogManager logManager, long lsn) {
        return lsn % logManager.getLogManagerProperties().getLogPartitionSize();
    }

    /*
     * given a lsn, get the file id that contains the log record.
     */
    public static long getFileId(ILogManager logManager, long lsn) {
        return lsn / logManager.getLogManagerProperties().getLogPartitionSize();
    }
}
