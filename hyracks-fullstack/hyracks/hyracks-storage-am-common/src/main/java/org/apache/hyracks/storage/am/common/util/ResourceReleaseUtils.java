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
package org.apache.hyracks.storage.am.common.util;

import java.util.List;

import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceReleaseUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    private ResourceReleaseUtils() {
    }

    /**
     * Close the cursor and suppress any Throwable thrown by the close call.
     * This method must NEVER throw any Throwable
     *
     * @param cursor
     *            the cursor to close
     * @param root
     *            the first exception encountered during release of resources
     * @return the root Throwable if not null or a new Throwable if any was thrown, otherwise, it returns null
     */
    public static Throwable close(IIndexCursor cursor, Throwable root) {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Throwable th) { // NOSONAR Will be re-thrown
                try {
                    LOGGER.log(Level.WARN, "Failure closing a closeable resource", th);
                } catch (Throwable loggingFailure) {
                    // Do nothing
                }
                root = ExceptionUtils.suppress(root, th);
            }
        }
        return root;
    }

    /**
     * Close the IIndexDataflowHelper and suppress any Throwable thrown by the close call.
     * This method must NEVER throw any Throwable
     *
     * @param indexHelper
     *            the indexHelper to close
     * @param root
     *            the first exception encountered during release of resources
     * @return the root Throwable if not null or a new Throwable if any was thrown, otherwise, it returns null
     */
    public static Throwable close(IIndexDataflowHelper indexHelper, Throwable root) {
        if (indexHelper != null) {
            try {
                indexHelper.close();
            } catch (Throwable th) { // NOSONAR Will be re-thrown
                try {
                    LOGGER.log(Level.WARN, "Failure closing a closeable resource", th);
                } catch (Throwable loggingFailure) {
                    // Do nothing
                }
                root = ExceptionUtils.suppress(root, th);
            }
        }
        return root;
    }

    /**
     * Close the IIndexDataflowHelpers and suppress any Throwable thrown by any close call.
     * This method must NEVER throw any Throwable
     *
     * @param indexHelpers
     *            the indexHelpers to close
     * @param root
     *            the first exception encountered during release of resources
     * @return the root Throwable if not null or a new Throwable if any was thrown, otherwise, it returns null
     */
    public static Throwable close(List<IIndexDataflowHelper> indexHelpers, Throwable root) {
        for (int i = 0; i < indexHelpers.size(); i++) {
            root = close(indexHelpers.get(i), root);
        }
        return root;
    }
}
