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

package org.apache.hyracks.storage.common.util;

import java.util.List;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.ISearchPredicate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IndexCursorUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    private IndexCursorUtils() {
    }

    /**
     * Close the IIndexCursor and suppress any Throwable thrown by the close call.
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
            } catch (Throwable th) { // NOSONAR Will be suppressed
                try {
                    LOGGER.log(Level.WARN, "Failure closing a cursor", th);
                } catch (Throwable loggingFailure) { // NOSONAR: Ignore catching Throwable
                    // NOSONAR ignore logging failure
                }
                root = ExceptionUtils.suppress(root, th); // NOSONAR: Using the same variable is not bad in this context
            }
        }
        return root;
    }

    public static void open(List<IIndexAccessor> accessors, IIndexCursor[] cursors, ISearchPredicate pred)
            throws HyracksDataException {
        int opened = 0;
        try {
            for (int i = 0; i < cursors.length; i++) {
                if (accessors.get(i) != null) {
                    accessors.get(i).search(cursors[i], pred);
                }
                opened++;
            }
        } catch (Throwable th) { // NOSONAR: Much catch all failures
            for (int j = 0; j < opened; j++) {
                IndexCursorUtils.close(cursors[j], th);
            }
            throw HyracksDataException.create(th);
        }
    }

    public static void open(IIndexAccessor[] accessors, IIndexCursor[] cursors, ISearchPredicate pred)
            throws HyracksDataException {
        int opened = 0;
        try {
            for (int i = 0; i < accessors.length; i++) {
                if (accessors[i] != null) {
                    accessors[i].search(cursors[i], pred);
                }
                opened++;
            }
        } catch (Throwable th) { // NOSONAR: Much catch all failures
            for (int j = 0; j < opened; j++) {
                IndexCursorUtils.close(cursors[j], th);
            }
            throw HyracksDataException.create(th);
        }
    }

    public static Throwable close(IIndexCursor[] cursors, Throwable th) {
        for (int j = 0; j < cursors.length; j++) {
            th = IndexCursorUtils.close(cursors[j], th); // NOSONAR: Using the same variable is cleaner in this context
        }
        return th;
    }

}
