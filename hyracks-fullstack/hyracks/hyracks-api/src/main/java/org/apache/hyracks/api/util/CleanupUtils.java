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
package org.apache.hyracks.api.util;

import org.apache.hyracks.api.comm.IFrameWriter;
import org.apache.hyracks.api.dataflow.IDestroyable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CleanupUtils {

    private static final Logger LOGGER = LogManager.getLogger();

    private CleanupUtils() {
    }

    public static Throwable destroy(Throwable root, IDestroyable... destroyables) {
        for (IDestroyable destroyable : destroyables) {
            if (destroyable != null) {
                try {
                    destroyable.destroy();
                } catch (Throwable th) { // NOSONAR. Had to be done to satisfy contracts
                    try {
                        LOGGER.log(ExceptionUtils.causedByInterrupt(th) ? Level.DEBUG : Level.WARN,
                                "Failure destroying a destroyable resource", th);
                    } catch (Throwable ignore) { // NOSONAR: Ignore catching Throwable
                        // NOSONAR Ignore logging failure
                    }
                    root = ExceptionUtils.suppress(root, th); // NOSONAR
                }
            }
        }
        return root;
    }

    /**
     * Close the IFrameWriter and suppress any Throwable thrown by the close call.
     * This method must NEVER throw any Throwable
     *
     * @param writer
     *            the writer to close
     * @param root
     *            the first exception encountered during release of resources
     * @return the root Throwable if not null or a new Throwable if any was thrown, otherwise, it returns null
     */
    public static Throwable close(IFrameWriter writer, Throwable root) {
        if (writer != null) {
            try {
                writer.close();
            } catch (Throwable th) { // NOSONAR Will be suppressed
                try {
                    LOGGER.log(ExceptionUtils.causedByInterrupt(th) ? Level.DEBUG : Level.WARN,
                            "Failure closing a closeable resource of class {}", writer.getClass().getName(), th);
                } catch (Throwable loggingFailure) { // NOSONAR: Ignore catching Throwable
                    // NOSONAR: Ignore logging failure
                }
                root = ExceptionUtils.suppress(root, th); // NOSONAR
            }
        }
        return root;
    }

    /**
     * Fail the IFrameWriter and suppress any Throwable thrown by the fail call.
     * This method must NEVER throw any Throwable
     *
     * @param writer
     *            the writer to fail
     * @param root
     *            the root failure
     */
    public static void fail(IFrameWriter writer, Throwable root) {
        try {
            writer.fail();
        } catch (Throwable th) { // NOSONAR Will be suppressed
            try {
                LOGGER.log(ExceptionUtils.causedByInterrupt(th) ? Level.DEBUG : Level.WARN,
                        "Failure failing " + writer.getClass().getName(), th);
            } catch (Throwable loggingFailure) { // NOSONAR: Ignore catching Throwable
                // NOSONAR ignore logging failure
            }
            ExceptionUtils.suppress(root, th);
        }
    }

    public static Throwable close(AutoCloseable[] closables, Throwable root) {
        return close(closables, root, false);
    }

    public static Throwable closeSilently(AutoCloseable[] closables, Throwable root) {
        return close(closables, root, true);
    }

    /**
     * Close the AutoCloseable and suppress any Throwable thrown by the close call.
     * This method must NEVER throw any Throwable
     *
     * @param closables
     *            the resource to close
     * @param root
     *            the first exception encountered during release of resources
     * @return the root Throwable if not null or a new Throwable if any was thrown, otherwise, it returns null
     */
    private static Throwable close(AutoCloseable[] closables, Throwable root, boolean silent) {
        if (closables != null) {
            for (AutoCloseable closable : closables) {
                root = close(closable, root, silent);
            }
        }
        return root;
    }

    public static Throwable close(AutoCloseable closable, Throwable root) {
        return close(closable, root, false);
    }

    public static Throwable closeSilently(AutoCloseable closable, Throwable root) {
        return close(closable, root, true);
    }

    /**
     * Close the AutoCloseable and suppress any Throwable thrown by the close call.
     * This method must NEVER throw any Throwable
     *
     * @param closable
     *            the resource to close
     * @param root
     *            the first exception encountered during release of resources
     * @return the root Throwable if not null or a new Throwable if any was thrown, otherwise, it returns null
     */
    private static Throwable close(AutoCloseable closable, Throwable root, boolean silent) {
        if (closable != null) {
            try {
                closable.close();
            } catch (Throwable th) { // NOSONAR Will be suppressed
                if (!silent) {
                    try {
                        LOGGER.log(ExceptionUtils.causedByInterrupt(th) ? Level.DEBUG : Level.WARN,
                                "Failure closing a closeable resource {}", closable.getClass().getName(), th);
                    } catch (Throwable loggingFailure) { // NOSONAR: Ignore catching Throwable
                        // NOSONAR ignore logging failure
                    }
                }
                root = ExceptionUtils.suppress(root, th); // NOSONAR
            }
        }
        return root;
    }
}
