/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.hyracks.api.util;

import java.io.Serializable;
import java.util.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ErrorMessageUtil {

    private static final Logger LOGGER = Logger.getLogger(ErrorMessageUtil.class.getName());
    public static final String NONE = "";

    private ErrorMessageUtil() {

    }

    /**
     * formats a error message
     * Example:
     * formatMessage(HYRACKS, ErrorCode.UNKNOWN, "%1$s -- %2$s", "one", "two") returns "HYR0000: one -- two"
     *
     * @param component
     *            the software component in which the error originated
     * @param errorCode
     *            the error code itself
     * @param message
     *            the user provided error message (a format string as specified in {@link java.util.Formatter})
     * @param params
     *            an array of objects taht will be provided to the {@link java.util.Formatter}
     * @return the formatted string
     */
    public static String formatMessage(String component, int errorCode, String message, Serializable... params) {
        try (Formatter fmt = new Formatter()) {
            if (!NONE.equals(component)) {
                fmt.format("%1$s%2$04d: ", component, errorCode);
            }
            fmt.format(message == null ? "null" : message, (Object[]) params);
            return fmt.out().toString();
        } catch (Exception e) {
            // Do not throw further exceptions during exception processing.
            LOGGER.log(Level.WARNING, e.getLocalizedMessage(), e);
            return e.getMessage();
        }
    }

}
