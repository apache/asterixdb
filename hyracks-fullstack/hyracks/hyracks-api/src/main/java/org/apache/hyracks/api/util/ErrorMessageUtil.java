/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.hyracks.api.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.apache.hyracks.api.exceptions.IError;
import org.apache.hyracks.api.exceptions.IFormattedException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ErrorMessageUtil {

    private static final Logger LOGGER = LogManager.getLogger();
    public static final String NONE = "";
    private static final String COMMA = ",";

    private ErrorMessageUtil() {

    }

    /**
     * Loads the mapping from error codes to error messages from a properties file.
     * The key of properties is in the form of comma-separated error codes and the value is the corresponding
     * error message template.
     *
     * Example entries in the properties file:
     * 4002=Extension Conflict between %1$s and %2$s both extensions extend %3$s
     * 2,1002 = Type mismatch: function %1$s expects its %2$s input parameter to be type %3$s
     *
     * @param resourceStream,
     *            the input stream of the properties file
     * @return the map that maps error codes (integers) to error message templates (strings).
     */
    public static Map<Integer, String> loadErrorMap(InputStream resourceStream) throws IOException {
        Properties prop = new Properties();
        Map<Integer, String> errorMessageMap = new HashMap<>();
        prop.load(resourceStream);
        prop.forEach((key1, value) -> {
            String key = (String) key1;
            String msg = (String) value;
            if (key.contains(COMMA)) {
                String[] codes = key.split(COMMA);
                for (String code : codes) {
                    errorMessageMap.put(Integer.parseInt(code), msg);
                }
            } else {
                errorMessageMap.put(Integer.parseInt(key), msg);
            }
        });
        return errorMessageMap;
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
     *            the user provided error message (a format string as specified in {@link Formatter})
     * @param sourceLoc
     *            the source location where the error originated
     * @param params
     *            an array of objects taht will be provided to the {@link Formatter}
     * @return the formatted string
     */
    public static String formatMessage(String component, int errorCode, String message, SourceLocation sourceLoc,
            Serializable... params) {
        try (Formatter fmt = new Formatter()) {
            if (!NONE.equals(component)) {
                fmt.format("%1$s%2$04d: ", component, errorCode);

                // if the message is already formatted, just return it
                if (message.startsWith(fmt.toString())) {
                    return message;
                }
            }
            if (message != null) {
                fmt.format(message, (Object[]) params);
            }
            if (sourceLoc != null) {
                fmt.out().append(" (in line ").append(String.valueOf(sourceLoc.getLine())).append(", at column ")
                        .append(String.valueOf(sourceLoc.getColumn())).append(')');
            }
            return fmt.out().toString();
        } catch (Exception e) {
            // Do not throw further exceptions during exception processing.
            final String paramsString = Arrays.toString(params);
            LOGGER.log(Level.WARN, "error formatting {}{}: message {} params {}", component, errorCode, message,
                    paramsString, e);
            return message + "; " + paramsString;
        }
    }

    public static String getMessageNoCode(String component, String message) {
        if (NONE.equals(component)) {
            return message;
        }
        return message.substring(message.indexOf(":") + 2);
    }

    public static String getCauseMessage(Throwable t) {
        if (t instanceof IFormattedException) {
            return t.getMessage();
        }
        return String.valueOf(t);
    }

    public static String[] defineMessageEnumOrdinalMap(IError[] values, String resourcePath) {
        String[] enumMessages = new String[values.length];
        try (InputStream resourceStream = values[0].getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            Map<Integer, String> errorMessageMap = loadErrorMap(resourceStream);
            for (IError error : values) {
                enumMessages[((Enum) error).ordinal()] = errorMessageMap.computeIfAbsent(error.intValue(), intValue -> {
                    throw new IllegalStateException("error message missing for " + error + " (" + intValue + ")");
                });
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return enumMessages;
    }

    public static void writeObjectWithError(IError error, ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeObject(error);
    }

    public static Optional<IError> readObjectWithError(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        try {
            return Optional.ofNullable((IError) in.readObject());
        } catch (IllegalArgumentException e) {
            // this is expected in case of error codes not available in this version; return null
            LOGGER.debug("unable to deserialize error object due to {}, the error reference will be empty",
                    String.valueOf(e));
            return Optional.empty();
        }
    }
}
