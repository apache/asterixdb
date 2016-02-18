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

package org.apache.asterix.experiment.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class ExperimentLogFormatter extends Formatter {

    // format string for printing the log record
    private static final String format = "{0} {1}\n{2}: {3}\n";
    private final Date dat = new Date();
    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Format the given LogRecord.
     * <p>
     * The formatting can be customized by specifying the <a href="../Formatter.html#syntax">format string</a> in the <a href="#formatting"> {@code java.util.logging.SimpleFormatter.format}</a> property. The given {@code LogRecord} will be formatted as if by calling:
     * 
     * <pre>
     *    {@link String#format String.format}(format, date, source, logger, level, message, thrown);
     * </pre>
     * 
     * where the arguments are:<br>
     * <ol>
     * <li>{@code format} - the {@link java.util.Formatter
     * java.util.Formatter} format string specified in the {@code java.util.logging.SimpleFormatter.format} property or the default format.</li>
     * <li>{@code date} - a {@link Date} object representing {@linkplain LogRecord#getMillis event time} of the log record.</li>
     * <li>{@code source} - a string representing the caller, if available; otherwise, the logger's name.</li>
     * <li>{@code logger} - the logger's name.</li>
     * <li>{@code level} - the {@linkplain Level#getLocalizedName
     * log level}.</li>
     * <li>{@code message} - the formatted log message returned from the {@link Formatter#formatMessage(LogRecord)} method. It uses {@link java.text.MessageFormat java.text} formatting and does not use the {@code java.util.Formatter format} argument.</li>
     * <li>{@code thrown} - a string representing the {@linkplain LogRecord#getThrown throwable} associated with the log record and its backtrace beginning with a newline character, if any; otherwise, an empty string.</li>
     * </ol>
     * <p>
     * Some example formats:<br>
     * <ul>
     * <li> {@code java.util.logging.SimpleFormatter.format="%4$s: %5$s [%1$tc]%n"}
     * <p>
     * This prints 1 line with the log level ({@code 4$}), the log message ({@code 5$}) and the timestamp ({@code 1$}) in a square bracket.
     * 
     * <pre>
     *     WARNING: warning message [Tue Mar 22 13:11:31 PDT 2011]
     * </pre>
     * 
     * </li>
     * <li> {@code java.util.logging.SimpleFormatter.format="%1$tc %2$s%n%4$s: %5$s%6$s%n"}
     * <p>
     * This prints 2 lines where the first line includes the timestamp ({@code 1$}) and the source ({@code 2$}); the second line includes the log level ({@code 4$}) and the log message ({@code 5$}) followed with the throwable and its backtrace ({@code 6$}), if any:
     * 
     * <pre>
     *     Tue Mar 22 13:11:31 PDT 2011 MyClass fatal
     *     SEVERE: several message with an exception
     *     java.lang.IllegalArgumentException: invalid argument
     *             at MyClass.mash(MyClass.java:9)
     *             at MyClass.crunch(MyClass.java:6)
     *             at MyClass.main(MyClass.java:3)
     * </pre>
     * 
     * </li>
     * <li> {@code java.util.logging.SimpleFormatter.format="%1$tb %1$td, %1$tY %1$tl:%1$tM:%1$tS %1$Tp %2$s%n%4$s: %5$s%n"}
     * <p>
     * This prints 2 lines similar to the example above with a different date/time formatting and does not print the throwable and its backtrace:
     * 
     * <pre>
     *     Mar 22, 2011 1:11:31 PM MyClass fatal
     *     SEVERE: several message with an exception
     * </pre>
     * 
     * </li>
     * </ul>
     * <p>
     * This method can also be overridden in a subclass. It is recommended to use the {@link Formatter#formatMessage} convenience method to localize and format the message field.
     * 
     * @param record
     *            the log record to be formatted.
     * @return a formatted log record
     */
    public synchronized String format(LogRecord record) {
        dat.setTime(record.getMillis());
        String source;
        if (record.getSourceClassName() != null) {
            source = record.getSourceClassName();
            if (record.getSourceMethodName() != null) {
                source += " " + record.getSourceMethodName();
            }
        } else {
            source = record.getLoggerName();
        }
        String message = formatMessage(record);
        String throwable = "";
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println();
            record.getThrown().printStackTrace(pw);
            pw.close();
            throwable = sw.toString();
        }
        return MessageFormat.format(format, sdf.format(dat), source, record.getLevel(),
                message, throwable);
    }

}
