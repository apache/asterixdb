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
package org.apache.asterix.app.result;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.asterix.api.http.servlet.APIServlet;
import org.apache.asterix.common.app.SessionConfig;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.http.ParseException;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ResultUtil {
    private static final Logger LOGGER = Logger.getLogger(ResultUtil.class.getName());
    public static final Map<Character, String> HTML_ENTITIES = Collections.unmodifiableMap(Stream.of(
            new AbstractMap.SimpleImmutableEntry<>('"', "&quot;"), new AbstractMap.SimpleImmutableEntry<>('&', "&amp;"),
            new AbstractMap.SimpleImmutableEntry<>('<', "&lt;"), new AbstractMap.SimpleImmutableEntry<>('>', "&gt;"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

    private ResultUtil() {

    }

    /**
     * escapes html entities in aString
     *
     * @param aString
     * @return escaped String
     */
    public static String escapeHTML(String aString) {
        String escaped = aString;
        for (Entry<Character, String> entry : HTML_ENTITIES.entrySet()) {
            if (escaped.indexOf(entry.getKey()) >= 0) {
                escaped = escaped.replace(entry.getKey().toString(), entry.getValue());
            }
        }
        return escaped;
    }

    public static void displayResults(ResultReader resultReader, SessionConfig conf, Stats stats,
            ARecordType recordType) throws HyracksDataException {
        new ResultPrinter(conf, stats, recordType).print(resultReader);
    }

    public static void displayResults(String record, SessionConfig conf, Stats stats, ARecordType recordType)
            throws HyracksDataException {
        new ResultPrinter(conf, stats, recordType).print(record);
    }

    public static JSONObject getErrorResponse(int errorCode, String errorMessage, String errorSummary,
            String errorStackTrace) {
        JSONObject errorResp = new JSONObject();
        JSONArray errorArray = new JSONArray();
        errorArray.put(errorCode);
        errorArray.put(errorMessage);
        try {
            errorResp.put("error-code", errorArray);
            if (!"".equals(errorSummary)) {
                errorResp.put("summary", errorSummary);
            } else {
                //parse exception
                errorResp.put("summary", errorMessage);
            }
            errorResp.put("stacktrace", errorStackTrace);
        } catch (JSONException e) {
            LOGGER.warn("Failed to build the result's JSON object", e);
            // TODO(madhusudancs): Figure out what to do when JSONException occurs while building the results.
        }
        return errorResp;
    }

    public static void webUIErrorHandler(PrintWriter out, Exception e) {
        String errorTemplate = readTemplateFile("/webui/errortemplate.html", "%s\n%s\n%s");

        String errorOutput = String.format(errorTemplate, escapeHTML(extractErrorMessage(e)),
                escapeHTML(extractErrorSummary(e)), escapeHTML(extractFullStackTrace(e)));
        out.println(errorOutput);
    }

    public static void webUIParseExceptionHandler(PrintWriter out, Throwable e, String query) {
        String errorTemplate = readTemplateFile("/webui/errortemplate_message.html", "<pre class=\"error\">%s\n</pre>");

        String errorOutput = String.format(errorTemplate, buildParseExceptionMessage(e, query));
        out.println(errorOutput);
    }

    public static void apiErrorHandler(PrintWriter out, Exception e) {
        int errorCode = 99;
        if (e instanceof ParseException) {
            errorCode = 2;
        } else if (e instanceof AlgebricksException) {
            errorCode = 3;
        } else if (e instanceof HyracksDataException) {
            errorCode = 4;
        }

        JSONObject errorResp = ResultUtil.getErrorResponse(errorCode, extractErrorMessage(e), extractErrorSummary(e),
                extractFullStackTrace(e));
        out.write(errorResp.toString());
    }

    public static String buildParseExceptionMessage(Throwable e, String query) {
        StringBuilder errorMessage = new StringBuilder();
        String message = e.getMessage();
        message = message.replace("<", "&lt");
        message = message.replace(">", "&gt");
        errorMessage.append("Error: " + message + "\n");
        int pos = message.indexOf("line");
        if (pos > 0) {
            Pattern p = Pattern.compile("\\d+");
            Matcher m = p.matcher(message);
            if (m.find(pos)) {
                int lineNo = Integer.parseInt(message.substring(m.start(), m.end()));
                String[] lines = query.split("\n");
                if (lineNo > lines.length) {
                    errorMessage.append("===> &ltBLANK LINE&gt \n");
                } else {
                    String line = lines[lineNo - 1];
                    errorMessage.append("==> " + line);
                }
            }
        }
        return errorMessage.toString();
    }

    public static Throwable getRootCause(Throwable cause) {
        Throwable currentCause = cause;
        Throwable nextCause = cause.getCause();
        while (nextCause != null && nextCause != currentCause) {
            currentCause = nextCause;
            nextCause = nextCause.getCause();
        }
        return currentCause;
    }

    /**
     * Extract the message in the root cause of the stack trace:
     *
     * @param e
     * @return error message string.
     */
    private static String extractErrorMessage(Throwable e) {
        Throwable cause = getRootCause(e);
        String fullyQualifiedExceptionClassName = cause.getClass().getName();
        String[] hierarchySplits = fullyQualifiedExceptionClassName.split("\\.");
        //try returning the class without package qualification
        String exceptionClassName = hierarchySplits[hierarchySplits.length - 1];
        String localizedMessage = cause.getLocalizedMessage();
        if (localizedMessage == null) {
            localizedMessage = "Internal error. Please check instance logs for further details.";
        }
        return localizedMessage + " [" + exceptionClassName + "]";
    }

    /**
     * Extract the meaningful part of a stack trace:
     * a. the causes in the stack trace hierarchy
     * b. the top exception for each cause
     *
     * @param e
     * @return the contacted message containing a and b.
     */
    private static String extractErrorSummary(Throwable e) {
        StringBuilder errorMessageBuilder = new StringBuilder();
        Throwable cause = e;
        errorMessageBuilder.append(cause.getLocalizedMessage());
        while (cause != null) {
            StackTraceElement[] stackTraceElements = cause.getStackTrace();
            errorMessageBuilder.append(stackTraceElements.length > 0 ? "\n caused by: " + stackTraceElements[0] : "");
            cause = cause.getCause();
        }
        return errorMessageBuilder.toString();
    }

    /**
     * Extract the full stack trace:
     *
     * @param e
     * @return the string containing the full stack trace of the error.
     */
    public static String extractFullStackTrace(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        e.printStackTrace(printWriter);
        return stringWriter.toString();
    }

    /**
     * Read the template file which is stored as a resource and return its content. If the file does not exist or is
     * not readable return the default template string.
     *
     * @param path
     *            The path to the resource template file
     * @param defaultTemplate
     *            The default template string if the template file does not exist or is not readable
     * @return The template string to be used to render the output.
     */
    //TODO(till|amoudi|mblow|yingyi|ceej|imaxon): path is ignored completely!!
    private static String readTemplateFile(String path, String defaultTemplate) {
        String errorTemplate = defaultTemplate;
        try {
            String resourcePath = "/webui/errortemplate_message.html";
            InputStream is = APIServlet.class.getResourceAsStream(resourcePath);
            InputStreamReader isr = new InputStreamReader(is);
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            errorTemplate = sb.toString();
        } catch (IOException ioe) {
            LOGGER.warn("Unable to read template error message file", ioe);
        }
        return errorTemplate;
    }
}
