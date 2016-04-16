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
package org.apache.asterix.result;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.api.common.SessionConfig;
import org.apache.asterix.api.common.SessionConfig.OutputFormat;
import org.apache.asterix.api.http.servlet.APIServlet;
import org.apache.asterix.api.http.servlet.JSONUtil;
import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.om.types.ARecordType;
import org.apache.http.ParseException;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.comm.IFrame;
import org.apache.hyracks.api.comm.IFrameTupleAccessor;
import org.apache.hyracks.api.comm.VSizeFrame;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ResultUtils {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    static Map<Character, String> HTML_ENTITIES = new HashMap<Character, String>();

    static {
        HTML_ENTITIES.put('"', "&quot;");
        HTML_ENTITIES.put('&', "&amp;");
        HTML_ENTITIES.put('<', "&lt;");
        HTML_ENTITIES.put('>', "&gt;");
    }

    public static class Stats {
        public long count;
        public long size;
    }

    public static String escapeHTML(String s) {
        for (Character c : HTML_ENTITIES.keySet()) {
            if (s.indexOf(c) >= 0) {
                s = s.replace(c.toString(), HTML_ENTITIES.get(c));
            }
        }
        return s;
    }

    public static void displayCSVHeader(ARecordType recordType, SessionConfig conf) throws AsterixException {
        if (recordType == null) {
            throw new AsterixException("Cannot output CSV with header without specifying output-record-type");
        }
        // If HTML-ifying, we have to output this here before the header -
        // pretty ugly
        if (conf.is(SessionConfig.FORMAT_HTML)) {
            conf.out().println("<h4>Results:</h4>");
            conf.out().println("<pre>");
        }

        String[] fieldNames = recordType.getFieldNames();
        boolean notfirst = false;
        for (String name : fieldNames) {
            if (notfirst) {
                conf.out().print(',');
            }
            notfirst = true;
            conf.out().print('"');
            conf.out().print(name.replace("\"", "\"\""));
            conf.out().print('"');
        }
        conf.out().print("\r\n");
    }

    public static FrameManager resultDisplayFrameMgr = new FrameManager(ResultReader.FRAME_SIZE);

    public static void displayResults(ResultReader resultReader, SessionConfig conf, Stats stats)
            throws HyracksDataException {
        // Whether we are wrapping the output sequence in an array
        boolean wrap_array = false;
        // Whether this is the first instance being output
        boolean notfirst = false;

        // If we're outputting CSV with a header, the HTML header was already
        // output by displayCSVHeader(), so skip it here
        if (conf.is(SessionConfig.FORMAT_HTML)
                && !(conf.fmt() == OutputFormat.CSV && conf.is(SessionConfig.FORMAT_CSV_HEADER))) {
            conf.out().println("<h4>Results:</h4>");
            conf.out().println("<pre>");
        }

        conf.resultPrefix(conf.out());

        switch (conf.fmt()) {
            case LOSSLESS_JSON:
            case CLEAN_JSON:
            case ADM:
                if (conf.is(SessionConfig.FORMAT_WRAPPER_ARRAY)) {
                    // Conveniently, LOSSLESS_JSON and ADM have the same syntax for an
                    // "ordered list", and our representation of the result of a
                    // statement is an ordered list of instances.
                    conf.out().print("[ ");
                    wrap_array = true;
                }
                break;
            default:
                break;
        }

        final boolean indentJSON = conf.is(SessionConfig.INDENT_JSON);

        final IFrameTupleAccessor fta = resultReader.getFrameTupleAccessor();
        final IFrame frame = new VSizeFrame(resultDisplayFrameMgr);

        while (resultReader.read(frame) > 0) {
            final ByteBuffer frameBuffer = frame.getBuffer();
            final byte[] frameBytes = frameBuffer.array();
            fta.reset(frameBuffer);
            final int last = fta.getTupleCount();
            for (int tIndex = 0; tIndex < last; tIndex++) {
                final int start = fta.getTupleStartOffset(tIndex);
                int length = fta.getTupleEndOffset(tIndex) - start;
                if (conf.fmt() == OutputFormat.CSV) {
                    if ((length > 0) && (frameBytes[start + length - 1] == '\n')) {
                        length--;
                    }
                }
                String result = new String(frameBytes, start, length, UTF_8);
                if (wrap_array && notfirst) {
                    conf.out().print(", ");
                }
                notfirst = true;
                if (indentJSON) {
                    // TODO(tillw): this is inefficient - do this during result generation
                    result = JSONUtil.indent(result, 2);
                }
                conf.out().print(result);
                if (conf.fmt() == OutputFormat.CSV) {
                    conf.out().print("\r\n");
                }
                ++stats.count;
                // TODO(tillw) fix this approximation
                stats.size += result.length();
            }
            frameBuffer.clear();
        }

        conf.out().flush();

        if (wrap_array) {
            conf.out().println(" ]");
        }

        conf.resultPostfix(conf.out());

        if (conf.is(SessionConfig.FORMAT_HTML)) {
            conf.out().println("</pre>");
        }
    }

    public static JSONObject getErrorResponse(int errorCode, String errorMessage, String errorSummary,
            String errorStackTrace) {
        JSONObject errorResp = new JSONObject();
        JSONArray errorArray = new JSONArray();
        errorArray.put(errorCode);
        errorArray.put(errorMessage);
        try {
            errorResp.put("error-code", errorArray);
            if (! "".equals(errorSummary)) {
                errorResp.put("summary", errorSummary);
            } else {
                //parse exception
                errorResp.put("summary", errorMessage);
            }
            errorResp.put("stacktrace", errorStackTrace);
        } catch (JSONException e) {
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

        JSONObject errorResp = ResultUtils.getErrorResponse(errorCode, extractErrorMessage(e), extractErrorSummary(e),
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

    private static Throwable getRootCause(Throwable cause) {
        Throwable nextCause = cause.getCause();
        while (nextCause != null) {
            cause = nextCause;
            nextCause = cause.getCause();
        }
        return cause;
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
            // If there is an IOException reading the error template html file, default value of error template is used.
        }
        return errorTemplate;
    }
}
