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
package org.apache.asterix.api.http.server;

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

import org.apache.asterix.app.result.ResultHandle;
import org.apache.asterix.app.result.ResultPrinter;
import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.lang.aql.parser.TokenMgrError;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.translator.IStatementExecutor.Stats;
import org.apache.asterix.translator.SessionOutput;
import org.apache.http.ParseException;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.prettyprint.AlgebricksAppendable;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.util.JSONUtil;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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

    public static void printResults(IApplicationContext appCtx, ResultReader resultReader, SessionOutput output,
            Stats stats, ARecordType recordType) throws HyracksDataException {
        new ResultPrinter(appCtx, output, stats, recordType).print(resultReader);
    }

    public static void printResults(IApplicationContext appCtx, String record, SessionOutput output, Stats stats,
            ARecordType recordType) throws HyracksDataException {
        new ResultPrinter(appCtx, output, stats, recordType).print(record);
    }

    public static void printResultHandle(SessionOutput output, ResultHandle handle) throws HyracksDataException {
        try {
            final AlgebricksAppendable app = new AlgebricksAppendable(output.out());
            output.appendHandle(app, handle.toString());
        } catch (AlgebricksException e) {
            LOGGER.warn("error printing handle", e);
        }
    }

    public static void printStatus(SessionOutput output, AbstractQueryApiServlet.ResultStatus rs) {
        try {
            final AlgebricksAppendable app = new AlgebricksAppendable(output.out());
            output.appendStatus(app, rs.str());
        } catch (AlgebricksException e) {
            LOGGER.warn("error printing status", e);
        }
    }

    public static void printStatus(PrintWriter pw, AbstractQueryApiServlet.ResultStatus rs, boolean comma) {
        printField(pw, AbstractQueryApiServlet.ResultFields.STATUS.str(), rs.str(), comma);
    }

    public static void printError(PrintWriter pw, Throwable e) {
        printError(pw, e, true);
    }

    public static void printError(PrintWriter pw, Throwable e, boolean comma) {
        printError(pw, e, 1, comma);
    }

    public static void printError(PrintWriter pw, Throwable e, int code, boolean comma) {
        Throwable rootCause = getRootCause(e);
        String msg = rootCause.getMessage();
        if (!(rootCause instanceof AlgebricksException || rootCause instanceof HyracksException
                || rootCause instanceof TokenMgrError
                || rootCause instanceof org.apache.asterix.aqlplus.parser.TokenMgrError)) {
            msg = rootCause.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
        }
        printError(pw, msg, code, comma);
    }

    public static void printError(PrintWriter pw, String msg, int code, boolean comma) {
        pw.print("\t\"");
        pw.print(AbstractQueryApiServlet.ResultFields.ERRORS.str());
        pw.print("\": [{ \n");
        printField(pw, QueryServiceServlet.ErrorField.CODE.str(), code);
        printField(pw, QueryServiceServlet.ErrorField.MSG.str(), JSONUtil.escape(msg), false);
        pw.print(comma ? "\t}],\n" : "\t}]\n");
    }

    public static void printField(PrintWriter pw, String name, String value) {
        printField(pw, name, value, true);
    }

    public static void printField(PrintWriter pw, String name, long value) {
        printField(pw, name, value, true);
    }

    public static void printField(PrintWriter pw, String name, String value, boolean comma) {
        printFieldInternal(pw, name, "\"" + value + "\"", comma);
    }

    public static void printField(PrintWriter pw, String name, long value, boolean comma) {
        printFieldInternal(pw, name, String.valueOf(value), comma);
    }

    protected static void printFieldInternal(PrintWriter pw, String name, String value, boolean comma) {
        pw.print("\t\"");
        pw.print(name);
        pw.print("\": ");
        pw.print(value);
        if (comma) {
            pw.print(',');
        }
        pw.print('\n');
    }

    public static ObjectNode getErrorResponse(int errorCode, String errorMessage, String errorSummary,
            String errorStackTrace) {
        ObjectMapper om = new ObjectMapper();
        ObjectNode errorResp = om.createObjectNode();
        ArrayNode errorArray = om.createArrayNode();
        errorArray.add(errorCode);
        errorArray.add(errorMessage);
        errorResp.set("error-code", errorArray);
        if (!"".equals(errorSummary)) {
            errorResp.put("summary", errorSummary);
        } else {
            //parse exception
            errorResp.put("summary", errorMessage);
        }
        errorResp.put("stacktrace", errorStackTrace);
        return errorResp;
    }

    public static void webUIErrorHandler(PrintWriter out, Exception e) {
        String errorTemplate = readTemplateFile("/webui/errortemplate.html", "%s\n%s\n%s");

        String errorOutput =
                String.format(errorTemplate, extractErrorMessage(e), extractErrorSummary(e), extractFullStackTrace(e));
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

        ObjectNode errorResp = ResultUtil.getErrorResponse(errorCode, extractErrorMessage(e), extractErrorSummary(e),
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
            InputStream is = ResultUtil.class.getResourceAsStream(resourcePath);
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

    public static SessionOutput.ResultDecorator createPreResultDecorator() {
        return new SessionOutput.ResultDecorator() {
            int resultNo = -1;

            @Override
            public AlgebricksAppendable append(AlgebricksAppendable app) throws AlgebricksException {
                app.append("\t\"");
                app.append(AbstractQueryApiServlet.ResultFields.RESULTS.str());
                if (resultNo >= 0) {
                    app.append('-').append(String.valueOf(resultNo));
                }
                ++resultNo;
                app.append("\": ");
                return app;
            }
        };
    }

    public static SessionOutput.ResultDecorator createPostResultDecorator() {
        return app -> app.append("\t,\n");
    }

    public static SessionOutput.ResultAppender createResultHandleAppender(String handleUrl) {
        return (app, handle) -> app.append("\t\"").append(AbstractQueryApiServlet.ResultFields.HANDLE.str())
                .append("\": \"").append(handleUrl).append(handle).append("\",\n");
    }

    public static SessionOutput.ResultAppender createResultStatusAppender() {
        return (app, status) -> app.append("\t\"").append(AbstractQueryApiServlet.ResultFields.STATUS.str())
                .append("\": \"").append(status).append("\",\n");
    }
}
