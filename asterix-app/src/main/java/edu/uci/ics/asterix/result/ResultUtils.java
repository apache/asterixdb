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
package edu.uci.ics.asterix.result;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sun.el.parser.ParseException;

import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ResultUtils {
    public static JSONArray getJSONFromBuffer(ByteBuffer buffer, IFrameTupleAccessor fta) throws HyracksDataException {
        JSONArray resultRecords = new JSONArray();
        ByteBufferInputStream bbis = new ByteBufferInputStream();

        try {
            fta.reset(buffer);
            for (int tIndex = 0; tIndex < fta.getTupleCount(); tIndex++) {
                int start = fta.getTupleStartOffset(tIndex);
                int length = fta.getTupleEndOffset(tIndex) - start;
                bbis.setByteBuffer(buffer, start);
                byte[] recordBytes = new byte[length];
                bbis.read(recordBytes, 0, length);
                resultRecords.put(new String(recordBytes, 0, length));
            }
        } finally {
            try {
                bbis.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
        return resultRecords;
    }

    public static JSONObject getErrorResponse(int errorCode, String errorMessage) {
        JSONObject errorResp = new JSONObject();
        JSONArray errorArray = new JSONArray();
        errorArray.put(errorCode);
        errorArray.put(errorMessage);
        try {
            errorResp.put("error-code", errorArray);
        } catch (JSONException e) {
            // TODO(madhusudancs): Figure out what to do when JSONException occurs while building the results.
        }
        return errorResp;
    }

    public static void prettyPrintHTML(PrintWriter out, JSONObject jsonResultObj) {
        JSONArray resultsWrapper;
        JSONArray resultsArray;
        try {
            resultsWrapper = jsonResultObj.getJSONArray("results");
            for (int i = 0; i < resultsWrapper.length(); i++) {
                resultsArray = resultsWrapper.getJSONArray(i);
                for (int j = 0; j < resultsArray.length(); j++) {
                    out.print(resultsArray.getString(j));
                }
            }
        } catch (JSONException e) {
            // TODO(madhusudancs): Figure out what to do when JSONException occurs while building the results.
        }
    }

    public static void webUIErrorHandler(PrintWriter out, Exception e) {
        String errPrefix = "";
        if (e instanceof AlgebricksException) {
            errPrefix = "Compilation error: ";
        } else if (e instanceof HyracksDataException) {
            errPrefix = "Runtime error: ";
        }

        Throwable cause = getRootCause(e);

        GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
        out.println("<pre class=\"error\">");
        out.println(errPrefix + cause.getMessage());
        out.println("</pre>");
    }

    public static void apiErrorHandler(PrintWriter out, Exception e) {
        int errorCode = 99;
        String errPrefix = "";
        if (e instanceof ParseException) {
            errorCode = 2;
        } else if (e instanceof AlgebricksException) {
            errorCode = 3;
            errPrefix = "Compilation error: ";
        } else if (e instanceof HyracksDataException) {
            errorCode = 4;
            errPrefix = "Runtime error: ";
        }

        Throwable cause = getRootCause(e);

        GlobalConfig.ASTERIX_LOGGER.log(Level.SEVERE, e.getMessage(), e);
        JSONObject errorResp = ResultUtils.getErrorResponse(errorCode, errPrefix + cause.getMessage());
        out.write(errorResp.toString());
    }

    public static String buildParseExceptionMessage(Throwable e, String query) {
        StringBuilder errorMessage = new StringBuilder();
        String message = e.getMessage();
        message = message.replace("<", "&lt");
        message = message.replace(">", "&gt");
        errorMessage.append("SyntaxError:" + message + "\n");
        int pos = message.indexOf("line");
        if (pos > 0) {
            int columnPos = message.indexOf(",", pos + 1 + "line".length());
            int lineNo = Integer.parseInt(message.substring(pos + "line".length() + 1, columnPos));
            String[] lines = query.split("\n");
            if (lineNo >= lines.length) {
                errorMessage.append("===> &ltBLANK LINE&gt \n");
            } else {
                String line = lines[lineNo - 1];
                errorMessage.append("==> " + line);
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
}
