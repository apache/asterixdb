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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import edu.uci.ics.asterix.api.common.APIFramework.DisplayFormat;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ByteArrayAccessibleOutputStream;

public class ResultUtils {
    // Default content length is 65536.
    public static final int DEFAULT_BUFFER_SIZE = 65536;

    // About 100 bytes of the response buffer is left for HTTP data.
    public static int HTTP_DATA_LENTH = 100;

    public static void writeResult(PrintWriter out, ResultReader resultReader, int bufferSize, DisplayFormat pdf)
            throws IOException, HyracksDataException {
        JsonFactory jsonFactory = new JsonFactory();
        ByteArrayAccessibleOutputStream baos = new ByteArrayAccessibleOutputStream(bufferSize);
        baos.reset();

        JsonGenerator generator = jsonFactory.createGenerator(baos);

        ByteBuffer buffer = ByteBuffer.allocate(ResultReader.FRAME_SIZE);
        buffer.clear();

        IFrameTupleAccessor fta = resultReader.getFrameTupleAccessor();

        String response;

        generator.writeStartObject();
        generator.writeArrayFieldStart("results");
        while (resultReader.read(buffer) > 0) {
            fta.reset(buffer);
            for (int tIndex = 0; tIndex < fta.getTupleCount(); tIndex++) {
                int start = fta.getTupleStartOffset(tIndex);
                int length = fta.getTupleEndOffset(tIndex) - start;
                if (pdf == DisplayFormat.HTML) {
                    response = new String(buffer.array(), start, length);
                    out.print(response);
                } else {
                    if ((baos.size() + length + HTTP_DATA_LENTH) > bufferSize) {
                        generator.writeEndArray();
                        generator.writeEndObject();
                        generator.close();
                        response = new String(baos.getByteArray(), 0, baos.size());
                        out.print(response);
                        out.flush();
                        baos.reset();
                        generator = jsonFactory.createGenerator(baos);
                        generator.writeStartObject();
                        generator.writeArrayFieldStart("results");
                    }
                    generator.writeUTF8String(buffer.array(), start, length);
                }
            }
            buffer.clear();
        }
        if (pdf != DisplayFormat.HTML) {
            generator.writeEndArray();
            generator.writeEndObject();
            generator.close();
            response = new String(baos.getByteArray(), 0, baos.size());
            out.print(response);
        }

        out.flush();
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
}
