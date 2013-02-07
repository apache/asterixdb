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

import java.io.DataInputStream;
import java.nio.ByteBuffer;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;

public class ResultUtils {
    public static JSONArray getJSONFromBuffer(ByteBuffer buffer, FrameTupleAccessor fta,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        JSONArray resultRecords = new JSONArray();
        ByteBufferInputStream bbis = new ByteBufferInputStream();
        DataInputStream di = new DataInputStream(bbis);

        try {
            fta.reset(buffer);
            for (int tIndex = 0; tIndex < fta.getTupleCount(); tIndex++) {
                int start = fta.getTupleStartOffset(tIndex) + fta.getFieldSlotsLength();
                bbis.setByteBuffer(buffer, start);
                Object[] record = new Object[recordDescriptor.getFieldCount()];
                JSONArray resultRecord = new JSONArray();
                for (int i = 0; i < record.length; ++i) {
                    IAObject instance = (IAObject) recordDescriptor.getFields()[i].deserialize(di);
                    resultRecord.put(instance.toJSON());
                }
                resultRecords.put(resultRecord);
            }
        } catch (JSONException e) {
            throw new HyracksDataException(e);
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
}
