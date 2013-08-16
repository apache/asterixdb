/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.pregelix.api.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SerDeUtils {

    public static byte[] serialize(Writable object) throws IOException {
        ByteArrayOutputStream bbos = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(bbos);
        object.write(output);
        return bbos.toByteArray();
    }

    public static void deserialize(Writable object, byte[] buffer) throws IOException {
        ByteArrayInputStream bbis = new ByteArrayInputStream(buffer);
        DataInput input = new DataInputStream(bbis);
        object.readFields(input);
    }

}
