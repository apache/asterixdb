/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hyracks.data.std.util;

import java.io.IOException;

import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class UTF8StringBuilder extends AbstractVarLenObjectBuilder {

    public void appendChar(char ch) throws IOException {
        UTF8StringUtil.writeCharAsModifiedUTF8(ch, out);
    }

    public void appendString(String string) throws IOException {
        for (int i = 0; i < string.length(); i++) {
            appendChar(string.charAt(i));
        }
    }

    public void appendUtf8StringPointable(UTF8StringPointable src, int byteStartOffset, int byteLength)
            throws IOException {
        out.write(src.getByteArray(), byteStartOffset, byteLength);
    }

    public void appendUtf8StringPointable(UTF8StringPointable src) throws IOException {
        appendUtf8StringPointable(src, src.getCharStartOffset(), src.getUTF8Length());
    }
}
