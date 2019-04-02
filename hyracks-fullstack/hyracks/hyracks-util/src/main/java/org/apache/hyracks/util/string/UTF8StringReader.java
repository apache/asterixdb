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

package org.apache.hyracks.util.string;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UTFDataFormatException;

public class UTF8StringReader implements Serializable {

    private static final long serialVersionUID = 1L;

    transient byte[] bytearr = null;
    transient char[] chararr = null;

    /**
     * Reads from the
     * stream <code>in</code> a representation
     * of a Unicode character string encoded in
     * <a href="DataInput.html#modified-utf-8">modified UTF-8</a> format;
     * this string of characters is then returned as a <code>String</code>.
     * The details of the modified UTF-8 representation
     * are exactly the same as for the <code>readUTF</code>
     * method of <code>DataInput</code>.
     *
     * @param in
     *            a data input stream.
     * @return a Unicode string.
     * @throws EOFException
     *             if the input stream reaches the end
     *             before all the bytes.
     * @throws IOException
     *             the stream has been closed and the contained
     *             input stream does not support reading after close, or
     *             another I/O error occurs.
     * @throws UTFDataFormatException
     *             if the bytes do not represent a
     *             valid modified UTF-8 encoding of a Unicode string.
     * @see java.io.DataInputStream#readUnsignedShort()
     */
    public final String readUTF(DataInput in) throws IOException {
        return UTF8StringUtil.readUTF8(in, this);
    }
}
