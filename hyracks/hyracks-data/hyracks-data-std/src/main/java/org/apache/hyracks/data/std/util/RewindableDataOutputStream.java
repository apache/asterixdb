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

package org.apache.hyracks.data.std.util;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.OutputStream;

public class RewindableDataOutputStream extends DataOutputStream {
    /**
     * Creates a new data output stream to write data to the specified
     * underlying output stream. The counter <code>written</code> is
     * set to zero.
     *
     * @param out the underlying output stream, to be saved for later
     *            use.
     * @see FilterOutputStream#out
     */
    public RewindableDataOutputStream(OutputStream out) {
        super(out);
    }

    /**
     * Rewind the current position by {@code delta} to a previous position.
     * This function is used to drop the already written delta bytes.
     * In some cases, we write some bytes, and afterward we found we've written more than expected.
     * Then we need to fix the position by rewind the current position to the expected one.
     * Currently, it is used by the {@link AbstractVarLenObjectBuilder} which may take more space than required
     * at beginning, and it will shift the data and fix the position whenever required.
     *
     * @param delta
     */
    public void rewindWrittenBy(int delta) {
        if (written < delta) {
            throw new IndexOutOfBoundsException();
        }
        written -= delta;
    }
}
