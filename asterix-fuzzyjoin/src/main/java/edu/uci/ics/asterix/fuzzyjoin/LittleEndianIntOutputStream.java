/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package edu.uci.ics.asterix.fuzzyjoin;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LittleEndianIntOutputStream extends FilterOutputStream {
    public LittleEndianIntOutputStream(OutputStream in) {
        super(in);
    }

    public void writeInt(int v) throws IOException {
        write((byte) (0xff & v));
        write((byte) (0xff & (v >> 8)));
        write((byte) (0xff & (v >> 16)));
        write((byte) (0xff & (v >> 24)));
    }
}
