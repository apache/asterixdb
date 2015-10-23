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

package org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers;

import java.io.IOException;

import org.apache.hyracks.data.std.util.GrowableArray;
import org.apache.hyracks.data.std.util.UTF8StringBuilder;

public class UTF8NGramToken extends AbstractUTF8Token implements INGramToken {

    public final static char PRECHAR = '#';

    public final static char POSTCHAR = '$';

    protected int numPreChars;
    protected int numPostChars;

    private UTF8StringBuilder builder = new UTF8StringBuilder();

    public UTF8NGramToken(byte tokenTypeTag, byte countTypeTag) {
        super(tokenTypeTag, countTypeTag);
    }

    @Override
    public int getNumPostChars() {
        return numPreChars;
    }

    @Override
    public int getNumPreChars() {
        return numPostChars;
    }

    @Override
    public void serializeToken(GrowableArray out) throws IOException {
        super.serializeToken(builder, out, numPreChars, numPostChars, PRECHAR, POSTCHAR);
    }

    public void setNumPrePostChars(int numPreChars, int numPostChars) {
        this.numPreChars = numPreChars;
        this.numPostChars = numPostChars;
    }
}
