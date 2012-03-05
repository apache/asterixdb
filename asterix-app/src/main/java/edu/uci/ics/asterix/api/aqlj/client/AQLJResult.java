/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.api.aqlj.client;

import java.io.IOException;

import edu.uci.ics.asterix.api.aqlj.common.AQLJException;

/**
 * This class is special type of ADMCursor in that it has a result buffer
 * associated with it. It can be thought of as the "base" cursor for some ADM
 * results.
 * 
 * @author zheilbron
 */
public class AQLJResult extends ADMCursor implements IAQLJResult {
    private final ResultBuffer resultBuffer;

    public AQLJResult(ResultBuffer buffer) {
        super(null);
        this.resultBuffer = buffer;
    }

    @Override
    public boolean next() throws AQLJException {
        currentObject = resultBuffer.get();
        if (currentObject == null) {
            return false;
        }
        return true;
    }

    public void close() throws IOException {
        resultBuffer.close();
    }
}
