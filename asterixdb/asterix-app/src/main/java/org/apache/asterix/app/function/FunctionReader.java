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
package org.apache.asterix.app.function;

import java.io.IOException;

import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.dataflow.AbstractFeedDataFlowController;
import org.apache.asterix.external.util.FeedLogManager;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public abstract class FunctionReader implements IRecordReader<char[]> {

    @Override
    public void close() throws IOException {
        // No Op for function reader
    }

    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public void setController(AbstractFeedDataFlowController controller) {
        // No Op for function reader
    }

    @Override
    public void setFeedLogManager(FeedLogManager feedLogManager) throws HyracksDataException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean handleException(Throwable th) {
        return false;
    }

}
