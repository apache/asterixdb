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
package org.apache.asterix.external.dataset.adapter;

import org.apache.asterix.common.feeds.api.IDataSourceAdapter;
import org.apache.asterix.external.api.IDataFlowController;
import org.apache.hyracks.api.comm.IFrameWriter;

public class GenericAdapter implements IDataSourceAdapter {

    private static final long serialVersionUID = 1L;
    private final IDataFlowController controller;

    public GenericAdapter(IDataFlowController controller) {
        this.controller = controller;
    }

    @Override
    public void start(int partition, IFrameWriter writer) throws Exception {
        controller.start(writer);
    }

    @Override
    public boolean stop() throws Exception {
        return controller.stop();
    }

    @Override
    public boolean handleException(Throwable e) {
        return controller.handleException(e);
    }
}
