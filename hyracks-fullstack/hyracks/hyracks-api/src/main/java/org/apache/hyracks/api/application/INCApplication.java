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
package org.apache.hyracks.api.application;

import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.io.IFileDeviceResolver;
import org.apache.hyracks.api.job.resource.NodeCapacity;

public interface INCApplication extends IApplication {

    void preStop() throws Exception; //NOSONAR

    NodeCapacity getCapacity();

    /**
     * @return the file device resolver which resolves the relative path of a storage
     *         file into an io device.
     */
    IFileDeviceResolver getFileDeviceResolver();

    void onRegisterNode(CcId ccId) throws Exception;
}
