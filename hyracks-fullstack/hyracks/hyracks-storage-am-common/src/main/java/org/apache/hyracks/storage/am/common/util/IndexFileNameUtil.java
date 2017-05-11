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

package org.apache.hyracks.storage.am.common.util;

import java.io.File;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.FileReference;
import org.apache.hyracks.api.io.FileSplit;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.dataflow.std.file.IFileSplitProvider;

public class IndexFileNameUtil {

    public static final String IO_DEVICE_NAME_PREFIX = "device_id_";

    @Deprecated
    public static String prepareFileName(String path, int ioDeviceId) {
        return path + File.separator + IO_DEVICE_NAME_PREFIX + ioDeviceId;
    }

    public static FileReference getIndexAbsoluteFileRef(IFileSplitProvider fileSplitProvider, int partition,
            IIOManager ioManager) throws HyracksDataException {
        FileSplit split = fileSplitProvider.getFileSplits()[partition];
        return split.getFileReference(ioManager);
    }
}
