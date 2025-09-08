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
package org.apache.asterix.external.input.record.reader.aws.delta;

import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.defaults.engine.fileio.FileIO;
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO;
import io.delta.kernel.engine.ParquetHandler;

public class DeltaEngine extends DefaultEngine {

    private final FileIO fileIO;
    private final Configuration conf;

    protected DeltaEngine(FileIO fileIO, Configuration conf) {
        super(fileIO);
        this.fileIO = fileIO;
        this.conf = conf;
    }

    public static DeltaEngine create(Configuration configuration) {
        return new DeltaEngine(new HadoopFileIO(configuration), configuration);
    }

    public ParquetHandler getParquetHandler() {
        return new DeltaParquetHandler(this.fileIO, this.conf);
    }

}
