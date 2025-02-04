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
package org.apache.asterix.external.input.record.reader.gcs.delta;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.external.input.record.reader.aws.delta.DeltaReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.google.gcs.GCSAuthUtils;
import org.apache.asterix.external.util.google.gcs.GCSUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class GCSDeltaReaderFactory extends DeltaReaderFactory {
    private static final long serialVersionUID = 1L;
    private static final List<String> RECORD_READER_NAMES =
            Collections.singletonList(ExternalDataConstants.KEY_ADAPTER_NAME_GCS);

    @Override
    protected void configureJobConf(IApplicationContext appCtx, JobConf conf, Map<String, String> configuration)
            throws AlgebricksException {
        int numberOfPartitions = getPartitionConstraint().getLocations().length;
        GCSAuthUtils.configureHdfsJobConf(conf, configuration, numberOfPartitions);
    }

    @Override
    protected String getTablePath(Map<String, String> configuration) throws AlgebricksException {
        return GCSUtils.getPath(configuration);
    }

    @Override
    public List<String> getRecordReaderNames() {
        return RECORD_READER_NAMES;
    }

}
