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

import static org.apache.asterix.external.util.aws.s3.S3AuthUtils.configureAwsS3HdfsJobConf;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.hadoop.mapred.JobConf;

public class AwsS3DeltaReaderFactory extends DeltaReaderFactory {
    private static final long serialVersionUID = 1L;
    private static final List<String> RECORD_READER_NAMES =
            Collections.singletonList(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);

    @Override
    protected void configureJobConf(IApplicationContext appCtx, JobConf conf, Map<String, String> configuration)
            throws CompilationException {
        configureAwsS3HdfsJobConf(appCtx, conf, configuration);
    }

    @Override
    protected String getTablePath(Map<String, String> configuration) {
        return S3Utils.getPath(configuration);
    }

    @Override
    public List<String> getRecordReaderNames() {
        return RECORD_READER_NAMES;
    }

}
