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

import static org.apache.asterix.external.util.ExternalDataUtils.prepareDeltaTableFormat;
import static org.apache.asterix.external.util.aws.s3.S3Constants.SERVICE_END_POINT_FIELD_NAME;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.external.IExternalFilterEvaluatorFactory;
import org.apache.asterix.external.input.record.reader.aws.parquet.AwsS3ParquetReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;

public class AwsS3DeltaReaderFactory extends AwsS3ParquetReaderFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration,
            IWarningCollector warningCollector, IExternalFilterEvaluatorFactory filterEvaluatorFactory)
            throws AlgebricksException, HyracksDataException {

        Configuration conf = new Configuration();
        conf.set(S3Constants.HADOOP_ACCESS_KEY_ID, configuration.get(S3Constants.ACCESS_KEY_ID_FIELD_NAME));
        conf.set(S3Constants.HADOOP_SECRET_ACCESS_KEY, configuration.get(S3Constants.SECRET_ACCESS_KEY_FIELD_NAME));
        conf.set(S3Constants.HADOOP_REGION, configuration.get(S3Constants.REGION_FIELD_NAME));
        String serviceEndpoint = configuration.get(SERVICE_END_POINT_FIELD_NAME);
        if (serviceEndpoint != null) {
            conf.set(S3Constants.HADOOP_SERVICE_END_POINT, serviceEndpoint);
        }
        String tableMetadataPath = S3Constants.HADOOP_S3_PROTOCOL + "://"
                + configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME) + '/'
                + configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);

        prepareDeltaTableFormat(configuration, conf, tableMetadataPath);
        super.configure(serviceCtx, configuration, warningCollector, filterEvaluatorFactory);
    }

    @Override
    public Set<String> getReaderSupportedFormats() {
        return Collections.singleton(ExternalDataConstants.FORMAT_DELTA);
    }

}
