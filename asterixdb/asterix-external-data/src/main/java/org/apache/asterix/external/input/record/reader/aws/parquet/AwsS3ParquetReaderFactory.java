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
package org.apache.asterix.external.input.record.reader.aws.parquet;

import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.input.HDFSDataSourceFactory;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory.IncludeExcludeMatcher;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.asterix.external.util.aws.s3.S3Constants;
import org.apache.asterix.external.util.aws.s3.S3Utils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.util.ExceptionUtils;

import com.amazonaws.SdkBaseException;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AwsS3ParquetReaderFactory extends HDFSDataSourceFactory {
    private static final long serialVersionUID = -6140824803254158253L;
    private static final List<String> recordReaderNames =
            Collections.singletonList(ExternalDataConstants.KEY_ADAPTER_NAME_AWS_S3);

    @Override
    public void configure(IServiceContext serviceCtx, Map<String, String> configuration,
            IWarningCollector warningCollector) throws AlgebricksException, HyracksDataException {
        //Get path
        String path = configuration.containsKey(ExternalDataConstants.KEY_PATH)
                ? configuration.get(ExternalDataConstants.KEY_PATH) : buildPathURIs(configuration, warningCollector);
        //Put S3 configurations to AsterixDB's Hadoop configuration
        putS3ConfToHadoopConf(configuration, path);

        //Configure Hadoop S3 input splits
        try {
            JobConf conf = createHdfsConf(serviceCtx, configuration);
            int numberOfPartitions = getPartitionConstraint().getLocations().length;
            S3Utils.configureAwsS3HdfsJobConf(conf, configuration, numberOfPartitions);
            configureHdfsConf(conf, configuration);
        } catch (SdkException | SdkBaseException ex) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(ex));
        } catch (AlgebricksException ex) {
            Throwable root = ExceptionUtils.getRootCause(ex);
            if (root instanceof SdkException || root instanceof SdkBaseException) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_SOURCE_ERROR, getMessageOrToString(root));
            }
            throw ex;
        }
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    @Override
    public Set<String> getReaderSupportedFormats() {
        return Collections.singleton(ExternalDataConstants.FORMAT_PARQUET);
    }

    /**
     * Prepare Hadoop configurations to read parquet files
     *
     * @param path Comma-delimited paths
     */
    private static void putS3ConfToHadoopConf(Map<String, String> configuration, String path) {
        configuration.put(ExternalDataConstants.KEY_PATH, path);
        configuration.put(ExternalDataConstants.KEY_INPUT_FORMAT, ExternalDataConstants.INPUT_FORMAT_PARQUET);
        configuration.put(ExternalDataConstants.KEY_PARSER, ExternalDataConstants.FORMAT_NOOP);
    }

    /**
     * Build S3 path-style for the requested files
     *
     * @param configuration    properties
     * @param warningCollector warning collector
     * @return Comma-delimited paths (e.g., "s3a://bucket/file1.parquet,s3a://bucket/file2.parquet")
     * @throws CompilationException Compilation exception
     */
    private static String buildPathURIs(Map<String, String> configuration, IWarningCollector warningCollector)
            throws CompilationException {
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        IncludeExcludeMatcher includeExcludeMatcher = ExternalDataUtils.getIncludeExcludeMatchers(configuration);
        List<S3Object> filesOnly = S3Utils.listS3Objects(configuration, includeExcludeMatcher, warningCollector);
        StringBuilder builder = new StringBuilder();

        if (!filesOnly.isEmpty()) {
            appendFileURI(builder, container, filesOnly.get(0));
            for (int i = 1; i < filesOnly.size(); i++) {
                builder.append(',');
                appendFileURI(builder, container, filesOnly.get(i));
            }
        }

        return builder.toString();
    }

    private static void appendFileURI(StringBuilder builder, String container, S3Object file) {
        builder.append(S3Constants.HADOOP_S3_PROTOCOL);
        builder.append("://");
        builder.append(container);
        builder.append('/');
        builder.append(file.key());
    }
}
