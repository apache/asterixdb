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
package org.apache.asterix.external.util.google.gcs;

import static org.apache.asterix.external.util.ExternalDataUtils.getPrefix;
import static org.apache.asterix.external.util.ExternalDataUtils.isDeltaTable;
import static org.apache.asterix.external.util.ExternalDataUtils.validateDeltaTableProperties;
import static org.apache.asterix.external.util.ExternalDataUtils.validateIncludeExclude;
import static org.apache.asterix.external.util.google.gcs.GCSAuthUtils.buildClient;
import static org.apache.hyracks.api.util.ExceptionUtils.getMessageOrToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.external.input.record.reader.abstracts.AbstractExternalInputStreamFactory.IncludeExcludeMatcher;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataPrefix;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;

public class GCSUtils {
    private GCSUtils() {
        throw new AssertionError("do not instantiate");

    }

    /**
     * Validate external dataset properties
     *
     * @param appCtx application context
     * @param configuration properties
     * @param srcLoc source location
     * @param collector warning collector
     * @throws CompilationException Compilation exception
     */
    public static void validateProperties(IApplicationContext appCtx, Map<String, String> configuration,
            SourceLocation srcLoc, IWarningCollector collector) throws CompilationException {
        if (isDeltaTable(configuration)) {
            validateDeltaTableProperties(configuration);
        }
        // check if the format property is present
        else if (configuration.get(ExternalDataConstants.KEY_FORMAT) == null) {
            throw new CompilationException(ErrorCode.PARAMETERS_REQUIRED, srcLoc, ExternalDataConstants.KEY_FORMAT);
        }

        validateIncludeExclude(configuration);
        try {
            new ExternalDataPrefix(configuration);
        } catch (AlgebricksException ex) {
            throw new CompilationException(ErrorCode.FAILED_TO_CALCULATE_COMPUTED_FIELDS, ex);
        }

        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);

        try (Storage storage = buildClient(appCtx, configuration)) {
            Storage.BlobListOption limitOption = Storage.BlobListOption.pageSize(1);
            Storage.BlobListOption prefixOption = Storage.BlobListOption.prefix(getPrefix(configuration));
            Page<Blob> items = storage.list(container, limitOption, prefixOption);

            if (!items.iterateAll().iterator().hasNext() && collector.shouldWarn()) {
                Warning warning = Warning.of(srcLoc, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
                collector.warn(warning);
            }
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }
    }

    public static List<Blob> listItems(IApplicationContext appCtx, Map<String, String> configuration,
            IncludeExcludeMatcher includeExcludeMatcher, IWarningCollector warningCollector,
            ExternalDataPrefix externalDataPrefix, IExternalFilterEvaluator evaluator)
            throws CompilationException, HyracksDataException {
        // Prepare to retrieve the objects
        List<Blob> filesOnly = new ArrayList<>();
        String container = configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME);
        Storage.BlobListOption options = Storage.BlobListOption.prefix(ExternalDataUtils.getPrefix(configuration));
        Page<Blob> items;

        try (Storage gcs = buildClient(appCtx, configuration)) {
            items = gcs.list(container, options);
        } catch (Exception ex) {
            throw new CompilationException(ErrorCode.EXTERNAL_SOURCE_ERROR, ex, getMessageOrToString(ex));
        }

        // Collect the paths to files only
        collectAndFilterFiles(items, includeExcludeMatcher.getPredicate(), includeExcludeMatcher.getMatchersList(),
                filesOnly, externalDataPrefix, evaluator, warningCollector);

        // Warn if no files are returned
        if (filesOnly.isEmpty() && warningCollector.shouldWarn()) {
            Warning warning = Warning.of(null, ErrorCode.EXTERNAL_SOURCE_CONFIGURATION_RETURNED_NO_FILES);
            warningCollector.warn(warning);
        }

        return filesOnly;
    }

    /**
     * Excludes paths ending with "/" as that's a directory indicator, we need to return the files only
     *
     * @param items List of returned objects
     */
    private static void collectAndFilterFiles(Page<Blob> items, BiPredicate<List<Matcher>, String> predicate,
            List<Matcher> matchers, List<Blob> filesOnly, ExternalDataPrefix externalDataPrefix,
            IExternalFilterEvaluator evaluator, IWarningCollector warningCollector) throws HyracksDataException {
        for (Blob item : items.iterateAll()) {
            if (ExternalDataUtils.evaluate(item.getName(), predicate, matchers, externalDataPrefix, evaluator,
                    warningCollector)) {
                filesOnly.add(item);
            }
        }
    }

    public static String getPath(Map<String, String> configuration) {
        return GCSConstants.HADOOP_GCS_PROTOCOL + "://"
                + configuration.get(ExternalDataConstants.CONTAINER_NAME_FIELD_NAME) + '/'
                + configuration.get(ExternalDataConstants.DEFINITION_FIELD_NAME);
    }
}
