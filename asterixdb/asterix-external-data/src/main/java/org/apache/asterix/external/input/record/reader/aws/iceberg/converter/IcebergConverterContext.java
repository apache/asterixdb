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
package org.apache.asterix.external.input.record.reader.aws.iceberg.converter;

import static org.apache.asterix.external.util.ExternalDataConstants.FALSE;
import static org.apache.asterix.external.util.ExternalDataConstants.IcebergOptions.DATE_AS_INT;
import static org.apache.asterix.external.util.ExternalDataConstants.IcebergOptions.DECIMAL_TO_DOUBLE;
import static org.apache.asterix.external.util.ExternalDataConstants.IcebergOptions.DEFAULT_VARIANT_DEPTH;
import static org.apache.asterix.external.util.ExternalDataConstants.IcebergOptions.TIMESTAMP_AS_LONG;
import static org.apache.asterix.external.util.ExternalDataConstants.IcebergOptions.TIME_AS_INT;
import static org.apache.asterix.external.util.ExternalDataConstants.IcebergOptions.VARIANT_DEPTH;

import java.time.ZoneId;
import java.util.Map;
import java.util.TimeZone;

import org.apache.asterix.external.parser.jackson.ParserContext;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;

public class IcebergConverterContext extends ParserContext {

    private final boolean decimalToDouble;
    private final boolean dateAsInt;
    private final boolean timeAsInt;
    private final boolean timestampAsLong;
    private final ZoneId timeZoneId;
    private final int maxVariantDepth;
    private final IWarningCollector warningCollector;

    public IcebergConverterContext(Map<String, String> configuration, IWarningCollector warningCollector) {
        this.warningCollector = warningCollector;
        decimalToDouble = Boolean.parseBoolean(configuration.getOrDefault(DECIMAL_TO_DOUBLE, FALSE));
        dateAsInt = Boolean.parseBoolean(configuration.getOrDefault(DATE_AS_INT, FALSE));
        timeAsInt = Boolean.parseBoolean(configuration.getOrDefault(TIME_AS_INT, FALSE));
        timestampAsLong = Boolean.parseBoolean(configuration.getOrDefault(TIMESTAMP_AS_LONG, FALSE));

        String configuredVariantDepth = configuration.get(VARIANT_DEPTH);
        maxVariantDepth = (configuredVariantDepth != null && !configuredVariantDepth.isEmpty())
                ? Integer.parseInt(configuredVariantDepth) : DEFAULT_VARIANT_DEPTH;

        TimeZone timeZone = ExternalDataUtils
                .resolveTimeZoneOrWarn(configuration.get(ExternalDataConstants.KEY_TIMEZONE), this::warn);
        timeZoneId = timeZone == null ? null : timeZone.toZoneId();
    }

    public boolean isDecimalToDoubleEnabled() {
        return decimalToDouble;
    }

    private void warn(Warning warning) {
        if (warningCollector.shouldWarn()) {
            warningCollector.warn(warning);
        }
    }

    public ZoneId getTimeZoneId() {
        return timeZoneId;
    }

    public boolean isTimestampAsLong() {
        return timestampAsLong;
    }

    public boolean isTimeAsInt() {
        return timeAsInt;
    }

    public boolean isDateAsInt() {
        return dateAsInt;
    }

    public int getMaxVariantDepth() {
        return maxVariantDepth;
    }
}
