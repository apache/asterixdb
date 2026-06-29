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
import org.apache.hyracks.util.annotations.AiProvenance;

public class IcebergConverterContext extends ParserContext {

    private final boolean decimalToDouble;
    private final boolean dateAsInt;
    private final boolean timeAsInt;
    private final boolean timestampAsLong;
    private final ZoneId timeZoneId;
    private final int maxVariantDepth;

    @AiProvenance(agent = AiProvenance.Agent.CLAUDE_SONNET_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.REFACTORED, notes = "Reads the variantDepth WITH-clause option (default 500), used by IcebergParquetDataParser's Variant nesting depth guard. Mirrors the timezone handling below: an empty value is treated as absent and falls back to the default, since Map.getOrDefault only falls back on an absent key, not an empty value")
    public IcebergConverterContext(Map<String, String> configuration) {
        decimalToDouble = Boolean.parseBoolean(configuration.getOrDefault(DECIMAL_TO_DOUBLE, FALSE));
        dateAsInt = Boolean.parseBoolean(configuration.getOrDefault(DATE_AS_INT, FALSE));
        timeAsInt = Boolean.parseBoolean(configuration.getOrDefault(TIME_AS_INT, FALSE));
        timestampAsLong = Boolean.parseBoolean(configuration.getOrDefault(TIMESTAMP_AS_LONG, FALSE));

        String configuredVariantDepth = configuration.get(VARIANT_DEPTH);
        maxVariantDepth = (configuredVariantDepth != null && !configuredVariantDepth.isEmpty())
                ? Integer.parseInt(configuredVariantDepth) : DEFAULT_VARIANT_DEPTH;

        String configuredTimeZoneId = configuration.get(ExternalDataConstants.IcebergOptions.TIMEZONE);
        if (configuredTimeZoneId != null && !configuredTimeZoneId.isEmpty()) {
            timeZoneId = TimeZone.getTimeZone(configuredTimeZoneId).toZoneId();
        } else {
            timeZoneId = null;
        }
    }

    public boolean isDecimalToDoubleEnabled() {
        return decimalToDouble;
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
