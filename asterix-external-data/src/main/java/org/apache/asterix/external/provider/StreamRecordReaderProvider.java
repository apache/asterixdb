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
package org.apache.asterix.external.provider;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.external.api.AsterixInputStream;
import org.apache.asterix.external.input.record.reader.stream.EmptyLineSeparatedRecordReader;
import org.apache.asterix.external.input.record.reader.stream.LineRecordReader;
import org.apache.asterix.external.input.record.reader.stream.QuotedLineRecordReader;
import org.apache.asterix.external.input.record.reader.stream.SemiStructuredRecordReader;
import org.apache.asterix.external.input.record.reader.stream.StreamRecordReader;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.asterix.external.util.ExternalDataUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class StreamRecordReaderProvider {
    public enum Format {
        SEMISTRUCTURED,
        CSV,
        LINE_SEPARATED
    }

    public static Format getReaderFormat(Map<String, String> configuration) throws AsterixException {
        String format = configuration.get(ExternalDataConstants.KEY_FORMAT);
        if (format != null) {
            switch (format) {
                case ExternalDataConstants.FORMAT_ADM:
                case ExternalDataConstants.FORMAT_JSON:
                case ExternalDataConstants.FORMAT_SEMISTRUCTURED:
                    return Format.SEMISTRUCTURED;
                case ExternalDataConstants.FORMAT_LINE_SEPARATED:
                    return Format.LINE_SEPARATED;
                case ExternalDataConstants.FORMAT_DELIMITED_TEXT:
                case ExternalDataConstants.FORMAT_CSV:
                    return Format.CSV;
            }
            throw new AsterixException("Unknown format: " + format);
        }
        throw new AsterixException("Unspecified paramter: " + ExternalDataConstants.KEY_FORMAT);
    }

    public static StreamRecordReader createRecordReader(Format format, AsterixInputStream inputStream,
            Map<String, String> configuration) throws HyracksDataException {
        switch (format) {
            case CSV:
                String quoteString = configuration.get(ExternalDataConstants.KEY_QUOTE);
                boolean hasHeader = ExternalDataUtils.hasHeader(configuration);
                if (quoteString != null) {
                    return new QuotedLineRecordReader(hasHeader, inputStream, quoteString);
                } else {
                    return new LineRecordReader(hasHeader, inputStream);
                }
            case LINE_SEPARATED:
                return new EmptyLineSeparatedRecordReader(inputStream);
            case SEMISTRUCTURED:
                return new SemiStructuredRecordReader(inputStream,
                        configuration.get(ExternalDataConstants.KEY_RECORD_START),
                        configuration.get(ExternalDataConstants.KEY_RECORD_END));
            default:
                throw new HyracksDataException("Unknown format: " + format);
        }
    }
}
