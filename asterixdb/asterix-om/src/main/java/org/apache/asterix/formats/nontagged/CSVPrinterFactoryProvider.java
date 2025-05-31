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
package org.apache.asterix.formats.nontagged;

import static org.apache.asterix.common.utils.CSVConstants.KEY_DELIMITER;
import static org.apache.asterix.common.utils.CSVConstants.KEY_ESCAPE;
import static org.apache.asterix.common.utils.CSVConstants.KEY_FORCE_QUOTE;
import static org.apache.asterix.common.utils.CSVConstants.KEY_NULL_STR;
import static org.apache.asterix.common.utils.CSVConstants.KEY_QUOTE;

import java.util.Collections;
import java.util.Map;

import org.apache.asterix.dataflow.data.nontagged.printers.adm.ShortWithoutTypeInfoPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ABooleanPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ACirclePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADatePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADateTimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADayTimeDurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADoublePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ADurationPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AFloatPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AInt16PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AInt32PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AInt64PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AInt8PrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AIntervalPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ALinePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ANullPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AObjectPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AOptionalFieldPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.APoint3DPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.APointPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.APolygonPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ARecordPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ARectanglePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AStringPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.ATimePrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AUUIDPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AUnionPrinterFactory;
import org.apache.asterix.dataflow.data.nontagged.printers.csv.AYearMonthDurationPrinterFactory;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.data.IPrinterFactory;
import org.apache.hyracks.algebricks.data.IPrinterFactoryProvider;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class CSVPrinterFactoryProvider implements IPrinterFactoryProvider {
    private final ARecordType itemType;
    private final Map<String, String> configuration;
    private final Map<String, String> formatConfigs;
    private final SourceLocation sourceLocation;

    public static final CSVPrinterFactoryProvider INSTANCE =
            new CSVPrinterFactoryProvider(null, Collections.emptyMap(), null, null);

    public static CSVPrinterFactoryProvider createInstance(ARecordType itemType, Map<String, String> configuration,
            Map<String, String> formatConfigs, SourceLocation sourceLocation) {
        return new CSVPrinterFactoryProvider(itemType, configuration, formatConfigs, sourceLocation);
    }

    private CSVPrinterFactoryProvider(ARecordType itemType, Map<String, String> configuration,
            Map<String, String> formatConfigs, SourceLocation sourceLocation) {
        this.itemType = itemType;
        this.configuration = configuration;
        this.formatConfigs = formatConfigs;
        this.sourceLocation = sourceLocation;
    }

    @Override
    public IPrinterFactory getPrinterFactory(Object typeInfo) {
        IAType type = (IAType) typeInfo;

        if (type != null) {
            switch (type.getTypeTag()) {
                case TINYINT:
                    return AInt8PrinterFactory.INSTANCE;
                case SMALLINT:
                    return AInt16PrinterFactory.INSTANCE;
                case INTEGER:
                    return AInt32PrinterFactory.INSTANCE;
                case BIGINT:
                    return AInt64PrinterFactory.INSTANCE;
                case MISSING:
                case NULL:
                    return ANullPrinterFactory.createInstance(configuration.get(KEY_NULL_STR));
                case BOOLEAN:
                    return ABooleanPrinterFactory.INSTANCE;
                case FLOAT:
                    return AFloatPrinterFactory.INSTANCE;
                case DOUBLE:
                    return ADoublePrinterFactory.INSTANCE;
                case TIME:
                    return ATimePrinterFactory.INSTANCE;
                case DATE:
                    return ADatePrinterFactory.INSTANCE;
                case DATETIME:
                    return ADateTimePrinterFactory.INSTANCE;
                case DURATION:
                    return ADurationPrinterFactory.INSTANCE;
                case YEARMONTHDURATION:
                    return AYearMonthDurationPrinterFactory.INSTANCE;
                case DAYTIMEDURATION:
                    return ADayTimeDurationPrinterFactory.INSTANCE;
                case INTERVAL:
                    return AIntervalPrinterFactory.INSTANCE;
                case POINT:
                    return APointPrinterFactory.INSTANCE;
                case POINT3D:
                    return APoint3DPrinterFactory.INSTANCE;
                case LINE:
                    return ALinePrinterFactory.INSTANCE;
                case POLYGON:
                    return APolygonPrinterFactory.INSTANCE;
                case CIRCLE:
                    return ACirclePrinterFactory.INSTANCE;
                case RECTANGLE:
                    return ARectanglePrinterFactory.INSTANCE;
                case STRING:
                    return AStringPrinterFactory.createInstance(configuration.get(KEY_QUOTE),
                            configuration.get(KEY_FORCE_QUOTE), configuration.get(KEY_ESCAPE),
                            configuration.get(KEY_DELIMITER));
                case OBJECT:
                    return new ARecordPrinterFactory((ARecordType) type, itemType, formatConfigs, configuration);
                case ARRAY:
                    throw new NotImplementedException("'OrderedList' type unsupported for CSV output");
                case MULTISET:
                    throw new NotImplementedException("'UnorderedList' type unsupported for CSV output");
                case UNION:
                    if (((AUnionType) type).isUnknownableType()) {
                        return new AOptionalFieldPrinterFactory((AUnionType) type);
                    } else {
                        return new AUnionPrinterFactory((AUnionType) type);
                    }
                case UUID:
                    return AUUIDPrinterFactory.INSTANCE;
                case SHORTWITHOUTTYPEINFO:
                    return ShortWithoutTypeInfoPrinterFactory.INSTANCE;
                case ANY:
                case BINARY:
                case BITARRAY:
                case ENUM:
                case SPARSOBJECT:
                case SYSTEM_NULL:
                case TYPE:
                case UINT16:
                case UINT32:
                case UINT64:
                case UINT8:
                    // These types are not intended to be printed to the user.
                    break;
            }
        }
        return AObjectPrinterFactory.createInstance(itemType, formatConfigs, configuration);

    }
}
