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
package org.apache.asterix.external.indexing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import org.apache.asterix.formats.nontagged.AqlTypeTraitProvider;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.algebricks.runtime.evaluators.TupleFieldEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;

@SuppressWarnings("rawtypes")
public class IndexingConstants {

    public static final String EXTERNAL_FILE_INDEX_NAME_SUFFIX = "FilesIndex";
    public static final String KEY_INPUT_FORMAT = "input-format";
    public static final String INPUT_FORMAT_RC = "rc-input-format";
    public static final String INPUT_FORMAT_RC_FULLY_QUALIFIED = "org.apache.hadoop.hive.ql.io.RCFileInputFormat";

    //Field Types
    public static final IAType FILE_NUMBER_FIELD_TYPE = BuiltinType.AINT32;
    public static final IAType RECORD_OFFSET_FIELD_TYPE = BuiltinType.AINT64;
    public static final IAType ROW_NUMBER_FIELD_TYPE = BuiltinType.AINT32;

    //Comparator Factories
    private static final IBinaryComparatorFactory fileNumberCompFactory = AqlBinaryComparatorFactoryProvider.INSTANCE
            .getBinaryComparatorFactory(BuiltinType.AINT32, true);
    private static final IBinaryComparatorFactory recordOffsetCompFactory = AqlBinaryComparatorFactoryProvider.INSTANCE
            .getBinaryComparatorFactory(BuiltinType.AINT64, true);
    private static final IBinaryComparatorFactory rowNumberCompFactory = AqlBinaryComparatorFactoryProvider.INSTANCE
            .getBinaryComparatorFactory(BuiltinType.AINT32, true);

    private static final IBinaryComparatorFactory[] rCFileRIDComparatorFactories = { fileNumberCompFactory,
            recordOffsetCompFactory, rowNumberCompFactory };
    private static final IBinaryComparatorFactory[] txtSeqFileRIDComparatorFactories = { fileNumberCompFactory,
            recordOffsetCompFactory };

    private static final IBinaryComparatorFactory[] buddyBtreeComparatorFactories = { fileNumberCompFactory };

    //Serdes
    private static ISerializerDeserializer fileNumberSerializerDeserializer;
    private static ISerializerDeserializer recordOffsetSerializerDeserializer;
    private static ISerializerDeserializer rowNumberSerializerDeserializer;

    //Type Traits
    private static ITypeTraits fileNumberTypeTraits;
    private static ITypeTraits recordOffsetTypeTraits;
    private static ITypeTraits rowNumberTypeTraits;

    //IScalarEvaluatorFactories
    private static final IScalarEvaluatorFactory fileNumberEvalFactory;
    private static final IScalarEvaluatorFactory recordOffsetEvalFactory;
    private static final IScalarEvaluatorFactory rowNumberEvalFactory;
    public static final int FILE_NUMBER_FIELD_INDEX = 0;
    public static final int RECORD_OFFSET_FIELD_INDEX = 1;
    public static final int ROW_NUMBER_FIELD_INDEX = 2;

    public static final ArrayList<List<String>> RecordIDFields = new ArrayList<List<String>>();

    static {

        fileNumberSerializerDeserializer = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(FILE_NUMBER_FIELD_TYPE);
        recordOffsetSerializerDeserializer = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(RECORD_OFFSET_FIELD_TYPE);
        rowNumberSerializerDeserializer = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(ROW_NUMBER_FIELD_TYPE);

        fileNumberTypeTraits = AqlTypeTraitProvider.INSTANCE.getTypeTrait(FILE_NUMBER_FIELD_TYPE);
        recordOffsetTypeTraits = AqlTypeTraitProvider.INSTANCE.getTypeTrait(RECORD_OFFSET_FIELD_TYPE);
        rowNumberTypeTraits = AqlTypeTraitProvider.INSTANCE.getTypeTrait(ROW_NUMBER_FIELD_TYPE);

        fileNumberEvalFactory = new TupleFieldEvaluatorFactory(1);
        recordOffsetEvalFactory = new TupleFieldEvaluatorFactory(2);
        rowNumberEvalFactory = new TupleFieldEvaluatorFactory(3);

        // Add field names
        RecordIDFields.add(new ArrayList<String>(Arrays.asList("FileNumber")));
        RecordIDFields.add(new ArrayList<String>(Arrays.asList("RecordOffset")));
        RecordIDFields.add(new ArrayList<String>(Arrays.asList("RowNumber")));
    }

    // This function returns the size of the RID for the passed file input format
    public static int getRIDSize(String fileInputFormat) {
        if (fileInputFormat.equals(INPUT_FORMAT_RC) || fileInputFormat.equals(INPUT_FORMAT_RC_FULLY_QUALIFIED))
            return 3;
        else
            return 2;
    }

    // This function returns the size of the RID for the passed file input format
    public static IBinaryComparatorFactory[] getComparatorFactories(String fileInputFormat) {
        if (fileInputFormat.equals(INPUT_FORMAT_RC) || fileInputFormat.equals(INPUT_FORMAT_RC_FULLY_QUALIFIED))
            return rCFileRIDComparatorFactories;
        else
            return txtSeqFileRIDComparatorFactories;
    }

    public static IAType getFieldType(int fieldNumber) throws AsterixException {
        switch (fieldNumber) {
            case 0:
                return FILE_NUMBER_FIELD_TYPE;
            case 1:
                return RECORD_OFFSET_FIELD_TYPE;
            case 2:
                return ROW_NUMBER_FIELD_TYPE;
            default:
                throw new AsterixException("Unknown external field RID number");
        }
    }

    public static IBinaryComparatorFactory getComparatorFactory(int fieldNumber) throws AsterixException {
        switch (fieldNumber) {
            case 0:
                return fileNumberCompFactory;
            case 1:
                return recordOffsetCompFactory;
            case 2:
                return rowNumberCompFactory;
            default:
                throw new AsterixException("Unknown external field RID number");
        }
    }

    public static ISerializerDeserializer getSerializerDeserializer(int fieldNumber) throws AsterixException {
        switch (fieldNumber) {
            case 0:
                return fileNumberSerializerDeserializer;
            case 1:
                return recordOffsetSerializerDeserializer;
            case 2:
                return rowNumberSerializerDeserializer;
            default:
                throw new AsterixException("Unknown external field RID number");
        }
    }

    public static ITypeTraits getTypeTraits(int fieldNumber) throws AsterixException {
        switch (fieldNumber) {
            case 0:
                return fileNumberTypeTraits;
            case 1:
                return recordOffsetTypeTraits;
            case 2:
                return rowNumberTypeTraits;
            default:
                throw new AsterixException("Unknown external field RID number");
        }
    }

    public static IScalarEvaluatorFactory getEvalFactory(int fieldNumber) throws AsterixException {
        switch (fieldNumber) {
            case 0:
                return fileNumberEvalFactory;
            case 1:
                return recordOffsetEvalFactory;
            case 2:
                return rowNumberEvalFactory;
            default:
                throw new AsterixException("Unknown external field RID number");
        }
    }

    public static IBinaryComparatorFactory[] getBuddyBtreeComparatorFactories() {
        return buddyBtreeComparatorFactories;
    }

    public static int getRIDSize(Map<String, String> properties) {
        return getRIDSize(properties.get(KEY_INPUT_FORMAT));
    }

    public static List<List<String>> getRIDKeys(Map<String, String> properties) {
        String fileInputFormat = properties.get(KEY_INPUT_FORMAT);
        if (fileInputFormat.equals(INPUT_FORMAT_RC) || fileInputFormat.equals(INPUT_FORMAT_RC_FULLY_QUALIFIED))
            return RecordIDFields;
        else
            return RecordIDFields.subList(0, ROW_NUMBER_FIELD_INDEX);
    }
}
