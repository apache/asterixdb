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

package org.apache.asterix.metadata.bootstrap;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.metadata.MetadataException;
import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.exceptions.HyracksDataException;

/**
 * Contains static ARecordType's of all metadata record types.
 */
public final class MetadataRecordTypes {
    public static ARecordType DATAVERSE_RECORDTYPE;
    public static ARecordType DATASET_RECORDTYPE;
    public static ARecordType INTERNAL_DETAILS_RECORDTYPE;
    public static ARecordType EXTERNAL_DETAILS_RECORDTYPE;
    public static ARecordType FEED_DETAILS_RECORDTYPE;
    public static ARecordType DATASET_HINTS_RECORDTYPE;
    public static ARecordType COMPACTION_POLICY_PROPERTIES_RECORDTYPE;
    public static ARecordType DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE;
    public static ARecordType FIELD_RECORDTYPE;
    public static ARecordType RECORD_RECORDTYPE;
    public static ARecordType DERIVEDTYPE_RECORDTYPE;
    public static ARecordType DATATYPE_RECORDTYPE;
    public static ARecordType INDEX_RECORDTYPE;
    public static ARecordType NODE_RECORDTYPE;
    public static ARecordType NODEGROUP_RECORDTYPE;
    public static ARecordType FUNCTION_RECORDTYPE;
    public static ARecordType DATASOURCE_ADAPTER_RECORDTYPE;
    public static ARecordType FEED_RECORDTYPE;
    public static ARecordType PRIMARY_FEED_DETAILS_RECORDTYPE;
    public static ARecordType SECONDARY_FEED_DETAILS_RECORDTYPE;
    public static ARecordType FEED_ADAPTER_CONFIGURATION_RECORDTYPE;
    public static ARecordType FEED_POLICY_RECORDTYPE;
    public static ARecordType POLICY_PARAMS_RECORDTYPE;
    public static ARecordType LIBRARY_RECORDTYPE;
    public static ARecordType COMPACTION_POLICY_RECORDTYPE;
    public static ARecordType EXTERNAL_FILE_RECORDTYPE;

    /**
     * Create all metadata record types.
     *
     * @throws HyracksDataException
     */
    public static void init() throws MetadataException, HyracksDataException {
        // Attention: The order of these calls is important because some types
        // depend on other types being created first.
        // These calls are one "dependency chain".
        try {
            POLICY_PARAMS_RECORDTYPE = createPropertiesRecordType();
            DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE = createPropertiesRecordType();
            COMPACTION_POLICY_PROPERTIES_RECORDTYPE = createPropertiesRecordType();
            INTERNAL_DETAILS_RECORDTYPE = createInternalDetailsRecordType();
            EXTERNAL_DETAILS_RECORDTYPE = createExternalDetailsRecordType();
            FEED_DETAILS_RECORDTYPE = createFeedDetailsRecordType();
            DATASET_HINTS_RECORDTYPE = createPropertiesRecordType();
            DATASET_RECORDTYPE = createDatasetRecordType();

            // Starting another dependency chain.
            FIELD_RECORDTYPE = createFieldRecordType();
            RECORD_RECORDTYPE = createRecordTypeRecordType();
            DERIVEDTYPE_RECORDTYPE = createDerivedTypeRecordType();
            DATATYPE_RECORDTYPE = createDatatypeRecordType();

            // Independent of any other types.
            DATAVERSE_RECORDTYPE = createDataverseRecordType();
            INDEX_RECORDTYPE = createIndexRecordType();
            NODE_RECORDTYPE = createNodeRecordType();
            NODEGROUP_RECORDTYPE = createNodeGroupRecordType();
            FUNCTION_RECORDTYPE = createFunctionRecordType();
            DATASOURCE_ADAPTER_RECORDTYPE = createDatasourceAdapterRecordType();

            FEED_ADAPTER_CONFIGURATION_RECORDTYPE = createPropertiesRecordType();
            PRIMARY_FEED_DETAILS_RECORDTYPE = createPrimaryFeedDetailsRecordType();
            SECONDARY_FEED_DETAILS_RECORDTYPE = createSecondaryFeedDetailsRecordType();
            FEED_RECORDTYPE = createFeedRecordType();
            FEED_POLICY_RECORDTYPE = createFeedPolicyRecordType();
            LIBRARY_RECORDTYPE = createLibraryRecordType();

            COMPACTION_POLICY_RECORDTYPE = createCompactionPolicyRecordType();

            EXTERNAL_FILE_RECORDTYPE = createExternalFileRecordType();

            //generate nested type names
            DATASET_RECORDTYPE.generateNestedDerivedTypeNames();
            DATATYPE_RECORDTYPE.generateNestedDerivedTypeNames();
            DATAVERSE_RECORDTYPE.generateNestedDerivedTypeNames();
            INDEX_RECORDTYPE.generateNestedDerivedTypeNames();
            NODE_RECORDTYPE.generateNestedDerivedTypeNames();
            NODEGROUP_RECORDTYPE.generateNestedDerivedTypeNames();
            FUNCTION_RECORDTYPE.generateNestedDerivedTypeNames();
            DATASOURCE_ADAPTER_RECORDTYPE.generateNestedDerivedTypeNames();
            FEED_RECORDTYPE.generateNestedDerivedTypeNames();
            PRIMARY_FEED_DETAILS_RECORDTYPE.generateNestedDerivedTypeNames();
            SECONDARY_FEED_DETAILS_RECORDTYPE.generateNestedDerivedTypeNames();
            FEED_POLICY_RECORDTYPE.generateNestedDerivedTypeNames();
            LIBRARY_RECORDTYPE.generateNestedDerivedTypeNames();
            COMPACTION_POLICY_RECORDTYPE.generateNestedDerivedTypeNames();
            EXTERNAL_FILE_RECORDTYPE.generateNestedDerivedTypeNames();
        } catch (AsterixException e) {
            throw new MetadataException(e);
        }
    }

    public static final int FEED_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int FEED_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX = 1;
    public static final int FEED_POLICY_ARECORD_DESCRIPTION_FIELD_INDEX = 2;
    public static final int FEED_POLICY_ARECORD_PROPERTIES_FIELD_INDEX = 3;

    private static ARecordType createFeedPolicyRecordType() throws AsterixException {
        AUnorderedListType listPropertiesType = new AUnorderedListType(POLICY_PARAMS_RECORDTYPE, null);
        String[] fieldNames = { "DataverseName", "PolicyName", "Description", "Properties" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, listPropertiesType };
        return new ARecordType("FeedPolicyRecordType", fieldNames, fieldTypes, true);
    }

    // Helper constants for accessing fields in an ARecord of type
    // DataverseRecordType.
    public static final int DATAVERSE_ARECORD_NAME_FIELD_INDEX = 0;
    public static final int DATAVERSE_ARECORD_FORMAT_FIELD_INDEX = 1;
    public static final int DATAVERSE_ARECORD_TIMESTAMP_FIELD_INDEX = 2;
    public static final int DATAVERSE_ARECORD_PENDINGOP_FIELD_INDEX = 3;

    private static final ARecordType createDataverseRecordType() throws AsterixException {
        return new ARecordType("DataverseRecordType",
                new String[] { "DataverseName", "DataFormat", "Timestamp", "PendingOp" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32 },
                true);
    }

    // Helper constants for accessing fields in an ARecord of anonymous type
    // dataset properties.
    // Used for dataset hints or dataset adapter properties.
    public static final int PROPERTIES_NAME_FIELD_INDEX = 0;
    public static final int PROPERTIES_VALUE_FIELD_INDEX = 1;

    private static final ARecordType createPropertiesRecordType() throws AsterixException {
        String[] fieldNames = { "Name", "Value" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    };

    // Helper constants for accessing fields in an ARecord of anonymous type
    // internal details.
    public static final int INTERNAL_DETAILS_ARECORD_FILESTRUCTURE_FIELD_INDEX = 0;
    public static final int INTERNAL_DETAILS_ARECORD_PARTITIONSTRATEGY_FIELD_INDEX = 1;
    public static final int INTERNAL_DETAILS_ARECORD_PARTITIONKEY_FIELD_INDEX = 2;
    public static final int INTERNAL_DETAILS_ARECORD_PRIMARYKEY_FIELD_INDEX = 3;
    public static final int INTERNAL_DETAILS_ARECORD_AUTOGENERATED_FIELD_INDEX = 4;

    private static final ARecordType createInternalDetailsRecordType() throws AsterixException {
        AOrderedListType olType = new AOrderedListType(BuiltinType.ASTRING, null);
        AOrderedListType ololType = new AOrderedListType(olType, null);
        String[] fieldNames = { "FileStructure", "PartitioningStrategy", "PartitioningKey", "PrimaryKey",
                "Autogenerated" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, ololType, ololType, BuiltinType.ABOOLEAN };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    // Helper constants for accessing fields in an ARecord of anonymous type
    // external details.
    public static final int EXTERNAL_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX = 0;
    public static final int EXTERNAL_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX = 1;
    public static final int EXTERNAL_DETAILS_ARECORD_LAST_REFRESH_TIME_FIELD_INDEX = 2;
    public static final int EXTERNAL_DETAILS_ARECORD_TRANSACTION_STATE_FIELD_INDEX = 3;

    private static final ARecordType createExternalDetailsRecordType() throws AsterixException {
        AOrderedListType orderedPropertyListType = new AOrderedListType(DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE, null);
        String[] fieldNames = { "DatasourceAdapter", "Properties", "LastRefreshTime", "TransactionState", };
        IAType[] fieldTypes = { BuiltinType.ASTRING, orderedPropertyListType, BuiltinType.ADATETIME,
                BuiltinType.AINT32 };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    public static final int COMPACTION_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int COMPACTION_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX = 1;
    public static final int COMPACTION_POLICY_ARECORD_CLASSNAME_FIELD_INDEX = 2;

    private static ARecordType createCompactionPolicyRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "CompactionPolicy", "Classname" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING };
        return new ARecordType("CompactionPolicyRecordType", fieldNames, fieldTypes, true);
    }

    public static final int FEED_DETAILS_ARECORD_FILESTRUCTURE_FIELD_INDEX = 0;
    public static final int FEED_DETAILS_ARECORD_PARTITIONSTRATEGY_FIELD_INDEX = 1;
    public static final int FEED_DETAILS_ARECORD_PARTITIONKEY_FIELD_INDEX = 2;
    public static final int FEED_DETAILS_ARECORD_PRIMARYKEY_FIELD_INDEX = 3;
    public static final int FEED_DETAILS_ARECORD_GROUPNAME_FIELD_INDEX = 4;
    public static final int FEED_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX = 5;
    public static final int FEED_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX = 6;
    public static final int FEED_DETAILS_ARECORD_FUNCTION_FIELD_INDEX = 7;
    public static final int FEED_DETAILS_ARECORD_STATE_FIELD_INDEX = 8;
    public static final int FEED_DETAILS_ARECORD_COMPACTION_POLICY_FIELD_INDEX = 9;
    public static final int FEED_DETAILS_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX = 10;

    private static final ARecordType createFeedDetailsRecordType() throws AsterixException {
        AOrderedListType orderedListType = new AOrderedListType(BuiltinType.ASTRING, null);
        AOrderedListType orderedListOfPropertiesType = new AOrderedListType(DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE,
                null);
        AOrderedListType compactionPolicyPropertyListType = new AOrderedListType(
                COMPACTION_POLICY_PROPERTIES_RECORDTYPE, null);
        String[] fieldNames = { "FileStructure", "PartitioningStrategy", "PartitioningKey", "PrimaryKey", "GroupName",
                "DatasourceAdapter", "Properties", "Function", "Status", "CompactionPolicy",
                "CompactionPolicyProperties" };

        AUnionType feedFunctionUnion = AUnionType.createNullableType(BuiltinType.ASTRING);

        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, orderedListType, orderedListType,
                BuiltinType.ASTRING, BuiltinType.ASTRING, orderedListOfPropertiesType, feedFunctionUnion,
                BuiltinType.ASTRING, BuiltinType.ASTRING, compactionPolicyPropertyListType };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    public static final int DATASET_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int DATASET_ARECORD_DATASETNAME_FIELD_INDEX = 1;
    public static final int DATASET_ARECORD_DATATYPEDATAVERSENAME_FIELD_INDEX = 2;
    public static final int DATASET_ARECORD_DATATYPENAME_FIELD_INDEX = 3;
    public static final int DATASET_ARECORD_DATASETTYPE_FIELD_INDEX = 4;
    public static final int DATASET_ARECORD_GROUPNAME_FIELD_INDEX = 5;
    public static final int DATASET_ARECORD_COMPACTION_POLICY_FIELD_INDEX = 6;
    public static final int DATASET_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX = 7;
    public static final int DATASET_ARECORD_INTERNALDETAILS_FIELD_INDEX = 8;
    public static final int DATASET_ARECORD_EXTERNALDETAILS_FIELD_INDEX = 9;
    public static final int DATASET_ARECORD_HINTS_FIELD_INDEX = 10;
    public static final int DATASET_ARECORD_TIMESTAMP_FIELD_INDEX = 11;
    public static final int DATASET_ARECORD_DATASETID_FIELD_INDEX = 12;
    public static final int DATASET_ARECORD_PENDINGOP_FIELD_INDEX = 13;

    private static final ARecordType createDatasetRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "DatasetName", "DatatypeDataverseName", "DatatypeName", "DatasetType",
                "GroupName", "CompactionPolicy", "CompactionPolicyProperties", "InternalDetails", "ExternalDetails",
                "Hints", "Timestamp", "DatasetId", "PendingOp" };

        AUnionType internalRecordUnion = AUnionType.createNullableType(INTERNAL_DETAILS_RECORDTYPE);
        AUnionType externalRecordUnion = AUnionType.createNullableType(EXTERNAL_DETAILS_RECORDTYPE);
        AOrderedListType compactionPolicyPropertyListType = new AOrderedListType(
                COMPACTION_POLICY_PROPERTIES_RECORDTYPE, null);

        AUnorderedListType unorderedListOfHintsType = new AUnorderedListType(DATASET_HINTS_RECORDTYPE, null);

        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, compactionPolicyPropertyListType,
                internalRecordUnion, externalRecordUnion, unorderedListOfHintsType, BuiltinType.ASTRING,
                BuiltinType.AINT32, BuiltinType.AINT32 };
        return new ARecordType("DatasetRecordType", fieldNames, fieldTypes, true);
    }

    // Helper constants for accessing fields in an ARecord of anonymous type
    // field type.
    public static final int FIELD_ARECORD_FIELDNAME_FIELD_INDEX = 0;
    public static final int FIELD_ARECORD_FIELDTYPE_FIELD_INDEX = 1;
    public static final int FIELD_ARECORD_ISNULLABLE_FIELD_INDEX = 2;

    private static final ARecordType createFieldRecordType() throws AsterixException {
        String[] fieldNames = { "FieldName", "FieldType", "IsNullable" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ABOOLEAN };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    };

    // Helper constants for accessing fields in an ARecord of anonymous type
    // record type.
    public static final int RECORDTYPE_ARECORD_ISOPEN_FIELD_INDEX = 0;
    public static final int RECORDTYPE_ARECORD_FIELDS_FIELD_INDEX = 1;

    private static final ARecordType createRecordTypeRecordType() throws AsterixException {
        AOrderedListType olType = new AOrderedListType(FIELD_RECORDTYPE, null);
        String[] fieldNames = { "IsOpen", "Fields" };
        IAType[] fieldTypes = { BuiltinType.ABOOLEAN, olType };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    };

    // Helper constants for accessing fields in an ARecord of anonymous type
    // derived type.
    public static final int DERIVEDTYPE_ARECORD_TAG_FIELD_INDEX = 0;
    public static final int DERIVEDTYPE_ARECORD_ISANONYMOUS_FIELD_INDEX = 1;
    public static final int DERIVEDTYPE_ARECORD_RECORD_FIELD_INDEX = 2;
    public static final int DERIVEDTYPE_ARECORD_UNORDEREDLIST_FIELD_INDEX = 3;
    public static final int DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX = 4;

    private static final ARecordType createDerivedTypeRecordType() throws AsterixException {
        String[] fieldNames = { "Tag", "IsAnonymous", "Record", "UnorderedList", "OrderedList" };
        AUnionType recordUnion = AUnionType.createNullableType(RECORD_RECORDTYPE);
        AUnionType collectionUnion = AUnionType.createNullableType(BuiltinType.ASTRING);

        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ABOOLEAN, recordUnion, collectionUnion,
                collectionUnion };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    };

    // Helper constants for accessing fields in an ARecord of type
    // DatatypeRecordType.
    public static final int DATATYPE_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int DATATYPE_ARECORD_DATATYPENAME_FIELD_INDEX = 1;
    public static final int DATATYPE_ARECORD_DERIVED_FIELD_INDEX = 2;
    public static final int DATATYPE_ARECORD_TIMESTAMP_FIELD_INDEX = 3;

    private static final ARecordType createDatatypeRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "DatatypeName", "Derived", "Timestamp" };
        AUnionType recordUnion = AUnionType.createNullableType(DERIVEDTYPE_RECORDTYPE);
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, recordUnion, BuiltinType.ASTRING };
        return new ARecordType("DatatypeRecordType", fieldNames, fieldTypes, true);
    };

    // Helper constants for accessing fields in an ARecord of type
    // IndexRecordType.
    public static final int INDEX_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int INDEX_ARECORD_DATASETNAME_FIELD_INDEX = 1;
    public static final int INDEX_ARECORD_INDEXNAME_FIELD_INDEX = 2;
    public static final int INDEX_ARECORD_INDEXSTRUCTURE_FIELD_INDEX = 3;
    public static final int INDEX_ARECORD_SEARCHKEY_FIELD_INDEX = 4;
    public static final int INDEX_ARECORD_ISPRIMARY_FIELD_INDEX = 5;
    public static final int INDEX_ARECORD_TIMESTAMP_FIELD_INDEX = 6;
    public static final int INDEX_ARECORD_PENDINGOP_FIELD_INDEX = 7;

    private static final ARecordType createIndexRecordType() throws AsterixException {
        AOrderedListType olType = new AOrderedListType(BuiltinType.ASTRING, null);
        AOrderedListType ololType = new AOrderedListType(olType, null);
        String[] fieldNames = { "DataverseName", "DatasetName", "IndexName", "IndexStructure", "SearchKey", "IsPrimary",
                "Timestamp", "PendingOp" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                ololType, BuiltinType.ABOOLEAN, BuiltinType.ASTRING, BuiltinType.AINT32 };
        return new ARecordType("IndexRecordType", fieldNames, fieldTypes, true);
    };

    // Helper constants for accessing fields in an ARecord of type
    // NodeRecordType.
    public static final int NODE_ARECORD_NODENAME_FIELD_INDEX = 0;
    public static final int NODE_ARECORD_NUMBEROFCORES_FIELD_INDEX = 1;
    public static final int NODE_ARECORD_WORKINGMEMORYSIZE_FIELD_INDEX = 2;

    private static final ARecordType createNodeRecordType() throws AsterixException {
        String[] fieldNames = { "NodeName", "NumberOfCores", "WorkingMemorySize" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.AINT64, BuiltinType.AINT64 };
        return new ARecordType("NodeRecordType", fieldNames, fieldTypes, true);
    };

    // Helper constants for accessing fields in an ARecord of type
    // NodeGroupRecordType.
    public static final int NODEGROUP_ARECORD_GROUPNAME_FIELD_INDEX = 0;
    public static final int NODEGROUP_ARECORD_NODENAMES_FIELD_INDEX = 1;
    public static final int NODEGROUP_ARECORD_TIMESTAMP_FIELD_INDEX = 2;

    private static final ARecordType createNodeGroupRecordType() throws AsterixException {
        AUnorderedListType ulType = new AUnorderedListType(BuiltinType.ASTRING, null);
        String[] fieldNames = { "GroupName", "NodeNames", "Timestamp" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, ulType, BuiltinType.ASTRING };
        return new ARecordType("NodeGroupRecordType", fieldNames, fieldTypes, true);
    };

    private static IAType createFunctionParamsRecordType() {
        AOrderedListType orderedParamListType = new AOrderedListType(BuiltinType.ASTRING, null);
        return orderedParamListType;

    }

    public static final int FUNCTION_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int FUNCTION_ARECORD_FUNCTIONNAME_FIELD_INDEX = 1;
    public static final int FUNCTION_ARECORD_FUNCTION_ARITY_FIELD_INDEX = 2;
    public static final int FUNCTION_ARECORD_FUNCTION_PARAM_LIST_FIELD_INDEX = 3;
    public static final int FUNCTION_ARECORD_FUNCTION_RETURN_TYPE_FIELD_INDEX = 4;
    public static final int FUNCTION_ARECORD_FUNCTION_DEFINITION_FIELD_INDEX = 5;
    public static final int FUNCTION_ARECORD_FUNCTION_LANGUAGE_FIELD_INDEX = 6;
    public static final int FUNCTION_ARECORD_FUNCTION_KIND_FIELD_INDEX = 7;

    private static final ARecordType createFunctionRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "Name", "Arity", "Params", "ReturnType", "Definition", "Language",
                "Kind" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                createFunctionParamsRecordType(), BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.ASTRING };
        return new ARecordType("FunctionRecordType", fieldNames, fieldTypes, true);
    }

    public static final int DATASOURCE_ADAPTER_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int DATASOURCE_ADAPTER_ARECORD_NAME_FIELD_INDEX = 1;
    public static final int DATASOURCE_ADAPTER_ARECORD_CLASSNAME_FIELD_INDEX = 2;
    public static final int DATASOURCE_ADAPTER_ARECORD_TYPE_FIELD_INDEX = 3;
    public static final int DATASOURCE_ADAPTER_ARECORD_TIMESTAMP_FIELD_INDEX = 4;

    private static ARecordType createDatasourceAdapterRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "Name", "Classname", "Type", "Timestamp" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.ASTRING };
        return new ARecordType("DatasourceAdapterRecordType", fieldNames, fieldTypes, true);
    }

    // Helper constants for accessing fields in an ARecord of type
    // FeedActivityRecordType.
    public static final int FEED_ACTIVITY_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int FEED_ACTIVITY_ARECORD_FEED_NAME_FIELD_INDEX = 1;
    public static final int FEED_ACTIVITY_ARECORD_DATASET_NAME_FIELD_INDEX = 2;
    public static final int FEED_ACTIVITY_ARECORD_ACTIVITY_ID_FIELD_INDEX = 3;
    public static final int FEED_ACTIVITY_ARECORD_ACTIVITY_TYPE_FIELD_INDEX = 4;
    public static final int FEED_ACTIVITY_ARECORD_DETAILS_FIELD_INDEX = 5;
    public static final int FEED_ACTIVITY_ARECORD_LAST_UPDATE_TIMESTAMP_FIELD_INDEX = 6;

    public static final int FEED_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int FEED_ARECORD_FEED_NAME_FIELD_INDEX = 1;
    public static final int FEED_ARECORD_FUNCTION_FIELD_INDEX = 2;
    public static final int FEED_ARECORD_FEED_TYPE_FIELD_INDEX = 3;
    public static final int FEED_ARECORD_PRIMARY_TYPE_DETAILS_FIELD_INDEX = 4;
    public static final int FEED_ARECORD_SECONDARY_TYPE_DETAILS_FIELD_INDEX = 5;
    public static final int FEED_ARECORD_TIMESTAMP_FIELD_INDEX = 6;

    public static final int FEED_ARECORD_PRIMARY_FIELD_DETAILS_ADAPTOR_NAME_FIELD_INDEX = 0;
    public static final int FEED_ARECORD_PRIMARY_FIELD_DETAILS_ADAPTOR_CONFIGURATION_FIELD_INDEX = 1;

    public static final int FEED_ARECORD_SECONDARY_FIELD_DETAILS_SOURCE_FEED_NAME_FIELD_INDEX = 0;

    private static ARecordType createFeedRecordType() throws AsterixException, HyracksDataException {

        AUnionType feedFunctionUnion = AUnionType.createNullableType(BuiltinType.ASTRING);
        AUnionType primaryRecordUnion = AUnionType.createNullableType(PRIMARY_FEED_DETAILS_RECORDTYPE);
        AUnionType secondaryRecordUnion = AUnionType.createNullableType(SECONDARY_FEED_DETAILS_RECORDTYPE);

        String[] fieldNames = { "DataverseName", "FeedName", "Function", "FeedType", "PrimaryTypeDetails",
                "SecondaryTypeDetails", "Timestamp" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, feedFunctionUnion, BuiltinType.ASTRING,
                primaryRecordUnion, secondaryRecordUnion, BuiltinType.ASTRING };

        return new ARecordType("FeedRecordType", fieldNames, fieldTypes, true);
    }

    public static final int FEED_TYPE_PRIMARY_ARECORD_ADAPTER_NAME_FIELD_INDEX = 0;
    public static final int FEED_TYPE_PRIMARY_ARECORD_ADAPTER_CONFIGURATION_FIELD_INDEX = 1;

    private static final ARecordType createPrimaryFeedDetailsRecordType()
            throws AsterixException, HyracksDataException {
        AUnorderedListType unorderedAdaptorPropertyListType = new AUnorderedListType(
                DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE, null);

        String[] fieldNames = { "AdapterName", "AdapterConfiguration" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, unorderedAdaptorPropertyListType };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    public static final int FEED_TYPE_SECONDARY_ARECORD_SOURCE_FEED_NAME_FIELD_INDEX = 0;

    private static final ARecordType createSecondaryFeedDetailsRecordType()
            throws AsterixException, HyracksDataException {
        String[] fieldNames = { "SourceFeedName" };
        IAType[] fieldTypes = { BuiltinType.ASTRING };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    public static final int LIBRARY_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int LIBRARY_ARECORD_NAME_FIELD_INDEX = 1;
    public static final int LIBRARY_ARECORD_TIMESTAMP_FIELD_INDEX = 2;

    private static ARecordType createLibraryRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "Name", "Timestamp" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING };
        return new ARecordType("LibraryRecordType", fieldNames, fieldTypes, true);
    }

    public static final int EXTERNAL_FILE_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int EXTERNAL_FILE_ARECORD_DATASET_NAME_FIELD_INDEX = 1;
    public static final int EXTERNAL_FILE_ARECORD_FILE_NUMBER_FIELD_INDEX = 2;
    public static final int EXTERNAL_FILE_ARECORD_FILE_NAME_FIELD_INDEX = 3;
    public static final int EXTERNAL_FILE_ARECORD_FILE_SIZE_FIELD_INDEX = 4;
    public static final int EXTERNAL_FILE_ARECORD_FILE_MOD_DATE_FIELD_INDEX = 5;
    public static final int EXTERNAL_FILE_ARECORD_FILE_PENDING_OP_FIELD_INDEX = 6;

    private static ARecordType createExternalFileRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "DatasetName", "FileNumber", "FileName", "FileSize", "FileModTime",
                "PendingOp" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.ASTRING,
                BuiltinType.AINT64, BuiltinType.ADATETIME, BuiltinType.AINT32 };
        return new ARecordType("ExternalFileRecordType", fieldNames, fieldTypes, true);
    }
}
