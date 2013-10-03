/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.asterix.metadata.bootstrap;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

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
    public static ARecordType COMPACTION_POLICY_RECORDTYPE;

    /**
     * Create all metadata record types.
     */
    public static void init() throws MetadataException {
        // Attention: The order of these calls is important because some types
        // depend on other types being created first.
        // These calls are one "dependency chain".
        try {
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
            COMPACTION_POLICY_RECORDTYPE = createCompactionPolicyRecordType();
        } catch (AsterixException e) {
            throw new MetadataException(e);
        }
    }

    // Helper constants for accessing fields in an ARecord of type
    // DataverseRecordType.
    public static final int DATAVERSE_ARECORD_NAME_FIELD_INDEX = 0;
    public static final int DATAVERSE_ARECORD_FORMAT_FIELD_INDEX = 1;
    public static final int DATAVERSE_ARECORD_TIMESTAMP_FIELD_INDEX = 2;
    public static final int DATAVERSE_ARECORD_PENDINGOP_FIELD_INDEX = 3;

    private static final ARecordType createDataverseRecordType() throws AsterixException {
        return new ARecordType("DataverseRecordType", new String[] { "DataverseName", "DataFormat", "Timestamp",
                "PendingOp" }, new IAType[] { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                BuiltinType.AINT32 }, true);
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
    public static final int INTERNAL_DETAILS_ARECORD_GROUPNAME_FIELD_INDEX = 4;
    public static final int INTERNAL_DETAILS_ARECORD_COMPACTION_POLICY_FIELD_INDEX = 5;
    public static final int INTERNAL_DETAILS_ARECORD_COMPACTION_POLICY_PROPERTIES_FIELD_INDEX = 6;

    private static final ARecordType createInternalDetailsRecordType() throws AsterixException {
        AOrderedListType olType = new AOrderedListType(BuiltinType.ASTRING, null);
        AOrderedListType compactionPolicyPropertyListType = new AOrderedListType(
                COMPACTION_POLICY_PROPERTIES_RECORDTYPE, null);
        String[] fieldNames = { "FileStructure", "PartitioningStrategy", "PartitioningKey", "PrimaryKey", "GroupName",
                "CompactionPolicy", "CompactionPolicyProperties" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, olType, olType, BuiltinType.ASTRING,
                BuiltinType.ASTRING, compactionPolicyPropertyListType };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    // Helper constants for accessing fields in an ARecord of anonymous type
    // external details.
    public static final int EXTERNAL_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX = 0;
    public static final int EXTERNAL_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX = 1;

    private static final ARecordType createExternalDetailsRecordType() throws AsterixException {

        AOrderedListType orderedPropertyListType = new AOrderedListType(DATASOURCE_ADAPTER_PROPERTIES_RECORDTYPE, null);
        String[] fieldNames = { "DatasourceAdapter", "Properties" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, orderedPropertyListType };
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    public static final int COMPACTION_POLICY_ARECORD_DATAVERSE_NAME_FIELD_INDEX = 0;
    public static final int COMPACTION_POLICY_ARECORD_POLICY_NAME_FIELD_INDEX = 1;
    public static final int COMPACTION_POLICY_ARECORD_CLASSNAME_FIELD_INDEX = 2;

    private static ARecordType createCompactionPolicyRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "PolicyName", "Classname" };
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

        List<IAType> feedFunctionUnionList = new ArrayList<IAType>();
        feedFunctionUnionList.add(BuiltinType.ANULL);
        feedFunctionUnionList.add(BuiltinType.ASTRING);
        AUnionType feedFunctionUnion = new AUnionType(feedFunctionUnionList, null);

        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, orderedListType, orderedListType,
                BuiltinType.ASTRING, BuiltinType.ASTRING, orderedListOfPropertiesType, feedFunctionUnion,
                BuiltinType.ASTRING, BuiltinType.ASTRING, compactionPolicyPropertyListType };

        return new ARecordType(null, fieldNames, fieldTypes, true);
    }

    public static final int DATASET_ARECORD_DATAVERSENAME_FIELD_INDEX = 0;
    public static final int DATASET_ARECORD_DATASETNAME_FIELD_INDEX = 1;
    public static final int DATASET_ARECORD_DATATYPENAME_FIELD_INDEX = 2;
    public static final int DATASET_ARECORD_DATASETTYPE_FIELD_INDEX = 3;
    public static final int DATASET_ARECORD_INTERNALDETAILS_FIELD_INDEX = 4;
    public static final int DATASET_ARECORD_EXTERNALDETAILS_FIELD_INDEX = 5;
    public static final int DATASET_ARECORD_FEEDDETAILS_FIELD_INDEX = 6;
    public static final int DATASET_ARECORD_HINTS_FIELD_INDEX = 7;
    public static final int DATASET_ARECORD_TIMESTAMP_FIELD_INDEX = 8;
    public static final int DATASET_ARECORD_DATASETID_FIELD_INDEX = 9;
    public static final int DATASET_ARECORD_PENDINGOP_FIELD_INDEX = 10;

    private static final ARecordType createDatasetRecordType() throws AsterixException {
        String[] fieldNames = { "DataverseName", "DatasetName", "DataTypeName", "DatasetType", "InternalDetails",
                "ExternalDetails", "FeedDetails", "Hints", "Timestamp", "DatasetId", "PendingOp" };

        List<IAType> internalRecordUnionList = new ArrayList<IAType>();
        internalRecordUnionList.add(BuiltinType.ANULL);
        internalRecordUnionList.add(INTERNAL_DETAILS_RECORDTYPE);
        AUnionType internalRecordUnion = new AUnionType(internalRecordUnionList, null);

        List<IAType> externalRecordUnionList = new ArrayList<IAType>();
        externalRecordUnionList.add(BuiltinType.ANULL);
        externalRecordUnionList.add(EXTERNAL_DETAILS_RECORDTYPE);
        AUnionType externalRecordUnion = new AUnionType(externalRecordUnionList, null);

        List<IAType> feedRecordUnionList = new ArrayList<IAType>();
        feedRecordUnionList.add(BuiltinType.ANULL);
        feedRecordUnionList.add(FEED_DETAILS_RECORDTYPE);
        AUnionType feedRecordUnion = new AUnionType(feedRecordUnionList, null);

        AUnorderedListType unorderedListOfHintsType = new AUnorderedListType(DATASET_HINTS_RECORDTYPE, null);

        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                internalRecordUnion, externalRecordUnion, feedRecordUnion, unorderedListOfHintsType,
                BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.AINT32 };
        return new ARecordType("DatasetRecordType", fieldNames, fieldTypes, true);
    }

    // Helper constants for accessing fields in an ARecord of anonymous type
    // field type.
    public static final int FIELD_ARECORD_FIELDNAME_FIELD_INDEX = 0;
    public static final int FIELD_ARECORD_FIELDTYPE_FIELD_INDEX = 1;

    private static final ARecordType createFieldRecordType() throws AsterixException {
        String[] fieldNames = { "FieldName", "FieldType" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING };
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
    public static final int DERIVEDTYPE_ARECORD_ENUMVALUES_FIELD_INDEX = 2;
    public static final int DERIVEDTYPE_ARECORD_RECORD_FIELD_INDEX = 3;
    public static final int DERIVEDTYPE_ARECORD_UNION_FIELD_INDEX = 4;
    public static final int DERIVEDTYPE_ARECORD_UNORDEREDLIST_FIELD_INDEX = 5;
    public static final int DERIVEDTYPE_ARECORD_ORDEREDLIST_FIELD_INDEX = 6;

    private static final ARecordType createDerivedTypeRecordType() throws AsterixException {
        String[] fieldNames = { "Tag", "IsAnonymous", "EnumValues", "Record", "Union", "UnorderedList", "OrderedList" };
        List<IAType> recordUnionList = new ArrayList<IAType>();
        recordUnionList.add(BuiltinType.ANULL);
        recordUnionList.add(RECORD_RECORDTYPE);
        AUnionType recordUnion = new AUnionType(recordUnionList, null);

        List<IAType> unionUnionList = new ArrayList<IAType>();
        unionUnionList.add(BuiltinType.ANULL);
        unionUnionList.add(new AOrderedListType(BuiltinType.ASTRING, null));
        AUnionType unionUnion = new AUnionType(unionUnionList, null);

        List<IAType> collectionUnionList = new ArrayList<IAType>();
        collectionUnionList.add(BuiltinType.ANULL);
        collectionUnionList.add(BuiltinType.ASTRING);
        AUnionType collectionUnion = new AUnionType(collectionUnionList, null);

        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ABOOLEAN, unionUnion, recordUnion, unionUnion,
                collectionUnion, collectionUnion };
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
        List<IAType> recordUnionList = new ArrayList<IAType>();
        recordUnionList.add(BuiltinType.ANULL);
        recordUnionList.add(DERIVEDTYPE_RECORDTYPE);
        AUnionType recordUnion = new AUnionType(recordUnionList, null);
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
        String[] fieldNames = { "DataverseName", "DatasetName", "IndexName", "IndexStructure", "SearchKey",
                "IsPrimary", "Timestamp", "PendingOp" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING,
                olType, BuiltinType.ABOOLEAN, BuiltinType.ASTRING, BuiltinType.AINT32 };
        return new ARecordType("IndexRecordType", fieldNames, fieldTypes, true);
    };

    // Helper constants for accessing fields in an ARecord of type
    // NodeRecordType.
    public static final int NODE_ARECORD_NODENAME_FIELD_INDEX = 0;
    public static final int NODE_ARECORD_NUMBEROFCORES_FIELD_INDEX = 1;
    public static final int NODE_ARECORD_WORKINGMEMORYSIZE_FIELD_INDEX = 2;

    private static final ARecordType createNodeRecordType() throws AsterixException {
        String[] fieldNames = { "NodeName", "NumberOfCores", "WorkingMemorySize" };
        IAType[] fieldTypes = { BuiltinType.ASTRING, BuiltinType.AINT32, BuiltinType.AINT32 };
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

}