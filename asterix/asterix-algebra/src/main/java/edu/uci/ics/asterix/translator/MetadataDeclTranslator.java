package edu.uci.ics.asterix.translator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import edu.uci.ics.asterix.aql.expression.OrderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition.RecordKind;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeExpression;
import edu.uci.ics.asterix.aql.expression.TypeReferenceExpression;
import edu.uci.ics.asterix.aql.expression.UnorderedListTypeDefinition;
import edu.uci.ics.asterix.common.annotations.IRecordFieldDataGen;
import edu.uci.ics.asterix.common.annotations.RecordDataGenAnnotation;
import edu.uci.ics.asterix.common.annotations.TypeDataGen;
import edu.uci.ics.asterix.common.config.GlobalConfig;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixProperties;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinTypeMap;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.core.algebra.data.IAWriterFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public final class MetadataDeclTranslator {
    private final MetadataTransactionContext mdTxnCtx;
    private final String dataverseName;
    private final List<TypeDecl> typeDeclarations;
    private final FileSplit outputFile;
    private final Map<String, String> config;
    private final IAWriterFactory writerFactory;

    public MetadataDeclTranslator(MetadataTransactionContext mdTxnCtx, String dataverseName, FileSplit outputFile,
            IAWriterFactory writerFactory, Map<String, String> config, List<TypeDecl> typeDeclarations) {
        this.mdTxnCtx = mdTxnCtx;
        this.dataverseName = dataverseName;
        this.outputFile = outputFile;
        this.writerFactory = writerFactory;
        this.config = config;
        this.typeDeclarations = typeDeclarations;
    }

    // TODO: Should this not throw an AsterixException?
    public AqlCompiledMetadataDeclarations computeMetadataDeclarations(boolean online) throws AlgebricksException,
            MetadataException {
        Map<String, TypeDataGen> typeDataGenMap = new HashMap<String, TypeDataGen>();
        for (TypeDecl td : typeDeclarations) {
            TypeDataGen tdg = td.getDatagenAnnotation();
            if (tdg != null) {
                typeDataGenMap.put(td.getIdent().getValue(), tdg);
            }
        }
        Map<String, IAType> typeMap = computeTypes();
        Map<String, String[]> stores = AsterixProperties.INSTANCE.getStores();
        return new AqlCompiledMetadataDeclarations(mdTxnCtx, dataverseName, outputFile, config, stores, typeMap,
                typeDataGenMap, writerFactory, online);
    }

    private Map<String, IAType> computeTypes() throws AlgebricksException, MetadataException {
        Map<String, IAType> typeMap = new HashMap<String, IAType>();
        Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes = new HashMap<String, Map<ARecordType, List<Integer>>>();
        Map<String, List<AbstractCollectionType>> incompleteItemTypes = new HashMap<String, List<AbstractCollectionType>>();
        Map<String, List<String>> incompleteTopLevelTypeReferences = new HashMap<String, List<String>>();

        firstPass(typeMap, incompleteFieldTypes, incompleteItemTypes, incompleteTopLevelTypeReferences);
        secondPass(typeMap, incompleteFieldTypes, incompleteItemTypes, incompleteTopLevelTypeReferences);
        return typeMap;
    }

    private void secondPass(Map<String, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, List<String>> incompleteTopLevelTypeReferences) throws AlgebricksException, MetadataException {
        // solve remaining top level references
        for (String trefName : incompleteTopLevelTypeReferences.keySet()) {
            IAType t = typeMap.get(trefName);
            if (t == null) {
                throw new AlgebricksException("Could not resolve type " + trefName);
            }
            for (String tname : incompleteTopLevelTypeReferences.get(trefName)) {
                typeMap.put(tname, t);
            }
        }
        // solve remaining field type references
        for (String trefName : incompleteFieldTypes.keySet()) {
            IAType t = typeMap.get(trefName);
            if (t == null) {
                // Try to get type from the metadata manager.
                Datatype metadataDataType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, dataverseName, trefName);
                if (metadataDataType == null) {
                    throw new AlgebricksException("Could not resolve type " + trefName);
                }
                t = metadataDataType.getDatatype();
                typeMap.put(trefName, t);
            }
            Map<ARecordType, List<Integer>> fieldsToFix = incompleteFieldTypes.get(trefName);
            for (ARecordType recType : fieldsToFix.keySet()) {
                List<Integer> positions = fieldsToFix.get(recType);
                IAType[] fldTypes = recType.getFieldTypes();
                for (Integer pos : positions) {
                    if (fldTypes[pos] == null) {
                        fldTypes[pos] = t;
                    } else { // nullable
                        AUnionType nullableUnion = (AUnionType) fldTypes[pos];
                        nullableUnion.setTypeAtIndex(t, 1);
                    }
                }
            }
        }
        // solve remaining item type references
        for (String trefName : incompleteItemTypes.keySet()) {
            IAType t = typeMap.get(trefName);
            if (t == null) {
                throw new AlgebricksException("Could not resolve type " + trefName);
            }
            for (AbstractCollectionType act : incompleteItemTypes.get(trefName)) {
                act.setItemType(t);
            }
        }
    }

    private void firstPass(Map<String, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, List<String>> incompleteTopLevelTypeReferences) throws AlgebricksException {
        for (TypeDecl td : typeDeclarations) {
            TypeExpression texpr = td.getTypeDef();
            String tdname = td.getIdent().getValue();
            if (AsterixBuiltinTypeMap.getBuiltinTypes().get(tdname) != null) {
                throw new AlgebricksException("Cannot redefine builtin type " + tdname + " .");
            }
            switch (texpr.getTypeKind()) {
                case TYPEREFERENCE: {
                    TypeReferenceExpression tre = (TypeReferenceExpression) texpr;
                    IAType t = solveTypeReference(tre, typeMap);
                    if (t != null) {
                        typeMap.put(tdname, t);
                    } else {
                        addIncompleteTopLevelTypeReference(tdname, tre, incompleteTopLevelTypeReferences);
                    }
                    break;
                }
                case RECORD: {
                    RecordTypeDefinition rtd = (RecordTypeDefinition) texpr;
                    ARecordType recType = computeRecordType(tdname, rtd, typeMap, incompleteFieldTypes,
                            incompleteItemTypes);
                    typeMap.put(tdname, recType);
                    break;
                }
                case ORDEREDLIST: {
                    OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) texpr;
                    AOrderedListType olType = computeOrderedListType(tdname, oltd, typeMap, incompleteItemTypes,
                            incompleteFieldTypes);
                    typeMap.put(tdname, olType);
                    break;
                }
                case UNORDEREDLIST: {
                    UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) texpr;
                    AUnorderedListType ulType = computeUnorderedListType(tdname, ultd, typeMap, incompleteItemTypes,
                            incompleteFieldTypes);
                    typeMap.put(tdname, ulType);
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
        }
    }

    private AOrderedListType computeOrderedListType(String typeName, OrderedListTypeDefinition oltd,
            Map<String, IAType> typeMap, Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes) {
        TypeExpression tExpr = oltd.getItemTypeExpression();
        AOrderedListType aolt = new AOrderedListType(null, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, aolt);
        return aolt;
    }

    private AUnorderedListType computeUnorderedListType(String typeName, UnorderedListTypeDefinition ultd,
            Map<String, IAType> typeMap, Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes) {
        TypeExpression tExpr = ultd.getItemTypeExpression();
        AUnorderedListType ault = new AUnorderedListType(null, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, ault);
        return ault;
    }

    private void setCollectionItemType(TypeExpression tExpr, Map<String, IAType> typeMap,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes, AbstractCollectionType act) {
        switch (tExpr.getTypeKind()) {
            case ORDEREDLIST: {
                OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) tExpr;
                IAType t = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes);
                act.setItemType(t);
                break;
            }
            case UNORDEREDLIST: {
                UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) tExpr;
                IAType t = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes, incompleteFieldTypes);
                act.setItemType(t);
                break;
            }
            case RECORD: {
                RecordTypeDefinition rtd = (RecordTypeDefinition) tExpr;
                IAType t = computeRecordType(null, rtd, typeMap, incompleteFieldTypes, incompleteItemTypes);
                act.setItemType(t);
                break;
            }
            case TYPEREFERENCE: {
                TypeReferenceExpression tre = (TypeReferenceExpression) tExpr;
                IAType tref = solveTypeReference(tre, typeMap);
                if (tref != null) {
                    act.setItemType(tref);
                } else {
                    addIncompleteCollectionTypeReference(act, tre, incompleteItemTypes);
                }
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private ARecordType computeRecordType(String typeName, RecordTypeDefinition rtd, Map<String, IAType> typeMap,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes) {
        List<String> names = rtd.getFieldNames();
        int n = names.size();
        String[] fldNames = new String[n];
        IAType[] fldTypes = new IAType[n];
        int i = 0;
        for (String s : names) {
            fldNames[i++] = s;
        }
        boolean isOpen = rtd.getRecordKind() == RecordKind.OPEN;
        ARecordType recType = new ARecordType(typeName, fldNames, fldTypes, isOpen);

        List<IRecordFieldDataGen> fieldDataGen = rtd.getFieldDataGen();
        if (fieldDataGen.size() == n) {
            IRecordFieldDataGen[] rfdg = new IRecordFieldDataGen[n];
            rfdg = fieldDataGen.toArray(rfdg);
            recType.getAnnotations().add(new RecordDataGenAnnotation(rfdg, rtd.getUndeclaredFieldsDataGen()));
        }

        for (int j = 0; j < n; j++) {
            TypeExpression texpr = rtd.getFieldTypes().get(j);
            switch (texpr.getTypeKind()) {
                case TYPEREFERENCE: {
                    TypeReferenceExpression tre = (TypeReferenceExpression) texpr;
                    IAType tref = solveTypeReference(tre, typeMap);
                    if (tref != null) {
                        if (!rtd.getNullableFields().get(j)) { // not nullable
                            fldTypes[j] = tref;
                        } else { // nullable
                            fldTypes[j] = makeUnionWithNull(null, tref);
                        }
                    } else {
                        addIncompleteFieldTypeReference(recType, j, tre, incompleteFieldTypes);
                        if (rtd.getNullableFields().get(j)) {
                            fldTypes[j] = makeUnionWithNull(null, null);
                        }
                    }
                    break;
                }
                case RECORD: {
                    RecordTypeDefinition recTypeDef2 = (RecordTypeDefinition) texpr;
                    IAType t2 = computeRecordType(null, recTypeDef2, typeMap, incompleteFieldTypes, incompleteItemTypes);
                    if (!rtd.getNullableFields().get(j)) { // not nullable
                        fldTypes[j] = t2;
                    } else { // nullable
                        fldTypes[j] = makeUnionWithNull(null, t2);
                    }
                    break;
                }
                case ORDEREDLIST: {
                    OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) texpr;
                    IAType t2 = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes);
                    fldTypes[j] = (rtd.getNullableFields().get(j)) ? makeUnionWithNull(null, t2) : t2;
                    break;
                }
                case UNORDEREDLIST: {
                    UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) texpr;
                    IAType t2 = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes, incompleteFieldTypes);
                    fldTypes[j] = (rtd.getNullableFields().get(j)) ? makeUnionWithNull(null, t2) : t2;
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }

        }

        return recType;
    }

    private AUnionType makeUnionWithNull(String unionTypeName, IAType type) {
        ArrayList<IAType> unionList = new ArrayList<IAType>(2);
        unionList.add(BuiltinType.ANULL);
        unionList.add(type);
        return new AUnionType(unionList, unionTypeName);
    }

    private void addIncompleteCollectionTypeReference(AbstractCollectionType collType, TypeReferenceExpression tre,
            Map<String, List<AbstractCollectionType>> incompleteItemTypes) {
        String typeName = tre.getIdent().getValue();
        List<AbstractCollectionType> typeList = incompleteItemTypes.get(typeName);
        if (typeList == null) {
            typeList = new LinkedList<AbstractCollectionType>();
            incompleteItemTypes.put(typeName, typeList);
        }
        typeList.add(collType);
    }

    private void addIncompleteFieldTypeReference(ARecordType recType, int fldPosition, TypeReferenceExpression tre,
            Map<String, Map<ARecordType, List<Integer>>> incompleteFieldTypes) {
        String typeName = tre.getIdent().getValue();
        Map<ARecordType, List<Integer>> refMap = incompleteFieldTypes.get(typeName);
        if (refMap == null) {
            refMap = new HashMap<ARecordType, List<Integer>>();
            incompleteFieldTypes.put(typeName, refMap);
        }
        List<Integer> typeList = refMap.get(recType);
        if (typeList == null) {
            typeList = new ArrayList<Integer>();
            refMap.put(recType, typeList);
        }
        typeList.add(fldPosition);
    }

    private void addIncompleteTopLevelTypeReference(String tdeclName, TypeReferenceExpression tre,
            Map<String, List<String>> incompleteTopLevelTypeReferences) {
        String name = tre.getIdent().getValue();
        List<String> refList = incompleteTopLevelTypeReferences.get(name);
        if (refList == null) {
            refList = new LinkedList<String>();
            incompleteTopLevelTypeReferences.put(name, refList);
        }
        refList.add(tdeclName);
    }

    private IAType solveTypeReference(TypeReferenceExpression tre, Map<String, IAType> typeMap) {
        String name = tre.getIdent().getValue();
        IAType builtin = AsterixBuiltinTypeMap.getBuiltinTypes().get(name);
        if (builtin != null) {
            return builtin;
        } else {
            return typeMap.get(name);
        }
    }
}
