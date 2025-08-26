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

package org.apache.asterix.metadata.entitytupletranslators;

import java.util.Calendar;
import java.util.Map;

import org.apache.asterix.external.util.iceberg.IcebergUtils;
import org.apache.asterix.metadata.ICatalogDetails;
import org.apache.asterix.metadata.bootstrap.CatalogEntity;
import org.apache.asterix.metadata.bootstrap.MetadataRecordTypes;
import org.apache.asterix.metadata.entities.Catalog;
import org.apache.asterix.metadata.entities.IcebergCatalog;
import org.apache.asterix.metadata.entities.IcebergCatalogDetails;
import org.apache.asterix.metadata.utils.Creator;
import org.apache.asterix.metadata.utils.TupleTranslatorUtils;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AOrderedList;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IACursor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Catalog metadata entity to an ITupleReference and vice versa.
 */
public class CatalogTupleTranslator extends AbstractTupleTranslator<Catalog> {

    private final CatalogEntity catalogEntity;
    private AMutableInt32 aInt32;

    protected CatalogTupleTranslator(boolean getTuple, CatalogEntity catalogEntity) {
        super(getTuple, catalogEntity.getIndex(), catalogEntity.payloadPosition());
        this.catalogEntity = catalogEntity;
        if (getTuple) {
            aInt32 = new AMutableInt32(-1);
        }
    }

    @Override
    protected Catalog createMetadataEntityFromARecord(ARecord catalogRecord) throws AlgebricksException {
        String catalogName =
                ((AString) catalogRecord.getValueByPos(catalogEntity.getCatalogNameIndex())).getStringValue();
        String catalogType =
                ((AString) catalogRecord.getValueByPos(catalogEntity.getCatalogTypeIndex())).getStringValue();
        int pendingOp = ((AInt32) catalogRecord.getValueByPos(catalogEntity.getPendingOpIndex())).getIntegerValue();
        Creator creator = Creator.createOrDefault(catalogRecord);

        // catalog details
        if (IcebergUtils.isIcebergCatalog(catalogType)) {
            ARecord catalogDetailsRecord =
                    (ARecord) catalogRecord.getValueByPos(catalogEntity.getCatalogDetailsIndex());
            String adapter = ((AString) catalogDetailsRecord
                    .getValueByPos(MetadataRecordTypes.CATALOG_DETAILS_ARECORD_DATASOURCE_ADAPTER_FIELD_INDEX))
                            .getStringValue();
            IACursor cursor = ((AOrderedList) catalogDetailsRecord
                    .getValueByPos(MetadataRecordTypes.CATALOG_DETAILS_ARECORD_PROPERTIES_FIELD_INDEX)).getCursor();
            Map<String, String> properties = TupleTranslatorUtils.getPropertiesFromIaCursor(cursor);
            ICatalogDetails catalogDetails = new IcebergCatalogDetails(adapter, properties);
            return new IcebergCatalog(catalogName, catalogType, catalogDetails, pendingOp, creator);
        }
        return new Catalog(catalogName, catalogType, pendingOp, creator);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Catalog catalog) throws HyracksDataException {
        tupleBuilder.reset();

        // write the catalog name key in the first field of the tuple
        aString.setValue(catalog.getCatalogName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // reset the record builder with the catalog entity record type
        recordBuilder.reset(catalogEntity.getRecordType());

        // write "CatalogName" at index 0
        fieldValue.reset();
        aString.setValue(catalog.getCatalogName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(catalogEntity.getCatalogNameIndex(), fieldValue);

        // write "CatalogType" at index 1
        fieldValue.reset();
        aString.setValue(catalog.getCatalogType());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(catalogEntity.getCatalogTypeIndex(), fieldValue);

        // write "CatalogDetails" at index 2
        if (IcebergUtils.isIcebergCatalog(catalog.getCatalogType())) {
            fieldValue.reset();
            IcebergCatalog icebergCatalog = (IcebergCatalog) catalog;
            icebergCatalog.getCatalogDetails().writeCatalogDetailsRecordType(fieldValue.getDataOutput(), catalogEntity);
            recordBuilder.addField(catalogEntity.getCatalogDetailsIndex(), fieldValue);
        }

        // write "Timestamp" at index 3
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(catalogEntity.getTimestampIndex(), fieldValue);

        // write "PendingOp" at index 4
        fieldValue.reset();
        aInt32.setValue(catalog.getPendingOp());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(catalogEntity.getPendingOpIndex(), fieldValue);

        // write open fields
        writeOpenFields(catalog);

        // write the payload record to the tuple
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }

    protected void writeOpenFields(Catalog catalog) throws HyracksDataException {
        writeCatalogCreator(catalog);
    }

    private void writeCatalogCreator(Catalog catalog) throws HyracksDataException {
        if (catalogEntity.getCatalogNameIndex() >= 0) {
            TupleTranslatorUtils.writeCreator(catalog.getCreator(), recordBuilder, fieldName, fieldValue, aString,
                    stringSerde);
        }
    }
}
