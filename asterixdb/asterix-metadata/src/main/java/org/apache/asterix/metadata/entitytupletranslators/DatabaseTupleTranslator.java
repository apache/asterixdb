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

import org.apache.asterix.metadata.bootstrap.DatabaseEntity;
import org.apache.asterix.metadata.entities.Database;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.ARecord;
import org.apache.asterix.om.base.AString;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Translates a Database metadata entity to an ITupleReference and vice versa.
 */
public class DatabaseTupleTranslator extends AbstractTupleTranslator<Database> {

    private final DatabaseEntity databaseEntity;
    private AMutableInt32 aInt32;

    protected DatabaseTupleTranslator(boolean getTuple, DatabaseEntity databaseEntity) {
        super(getTuple, databaseEntity.getIndex(), databaseEntity.payloadPosition());
        this.databaseEntity = databaseEntity;
        if (getTuple) {
            aInt32 = new AMutableInt32(-1);
        }
    }

    @Override
    protected Database createMetadataEntityFromARecord(ARecord databaseRecord) throws AlgebricksException {
        String databaseName =
                ((AString) databaseRecord.getValueByPos(databaseEntity.databaseNameIndex())).getStringValue();
        boolean isSystemDatabase =
                ((ABoolean) databaseRecord.getValueByPos(databaseEntity.systemDatabaseIndex())).getBoolean();
        int pendingOp = ((AInt32) databaseRecord.getValueByPos(databaseEntity.pendingOpIndex())).getIntegerValue();
        return new Database(databaseName, isSystemDatabase, pendingOp);
    }

    @Override
    public ITupleReference getTupleFromMetadataEntity(Database database) throws HyracksDataException {
        tupleBuilder.reset();

        // write the database name key in the first field of the tuple
        aString.setValue(database.getDatabaseName());
        stringSerde.serialize(aString, tupleBuilder.getDataOutput());
        tupleBuilder.addFieldEndOffset();

        // write the payload in the second field of the tuple
        recordBuilder.reset(databaseEntity.getRecordType());

        // write "DatabaseName" at index 0
        fieldValue.reset();
        aString.setValue(database.getDatabaseName());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(databaseEntity.databaseNameIndex(), fieldValue);

        // write "SystemDatabase" at index 1
        fieldValue.reset();
        booleanSerde.serialize(database.isSystemDatabase() ? ABoolean.TRUE : ABoolean.FALSE,
                fieldValue.getDataOutput());
        recordBuilder.addField(databaseEntity.systemDatabaseIndex(), fieldValue);

        // write "Timestamp" at index 2
        fieldValue.reset();
        aString.setValue(Calendar.getInstance().getTime().toString());
        stringSerde.serialize(aString, fieldValue.getDataOutput());
        recordBuilder.addField(databaseEntity.timestampIndex(), fieldValue);

        // write "PendingOp" at index 3
        fieldValue.reset();
        aInt32.setValue(database.getPendingOp());
        int32Serde.serialize(aInt32, fieldValue.getDataOutput());
        recordBuilder.addField(databaseEntity.pendingOpIndex(), fieldValue);

        // write the payload record
        recordBuilder.write(tupleBuilder.getDataOutput(), true);
        tupleBuilder.addFieldEndOffset();

        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        return tuple;
    }
}
