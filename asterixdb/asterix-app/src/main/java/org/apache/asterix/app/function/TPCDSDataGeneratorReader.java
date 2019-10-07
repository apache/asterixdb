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
package org.apache.asterix.app.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.external.api.IRawRecord;
import org.apache.asterix.external.input.record.CharArrayRecord;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.teradata.tpcds.Results;
import com.teradata.tpcds.Session;
import com.teradata.tpcds.Table;

/**
 * Each partition will be running a TPCDS data generator reader instance. The data generator will parallelize its work
 * based on the number of partitions available. The reader is passed the parallelism level based on the number of
 * partition instances.
 *
 * The function automatically handles generating the data for a single specified table or for all the tables.
 */

public class TPCDSDataGeneratorReader extends FunctionReader {

    private final FunctionIdentifier functionIdentifier;

    // Table name will be added to each generated record
    private final static String TABLE_NAME_FIELD_NAME = "table_name";

    // When generating the values, a list is created, at index 0, all the values for the parent record exist, if a
    // child record is created, it is at index 1 in the list
    private static final int PARENT_VALUES_INDEX = 0;
    private static final int CHILD_VALUES_INDEX = 1;

    // Table members
    private final List<Table> selectedTables;
    private final StringBuilder builder = new StringBuilder();
    private final List<Iterator<List<List<String>>>> tableIterators = new ArrayList<>();
    private Table currentTable;
    private final int tableCount;
    private int currentTableIndex;

    // Parent tables will generate 2 records, 1 for the main, and another for the child's row, we'll push the parent
    // record, and in the next rotation we'll push the child row, then go to next row generation
    private String childRow = null;

    // This will guide the next() method to know where to get the next record from (Parent or child record)
    private boolean getFromParent = true;
    private final boolean generateAllTables;

    TPCDSDataGeneratorReader(String tableName, double scalingFactor, int parallelism, int partitionNumber,
            FunctionIdentifier functionIdentifier) throws HyracksDataException {
        this.functionIdentifier = functionIdentifier;

        // Note: chunk numbers start with 1, so each chunk will be partition number + 1
        // Create the session with the specified properties, the sessions also specifies the chunk to be generated
        Session session = Session.getDefaultSession().withScale(scalingFactor).withParallelism(parallelism)
                .withChunkNumber(partitionNumber + 1);

        // If the tableName is null, then we're generating all the tables
        generateAllTables = tableName == null;

        // Get the table(s)
        selectedTables = getTableFromStringTableName(tableName);

        // These variables will monitor and assist with each table's data generation
        currentTableIndex = 0;
        tableCount = selectedTables.size();
        currentTable = selectedTables.get(currentTableIndex);

        // Iterators for the tables to generate the data for
        for (Table table : selectedTables) {
            Results result = Results.constructResults(table, session);
            tableIterators.add(result.iterator());
        }
    }

    /**
     * Gets the table matching the provided string table name, throws an exception if no table is returned.
     *
     * @param tableName String table name to search for.
     * @return Table if found, throws an exception otherwise.
     */
    private List<Table> getTableFromStringTableName(String tableName) throws HyracksDataException {

        // Get all the tables
        if (generateAllTables) {
            // Remove the DBGEN_VERSION table and all children tables, parent tables will generate them
            return Table.getBaseTables().stream()
                    .filter(table -> !table.equals(Table.DBGEN_VERSION) && !table.isChild())
                    .collect(Collectors.toList());
        }

        // Search for the table
        List<Table> matchedTables = Table.getBaseTables().stream()
                .filter(table -> tableName.equalsIgnoreCase(table.getName())).collect(Collectors.toList());

        // Ensure the table was found
        if (matchedTables.isEmpty()) {
            throw new RuntimeDataException(ErrorCode.TPCDS_INVALID_TABLE_NAME, getFunctionIdentifier().getName(),
                    tableName);
        }

        return matchedTables;
    }

    /**
     * Gets the function identifier
     *
     * @return function identifier
     */
    private FunctionIdentifier getFunctionIdentifier() {
        return functionIdentifier;
    }

    @Override
    public boolean hasNext() {

        // Return children generated records of current table
        if (generateAllTables && childRow != null) {
            getFromParent = false;
            return true;
        }

        // Current table still has more
        if (tableIterators.get(currentTableIndex).hasNext()) {
            return true;
        }

        // We went over all the tables
        if (currentTableIndex == tableCount - 1) {
            return false;
        }

        // Go to the next table
        currentTableIndex++;
        currentTable = selectedTables.get(currentTableIndex);

        return hasNext();
    }

    @Override
    public IRawRecord<char[]> next() throws IOException, InterruptedException {
        CharArrayRecord record = new CharArrayRecord();

        if (getFromParent) {
            record.append((formatRecord(tableIterators.get(currentTableIndex).next())).toCharArray());
        } else {
            record.append(childRow.toCharArray());
            childRow = null; // Always reset the child row after writing it
            getFromParent = true;
        }

        record.endRecord();
        return record;
    }

    /**
     * Builds the string record from the generated values by the data generator. The field name for each value is
     * extracted from the table from which the data is being generated.
     *
     * @param values List containing all the generated column values
     *
     * @return The built string record from the generated values
     */
    private String formatRecord(List<List<String>> values) {
        // Clear the builder (This is faster than re-creating the builder each iteration)
        builder.setLength(0);

        // Construct the record
        constructRecord(values.get(PARENT_VALUES_INDEX), currentTable);

        // Reference to the parent row to be returned, before resetting the builder again
        String parentRow = builder.toString();

        // In some cases, the generate generates 2 records, one for current table, and one for child table. If a child
        // record is generated, we're gonna add it to a list, and start pushing it once all the parent records
        // are done
        if (generateAllTables && values.size() > 1) {
            builder.setLength(0);

            // Construct the record
            constructRecord(values.get(CHILD_VALUES_INDEX), currentTable.getChild());

            // Add it to the children rows list
            childRow = builder.toString();
        }

        // Return parent row
        return parentRow;
    }

    /**
     * Constructs the record with the appropriate data types.
     *
     * @param values list containing all the generated values for all columns in a string format.
     * @param table  Table the record is being constructed for
     */
    private void constructRecord(List<String> values, Table table) {
        // Add the table name to the record
        builder.append("{\"").append(TABLE_NAME_FIELD_NAME).append("\":\"").append(table.getName()).append("\"");

        // Build the record data
        for (int counter = 0; counter < values.size(); counter++) {

            // If the value is null, no need to check for the column type
            if (values.get(counter) == null) {
                builder.append(",\"");
                builder.append(table.getColumns()[counter].getName());
                builder.append("\":");
                builder.append(values.get(counter));
                continue;
            }

            String fieldName = table.getColumns()[counter].getName();
            String stringValue = values.get(counter);

            // Convert the value to the appropriate type based on the column type
            switch (table.getColumns()[counter].getType().getBase()) {
                case INTEGER:
                    builder.append(",\"").append(fieldName).append("\":").append(Integer.valueOf(stringValue));
                    break;
                case DECIMAL:
                    builder.append(",\"").append(fieldName).append("\":").append(Double.valueOf(stringValue));
                    break;
                // IDENTIFIER type could be any value, so we're taking it as a string
                // DATE and TIME are not supported, they are stored as strings and can be modified with date functions
                // CHAR and VARCHAR are handled as strings
                // any other type (default case) is handled as a string value
                case IDENTIFIER:
                case DATE:
                case TIME:
                case CHAR:
                case VARCHAR:
                default:
                    builder.append(",\"").append(fieldName).append("\":\"").append(stringValue).append("\"");
                    break;
            }
        }

        builder.append("}");
    }
}
