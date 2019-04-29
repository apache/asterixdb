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
 * Each partition will be running a TPCDSDataGeneratorReader instance. Depending on the number of partitions, the data
 * generator will parallelize its work based on the number of partitions. The reader is passed the parallelism level
 * (depending on the number of partitions).
 *
 * Note: The data generator does not apply the parallelism unless at least 1,000,000 rows generation is requested (this
 * depends on the scaling factor and the table for whose the rows are being generated). This means, despite the number
 * of available partitions, if the minimum number of rows is not met, a single partition will generate all the rows
 * while all the other partitions will generate 0 rows.
 */
public class TPCDSDataGeneratorReader extends FunctionReader {

    private final int parallelism;
    private final int chunkNumber;
    private final String tableName;
    private final double scalingFactor;
    private Table selectedTable;
    private final StringBuilder builder = new StringBuilder();
    private final Iterator<List<List<String>>> dataGeneratorIterator;

    public TPCDSDataGeneratorReader(String tableName, double scalingFactor, int parallelism, int partitionNumber)
            throws HyracksDataException {
        this.tableName = tableName;
        this.scalingFactor = scalingFactor;
        this.parallelism = parallelism;

        /*
         Since we already ensured the parallelism level for the TPC-DS matches the number of partitions we have, we
         need a way to tell each partition which chunk to generate. Since each TPCDSDataGeneratorReader is receiving
         the partition number that's running it, we're gonna use that as the chunk to be produced by the data
         generator.
         Note that the indexing for the partitions starts at position 0, but the data generator chunks start at 1,
         so the chunk will always be the partitionNumber + 1
         */
        chunkNumber = partitionNumber + 1;

        dataGeneratorIterator = getDataGeneratorIterator();
    }

    @Override
    public boolean hasNext() {
        return dataGeneratorIterator.hasNext();
    }

    @Override
    public IRawRecord<char[]> next() throws IOException, InterruptedException {
        CharArrayRecord record = new CharArrayRecord();
        record.append((formatRecord(dataGeneratorIterator.next())).toCharArray());
        record.endRecord();
        return record;
    }

    /**
     * Create the data generator iterator with the specified properties passed to the session.
     *
     * @return A lazy iterator to generate the data based on the specified properties.
     */
    private Iterator<List<List<String>>> getDataGeneratorIterator() throws HyracksDataException {
        selectedTable = getTableFromStringTableName(tableName);

        // Create the session with the specified properties, the sessions also specifies the chunk to be generated
        Session session = Session.getDefaultSession().withTable(selectedTable).withScale(scalingFactor)
                .withParallelism(parallelism).withChunkNumber(chunkNumber);

        // Construct the Results and Results iterator
        Results results = Results.constructResults(selectedTable, session);
        return results.iterator();
    }

    /**
     * Gets the table matching the provided string table name, throws an exception if no table is returned.
     *
     * @param tableName String table name to search for.
     * @return Table if found, throws an exception otherwise.
     */
    private Table getTableFromStringTableName(String tableName) throws HyracksDataException {

        List<Table> matchedTables = Table.getBaseTables().stream()
                .filter(table -> tableName.equalsIgnoreCase(table.getName())).collect(Collectors.toList());

        // Ensure the table was found
        if (matchedTables.size() != 1) {
            throw new RuntimeDataException(ErrorCode.TPCDS_INVALID_TABLE_NAME, getIdentifier().getName(), tableName);
        }

        return matchedTables.get(0);
    }

    /**
     * Builds the string record from the generated values by the data generator. The column name for each value is
     * extracted from the table from which the data is being generated.
     *
     * @param values List containing all the generated column values
     *
     * @return The built string record from the generated values
     */
    private String formatRecord(List<List<String>> values) {
        // Clear the builder (This is faster than re-creating the builder each iteration)
        builder.setLength(0);

        int counter;
        builder.append("{");

        // We loop only to the item before the last, then add the last item manually to avoid appending the ","
        // at the end, this way we avoid constantly checking if we're at the last item or substring the whole record
        for (counter = 0; counter < values.get(0).size() - 1; counter++) {
            builder.append("\"");
            builder.append(selectedTable.getColumns()[counter].getName());
            builder.append("\":\"");
            builder.append(values.get(0).get(counter));
            builder.append("\",");
        }

        // This is the last item to be appended, don't append the "," after appending the field
        builder.append("\"");
        builder.append(selectedTable.getColumns()[counter].getName());
        builder.append("\":\"");
        builder.append(values.get(0).get(counter));
        builder.append("\"}");

        return builder.toString();
    }

    private FunctionIdentifier getIdentifier() {
        return TPCDSDataGeneratorRewriter.TPCDS_DATA_GENERATOR;
    }
}
