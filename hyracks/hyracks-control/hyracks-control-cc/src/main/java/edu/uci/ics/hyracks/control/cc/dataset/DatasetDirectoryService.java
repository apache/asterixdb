/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.dataset;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.IDatasetDirectoryService;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;

/**
 * TODO(madhusudancs): The potential perils of this global dataset directory service implementation is that, the jobs
 * location information is never evicted from the memory and the memory usage grows as the number of jobs in the system
 * grows. What we should possibly do is, add an API call for the client to say that it received everything it has to for
 * the job (after it receives all the results) completely. Then we can just get rid of the location information for that
 * job.
 */
public class DatasetDirectoryService implements IDatasetDirectoryService {
    private final Map<JobId, Map<ResultSetId, ResultSetMetaData>> jobResultLocationsMap;

    public DatasetDirectoryService() {
        jobResultLocationsMap = new HashMap<JobId, Map<ResultSetId, ResultSetMetaData>>();
    }

    @Override
    public synchronized void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult,
            byte[] serializedRecordDescriptor, int partition, int nPartitions, NetworkAddress networkAddress) {
        Map<ResultSetId, ResultSetMetaData> rsMap = jobResultLocationsMap.get(jobId);
        if (rsMap == null) {
            rsMap = new HashMap<ResultSetId, ResultSetMetaData>();
            jobResultLocationsMap.put(jobId, rsMap);
        }

        ResultSetMetaData resultSetMetaData = rsMap.get(rsId);
        if (resultSetMetaData == null) {
            resultSetMetaData = new ResultSetMetaData(orderedResult, new DatasetDirectoryRecord[nPartitions],
                    serializedRecordDescriptor);
            rsMap.put(rsId, resultSetMetaData);
        }

        DatasetDirectoryRecord[] records = resultSetMetaData.getRecords();
        if (records[partition] == null) {
            records[partition] = new DatasetDirectoryRecord();
        }
        records[partition].setNetworkAddress(networkAddress);
        notifyAll();
    }

    @Override
    public synchronized byte[] getRecordDescriptor(JobId jobId, ResultSetId rsId) throws HyracksDataException {
        Map<ResultSetId, ResultSetMetaData> rsMap;
        while ((rsMap = jobResultLocationsMap.get(jobId)) == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }

        ResultSetMetaData resultSetMetaData = rsMap.get(rsId);
        if (resultSetMetaData == null || resultSetMetaData.getRecords() == null) {
            throw new HyracksDataException("ResultSet locations uninitialized when it is expected to be initialized.");
        }

        return resultSetMetaData.getSerializedRecordDescriptor();
    }

    @Override
    public synchronized DatasetDirectoryRecord[] getResultPartitionLocations(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords) throws HyracksDataException {
        DatasetDirectoryRecord[] newRecords;
        while ((newRecords = updatedRecords(jobId, rsId, knownRecords)) == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }
        return newRecords;
    }

    /**
     * Compares the records already known by the client for the given job's result set id with the records that the
     * dataset directory service knows and if there are any newly discovered records returns a whole array with the
     * new records filled in.
     * This method has a very convoluted logic. Here is the explanation of how it works.
     * If the ordering constraint has to be enforced, the method obtains the first null record in the known records in
     * the order of the partitions. It always traverses the array in the first to last order!
     * If known records array or the first element in that array is null in the but the record for that partition now
     * known to the directory service, the method fills in that record in the array and returns the array back.
     * However, if the first known null record is not a first element in the array, by induction, all the previous
     * known records should be known already be known to client and none of the records for the partitions ahead is
     * known by the client yet. So, we check if the client has reached the end of stream for the partition corresponding
     * to the record before the first known null record, i.e. the last known non-null record. If not, we just return
     * null because we cannot expose any new locations until the client reaches end of stream for the last known record.
     * If the client has reached the end of stream record for the last known non-null record, we check if the next record
     * is discovered by the dataset directory service and if so, we fill it in the records array and return it back or
     * send null otherwise.
     * If the ordering is not required, we are free to return any newly discovered records back, so we just check if
     * arrays are equal and if they are not we send the entire new updated array.
     * 
     * @param jobId
     *            - Id of the job for which the directory records should be retrieved.
     * @param rsId
     *            - Id of the result set for which the directory records should be retrieved.
     * @param knownRecords
     *            - An array of directory records that the client is already aware of.
     * @return
     *         - Returns null if there aren't any newly discovered partitions enforcing the ordering constraint
     * @throws HyracksDataException
     *             TODO(madhusudancs): Think about caching (and still be stateless) instead of this ugly O(n) iterations for
     *             every check. This already looks very expensive.
     */
    private DatasetDirectoryRecord[] updatedRecords(JobId jobId, ResultSetId rsId, DatasetDirectoryRecord[] knownRecords)
            throws HyracksDataException {
        Map<ResultSetId, ResultSetMetaData> rsMap = jobResultLocationsMap.get(jobId);
        if (rsMap == null) {
            return null;
        }

        ResultSetMetaData resultSetMetaData = rsMap.get(rsId);
        if (resultSetMetaData == null || resultSetMetaData.getRecords() == null) {
            throw new HyracksDataException("ResultSet locations uninitialized when it is expected to be initialized.");
        }

        boolean ordered = resultSetMetaData.getOrderedResult();
        DatasetDirectoryRecord[] records = resultSetMetaData.getRecords();
        /* If ordering is required, we should expose the dataset directory records only in the order, otherwise
         * we can simply check if there are any newly discovered records and send the whole array back if there are.
         */
        if (ordered) {
            // Iterate over the known records and find the last record which is not null.
            int i = 0;
            for (i = 0; i < records.length; i++) {
                if (knownRecords == null) {
                    if (records[0] != null) {
                        knownRecords = new DatasetDirectoryRecord[records.length];
                        knownRecords[0] = records[0];
                        return knownRecords;
                    }
                    return null;
                }
                if (knownRecords[i] == null) {
                    if ((i == 0 || knownRecords[i - 1].getEOS()) && records[i] != null) {
                        knownRecords[i] = records[i];
                        return knownRecords;
                    }
                    return null;
                }
            }
        } else {
            if (!Arrays.equals(records, knownRecords)) {
                return records;
            }
        }
        return null;
    }

    private class ResultSetMetaData {
        private final boolean ordered;

        private final DatasetDirectoryRecord[] records;

        private final byte[] serializedRecordDescriptor;

        public ResultSetMetaData(boolean ordered, DatasetDirectoryRecord[] records, byte[] serializedRecordDescriptor) {
            this.ordered = ordered;
            this.records = records;
            this.serializedRecordDescriptor = serializedRecordDescriptor;
        }

        public boolean getOrderedResult() {
            return ordered;
        }

        public DatasetDirectoryRecord[] getRecords() {
            return records;
        }

        public byte[] getSerializedRecordDescriptor() {
            return serializedRecordDescriptor;
        }
    }
}