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
package org.apache.hyracks.control.cc.dataset;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.dataset.DatasetDirectoryRecord;
import org.apache.hyracks.api.dataset.DatasetJobRecord;
import org.apache.hyracks.api.dataset.DatasetJobRecord.State;
import org.apache.hyracks.api.dataset.IDatasetStateRecord;
import org.apache.hyracks.api.dataset.ResultSetId;
import org.apache.hyracks.api.dataset.ResultSetMetaData;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.common.dataset.ResultStateSweeper;
import org.apache.hyracks.control.common.work.IResultCallback;

/**
 * TODO(madhusudancs): The potential perils of this global dataset directory service implementation is that, the jobs
 * location information is never evicted from the memory and the memory usage grows as the number of jobs in the system
 * grows. What we should possibly do is, add an API call for the client to say that it received everything it has to for
 * the job (after it receives all the results) completely. Then we can just get rid of the location information for that
 * job.
 */
public class DatasetDirectoryService implements IDatasetDirectoryService {

    private static final Logger LOGGER = Logger.getLogger(DatasetDirectoryService.class.getName());

    private final long resultTTL;

    private final long resultSweepThreshold;

    private final Map<JobId, JobResultInfo> jobResultLocations;

    public DatasetDirectoryService(long resultTTL, long resultSweepThreshold) {
        this.resultTTL = resultTTL;
        this.resultSweepThreshold = resultSweepThreshold;
        jobResultLocations = new LinkedHashMap<JobId, JobResultInfo>();
    }

    @Override
    public void init(ExecutorService executor) {
        executor.execute(new ResultStateSweeper(this, resultTTL, resultSweepThreshold, LOGGER));
    }

    @Override
    public synchronized void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(getClass().getSimpleName() + " notified of new job " + jobId);
        }
        if (jobResultLocations.get(jobId) != null) {
            throw HyracksDataException.create(ErrorCode.MORE_THAN_ONE_RESULT, jobId);
        }
        jobResultLocations.put(jobId, new JobResultInfo(new DatasetJobRecord(), null));
    }

    @Override
    public synchronized void notifyJobStart(JobId jobId) throws HyracksException {
        jobResultLocations.get(jobId).getRecord().start();
    }

    @Override
    public void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) throws HyracksException {
        // Auto-generated method stub
    }

    private DatasetJobRecord getDatasetJobRecord(JobId jobId) {
        final JobResultInfo jri = jobResultLocations.get(jobId);
        return jri == null ? null : jri.getRecord();
    }

    private DatasetJobRecord getNonNullDatasetJobRecord(JobId jobId) throws HyracksDataException {
        final DatasetJobRecord djr = getDatasetJobRecord(jobId);
        if (djr == null) {
            throw HyracksDataException.create(ErrorCode.NO_RESULT_SET, jobId);
        }
        return djr;
    }

    @Override
    public synchronized void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult,
            boolean emptyResult, int partition, int nPartitions, NetworkAddress networkAddress)
            throws HyracksDataException {
        DatasetJobRecord djr = getNonNullDatasetJobRecord(jobId);
        djr.setResultSetMetaData(rsId, orderedResult, nPartitions);
        DatasetDirectoryRecord record = djr.getOrCreateDirectoryRecord(rsId, partition);

        record.setNetworkAddress(networkAddress);
        record.setEmpty(emptyResult);
        record.start();

        final JobResultInfo jobResultInfo = jobResultLocations.get(jobId);
        Waiter waiter = jobResultInfo.getWaiter(rsId);
        if (waiter != null) {
            try {
                DatasetDirectoryRecord[] updatedRecords = updatedRecords(jobId, rsId, waiter.knownRecords);
                if (updatedRecords != null) {
                    jobResultInfo.removeWaiter(rsId);
                    waiter.callback.setValue(updatedRecords);
                }
            } catch (Exception e) {
                waiter.callback.setException(e);
            }
        }
        notifyAll();
    }

    @Override
    public synchronized void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition)
            throws HyracksDataException {
        DatasetJobRecord djr = getNonNullDatasetJobRecord(jobId);
        djr.getDirectoryRecord(rsId, partition).writeEOS();
        djr.updateState(rsId);
        notifyAll();
    }

    @Override
    public synchronized void reportJobFailure(JobId jobId, List<Exception> exceptions) {
        LOGGER.log(Level.INFO, "job " + jobId + " failed and is being reported to " + getClass().getSimpleName(),
                exceptions.get(0));
        DatasetJobRecord djr = getDatasetJobRecord(jobId);
        LOGGER.log(Level.INFO, "Dataset job record is " + djr);
        if (djr != null) {
            LOGGER.log(Level.INFO, "Setting exceptions in Dataset job record");
            djr.fail(exceptions);
        }
        final JobResultInfo jobResultInfo = jobResultLocations.get(jobId);
        LOGGER.log(Level.INFO, "Job result info is " + jobResultInfo);
        if (jobResultInfo != null) {
            LOGGER.log(Level.INFO, "Setting exceptions in Job result info");
            jobResultInfo.setException(exceptions.isEmpty() ? null : exceptions.get(0));
        }
        notifyAll();
    }

    @Override
    public synchronized DatasetJobRecord.Status getResultStatus(JobId jobId, ResultSetId rsId)
            throws HyracksDataException {
        return getNonNullDatasetJobRecord(jobId).getStatus();
    }

    @Override
    public synchronized Set<JobId> getJobIds() {
        return jobResultLocations.keySet();
    }

    @Override
    public IDatasetStateRecord getState(JobId jobId) {
        return getDatasetJobRecord(jobId);
    }

    @Override
    public synchronized long getResultTimestamp(JobId jobId) {
        return getState(jobId).getTimestamp();
    }

    @Override
    public synchronized void deinitState(JobId jobId) {
        jobResultLocations.remove(jobId);
    }

    @Override
    public synchronized void getResultPartitionLocations(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords, IResultCallback<DatasetDirectoryRecord[]> callback)
            throws HyracksDataException {
        DatasetDirectoryRecord[] updatedRecords = updatedRecords(jobId, rsId, knownRecords);
        if (updatedRecords == null) {
            jobResultLocations.get(jobId).addWaiter(rsId, knownRecords, callback);
        } else {
            callback.setValue(updatedRecords);
        }
    }

    /**
     * Compares the records already known by the client for the given job's result set id with the records that the
     * dataset directory service knows and if there are any newly discovered records returns a whole array with the
     * new records filled in.
     *
     * @param jobId
     *            - Id of the job for which the directory records should be retrieved.
     * @param rsId
     *            - Id of the result set for which the directory records should be retrieved.
     * @param knownRecords
     *            - An array of directory records that the client is already aware of.
     * @return
     *         Returns the updated records if new record were discovered, null otherwise
     * @throws HyracksDataException
     *             TODO(madhusudancs): Think about caching (and still be stateless) instead of this ugly O(n) iterations for
     *             every check. This already looks very expensive.
     */
    private DatasetDirectoryRecord[] updatedRecords(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords) throws HyracksDataException {
        DatasetJobRecord djr = getNonNullDatasetJobRecord(jobId);

        if (djr.getStatus().getState() == State.FAILED) {
            List<Exception> caughtExceptions = djr.getStatus().getExceptions();
            if (caughtExceptions != null && !caughtExceptions.isEmpty()) {
                final Exception cause = caughtExceptions.get(caughtExceptions.size() - 1);
                if (cause instanceof HyracksDataException) {
                    throw (HyracksDataException) cause;
                }
                throw HyracksDataException.create(ErrorCode.RESULT_FAILURE_EXCEPTION, cause, rsId, jobId);
            } else {
                throw HyracksDataException.create(ErrorCode.RESULT_FAILURE_NO_EXCEPTION, rsId, jobId);
            }
        }

        final ResultSetMetaData resultSetMetaData = djr.getResultSetMetaData(rsId);
        if (resultSetMetaData == null) {
            return null;
        }
        DatasetDirectoryRecord[] records = resultSetMetaData.getRecords();

        return Arrays.equals(records, knownRecords) ? null : records;
    }

    public PrintWriter print(PrintWriter pw) {
        for (JobId jId : getJobIds()) {
            pw.print(jId.toString());
            pw.print(" - ");
            pw.println(String.valueOf(getDatasetJobRecord(jId)));
        }
        pw.flush();
        return pw;
    }
}

class JobResultInfo {

    private DatasetJobRecord record;
    private Waiters waiters;
    private Exception exception;

    JobResultInfo(DatasetJobRecord record, Waiters waiters) {
        this.record = record;
        this.waiters = waiters;
    }

    DatasetJobRecord getRecord() {
        return record;
    }

    void addWaiter(ResultSetId rsId, DatasetDirectoryRecord[] knownRecords,
            IResultCallback<DatasetDirectoryRecord[]> callback) {
        if (waiters == null) {
            waiters = new Waiters();
        }
        waiters.put(rsId, new Waiter(knownRecords, callback));
        if (exception != null) {
            // Exception was set before the waiter is added.
            setException(exception);
        }
    }

    Waiter removeWaiter(ResultSetId rsId) {
        return waiters.remove(rsId);
    }

    Waiter getWaiter(ResultSetId rsId) {
        return waiters != null ? waiters.get(rsId) : null;
    }

    void setException(Exception exception) {
        if (waiters != null) {
            for (ResultSetId rsId : waiters.keySet()) {
                waiters.remove(rsId).callback.setException(exception);
            }
        }
        // Caches the exception anyway for future added waiters.
        this.exception = exception;
    }

    @Override
    public String toString() {
        return record.toString();
    }
}

class Waiters extends HashMap<ResultSetId, Waiter> {
    private static final long serialVersionUID = 1L;
}

class Waiter {
    DatasetDirectoryRecord[] knownRecords;
    IResultCallback<DatasetDirectoryRecord[]> callback;

    Waiter(DatasetDirectoryRecord[] knownRecords, IResultCallback<DatasetDirectoryRecord[]> callback) {
        this.knownRecords = knownRecords;
        this.callback = callback;
    }
}
