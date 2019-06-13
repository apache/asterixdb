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
package org.apache.hyracks.control.cc.result;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.result.IJobResultCallback;
import org.apache.hyracks.api.result.IResultMetadata;
import org.apache.hyracks.api.result.IResultStateRecord;
import org.apache.hyracks.api.result.ResultDirectoryRecord;
import org.apache.hyracks.api.result.ResultJobRecord;
import org.apache.hyracks.api.result.ResultJobRecord.State;
import org.apache.hyracks.api.result.ResultSetId;
import org.apache.hyracks.api.result.ResultSetMetaData;
import org.apache.hyracks.control.common.result.AbstractResultManager;
import org.apache.hyracks.control.common.result.ResultStateSweeper;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * TODO(madhusudancs): The potential perils of this global result directory service implementation is that, the jobs
 * location information is never evicted from the memory and the memory usage grows as the number of jobs in the system
 * grows. What we should possibly do is, add an API call for the client to say that it received everything it has to for
 * the job (after it receives all the results) completely. Then we can just get rid of the location information for that
 * job.
 */
public class ResultDirectoryService extends AbstractResultManager implements IResultDirectoryService {

    private static final Logger LOGGER = LogManager.getLogger();

    private final long resultSweepThreshold;

    private final Map<JobId, JobResultInfo> jobResultLocations;
    private IJobResultCallback jobResultCallback;

    public ResultDirectoryService(long resultTTL, long resultSweepThreshold) {
        super(resultTTL);
        this.resultSweepThreshold = resultSweepThreshold;
        jobResultLocations = new LinkedHashMap<>();
    }

    @Override
    public void init(ExecutorService executor, IJobResultCallback jobResultCallback) {
        executor.execute(new ResultStateSweeper(this, resultSweepThreshold, LOGGER));
        this.jobResultCallback = jobResultCallback;
    }

    @Override
    public synchronized void notifyJobCreation(JobId jobId, JobSpecification spec) throws HyracksException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getClass().getSimpleName() + " notified of new job " + jobId);
        }
        if (jobResultLocations.get(jobId) != null) {
            throw HyracksDataException.create(ErrorCode.MORE_THAN_ONE_RESULT, jobId);
        }
        jobResultLocations.put(jobId, new JobResultInfo(new ResultJobRecord(), null));
    }

    @Override
    public synchronized void notifyJobStart(JobId jobId) throws HyracksException {
        jobResultLocations.get(jobId).getRecord().start();
    }

    @Override
    public void notifyJobFinish(JobId jobId, JobStatus jobStatus, List<Exception> exceptions) throws HyracksException {
        if (exceptions == null || exceptions.isEmpty()) {
            final ResultJobRecord resultJobRecord = getResultJobRecord(jobId);
            if (resultJobRecord == null) {
                return;
            }
            resultJobRecord.finish();
            jobResultCallback.completed(jobId, resultJobRecord);
        }
    }

    private ResultJobRecord getResultJobRecord(JobId jobId) {
        final JobResultInfo jri = jobResultLocations.get(jobId);
        return jri == null ? null : jri.getRecord();
    }

    private ResultJobRecord getNonNullResultJobRecord(JobId jobId) throws HyracksDataException {
        final ResultJobRecord djr = getResultJobRecord(jobId);
        if (djr == null) {
            throw HyracksDataException.create(ErrorCode.NO_RESULT_SET, jobId);
        }
        return djr;
    }

    @Override
    public synchronized void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, IResultMetadata metadata,
            boolean emptyResult, int partition, int nPartitions, NetworkAddress networkAddress)
            throws HyracksDataException {
        ResultJobRecord djr = getNonNullResultJobRecord(jobId);
        djr.setResultSetMetaData(rsId, metadata, nPartitions);
        ResultDirectoryRecord record = djr.getOrCreateDirectoryRecord(partition);

        record.setNetworkAddress(networkAddress);
        record.setEmpty(emptyResult);
        record.start();

        final JobResultInfo jobResultInfo = jobResultLocations.get(jobId);
        Waiter waiter = jobResultInfo.getWaiter(rsId);
        if (waiter != null) {
            try {
                ResultDirectoryRecord[] updatedRecords = updatedRecords(jobId, rsId, waiter.knownRecords);
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
        ResultJobRecord djr = getNonNullResultJobRecord(jobId);
        djr.getDirectoryRecord(partition).writeEOS();
        djr.updateState();
        notifyAll();
    }

    @Override
    public synchronized void reportJobFailure(JobId jobId, List<Exception> exceptions) {
        Exception ex = exceptions.isEmpty() ? null : exceptions.get(0);
        Level logLevel = Level.DEBUG;
        if (LOGGER.isEnabled(logLevel)) {
            LOGGER.log(logLevel, "job " + jobId + " failed and is being reported to " + getClass().getSimpleName(), ex);
        }
        ResultJobRecord rjr = getResultJobRecord(jobId);
        if (rjr != null) {
            rjr.fail(exceptions);
        }
        final JobResultInfo jobResultInfo = jobResultLocations.get(jobId);
        if (jobResultInfo != null) {
            jobResultInfo.setException(ex);
        }
        notifyAll();
    }

    @Override
    public synchronized ResultJobRecord.Status getResultStatus(JobId jobId, ResultSetId rsId)
            throws HyracksDataException {
        return getNonNullResultJobRecord(jobId).getStatus();
    }

    @Override
    public synchronized IResultMetadata getResultMetadata(JobId jobId, ResultSetId rsId) throws HyracksDataException {
        return getNonNullResultJobRecord(jobId).getResultSetMetaData().getMetadata();
    }

    @Override
    public synchronized Set<JobId> getJobIds() {
        return jobResultLocations.keySet();
    }

    @Override
    public IResultStateRecord getState(JobId jobId) {
        return getResultJobRecord(jobId);
    }

    @Override
    public synchronized void sweep(JobId jobId) {
        jobResultLocations.remove(jobId);
    }

    @Override
    public synchronized void getResultPartitionLocations(JobId jobId, ResultSetId rsId,
            ResultDirectoryRecord[] knownRecords, IResultCallback<ResultDirectoryRecord[]> callback)
            throws HyracksDataException {
        ResultDirectoryRecord[] updatedRecords = updatedRecords(jobId, rsId, knownRecords);
        if (updatedRecords == null) {
            jobResultLocations.get(jobId).addWaiter(rsId, knownRecords, callback);
        } else {
            callback.setValue(updatedRecords);
        }
    }

    /**
     * Compares the records already known by the client for the given job's result set id with the records that the
     * result directory service knows and if there are any newly discovered records returns a whole array with the
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
     *             TODO(madhusudancs): Think about caching (and still be stateless) instead of this ugly O(n)
     *             iterations for every check. This already looks very expensive.
     */
    private ResultDirectoryRecord[] updatedRecords(JobId jobId, ResultSetId rsId, ResultDirectoryRecord[] knownRecords)
            throws HyracksDataException {
        ResultJobRecord djr = getNonNullResultJobRecord(jobId);
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
        final ResultSetMetaData resultSetMetaData = djr.getResultSetMetaData();
        if (resultSetMetaData == null) {
            return null;
        }
        ResultDirectoryRecord[] records = resultSetMetaData.getRecords();
        return Arrays.equals(records, knownRecords) ? null : records;
    }

    public PrintWriter print(PrintWriter pw) {
        for (JobId jId : getJobIds()) {
            pw.print(jId.toString());
            pw.print(" - ");
            pw.println(getResultJobRecord(jId));
        }
        pw.flush();
        return pw;
    }
}

class JobResultInfo {

    private ResultJobRecord record;
    private Waiters waiters;
    private Exception exception;

    JobResultInfo(ResultJobRecord record, Waiters waiters) {
        this.record = record;
        this.waiters = waiters;
    }

    ResultJobRecord getRecord() {
        return record;
    }

    void addWaiter(ResultSetId rsId, ResultDirectoryRecord[] knownRecords,
            IResultCallback<ResultDirectoryRecord[]> callback) {
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
    ResultDirectoryRecord[] knownRecords;
    IResultCallback<ResultDirectoryRecord[]> callback;

    Waiter(ResultDirectoryRecord[] knownRecords, IResultCallback<ResultDirectoryRecord[]> callback) {
        this.knownRecords = knownRecords;
        this.callback = callback;
    }
}
