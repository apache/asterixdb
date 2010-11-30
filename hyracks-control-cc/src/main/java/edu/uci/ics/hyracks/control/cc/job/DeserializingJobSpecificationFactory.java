package edu.uci.ics.hyracks.control.cc.job;

import java.io.IOException;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;
import edu.uci.ics.hyracks.api.application.ICCBootstrap;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IJobSpecificationFactory;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;

public class DeserializingJobSpecificationFactory implements IJobSpecificationFactory {
    public static final IJobSpecificationFactory INSTANCE = new DeserializingJobSpecificationFactory();

    private DeserializingJobSpecificationFactory() {
    }

    @Override
    public JobSpecification createJobSpecification(byte[] bytes, ICCBootstrap bootstrap, ICCApplicationContext appCtx)
            throws HyracksException {
        try {
            return (JobSpecification) JavaSerializationUtils.deserialize(bytes, appCtx.getClassLoader());
        } catch (IOException e) {
            throw new HyracksException(e);
        } catch (ClassNotFoundException e) {
            throw new HyracksException(e);
        }
    }
}