package edu.uci.ics.asterix.file;

import edu.uci.ics.asterix.common.config.AsterixCompilerProperties;
import edu.uci.ics.asterix.om.util.AsterixAppContextInfo;
import edu.uci.ics.hyracks.api.job.JobSpecification;

public class JobSpecificationUtils {
    public static JobSpecification createJobSpecification() {
        JobSpecification spec = new JobSpecification();
        AsterixCompilerProperties compilerProperties = AsterixAppContextInfo.getInstance().getCompilerProperties();
        int frameSize = compilerProperties.getFrameSize();
        spec.setFrameSize(frameSize);

        return spec;
    }
}