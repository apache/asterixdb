/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hivesterix.runtime.operator.filewrite;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.runtime.jobgen.Schema;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntime;
import edu.uci.ics.hyracks.algebricks.runtime.base.IPushRuntimeFactory;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

@SuppressWarnings("deprecation")
public class HivePushRuntimeFactory implements IPushRuntimeFactory {

    private static final long serialVersionUID = 1L;

    private final RecordDescriptor inputRecordDesc;
    private transient JobConf conf;
    private final FileSinkDesc fileSink;
    private final RowSchema outSchema;
    private final Schema schema;

    /**
     * the content of the configuration
     */
    private String confContent;

    public HivePushRuntimeFactory(RecordDescriptor inputRecordDesc, JobConf conf, FileSinkOperator fsp, Schema sch) {
        this.inputRecordDesc = inputRecordDesc;
        this.conf = conf;
        this.fileSink = fsp.getConf();
        outSchema = fsp.getSchema();
        this.schema = sch;

        writeConfContent();
    }

    @Override
    public String toString() {
        return "file write";
    }

    @Override
    public IPushRuntime createPushRuntime(IHyracksTaskContext context) throws AlgebricksException {
        if (conf == null)
            readConfContent();

        return new HiveFileWritePushRuntime(context, inputRecordDesc, conf, fileSink, outSchema, schema);
    }

    private void readConfContent() {
        File dir = new File("hadoop-conf-tmp");
        if (!dir.exists()) {
            dir.mkdir();
        }

        String fileName = "hadoop-conf-tmp/" + UUID.randomUUID() + System.currentTimeMillis() + ".xml";
        try {
            PrintWriter out = new PrintWriter((new OutputStreamWriter(new FileOutputStream(new File(fileName)))));
            out.write(confContent);
            out.close();
            conf = new JobConf(fileName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeConfContent() {
        File dir = new File("hadoop-conf-tmp");
        if (!dir.exists()) {
            dir.mkdir();
        }

        String fileName = "hadoop-conf-tmp/" + UUID.randomUUID() + System.currentTimeMillis() + ".xml";
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(new File(fileName)));
            conf.writeXml(out);
            out.close();

            DataInputStream in = new DataInputStream(new FileInputStream(fileName));
            StringBuffer buffer = new StringBuffer();
            String line;
            while ((line = in.readLine()) != null) {
                buffer.append(line + "\n");
            }
            in.close();
            confContent = buffer.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
