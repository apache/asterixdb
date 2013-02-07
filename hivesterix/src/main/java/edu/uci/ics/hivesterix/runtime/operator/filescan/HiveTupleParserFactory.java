package edu.uci.ics.hivesterix.runtime.operator.filescan;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hivesterix.serde.lazy.LazySerDe;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

@SuppressWarnings("deprecation")
public class HiveTupleParserFactory implements ITupleParserFactory {

	private static final long serialVersionUID = 1L;

	private int[] outputColumns;

	private String outputSerDeClass = LazySerDe.class.getName();

	private String inputSerDeClass;

	private transient JobConf conf;

	private Properties tbl;

	private String confContent;

	private String inputFormatClass;

	public HiveTupleParserFactory(PartitionDesc desc, JobConf conf,
			int[] outputColumns) {
		this.conf = conf;
		tbl = desc.getProperties();
		inputFormatClass = (String) tbl.getProperty("file.inputformat");
		inputSerDeClass = (String) tbl.getProperty("serialization.lib");
		this.outputColumns = outputColumns;

		writeConfContent();
	}

	@Override
	public ITupleParser createTupleParser(IHyracksTaskContext ctx) {
		readConfContent();
		try {
			return new HiveTupleParser(inputFormatClass, inputSerDeClass,
					outputSerDeClass, tbl, conf, ctx, outputColumns);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private void writeConfContent() {
		File dir = new File("hadoop-conf-tmp");
		if (!dir.exists()) {
			dir.mkdir();
		}

		String fileName = "hadoop-conf-tmp/" + UUID.randomUUID()
				+ System.currentTimeMillis() + ".xml";
		try {
			DataOutputStream out = new DataOutputStream(new FileOutputStream(
					new File(fileName)));
			conf.writeXml(out);
			out.close();

			DataInputStream in = new DataInputStream(new FileInputStream(
					fileName));
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

	private void readConfContent() {
		File dir = new File("hadoop-conf-tmp");
		if (!dir.exists()) {
			dir.mkdir();
		}

		String fileName = "hadoop-conf-tmp/" + UUID.randomUUID()
				+ System.currentTimeMillis() + ".xml";
		try {
			PrintWriter out = new PrintWriter((new OutputStreamWriter(
					new FileOutputStream(new File(fileName)))));
			out.write(confContent);
			out.close();

			conf = new JobConf(fileName);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
