package edu.uci.ics.hivesterix.runtime.operator.filescan;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.UUID;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.eclipse.jetty.util.log.Log;

@SuppressWarnings({ "deprecation", "rawtypes" })
public class HiveFileSplitProvider extends AbstractHiveFileSplitProvider {
	private static final long serialVersionUID = 1L;

	private transient InputFormat format;
	private transient JobConf conf;
	private String confContent;
	final private int nPartition;
	private transient FileSplit[] splits;

	public HiveFileSplitProvider(JobConf conf, String filePath, int nPartition) {
		format = conf.getInputFormat();
		this.conf = conf;
		this.nPartition = nPartition;
		writeConfContent();
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

	@Override
	/**
	 * get the HDFS file split
	 */
	public FileSplit[] getFileSplitArray() {
		readConfContent();
		conf.setClassLoader(this.getClass().getClassLoader());
		format = conf.getInputFormat();
		// int splitSize = conf.getInt("mapred.min.split.size", 0);

		if (splits == null) {
			try {
				splits = (org.apache.hadoop.mapred.FileSplit[]) format
						.getSplits(conf, nPartition);
				System.out.println("hdfs split number: " + splits.length);
			} catch (IOException e) {
				String inputPath = conf.get("mapred.input.dir");
				String hdfsURL = conf.get("fs.default.name");
				String alternatePath = inputPath.replaceAll(hdfsURL, "file:");
				conf.set("mapred.input.dir", alternatePath);
				try {
					splits = (org.apache.hadoop.mapred.FileSplit[]) format
							.getSplits(conf, nPartition);
					System.out.println("hdfs split number: " + splits.length);
				} catch (IOException e1) {
					e1.printStackTrace();
					Log.debug(e1.getMessage());
					return null;
				}
			}
		}
		return splits;
	}
}
