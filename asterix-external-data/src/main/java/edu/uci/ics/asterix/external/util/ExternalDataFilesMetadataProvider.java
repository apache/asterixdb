package edu.uci.ics.asterix.external.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.asterix.external.adapter.factory.HDFSAdapterFactory;
import edu.uci.ics.asterix.external.dataset.adapter.AbstractDatasourceAdapter;
import edu.uci.ics.asterix.external.dataset.adapter.HDFSAdapter;

public class ExternalDataFilesMetadataProvider {
	public static ArrayList<FileStatus> getHDFSFileStatus(AbstractDatasourceAdapter adapter) throws IOException
	{
		ArrayList<FileStatus> files = new ArrayList<FileStatus>();
		//Configure hadoop connection
		Configuration conf = HDFSAdapterFactory.configureHadoopConnection(adapter.getConfiguration());
		FileSystem fs = FileSystem.get(conf);
		//get the list of paths from the adapter
		StringTokenizer tokenizer = new StringTokenizer(((String)adapter.getConfiguration().get(HDFSAdapter.KEY_PATH)),",");
		Path inputPath = null;
		FileStatus[] fileStatuses;
		while(tokenizer.hasMoreTokens())
		{
			inputPath = new Path(tokenizer.nextToken().trim());
			fileStatuses = fs.listStatus(inputPath);
			for(int i=0; i < fileStatuses.length; i++)
			{
				files.add(fileStatuses[i]);
			}
		}
		return files;
	}
}