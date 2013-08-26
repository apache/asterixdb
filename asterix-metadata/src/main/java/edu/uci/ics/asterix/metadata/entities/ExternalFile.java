package edu.uci.ics.asterix.metadata.entities;

import java.util.Date;

import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

public class ExternalFile implements IMetadataEntity{

	/**
	 * A class for metadata entity externalFile
	 * This class represents an external dataset file and is intended for use with external data indexes
	 */
	private static final long serialVersionUID = 1L;
	
	private String dataverseName;
	private String datasetName;
	private Date lastModefiedTime;
	private long size;
	private String fileName;
	private int fileNumber;
	
	
	public ExternalFile(String dataverseName, String datasetName, Date lastModefiedTime, long size, String fileName,
			int fileNumber) {
		this.dataverseName = dataverseName;
		this.datasetName = datasetName;
		this.lastModefiedTime = lastModefiedTime;
		this.size = size;
		this.fileName = fileName;
		this.fileNumber = fileNumber;
	}

	public String getDataverseName() {
		return dataverseName;
	}

	public void setDataverseName(String dataverseName) {
		this.dataverseName = dataverseName;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}
	public Date getLastModefiedTime() {
		return lastModefiedTime;
	}

	public void setLastModefiedTime(Date lastModefiedTime) {
		this.lastModefiedTime = lastModefiedTime;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getFileNumber() {
		return fileNumber;
	}

	public void setFileNumber(int fileNumber) {
		this.fileNumber = fileNumber;
	}

	@Override
	public Object addToCache(MetadataCache cache) {
		//return cache.addExternalFileIfNotExists(this);
		return null;
	}

	@Override
	public Object dropFromCache(MetadataCache cache) {
		//cache.dropExternalFile(this);
		return null;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof ExternalFile))
            return false;
        ExternalFile anotherFile = (ExternalFile) obj;
        if(fileNumber != anotherFile.fileNumber)
        	return false;
        if(!fileName.equals(anotherFile.fileName))
        	return false;
        return true;
	}
	
}