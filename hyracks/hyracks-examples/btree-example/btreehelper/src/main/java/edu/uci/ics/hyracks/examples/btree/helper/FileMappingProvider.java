package edu.uci.ics.hyracks.examples.btree.helper;

import java.util.Hashtable;
import java.util.Map;

import edu.uci.ics.hyracks.storage.common.file.IFileMappingProvider;

public class FileMappingProvider implements IFileMappingProvider {				
	
	private static final long serialVersionUID = 1L;
	private int nextFileId = 0;
	private Map<String, Integer> map = new Hashtable<String, Integer>();
		
	@Override
	public Integer mapNameToFileId(String name, boolean create) {
		Integer val = map.get(name);						
		if(create) {
			if(val == null) {
				int ret = nextFileId;
				map.put(name, nextFileId++);
				return ret;
			}
			else {
				return null; // create requested but value already exists				
			}
		}
		else {
			return val; // just return value
		}			
	}
	
	@Override
	public void unmapName(String name) {
		map.remove(name);
	}
	
	@Override
	public Integer getFileId(String name) {
		return map.get(name);
	}
}
