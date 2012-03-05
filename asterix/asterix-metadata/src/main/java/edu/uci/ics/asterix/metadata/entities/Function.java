package edu.uci.ics.asterix.metadata.entities;

import java.util.List;

import edu.uci.ics.asterix.metadata.MetadataCache;
import edu.uci.ics.asterix.metadata.api.IMetadataEntity;

public class Function implements IMetadataEntity {

	private String dataverseName;
	private String functionName;
	private int arity;
	private List<String> params;
	private String functionBody;

	public Function(String dataverseName, String functionName, int arity,
			List<String> params,
			String functionBody) {
		this.dataverseName = dataverseName;
		this.functionName = functionName;
		this.arity = arity;
		this.params = params;
		this.functionBody = functionBody;
	}

	public String getDataverseName() {
		return dataverseName;
	}

	public void setDataverseName(String dataverseName) {
		this.dataverseName = dataverseName;
	}

	public String getFunctionName() {
		return functionName;
	}

	public int getFunctionArity() {
		return arity;
	}

	public List<String> getParams() {
		return params;
	}

	public void setParams(List<String> params) {
		this.params = params;
	}

	public String getFunctionBody() {
		return functionBody;
	}

	public void setFunctionBody(String functionBody) {
		this.functionBody = functionBody;
	}

	@Override
	public Object addToCache(MetadataCache cache) {
		return cache.addFunctionIfNotExists(this);
	}

	@Override
	public Object dropFromCache(MetadataCache cache) {
		return cache.dropFunction(this);
	}

}

