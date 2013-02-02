package edu.uci.ics.hivesterix.logical.expression;

import java.io.Serializable;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class HiveFunctionInfo implements IFunctionInfo, Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * primary function identifier
	 */
	private transient FunctionIdentifier fid;

	/**
	 * secondary function identifier: function name
	 */
	private transient Object secondaryFid;

	public HiveFunctionInfo(FunctionIdentifier fid, Object secondFid) {
		this.fid = fid;
		this.secondaryFid = secondFid;
	}

	@Override
	public FunctionIdentifier getFunctionIdentifier() {
		return fid;
	}

	public Object getInfo() {
		return secondaryFid;
	}

}
