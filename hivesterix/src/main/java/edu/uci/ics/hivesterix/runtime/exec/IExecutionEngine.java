package edu.uci.ics.hivesterix.runtime.exec;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Task;

public interface IExecutionEngine {

	/**
	 * compile the job
	 * 
	 * @param rootTasks
	 *            : Hive MapReduce plan
	 * @return 0 pass, 1 fail
	 */
	public int compileJob(List<Task<? extends Serializable>> rootTasks);

	/**
	 * execute the job with latest compiled plan
	 * 
	 * @return
	 */
	public int executeJob();
}
