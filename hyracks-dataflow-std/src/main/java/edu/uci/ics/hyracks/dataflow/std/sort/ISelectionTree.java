package edu.uci.ics.hyracks.dataflow.std.sort;

/**
 * @author pouria Defines the selection tree, used in sorting with replacement
 *         selection to manage the order of outputing tuples into the runs,
 *         during the run generation phase. At each point of time, this tree
 *         contains tuples, belonging to two different runs: - Current run,
 *         being written to the output - Next run
 */
public interface ISelectionTree {
	/**
	 * Inserts a new element into the selection.
	 * 
	 * @param element
	 *            contains the pointer to the memory slot, containing the tuple,
	 *            along with its run number
	 */
	void insert(int[] element);

	/**
	 * @return Removes and returns the smallest elemnt in the tree
	 */
	int[] getMin();

	/**
	 * @return Removes and returns the largest elemnt in the tree
	 */
	int[] getMax();

	/**
	 * @return True of the selection tree does not have any element, and false
	 *         otherwise.
	 */
	boolean isEmpty();

	/**
	 * Removes all the elements in the tree
	 */
	void reset();

	/**
	 * 
	 * @return Returns, but does NOT remove, the smallest element in the tree
	 */
	int[] peekMin();

	/**
	 * 
	 * @return Returns, but does NOT remove, the largest element in the tree
	 */
	int[] peekMax();

}