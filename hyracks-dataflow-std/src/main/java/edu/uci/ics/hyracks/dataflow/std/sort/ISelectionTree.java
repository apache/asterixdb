package edu.uci.ics.hyracks.dataflow.std.sort;

/**
 * @author pouria
 * 
 *         Defines the selection tree, used in sorting with replacement
 *         selection to manage the order of output tuples into the runs, during
 *         the run generation phase. This tree contains tuples, belonging to two
 *         different runs: - Current run (being written to the output) - Next
 *         run
 */

public interface ISelectionTree {

    /**
     * Inserts a new element into the selectionTree
     * 
     * @param element
     *            contains the pointer to the memory slot, containing the tuple,
     *            along with its run number
     */
    void insert(int[] element);

    /**
     * Removes and returns the smallest element in the tree
     * 
     * @param result
     *            is the array that will eventually contain minimum entry
     *            pointer
     */
    void getMin(int[] result);

    /**
     * Removes and returns the largest element in the tree
     * 
     * @param result
     *            is the array that will eventually contain maximum entry
     *            pointer
     */
    void getMax(int[] result);

    /**
     * @return True of the selection tree does not have any element, false
     *         otherwise
     */
    boolean isEmpty();

    /**
     * Removes all the elements in the tree
     */
    void reset();

    /**
     * Returns (and does NOT remove) the smallest element in the tree
     * 
     * @param result
     *            is the array that will eventually contain minimum entry
     *            pointer
     */
    void peekMin(int[] result);

    /**
     * Returns (and does NOT remove) the largest element in the tree
     * 
     * @param result
     *            is the array that will eventually contain maximum entry
     *            pointer
     */
    void peekMax(int[] result);

}