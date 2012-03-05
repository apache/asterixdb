package edu.uci.ics.asterix.transaction.management.service.locking;

/**
 * @author pouria
 *         Shows: - The conflict matrix for the locking protocol (whether two
 *         lock modes conflict with each other or not on a single resource) -
 *         Whether request to convert a lock mode to a new one is a conversion
 *         (i.e. the new lock mode is stringer than the current one) or not
 *         Each lock mode is shown/interpreted as an integer
 */

public interface ILockMatrix {

    /**
     * @param mask
     *            (current/expected) lock mask on the resource
     * @param reqLockMode
     *            index of the requested lockMode
     * @return true if the lock request conflicts with the mask
     */
    public boolean conflicts(int mask, int reqLockMode);

    /**
     * @param currentLockMode
     * @param reqLockMode
     * @return true if the request is a conversion
     */
    public boolean isConversion(int currentLockMode, int reqLockMode);

}
