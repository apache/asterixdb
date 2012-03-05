package edu.uci.ics.asterix.transaction.management.service.locking;

/**
 * @author pouria An implementation of the ILockMatrix Each lock mode is shown
 *         as an integer. More specifically:
 *         - i-th entry of the conflictTable corresponds to the i-th lock mode
 *         and it shows the conflicting mask of that mode. j-th bit of the i-th
 *         entry is 1 if and only if i-th lock mode conflicts with the j-th lock
 *         mode.
 *         - i-th entry of the conversionTable corresponds to the i-th lock mode
 *         and it shows whether going from that mode to a new mode is actually a
 *         conversion or not. j-th bit of the i-th entry is 1 if and only if
 *         j-th lock mode is "stronger" than the i-th mode, i.e. lock changing
 *         from i-th mode to the j-th mode is actually a conversion.
 */
public class LockMatrix implements ILockMatrix {

    int[] conflictTable;
    int[] conversionTable;

    public LockMatrix(int[] confTab, int[] convTab) {
        this.conflictTable = confTab;
        this.conversionTable = convTab;
    }

    @Override
    public boolean conflicts(int reqMask, int lockMode) {
        return ((reqMask & conflictTable[lockMode]) != 0);
    }

    @Override
    public boolean isConversion(int currentLockMode, int reqLockMode) {
        return ((conversionTable[currentLockMode] & (0x01 << reqLockMode)) != 0);
    }
}
