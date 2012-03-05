package edu.uci.ics.asterix.transaction.management.service.locking;

/**
 * @author pouria Interface for a hashTable, used in the internal data
 *         structures of lockManager
 * @param <K>
 *            Type of the objects, used as keys
 * @param <V>
 *            Type of the objects, used as values
 */
public interface ILockHashTable<K, V> {

    public void put(K key, V value);

    public V get(K key);

    public V remove(K key);

    public int getKeysetSize();

}
