package edu.uci.ics.hyracks.algebricks.core.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ListSet<E> implements Set<E> {
	private List<E> list = new ArrayList<E>();

	public ListSet() {
	}

	public ListSet(Collection<? extends E> arg) {
		this.addAll(arg);
	}

	@Override
	public boolean add(E arg0) {
		return list.add(arg0);
	}

	@Override
	public boolean addAll(Collection<? extends E> arg0) {
		return list.addAll(arg0);
	}

	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public boolean contains(Object arg0) {
		return list.contains(arg0);
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {
		return list.containsAll(arg0);
	}

	@Override
	public boolean isEmpty() {
		return list.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return list.iterator();
	}

	@Override
	public boolean remove(Object arg0) {
		return list.remove(arg0);
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {
		return list.removeAll(arg0);
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {
		return list.retainAll(arg0);
	}

	@Override
	public int size() {
		return list.size();
	}

	@Override
	public Object[] toArray() {
		return list.toArray();
	}

	@Override
	public <T> T[] toArray(T[] arg0) {
		return list.toArray(arg0);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object arg) {
		if (!(arg instanceof ListSet))
			return false;
		ListSet<E> set = (ListSet<E>) arg;
		for (E item : set) {
			if (!this.contains(item))
				return false;
		}
		for (E item : this) {
			if (!set.contains(item))
				return false;
		}
		return true;
	}
	
	@Override
	public String toString(){
		return list.toString();
	}

}
