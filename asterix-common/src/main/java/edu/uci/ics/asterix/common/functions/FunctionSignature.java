package edu.uci.ics.asterix.common.functions;

import java.io.Serializable;

public class FunctionSignature implements Serializable {
	private final String namespace;
	private final String name;
	private final int arity;
	private final String rep;

	public FunctionSignature(String namespace, String name, int arity) {
		this.namespace = namespace;
		this.name = name;
		this.arity = arity;
		rep = namespace + "." + name + "@" + arity;
	}

	public boolean equals(Object o) {
		if (!(o instanceof FunctionSignature)) {
			return false;
		} else {
			FunctionSignature f = ((FunctionSignature) o);
			return ((namespace != null && namespace.equals(f.getNamespace()) || (namespace == null && f
					.getNamespace() == null)))
					&& name.equals(f.getName())
					&& arity == f.getArity();
		}
	}

	public String toString() {
		return rep;
	}

	public int hashCode() {
		return rep.hashCode();
	}

	public String getNamespace() {
		return namespace;
	}

	public String getName() {
		return name;
	}

	public int getArity() {
		return arity;
	}

}
