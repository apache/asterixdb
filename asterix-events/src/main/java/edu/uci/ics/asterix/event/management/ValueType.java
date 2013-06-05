/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.event.management;

import edu.uci.ics.asterix.event.schema.pattern.Value;

public class ValueType {

	public static enum Type {
		ABS, RANDOM_RANGE, RANDOM_MIN_MAX
	}

	private Value value;
	private Type type;

	public ValueType(Value value) {
		this.value = value;
		if (value.getAbsvalue() != null) {
			type = Type.ABS;
		} else if (value.getRandom() != null) {
			if (value.getRandom().getMinmax() != null) {
				type = Type.RANDOM_MIN_MAX;
			} else if (value.getRandom().getRange() != null) {
				type = Type.RANDOM_RANGE;
			} else {
				throw new IllegalStateException("Incorrect value type");
			}
		}
	}

	public int getMin() {
		switch (type) {
		case RANDOM_MIN_MAX:
			return Integer.parseInt(value.getRandom().getMinmax().getMin());
		default:
			throw new IllegalStateException("");
		}
	}

	public int getMax() {
		switch (type) {
		case RANDOM_MIN_MAX:
			return Integer.parseInt(value.getRandom().getMinmax().getMax());
		default:
			throw new IllegalStateException("");
		}
	}

	public String[] getRangeSet() {
		switch (type) {
		case RANDOM_RANGE:
			return value.getRandom().getRange().getSet().split(" ");
		default:
			throw new IllegalStateException("");
		}
	}

	public String[] getRangeExcluded() {
		switch (type) {
		case RANDOM_RANGE:
			String exl = value.getRandom().getRange().getExclude();
			return exl != null ? exl.split(" ") : null;
		default:
			throw new IllegalStateException("");
		}
	}

	public String getAbsoluteValue() {
		switch (type) {
		case ABS:
			return value.getAbsvalue();
		default:
			throw new IllegalStateException("");
		}
	}

	public Type getType() {
		return type;
	}

}
