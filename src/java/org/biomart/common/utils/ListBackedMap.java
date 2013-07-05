/*
 Copyright (C) 2006 EBI
 
 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 2.1 of the License, or (at your option) any later version.
 
 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the itmplied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 Lesser General Public License for more details.
 
 You should have received a copy of the GNU Lesser General Public
 License along with this library; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.biomart.common.utils;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles list backed maps.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.1 $, $Date: 2007-11-09 11:36:28 $, modified by
 *          $Author: rh4 $
 * @since 0.7
 */
public class ListBackedMap extends AbstractMap implements Serializable {
	private static final long serialVersionUID = 1L;

	private final List keys = new ArrayList();

	private final List values = new ArrayList();

	/**
	 * Construct a new list backed map. Keys will be returned in the order they
	 * were added.
	 */
	public ListBackedMap() {
		super();
	}

	/**
	 * Construct a new list backed map. Keys will be returned in the order they
	 * were added.
	 * 
	 * @param map
	 *            a set of initial entries, to be added in the order returned by
	 *            the {@link Map#entrySet()} iterator.
	 */
	public ListBackedMap(final Map map) {
		super();
		this.putAll(map);
	}

	public Set entrySet() {
		return new AbstractSet() {
			public Iterator iterator() {
				return new Iterator() {
					private int i = -1;

					public boolean hasNext() {
						return this.i < size() - 1;
					}

					public Object next() {
						++i;
						return new Map.Entry() {
							public Object getKey() {
								return ListBackedMap.this.keys.get(i);
							}

							public Object getValue() {
								return ListBackedMap.this.values.get(i);
							}

							public Object setValue(final Object value) {
								return ListBackedMap.this.values.set(i, value);
							}
						};
					}

					public void remove() {
						ListBackedMap.this.keys.remove(this.i);
						ListBackedMap.this.values.remove(this.i);
					}
				};
			}

			public int size() {
				return ListBackedMap.this.keys.size();
			}
		};
	}

	public Object put(final Object key, final Object value) {
		if (this.keys.contains(key))
			// Replace.
			return this.values.set(this.keys.indexOf(key), value);
		else {
			// Append.
			this.keys.add(key);
			this.values.add(value);
			return value;
		}
	}

	/**
	 * Inserts the key after the previousKey, or at the beginning of the map if
	 * previousKey is null. Otherwise, see {@link #put(Object, Object)}.
	 * 
	 * @param previousKey
	 *            the previousKey to insert after.
	 * @param key
	 *            the key to insert.
	 * @param value
	 *            the value to insert.
	 * @return the previous value for that key, if any.
	 */
	public Object put(final Object previousKey, final Object key,
			final Object value) {
		// Remove existing if exists.
		Object returnObj = null;
		final int existingIndex = this.keys.indexOf(key);
		if (existingIndex >= 0) {
			this.keys.remove(existingIndex);
			returnObj = this.values.remove(existingIndex);
		}
		if (previousKey == null) {
			// Prepend.
			this.keys.add(0, key);
			this.values.add(0, value);
		} else {
			// Insert.
			final int index = this.keys.indexOf(previousKey) + 1;
			this.keys.add(index, key);
			this.values.add(index, value);
		}
		return returnObj;
	}
}
