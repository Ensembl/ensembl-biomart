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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This class defines an inverse view of a map.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.6 $, $Date: 2007-10-03 10:41:02 $, modified by 
 * 			$Author: rh4 $
 * @since 0.6
 */
public class InverseMap implements Map {

	final private Map map;

	/**
	 * Defines an inverse view. Only works if the values in the viewed map are
	 * unique.
	 * 
	 * @param map
	 *            the map to view inversely.
	 */
	public InverseMap(final Map map) {
		this.map = map;
	}

	public void clear() {
		this.map.clear();
	}

	public boolean containsKey(final Object key) {
		return this.map.containsValue(key);
	}

	public boolean containsValue(final Object value) {
		return this.map.containsKey(value);
	}

	public Set entrySet() {
		final HashSet entries = new HashSet();
		for (final Iterator i = this.map.entrySet().iterator(); i.hasNext();) {
			final Map.Entry me = (Map.Entry) i.next();
			entries.add(new Map.Entry() {
				private final Object key = me.getValue();

				private final Object value = me.getKey();

				public Object getKey() {
					return this.key;
				}

				public Object getValue() {
					return this.value;
				}

				public Object setValue(final Object value) {
					throw new UnsupportedOperationException();
				}
			});
		}
		return entries;
	}

	public Object get(final Object key) {
		for (final Iterator i = this.map.entrySet().iterator(); i.hasNext();) {
			final Map.Entry me = (Map.Entry) i.next();
			if (me.getValue().equals(key))
				return me.getKey();
		}
		return null;
	}

	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	public Set keySet() {
		return new HashSet(this.map.values());
	}

	public Object put(final Object key, final Object value) {
		throw new UnsupportedOperationException();
	}

	public void putAll(final Map t) {
		throw new UnsupportedOperationException();
	}

	public Object remove(final Object key) {
		throw new UnsupportedOperationException();
	}

	public int size() {
		return this.map.size();
	}

	public Collection values() {
		return this.map.keySet();
	}

	public int hashCode() {
		return this.map.hashCode();
	}

	public boolean equals(final Object o) {
		return this.map.equals(o);
	}

	public String toString() {
		return this.map.toString();
	}

}
