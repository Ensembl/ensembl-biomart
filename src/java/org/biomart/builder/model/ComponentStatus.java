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

package org.biomart.builder.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the status of any component with regard to how the system came to
 * know about it.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.15 $, $Date: 2007-10-26 12:24:08 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class ComponentStatus implements Comparable {
	private static final long serialVersionUID = 1L;

	private static final Map singletons = new HashMap();

	private final String name;

	/**
	 * Use this constant to refer to a component that was inferred but has
	 * had different settings specified by the user.
	 */
	public static final ComponentStatus MODIFIED = ComponentStatus
			.get("MODIFIED");

	/**
	 * Use this constant to refer to a component that was specified by the user.
	 */
	public static final ComponentStatus HANDMADE = ComponentStatus
			.get("HANDMADE");

	/**
	 * Use this constant to refer to a component that was inferred from the
	 * database and is assumed to be correct.
	 */
	public static final ComponentStatus INFERRED = ComponentStatus
			.get("INFERRED");

	/**
	 * Use this constant to refer to a component that was incorrectly inferred
	 * from the database and should be ignored.
	 */
	public static final ComponentStatus INFERRED_INCORRECT = ComponentStatus
			.get("INFERRED_INCORRECT");

	/**
	 * The static factory method creates and returns a status object with the
	 * given name. It ensures the object returned is a singleton. Note that the
	 * names of status objects are case-sensitive.
	 * 
	 * @param name
	 *            the name of the status object.
	 * @return the status object.
	 */
	public static ComponentStatus get(final String name) {
		// Do we already have this one?
		// If so, then return it.
		if (ComponentStatus.singletons.containsKey(name))
			return (ComponentStatus) ComponentStatus.singletons.get(name);

		// Otherwise, create it, remember it.
		final ComponentStatus s = new ComponentStatus(name);
		ComponentStatus.singletons.put(name, s);

		// Return it.
		return s;
	}

	private ComponentStatus(final String name) {
		this.name = name;
	}

	public int compareTo(final Object o) throws ClassCastException {
		final ComponentStatus c = (ComponentStatus) o;
		return this.toString().compareTo(c.toString());
	}

	public boolean equals(final Object o) {
		// We are dealing with singletons so can use == happily.
		return o == this;
	}

	public int hashCode() {
		return this.name.hashCode();
	}

	public String toString() {
		return this.name;
	}
}
