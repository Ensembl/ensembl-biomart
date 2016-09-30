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

import java.beans.PropertyChangeEvent;
import java.util.Set;

/**
 * This class wraps an existing set, and causes {@link PropertyChangeEvent}
 * events to be fired whenever it changes.
 * <p>
 * Adding objects to the set will result in events where the before value is
 * null and the after value is the value being added.
 * <p>
 * Removing them will result in events where the before value is they value
 * being removed and the after value is null.
 * <p>
 * Multiple add/remove events will have both before and after values of null.
 * <p>
 * All events will have a property of {@link BeanSet#propertyName}.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.2 $, $Date: 2007-10-03 10:41:02 $, modified by 
 * 			$Author: rh4 $
 * @since 0.7
 */
public class BeanSet extends BeanCollection implements Set {

	private static final long serialVersionUID = 1L;

	/**
	 * Construct a new instance that wraps the delegate set and produces
	 * {@link PropertyChangeEvent} events whenever the delegate set changes.
	 * 
	 * @param delegate
	 *            the delegate set.
	 */
	public BeanSet(final Set delegate) {
		super(delegate);
	}
}
