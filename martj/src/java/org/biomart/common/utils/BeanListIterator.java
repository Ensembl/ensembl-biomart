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
import java.util.ListIterator;

/**
 * This class wraps an existing iterator, and causes {@link PropertyChangeEvent}
 * events to be fired whenever it changes.
 * <p>
 * Adding values will result in events where the before value is null and the
 * after value is the value being added.
 * <p>
 * Removing values will result in events where the before value is the value
 * being removed and the after value is null.
 * <p>
 * All events will have a property of {@link BeanListIterator#propertyName}.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.3 $, $Date: 2007-10-03 10:41:02 $, modified by 
 * 			$Author: rh4 $
 * @since 0.7
 */
public class BeanListIterator extends BeanIterator implements ListIterator {

	private static final long serialVersionUID = 1L;

	private int currentIndex = 0;

	/**
	 * Construct a new instance that wraps the delegate iterator and produces
	 * {@link PropertyChangeEvent} events whenever the delegate iterator
	 * changes.
	 * 
	 * @param delegate
	 *            the delegate iterator.
	 */
	public BeanListIterator(final ListIterator delegate) {
		super(delegate);
	}

	public void add(final Object arg0) {
		((ListIterator) this.delegate).add(arg0);
		this.firePropertyChange(BeanIterator.propertyName, null, arg0);
	}

	public boolean hasPrevious() {
		return ((ListIterator) this.delegate).hasPrevious();
	}

	public int nextIndex() {
		return ((ListIterator) this.delegate).nextIndex();
	}

	public Object previous() {
		this.currentObj = ((ListIterator) this.delegate).previous();
		this.currentIndex--;
		return this.currentObj;
	}

	public int previousIndex() {
		return ((ListIterator) this.delegate).previousIndex();
	}

	public void set(final Object arg0) {
		final Object oldValue = this.currentObj;
		final Object newValue = arg0;
		((ListIterator) this.delegate).set(newValue);
		this.firePropertyChange(BeanIterator.propertyName, newValue, oldValue);
	}

	public Object next() {
		final Object result = super.next();
		this.currentIndex++;
		return result;
	}
}
