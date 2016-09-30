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
import java.beans.PropertyChangeListener;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

/**
 * This class wraps an existing list, and causes {@link PropertyChangeEvent}
 * events to be fired whenever it changes.
 * <p>
 * Adding objects to the list will result in events where the before value is
 * null and the after value is the value being added.
 * <p>
 * Removing them will result in events where the before value is they value
 * being removed and the after value is null.
 * <p>
 * Multiple add/remove events will have both before and after values of null.
 * <p>
 * All events will have a property of {@link BeanList#propertyName}.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.4 $, $Date: 2007-10-31 10:32:56 $, modified by 
 * 			$Author: rh4 $
 * @since 0.7
 */
public class BeanList extends BeanCollection implements List {

	private static final long serialVersionUID = 1L;

	/**
	 * Construct a new instance that wraps the delegate list and produces
	 * {@link PropertyChangeEvent} events whenever the delegate list changes.
	 * 
	 * @param delegate
	 *            the delegate list.
	 */
	public BeanList(final List delegate) {
		super(delegate);
	}

	public Object get(final int index) {
		return ((List) this.delegate).get(index);
	}

	public int indexOf(final Object o) {
		return ((List) this.delegate).indexOf(o);
	}

	public int lastIndexOf(final Object o) {
		return ((List) this.delegate).lastIndexOf(o);
	}

	private final PropertyChangeListener iteratorListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			BeanList.this.firePropertyChange(
					BeanCollection.propertyName, evt.getOldValue(),
					evt.getNewValue());
		}
	};
	
	public ListIterator listIterator() {
		// Wrap the entry set in a BeanIterator.
		final BeanListIterator beanListIterator = new BeanListIterator(
				((List) this.delegate).listIterator());
		// Add a PropertyChangeListener to the BeanSet
		// which fires events as if they came from us.
		beanListIterator
				.addPropertyChangeListener(this.iteratorListener);
		// Return the wrapped entry set.
		return beanListIterator;
	}

	public ListIterator listIterator(final int index) {
		// Wrap the entry set in a BeanIterator.
		final BeanListIterator beanListIterator = new BeanListIterator(
				((List) this.delegate).listIterator(index));
		// Add a PropertyChangeListener to the BeanSet
		// which fires events as if they came from us.
		beanListIterator
				.addPropertyChangeListener(this.iteratorListener);
		// Return the wrapped entry set.
		return beanListIterator;
	}

	public Object remove(final int index) {
		final Object result = ((List) this.delegate).remove(index);
		this.firePropertyChange(BeanCollection.propertyName, result, null);
		return result;
	}

	private final PropertyChangeListener subListIterator = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			BeanList.this.firePropertyChange(BeanCollection.propertyName,
					evt.getOldValue(), evt.getNewValue());
		}
	};
	
	public List subList(final int fromIndex, final int toIndex) {
		final BeanList subList = new BeanList(((List) this.delegate).subList(
				fromIndex, toIndex));
		subList.addPropertyChangeListener(this.subListIterator);
		return subList;
	}

	public void add(final int arg0, final Object arg1) {
		((List) this.delegate).add(arg0, arg1);
		this.firePropertyChange(BeanCollection.propertyName, null, arg1);
	}

	public boolean addAll(final int arg0, final Collection arg1) {
		final boolean result = ((List) this.delegate).addAll(arg0, arg1);
		if (result)
			this.firePropertyChange(BeanCollection.propertyName, null, arg1);
		return result;
	}

	public Object set(final int arg0, final Object arg1) {
		final Object result = ((List) this.delegate).set(arg0, arg1);
		this.firePropertyChange(BeanCollection.propertyName, null, arg1);
		return result;
	}
}
