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
import java.beans.PropertyChangeSupport;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class is similar to {@link PropertyChangeSupport} but it only holds weak
 * references to listeners.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.1 $, $Date: 2007-10-31 10:32:56 $, modified by 
 * 			$Author: rh4 $
 * @since 0.7
 */
public class WeakPropertyChangeSupport {

	private WeakReference parentRef;

	private final List globalListeners = new ArrayList();

	private final Map namedListeners = new HashMap();

	/**
	 * Construct a new property change support object.
	 * 
	 * @param parent
	 *            the parent object, which will be used when firing events, and
	 *            stored with a {@link WeakReference}.
	 */
	public WeakPropertyChangeSupport(final Object parent) {
		this.parentRef = new WeakReference(parent);
	}

	/**
	 * Returns all currently active change listeners on all propertys, including
	 * global ones.
	 * 
	 * @return the listeners.
	 */
	public PropertyChangeListener[] getPropertyChangeListeners() {
		final List lists = new ArrayList();
		for (final Iterator i = this.globalListeners.iterator(); i.hasNext();) {
			final WeakReference ref = (WeakReference) i.next();
			final PropertyChangeListener list = (PropertyChangeListener) ref
					.get();
			if (list == null)
				i.remove();
			else
				lists.add(list);
		}
		for (final Iterator j = this.namedListeners.values().iterator(); j
				.hasNext();)
			for (final Iterator i = ((List) j.next()).iterator(); i.hasNext();) {
				final WeakReference ref = (WeakReference) i.next();
				final PropertyChangeListener list = (PropertyChangeListener) ref
						.get();
				if (list == null)
					i.remove();
				else
					lists.add(list);
			}
		return (PropertyChangeListener[]) lists
				.toArray(new PropertyChangeListener[0]);
	}

	/**
	 * Returns all currently active change listeners on all the given property,
	 * including global ones.
	 * 
	 * @param property
	 *            the property to check.
	 * @return the listeners.
	 */
	public PropertyChangeListener[] getPropertyChangeListeners(
			final String property) {
		if (!this.namedListeners.containsKey(property))
			return new PropertyChangeListener[0];
		final List lists = new ArrayList();
		for (final Iterator i = this.globalListeners.iterator(); i.hasNext();) {
			final WeakReference ref = (WeakReference) i.next();
			final PropertyChangeListener list = (PropertyChangeListener) ref
					.get();
			if (list == null)
				i.remove();
			else
				lists.add(list);
		}
		for (final Iterator i = ((List) this.namedListeners.get(property))
				.iterator(); i.hasNext();) {
			final WeakReference ref = (WeakReference) i.next();
			final PropertyChangeListener list = (PropertyChangeListener) ref
					.get();
			if (list == null)
				i.remove();
			else
				lists.add(list);
		}
		if (((List) this.namedListeners.get(property)).isEmpty())
			this.namedListeners.remove(property);
		return (PropertyChangeListener[]) lists
				.toArray(new PropertyChangeListener[0]);
	}

	/**
	 * Add a global listener, which will be stored with a {@link WeakReference}.
	 * 
	 * @param listener
	 *            the listener.
	 */
	public void addPropertyChangeListener(final PropertyChangeListener listener) {
		this.globalListeners.add(new WeakReference(listener));
	}

	/**
	 * Add a property-specific listener, which will be stored with a
	 * {@link WeakReference}.
	 * 
	 * @param property
	 *            the property.
	 * @param listener
	 *            the listener.
	 */
	public void addPropertyChangeListener(final String property,
			final PropertyChangeListener listener) {
		if (!this.namedListeners.containsKey(property))
			this.namedListeners.put(property, new ArrayList());
		((List) this.namedListeners.get(property)).add(new WeakReference(
				listener));
	}

	/**
	 * Fire a property change event.
	 * 
	 * @param evt
	 *            the event to fire.
	 */
	public void firePropertyChange(final PropertyChangeEvent evt) {
		for (final Iterator i = this.globalListeners.iterator(); i.hasNext();) {
			final WeakReference ref = (WeakReference) i.next();
			final PropertyChangeListener list = (PropertyChangeListener) ref
					.get();
			if (list == null)
				i.remove();
			else
				list.propertyChange(evt);
		}
		final String property = evt.getPropertyName();
		if (this.namedListeners.containsKey(property)) {
			for (final Iterator i = ((List) this.namedListeners.get(property))
					.iterator(); i.hasNext();) {
				final WeakReference ref = (WeakReference) i.next();
				final PropertyChangeListener list = (PropertyChangeListener) ref
						.get();
				if (list == null)
					i.remove();
				else
					list.propertyChange(evt);
			}
			if (((List) this.namedListeners.get(property)).isEmpty())
				this.namedListeners.remove(property);
		}
	}

	/**
	 * Fire a property change event.
	 * 
	 * @param property
	 *            the property that is changing.
	 * @param oldValue
	 *            the old value.
	 * @param newValue
	 *            the new value.
	 */
	public void firePropertyChange(final String property,
			final Object oldValue, final Object newValue) {
		if (this.parentRef == null)
			return;
		final Object parent = this.parentRef.get();
		if (parent == null) {
			this.globalListeners.clear();
			this.namedListeners.clear();
			this.parentRef = null;
		}
		final PropertyChangeEvent evt = new PropertyChangeEvent(parent,
				property, oldValue, newValue);
		this.firePropertyChange(evt);
	}

	/**
	 * Fire a property change event.
	 * 
	 * @param property
	 *            the property that is changing.
	 * @param oldValue
	 *            the old value.
	 * @param newValue
	 *            the new value.
	 */
	public void firePropertyChange(final String property,
			final boolean oldValue, final boolean newValue) {
		this.firePropertyChange(property, Boolean.valueOf(oldValue), Boolean
				.valueOf(newValue));
	}

	/**
	 * Fire a property change event.
	 * 
	 * @param property
	 *            the property that is changing.
	 * @param oldValue
	 *            the old value.
	 * @param newValue
	 *            the new value.
	 */
	public void firePropertyChange(final String property, final int oldValue,
			final int newValue) {
		this.firePropertyChange(property, new Integer(oldValue), new Integer(
				newValue));
	}
}
