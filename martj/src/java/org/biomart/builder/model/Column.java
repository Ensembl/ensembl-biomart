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

import java.beans.PropertyChangeListener;
import java.util.HashSet;

import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.biomart.common.utils.BeanCollection;
import org.biomart.common.utils.BeanSet;
import org.biomart.common.utils.Transaction;
import org.biomart.common.utils.WeakPropertyChangeSupport;
import org.biomart.common.utils.Transaction.TransactionEvent;
import org.biomart.common.utils.Transaction.TransactionListener;

/**
 * A column is a simple representation of a column in some table. It has a name,
 * and knows which table it belongs to, but apart from that knows nothing much
 * else.
 * <p>
 * A {@link Column} class is provided for ease of implementation. It provides a
 * simple storage/retrieval mechanism for the parent table and column name.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.33 $, $Date: 2007-10-31 10:32:56 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class Column implements Comparable, TransactionListener {
	private static final long serialVersionUID = 1L;

	private final String name;

	private final Table table;
	
	private final BeanCollection schemaPartitions = new BeanSet(new HashSet());

	/**
	 * Some subclasses refer to this directly.
	 */
	protected boolean visibleModified = Transaction.getCurrentTransaction() == null ? false
			: Transaction.getCurrentTransaction().isAllowVisModChange();

	private boolean directModified = false;

	/**
	 * Subclasses use this field to fire events of their own.
	 */
	protected final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(this);

	/**
	 * This constructor creates a column and remembers the name and parent
	 * table.
	 * 
	 * @param name
	 *            the name of the column to create.
	 * @param table
	 *            the parent table.
	 */
	public Column(final Table table, String name) {
		Log.debug("Creating column " + name + " on table " + table);
		// Remember the values.
		this.table = table;
		// First we need to find out the base name, ie. the bit
		// we append numbers to make it unique, but before any
		// key suffix. If we appended numbers after the key
		// suffix then it would confuse MartEditor.
		String suffix = "";
		String baseName = name;
		if (name.endsWith(Resources.get("keySuffix"))) {
			suffix = Resources.get("keySuffix");
			baseName = name.substring(0, name.indexOf(suffix));
		}
		// Now simply check to see if the name is used, and
		// then add an incrementing number to it until it is unique.
		for (int i = 1; table.getColumns().containsKey(name); name = baseName
				+ "_" + i++ + suffix)
			;
		// Return it.
		Log.debug("Unique name is " + name);
		this.name = name;

		Transaction.addTransactionListener(this);
	}

	/**
	 * Does this exist for the given schema prefix?
	 * @param schemaPrefix the prefix.
	 * @return <tt>true</tt> if it does.
	 */
	public boolean existsForPartition(final String schemaPrefix) {
		return schemaPrefix==null || this.getSchemaPartitions().isEmpty() || this.getSchemaPartitions().contains(schemaPrefix);
	}

	public boolean isDirectModified() {
		return this.directModified;
	}

	public void setDirectModified(final boolean modified) {
		if (modified == this.directModified)
			return;
		final boolean oldValue = this.directModified;
		this.directModified = modified;
		this.pcs.firePropertyChange("directModified", oldValue, modified);
	}

	public boolean isVisibleModified() {
		return this.visibleModified;
	}

	public void setVisibleModified(final boolean modified) {
		if (modified == this.visibleModified)
			return;
		final boolean oldValue = this.visibleModified;
		this.visibleModified = modified;
		this.pcs.firePropertyChange("visibleModified", oldValue, modified);
		this.setDirectModified(true);
	}

	public void transactionResetVisibleModified() {
		this.setVisibleModified(false);
	}

	public void transactionResetDirectModified() {
		this.directModified = false;
	}

	public void transactionStarted(final TransactionEvent evt) {
		// Don't really care for now.
	}

	public void transactionEnded(final TransactionEvent evt) {
		// Don't really care for now.
	}

	/**
	 * Adds a property change listener.
	 * 
	 * @param listener
	 *            the listener to add.
	 */
	public void addPropertyChangeListener(final PropertyChangeListener listener) {
		this.pcs.addPropertyChangeListener(listener);
	}

	/**
	 * Adds a property change listener.
	 * 
	 * @param property
	 *            the property to listen to.
	 * @param listener
	 *            the listener to add.
	 */
	public void addPropertyChangeListener(final String property,
			final PropertyChangeListener listener) {
		this.pcs.addPropertyChangeListener(property, listener);
	}

	/**
	 * Retrieve the name of this column.
	 * 
	 * @return the name of this column.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Retrieve the parent table of this column.
	 * 
	 * @return the parent table of this column.
	 */
	public Table getTable() {
		return this.table;
	}
	
	/**
	 * Retrieve the set of schema partition names this column applies to.
	 * May be empty, in which case it applies to the default schema only.
	 * @return the set of schema partition names.
	 */
	public BeanCollection getSchemaPartitions() {
		return this.schemaPartitions;
	}

	public int compareTo(final Object o) {
		final Column k = (Column) o;
		return (this.getTable().getSchema().getMart().getUniqueId() + "_" + this
				.toString()).compareTo(k.getTable().getSchema().getMart()
				.getUniqueId()
				+ "_" + k.toString());
	}

	public boolean equals(final Object o) {
		if (o == this)
			return true;
		else if (o == null)
			return false;
		else if (o instanceof Column) {
			final Column k = (Column) o;
			return (k.getTable().getSchema().getMart().getUniqueId() + "_" + k
					.toString()).equals(this.getTable().getSchema().getMart()
					.getUniqueId()
					+ "_" + this.toString());
		} else
			return false;
	}

	public int hashCode() {
		return this.name.hashCode();
	}

	public String toString() {
		return this.name + " [" + this.getTable().toString() + "]";
	}
}
