/*
 * Table.java
 * Created on 23 March 2006, 14:34
 */

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

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.biomart.builder.model.Key.PrimaryKey;
import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.biomart.common.utils.BeanCollection;
import org.biomart.common.utils.BeanMap;
import org.biomart.common.utils.BeanSet;
import org.biomart.common.utils.Transaction;
import org.biomart.common.utils.WeakPropertyChangeSupport;
import org.biomart.common.utils.Transaction.TransactionEvent;
import org.biomart.common.utils.Transaction.TransactionListener;

/**
 * The table class provides the basic idea of what constitutes a database table
 * or an XML document entity. It has an optional primary key, zero or more
 * foreign keys, and one or more columns.
 * <p>
 * The {@link Table} class is provided as a template from which to build up more
 * complex implementations. It is able to keep track of keys and columns but it
 * does not provide any methods that process or analyse these.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.56 $, $Date: 2008-02-25 12:28:26 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class Table implements Comparable, TransactionListener {

	/**
	 * Subclasses use this field to fire events of their own.
	 */
	protected final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
			this);

	private static final long serialVersionUID = 1L;

	private int uniqueId;

	private final BeanMap columns;

	private final BeanCollection foreignKeys;

	private final String name;

	private final BeanCollection schemaPartitions = new BeanSet(new HashSet());

	private PrimaryKey primaryKey;

	private final Schema schema;

	private boolean masked = false;

	private final BeanCollection keyCache;

	private final BeanCollection relationCache;

	private final Collection columnCache;

	private boolean directModified = false;

	private final Map mods = new HashMap();

	private static final String DATASET_WIDE = "__DATASET_WIDE__";

	private final PropertyChangeListener relationCacheBuilder = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			Table.this.recalculateCaches();
		}
	};

	private final PropertyChangeListener listener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			Table.this.setDirectModified(true);
		}
	};

	/**
	 * Make a new table in the given schema. It won't add itself, so you'll need
	 * to do that separately.
	 * 
	 * @param schema
	 *            the schema this table should belong to.
	 * @param name
	 *            the name of the table. It will make this unique if there is a
	 *            clash with some other table already in the same schema.
	 */
	public Table(final Schema schema, String name) {
		Log.debug("Creating table " + name + " in " + schema);
		this.schema = schema;
		this.uniqueId = this.schema.getNextUniqueId();
		this.columns = new BeanMap(new HashMap());
		this.foreignKeys = new BeanCollection(new HashSet());
		// Make the name unique.
		final String baseName = name;
		for (int i = 1; schema.getTables().containsKey(name); name = baseName
				+ "_" + i++)
			;
		Log.debug("Unique name is " + name);
		this.name = name;

		Transaction.addTransactionListener(this);

		// Listen to own PK and FKs and update key+relation caches.
		this.keyCache = new BeanSet(new HashSet());
		this.relationCache = new BeanSet(new HashSet());
		this.columnCache = new HashSet();
		this.addPropertyChangeListener("primaryKey", this.relationCacheBuilder);
		this.getForeignKeys().addPropertyChangeListener(
				this.relationCacheBuilder);
		this.getColumns().addPropertyChangeListener(this.relationCacheBuilder);

		// All changes to us make us modified.
		this.addPropertyChangeListener("masked", this.listener);
		this.addPropertyChangeListener("restrictTable", this.listener);
		this.addPropertyChangeListener("bigTable", this.listener);
	}

	/**
	 * Change the unique ID for this table.
	 * 
	 * @param uniqueId
	 *            the new one to use.
	 */
	public void setUniqueId(final int uniqueId) {
		this.uniqueId = uniqueId;
	}

	/**
	 * Get the unique ID for this table.
	 * 
	 * @return the unique ID.
	 */
	public int getUniqueId() {
		return this.uniqueId;
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
	 * Does this exist for the given schema prefix?
	 * 
	 * @param schemaPrefix
	 *            the prefix.
	 * @return <tt>true</tt> if it does.
	 */
	public boolean existsForPartition(final String schemaPrefix) {
		return schemaPrefix == null || this.getSchemaPartitions().isEmpty()
				|| this.getSchemaPartitions().contains(schemaPrefix);
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
		// Compute this from all rels and cols - if any are vis
		// modified then we are too.
		for (final Iterator i = this.getRelations().iterator(); i.hasNext();)
			if (((Relation) i.next()).isVisibleModified())
				return true;
		for (final Iterator i = this.getColumns().values().iterator(); i
				.hasNext();)
			if (((Column) i.next()).isVisibleModified())
				return true;
		return false;
	}

	public void setVisibleModified(final boolean modified) {
		// We compute this on the fly so cannot set it.
	}

	public void transactionResetVisibleModified() {
		// We compute this on the fly so cannot set it.
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
	 * Drop modifications for the given dataset and optional table.
	 * 
	 * @param dataset
	 *            dataset
	 * @param tableKey
	 *            table key - <tt>null</tt> for all tables.
	 */
	public void dropMods(final DataSet dataset, final String tableKey) {
		// Drop all related mods.
		if (tableKey == null)
			this.mods.remove(dataset);
		else if (this.mods.containsKey(dataset))
			((Map) this.mods.get(dataset)).remove(tableKey);
	}

	/**
	 * This contains the set of modifications to this schema that apply to a
	 * particular dataset and table (null table means all tables in dataset).
	 * 
	 * @param dataset
	 *            the dataset to lookup.
	 * @param tableKey
	 *            the table to lookup.
	 * @return the set of tables that the property currently applies to. This
	 *         set can be added to or removed from accordingly. The keys of the
	 *         map are names, the values are optional subsidiary objects.
	 */
	public Map getMods(final DataSet dataset, String tableKey) {
		if (tableKey == null)
			tableKey = Table.DATASET_WIDE;
		if (!this.mods.containsKey(dataset))
			this.mods.put(dataset, new HashMap());
		final Map dsMap = (Map) this.mods.get(dataset);
		if (!dsMap.containsKey(tableKey))
			dsMap.put(tableKey.intern(), new HashMap());
		return (Map) dsMap.get(tableKey);
	}

	private synchronized void recalculateCaches() {
		final Collection newCols = new HashSet(this.getColumns().values());
		if (!newCols.equals(this.columnCache)) {
			this.setDirectModified(true);
			// Identify dropped ones.
			final Collection dropped = new HashSet(this.columnCache);
			dropped.removeAll(newCols);
			// Identify new ones.
			newCols.removeAll(this.columnCache);
			// Drop dropped ones.
			for (final Iterator i = dropped.iterator(); i.hasNext();)
				this.columnCache.remove(i.next());
			// Add added ones.
			for (final Iterator i = newCols.iterator(); i.hasNext();) {
				final Column column = (Column) i.next();
				column.addPropertyChangeListener("directModified",
						this.listener);
			}
			this.columnCache.clear();
			this.columnCache.addAll(this.getColumns().values());
		}
		final Collection newKeys = new HashSet();
		if (this.primaryKey != null)
			newKeys.add(this.primaryKey);
		newKeys.addAll(this.foreignKeys);
		if (!newKeys.equals(this.keyCache)) {
			this.setDirectModified(true);
			// Identify dropped ones.
			final Collection dropped = new HashSet(this.keyCache);
			dropped.removeAll(newKeys);
			// Identify new ones.
			newKeys.removeAll(this.keyCache);
			// Drop dropped ones.
			for (final Iterator i = dropped.iterator(); i.hasNext();)
				this.keyCache.remove(i.next());
			// Add added ones.
			for (final Iterator i = newKeys.iterator(); i.hasNext();) {
				final Key key = (Key) i.next();
				key.getRelations().addPropertyChangeListener(
						this.relationCacheBuilder);
				key.addPropertyChangeListener("directModified",
						this.relationCacheBuilder);
				key.addPropertyChangeListener("directModified", this.listener);
			}
			this.keyCache.clear();
			if (this.primaryKey != null)
				this.keyCache.add(this.primaryKey);
			this.keyCache.addAll(this.foreignKeys);
		}
		final Collection newRels = new HashSet();
		for (final Iterator i = this.keyCache.iterator(); i.hasNext();) {
			final Key key = (Key) i.next();
			newRels.addAll(key.getRelations());
		}
		if (!newRels.equals(this.relationCache)) {
			this.setDirectModified(true);
			this.relationCache.clear();
			this.relationCache.addAll(newRels);
		}
	}

	/**
	 * Obtain all keys on this table.
	 * 
	 * @return the unmodifiable collection of keys.
	 */
	public BeanCollection getKeys() {
		return this.keyCache;
	}

	/**
	 * Obtain all relations on this table.
	 * 
	 * @return the unmodifiable collection of relations.
	 */
	public BeanCollection getRelations() {
		return this.relationCache;
	}

	/**
	 * Returns a set of the columns of this table. It may be empty, indicating
	 * that the table has no columns, however this is highly unlikely! It will
	 * never return <tt>null</tt>.
	 * 
	 * @return the set of columns for this table.
	 */
	public BeanMap getColumns() {
		return this.columns;
	}

	/**
	 * Returns a set of the foreign keys of this table. It may be empty,
	 * indicating that the table has no foreign keys. It will never return
	 * <tt>null</tt>.
	 * 
	 * @return the set of foreign keys for this table.
	 */
	public BeanCollection getForeignKeys() {
		return this.foreignKeys;
	}

	/**
	 * Returns the name of this table.
	 * 
	 * @return the name of this table.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Retrieve the set of schema partition names this column applies to. May be
	 * empty, in which case it applies to the default schema only.
	 * 
	 * @return the set of schema partition names.
	 */
	public BeanCollection getSchemaPartitions() {
		return this.schemaPartitions;
	}

	/**
	 * Checks whether this table is masked or not.
	 * 
	 * @return <tt>true</tt> if it is, <tt>false</tt> if it isn't.
	 */
	public boolean isMasked() {
		return this.masked;
	}

	/**
	 * Sets whether this table is masked or not.
	 * 
	 * @param masked
	 *            <tt>true</tt> if it is, <tt>false</tt> if it isn't.
	 */
	public void setMasked(final boolean masked) {
		Log.debug("Setting masked table on " + this + " to " + masked);
		final boolean oldValue = this.masked;
		if (this.masked == masked)
			return;
		this.masked = masked;
		this.pcs.firePropertyChange("masked", oldValue, masked);
	}

	/**
	 * Returns a reference to the primary key of this table. It may be
	 * <tt>null</tt>, indicating that the table has no primary key.
	 * 
	 * @return the primary key of this table.
	 */
	public PrimaryKey getPrimaryKey() {
		return this.primaryKey;
	}

	/**
	 * Sets the primary key of this table. It may be <tt>null</tt>,
	 * indicating that the table has no primary key.
	 * 
	 * @param pk
	 *            the primary key of this table.
	 */
	public void setPrimaryKey(final PrimaryKey pk) {
		Log.debug("Changing PK on table " + this + " to " + pk);
		final PrimaryKey oldValue = this.primaryKey;
		if (this.primaryKey == pk || this.primaryKey != null
				&& this.primaryKey.equals(pk))
			return;
		this.primaryKey = pk;
		this.pcs.firePropertyChange("primaryKey", oldValue, pk);
	}

	/**
	 * Returns the schema for this table.
	 * 
	 * @return the schema for this table.
	 */
	public Schema getSchema() {
		return this.schema;
	}

	/**
	 * Is this table restricted?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return the def to use if it is, null otherwise.
	 */
	public RestrictedTableDefinition getRestrictTable(final DataSet dataset,
			final String tableKey) {
		return (RestrictedTableDefinition) this.getMods(dataset, tableKey).get(
				"restrictTable");
	}

	/**
	 * Restrict this table.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param def
	 *            the definition to set - if null, it undoes it.
	 */
	public void setRestrictTable(final DataSet dataset, final String tableKey,
			final RestrictedTableDefinition def) {
		final RestrictedTableDefinition oldValue = this.getRestrictTable(
				dataset, tableKey);
		if (def == oldValue || oldValue != null && oldValue.equals(def))
			return;

		if (def != null) {
			this.getMods(dataset, tableKey).put("restrictTable", def);
			def.addPropertyChangeListener("directModified", this.listener);
			this.pcs.firePropertyChange("restrictTable", null, tableKey);
		} else {
			this.getMods(dataset, tableKey).remove("restrictTable");
			this.pcs.firePropertyChange("restrictTable", tableKey, null);
		}

	}

	/**
	 * Is this table big?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return the big-ness to use if it is, 0 otherwise.
	 */
	public int getBigTable(final DataSet dataset) {
		final Integer val = (Integer) this.getMods(dataset, null).get(
				"bigTable");
		return val == null ? 0 : val.intValue();
	}

	/**
	 * Is this table big?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return the big-ness to use if it is, 0 otherwise.
	 */
	public int getBigTable(final DataSet dataset, final String tableKey) {
		Integer val = (Integer) this.getMods(dataset, tableKey).get("bigTable");
		if (val == null)
			return this.getBigTable(dataset);
		else
			return val.intValue();
	}

	/**
	 * Big-up this table.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param bigness
	 *            the bigness to set - if 0, it undoes it.
	 */
	public void setBigTable(final DataSet dataset, final int bigness) {
		final int oldValue = this.getBigTable(dataset);
		if (bigness == oldValue)
			return;
		if (bigness > 0) {
			this.getMods(dataset, null).put("bigTable", new Integer(bigness));
			this.pcs.firePropertyChange("bigTable", null, dataset);
		} else {
			this.getMods(dataset, null).remove("bigTable");
			this.pcs.firePropertyChange("bigTable", dataset, null);
		}
	}

	/**
	 * Big-up this table.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param bigness
	 *            the bigness to set - if 0, it undoes it.
	 */
	public void setBigTable(final DataSet dataset, final String tableKey,
			final int bigness) {
		final int oldValue = this.getBigTable(dataset, tableKey);
		if (bigness == oldValue)
			return;
		if (bigness > 0) {
			this.getMods(dataset, tableKey).put("bigTable",
					new Integer(bigness));
			this.pcs.firePropertyChange("bigTable", null, tableKey);
		} else {
			this.getMods(dataset, tableKey).remove("bigTable");
			this.pcs.firePropertyChange("bigTable", tableKey, null);
		}
	}

	/**
	 * Is this table a transform start?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return true if it is, false otherwise.
	 */
	public boolean isTransformStart(final DataSet dataset, final String tableKey) {
		return this.getMods(dataset, tableKey).containsKey("transformStart");
	}

	/**
	 * Transform-start this table.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param doIt
	 *            if false, it undoes it.
	 */
	public void setTransformStart(final DataSet dataset, final String tableKey,
			final boolean doIt) {
		final boolean oldValue = this.isTransformStart(dataset, tableKey);
		if (doIt == oldValue)
			return;
		if (doIt) {
			this.getMods(dataset, tableKey).put("transformStart", null);
			this.pcs.firePropertyChange("transformStart", null, tableKey);
		} else {
			this.getMods(dataset, tableKey).remove("transformStart");
			this.pcs.firePropertyChange("transformStart", tableKey, null);
		}
	}

	public int compareTo(final Object o) throws ClassCastException {
		final Table t = (Table) o;
		return (this.schema.getMart().getUniqueId() + "_" + this.toString())
				.compareTo(t.schema.getMart().getUniqueId() + "_"
						+ t.toString());
	}

	public boolean equals(final Object o) {
		if (o == this)
			return true;
		else if (o == null)
			return false;
		else if (o instanceof Table) {
			final Table t = (Table) o;
			return (t.schema.getMart().getUniqueId() + "_" + t.toString())
					.equals(this.schema.getMart().getUniqueId() + "_"
							+ this.toString());
		} else
			return false;
	}

	public int hashCode() {
		return this.name.hashCode();
	}

	public String toString() {
		return "(" + this.schema + ") " + this.name;
	}

	/**
	 * Defines the restriction on a table, ie. a where-clause.
	 */
	public static class RestrictedTableDefinition implements
			TransactionListener {
		private static final long serialVersionUID = 1L;

		private BeanMap aliases;

		private String expr;

		private boolean directModified = false;

		private final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
				this);

		private final PropertyChangeListener listener = new PropertyChangeListener() {
			public void propertyChange(final PropertyChangeEvent e) {
				RestrictedTableDefinition.this.setDirectModified(true);
			}
		};

		/**
		 * This constructor gives the restriction an initial expression and a
		 * set of aliases. The expression may not be empty, and neither can the
		 * alias map.
		 * 
		 * @param expr
		 *            the expression to define for this restriction.
		 * @param aliases
		 *            the aliases to use for columns.
		 */
		public RestrictedTableDefinition(final String expr, final Map aliases) {
			// Test for good arguments.
			if (expr == null || expr.trim().length() == 0)
				throw new IllegalArgumentException(Resources
						.get("tblRestrictMissingExpression"));
			if (aliases == null || aliases.isEmpty())
				throw new IllegalArgumentException(Resources
						.get("tblRestrictMissingAliases"));

			// Remember the settings.
			this.aliases = new BeanMap(new HashMap(aliases));
			this.expr = expr;

			Transaction.addTransactionListener(this);

			this.addPropertyChangeListener(this.listener);
			this.aliases.addPropertyChangeListener(this.listener);
		}

		/**
		 * Replicate ourselves.
		 * 
		 * @return the copy.
		 */
		public RestrictedTableDefinition replicate() {
			return new RestrictedTableDefinition(this.expr, this.aliases);
		}

		/**
		 * Adds a property change listener.
		 * 
		 * @param listener
		 *            the listener to add.
		 */
		public void addPropertyChangeListener(
				final PropertyChangeListener listener) {
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
			return false;
		}

		public void setVisibleModified(final boolean modified) {
			// Ignore for now.
		}

		public void transactionResetVisibleModified() {
			// Ignore for now.
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
		 * Retrieves the map used for setting up aliases.
		 * 
		 * @return the aliases map. Keys must be {@link Column} instances, and
		 *         values are aliases used in the expression.
		 */
		public BeanMap getAliases() {
			return this.aliases;
		}

		/**
		 * Returns the expression, <i>without</i> substitution. This value is
		 * RDBMS-specific.
		 * 
		 * @return the unsubstituted expression.
		 */
		public String getExpression() {
			return this.expr;
		}

		/**
		 * Returns the expression, <i>with</i> substitution. This value is
		 * RDBMS-specific.
		 * 
		 * @param schemaPrefix
		 *            the value to substitute for ':schemaPrefix'.
		 * @param tablePrefix
		 *            the prefix to use for the table in the expression.
		 * @return the substituted expression.
		 */
		public String getSubstitutedExpression(final String schemaPrefix,
				final String tablePrefix) {
			Log.debug("Calculating restricted table expression");
			String sub = this.expr;
			for (final Iterator i = this.aliases.entrySet().iterator(); i
					.hasNext();) {
				final Map.Entry entry = (Map.Entry) i.next();
				final Column col = (Column) entry.getKey();
				final String alias = ":" + (String) entry.getValue();
				sub = sub.replaceAll(alias, tablePrefix + "." + col.getName());
			}
			sub = sub.replaceAll(":" + Resources.get("schemaPrefix"),
					schemaPrefix == null ? "null" : schemaPrefix);
			Log.debug("Expression is: " + sub);
			return sub;
		}

		/**
		 * The actual expression. The values from the alias maps will be used to
		 * refer to various columns. This value is RDBMS-specific.
		 * 
		 * @param expr
		 *            the actual expression to use.
		 */
		public void setExpression(final String expr) {
			if (expr == this.expr || expr.equals(this.expr))
				return;
			final String oldValue = this.expr;
			this.expr = expr;
			this.pcs.firePropertyChange("expression", oldValue, expr);
		}
	}
}
