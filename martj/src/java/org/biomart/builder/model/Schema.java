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

import java.awt.Frame;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.swing.JDialog;
import javax.swing.JOptionPane;

import org.biomart.builder.controller.dialects.DatabaseDialect;
import org.biomart.builder.model.Key.ForeignKey;
import org.biomart.builder.model.Key.PrimaryKey;
import org.biomart.builder.model.Relation.Cardinality;
import org.biomart.builder.view.gui.dialogs.SaveOrphanKeyDialog;
import org.biomart.common.exceptions.AssociationException;
import org.biomart.common.exceptions.BioMartError;
import org.biomart.common.exceptions.DataModelException;
import org.biomart.common.exceptions.TransactionException;
import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.biomart.common.resources.Settings;
import org.biomart.common.utils.BeanCollection;
import org.biomart.common.utils.BeanMap;
import org.biomart.common.utils.BeanSet;
import org.biomart.common.utils.InverseMap;
import org.biomart.common.utils.Transaction;
import org.biomart.common.utils.WeakPropertyChangeSupport;
import org.biomart.common.utils.Transaction.TransactionEvent;
import org.biomart.common.utils.Transaction.TransactionListener;


/**
 * A schema provides one or more table objects with unique names for the user to
 * use. It could be a relational database, or an XML document, or any other
 * source of potentially tabular information.
 * <p>
 * The generic implementation provided should suffice for most tasks involved in
 * keeping track of the tables a schema provides.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.67.4.1 $, $Date: 2009-04-16 12:02:07 $, modified by
 *          $Author: syed $
 * @since 0.5
 */
public class Schema implements Comparable, DataLink, TransactionListener {

	/**
	 * Subclasses use this field to fire events of their own.
	 */
	protected final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
			this);

	private final Mart mart;

	private int uniqueId;

	/**
	 * Subclasses can reference this to alter it - e.g. DataSet does this.
	 */
	protected String name;

	/**
	 * Subclasses can reference this to alter it - e.g. DataSet does this.
	 */
	protected String originalName;

	private boolean keyGuessing;

	private boolean masked;

	private String dataLinkSchema;

	private String dataLinkDatabase;

	private BeanMap tables;

	private String partitionRegex;

	private String partitionNameExpression;

	private final Map partitionCache = new TreeMap();

	/**
	 * Subclasses use this to notify update requirements.
	 */
	protected boolean needsFullSync;

	private boolean hideMasked = false;

	private boolean directModified = false;

	private final Collection tableCache;

	private final BeanCollection relationCache;

	/**
	 * Subclasses use this to update synchronisation progress.
	 */
	protected double progress = 0.0;

	private final PropertyChangeListener relationCacheBuilder = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			Schema.this.recalculateCaches();
		}
	};

	/**
	 * Internal use only.
	 */
	protected final PropertyChangeListener listener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			Schema.this.setDirectModified(true);
		}
	};

	/**
	 * The constructor creates a schema with the given name. Keyguessing is
	 * turned off.
	 * 
	 * @param mart
	 *            the mart this schema will belong to.
	 * @param name
	 *            the name for this new schema.
	 * @param dataLinkDatabase
	 *            the database name we are using.
	 * @param dataLinkSchema
	 *            the database schema name we are using.
	 * @param partitionRegex
	 *            partition stuff.
	 * @param partitionNameExpression
	 *            partition stuff.
	 */
	public Schema(final Mart mart, final String name,
			final String dataLinkDatabase, final String dataLinkSchema,
			final String partitionRegex, final String partitionNameExpression) {
		this(mart, name, false, dataLinkDatabase, dataLinkSchema,
				partitionRegex, partitionNameExpression);
	}

	/**
	 * This constructor creates a schema with the given name, and with
	 * keyguessing set to the given value.
	 * 
	 * @param mart
	 *            the mart this schema will belong to.
	 * @param name
	 *            the name for the new schema.
	 * @param keyGuessing
	 *            <tt>true</tt>if you want keyguessing, <tt>false</tt> if
	 *            not.
	 * @param dataLinkDatabase
	 *            the database name we are using.
	 * @param dataLinkSchema
	 *            the database schema name we are using.
	 * @param partitionRegex
	 *            partition stuff.
	 * @param partitionNameExpression
	 *            partition stuff.
	 */
	public Schema(final Mart mart, final String name,
			final boolean keyGuessing, final String dataLinkDatabase,
			final String dataLinkSchema, final String partitionRegex,
			final String partitionNameExpression) {
		Log.debug("Creating schema " + name);
		this.mart = mart;
		this.uniqueId = this.mart.getNextUniqueId();
		this.setName(name);
		this.setOriginalName(name);
		this.setKeyGuessing(keyGuessing);
		this.setDataLinkSchema(dataLinkSchema);
		this.setDataLinkDatabase(dataLinkDatabase);
		this.setMasked(false);
		this.setPartitionRegex(partitionRegex);
		this.setPartitionNameExpression(partitionNameExpression);
		// TreeMap keeps the partition cache in alphabetical order by name.
		this.tables = new BeanMap(new HashMap());
		this.needsFullSync = false;

		Transaction.addTransactionListener(this);

		// Listen to own tables and update key+relation caches.
		this.tableCache = new HashSet();
		this.relationCache = new BeanSet(new HashSet());
		this.getTables().addPropertyChangeListener(this.relationCacheBuilder);

		// All changes to us make us modified.
		this.addPropertyChangeListener("dataLinkSchema", this.listener);
		this.addPropertyChangeListener("keyGuessing", this.listener);
		this.addPropertyChangeListener("masked", this.listener);
		this.addPropertyChangeListener("name", this.listener);
		this
				.addPropertyChangeListener("partitionNameExpression",
						this.listener);
		this.addPropertyChangeListener("partitionRegex", this.listener);
	}

	/**
	 * Do a 'select distinct' on the given column in the given schema.
	 * 
	 * @param schemaPrefix
	 *            the schema prefix identifier. Use a sensible default if null
	 *            given.
	 * @param column
	 *            the column to select.
	 * @return the values.
	 * @throws SQLException
	 *             if it goes wrong.
	 */
	public Collection getUniqueValues(final String schemaPrefix,
			final Column column) throws SQLException {
		return Collections.EMPTY_SET;
	}

	/**
	 * Change the unique ID for this schema.
	 * 
	 * @param uniqueId
	 *            the new one to use.
	 */
	public void setUniqueId(final int uniqueId) {
		this.uniqueId = uniqueId;
	}

	/**
	 * Get the unique ID for this schema.
	 * 
	 * @return the unique ID.
	 */
	public int getUniqueId() {
		return this.uniqueId;
	}

	/**
	 * Obtain the next unique ID to use for a table.
	 * 
	 * @return the next ID.
	 */
	public int getNextUniqueId() {
		int x = 0;
		for (final Iterator i = this.tableCache.iterator(); i.hasNext();)
			x = Math.max(x, ((Table) i.next()).getUniqueId());
		return x + 1;
	}

	/**
	 * Work out how far synchronising has got. If this returns a value greater
	 * than or equal to 100.0 then syncing is complete.
	 * 
	 * @return the progress so far on a scale of 0.0 to 100.0.
	 */
	public double getProgress() {
		return this.progress;
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
		// If any table is visible modified, then we are too.
		for (final Iterator i = this.getTables().values().iterator(); i
				.hasNext();)
			if (((Table) i.next()).isVisibleModified())
				return true;
		return false;
	}

	public void setVisibleModified(final boolean modified) {
		// If any table is visible modified, then we are too.
	}

	public void transactionResetVisibleModified() {
		// If any table is visible modified, then we are too.
	}

	public void transactionResetDirectModified() {
		this.directModified = false;
	}

	public void transactionStarted(final TransactionEvent evt) {
		// Don't really care for now.
	}

	public void transactionEnded(final TransactionEvent evt)
			throws TransactionException {
		if (this.needsFullSync)
			try {
				this.synchronise();
			} catch (final Exception e) {
				throw new TransactionException(e);
			}
	}

	/**
	 * Indicate that a table has been dropped.
	 * 
	 * @param table
	 *            the table that has been dropped.
	 */
	protected void tableDropped(final Table table) {
		// Do nothing here.
	}

	private synchronized void recalculateCaches() {
		final Collection newTables = new HashSet(this.tables.values());
		newTables.addAll(this.tables.values());
		if (!newTables.equals(this.tableCache)) {
			this.setDirectModified(true);
			// Identify dropped ones.
			final Collection dropped = new HashSet(this.tableCache);
			dropped.removeAll(newTables);
			// Identify new ones.
			newTables.removeAll(this.tableCache);
			// Drop dropped ones.
			for (final Iterator i = dropped.iterator(); i.hasNext();) {
				final Table table = (Table) i.next();
				this.tableDropped(table);
				this.tableCache.remove(table);
			}
			// Add added ones.
			for (final Iterator i = newTables.iterator(); i.hasNext();) {
				final Table table = (Table) i.next();
				this.tableCache.add(table);
				table.getRelations().addPropertyChangeListener(
						this.relationCacheBuilder);
				table
						.addPropertyChangeListener("directModified",
								this.listener);
			}
		}
		final Collection newRels = new HashSet();
		for (final Iterator i = this.tableCache.iterator(); i.hasNext();) {
			final Table table = (Table) i.next();
			newRels.addAll(table.getRelations());
		}
		if (!newRels.equals(this.relationCache)) {
			this.setDirectModified(true);
			this.relationCache.clear();
			this.relationCache.addAll(newRels);
		}
	}

	/**
	 * Is this schema hiding masked components?
	 * 
	 * @param hideMasked
	 *            true if it is.
	 */
	public void setHideMasked(final boolean hideMasked) {
		Log
				.debug("Setting hide masked schema on " + this + " to "
						+ hideMasked);
		final boolean oldValue = this.hideMasked;
		if (this.hideMasked == hideMasked)
			return;
		this.hideMasked = hideMasked;
		this.pcs.firePropertyChange("hideMasked", oldValue, hideMasked);
	}

	/**
	 * Is this schema hiding masked components?
	 * 
	 * @return true if it is.
	 */
	public boolean isHideMasked() {
		return this.hideMasked;
	}

	/**
	 * Obtain all relations on this schema.
	 * 
	 * @return the unmodifiable collection of relations.
	 */
	public BeanCollection getRelations() {
		return this.relationCache;
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
	 * Obtain the tables in this schema.
	 * 
	 * @return the tables. The keys of the map are the names as returned by
	 *         {@link Table#getName()}. The values are the table objects
	 *         themselves.
	 */
	public BeanMap getTables() {
		return this.tables;
	}

	/**
	 * Gets the mart for this schema.
	 * 
	 * @return the mart for this schema.
	 */
	public Mart getMart() {
		return this.mart;
	}

	/**
	 * Gets the name of this schema.
	 * 
	 * @return the name of this schema.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Gets the original name of this schema.
	 * 
	 * @return the original name of this schema.
	 */
	public String getOriginalName() {
		return this.originalName;
	}

	public String getDataLinkDatabase() {
		return this.dataLinkDatabase;
	}

	public void setDataLinkDatabase(final String dataLinkDatabase) {
		Log.debug("Setting data link database on " + this + " to "
				+ dataLinkDatabase);
		final String oldValue = this.dataLinkDatabase;
		if (this.dataLinkDatabase == dataLinkDatabase
				|| this.dataLinkDatabase != null
				&& this.dataLinkDatabase.equals(dataLinkDatabase))
			return;
		this.dataLinkDatabase = dataLinkDatabase;
		this.needsFullSync = true;
		this.pcs.firePropertyChange("dataLinkDatabase", oldValue,
				dataLinkDatabase);
	}

	public String getDataLinkSchema() {
		return this.dataLinkSchema;
	}

	public void setDataLinkSchema(final String dataLinkSchema) {
		Log.debug("Setting data link schema on " + this + " to "
				+ dataLinkSchema);
		final String oldValue = this.dataLinkSchema;
		if (this.dataLinkSchema == dataLinkSchema
				|| this.dataLinkSchema != null
				&& this.dataLinkSchema.equals(dataLinkSchema))
			return;
		this.dataLinkSchema = dataLinkSchema;
		this.needsFullSync = true;
		this.pcs.firePropertyChange("dataLinkSchema", oldValue, dataLinkSchema);
	}

	/**
	 * Checks whether this schema is masked or not.
	 * 
	 * @return <tt>true</tt> if it is, <tt>false</tt> if it isn't.
	 */
	public boolean isMasked() {
		return this.masked;
	}

	/**
	 * Sets a new name for this schema. It checks with the mart first, and
	 * renames it if is not unique.
	 * 
	 * @param name
	 *            the new name for the schema.
	 */
	public void setName(String name) {
		Log.debug("Renaming schema " + this + " to " + name);
		final String oldValue = this.name;
		if (this.name == name || this.name != null && this.name.equals(name))
			return;
		// Work out all used names.
		final Set usedNames = new HashSet();
		for (final Iterator i = this.mart.getSchemas().values().iterator(); i
				.hasNext();)
			usedNames.add(((Schema) i.next()).getName());
		// Make new name unique.
		final String baseName = name;
		for (int i = 1; usedNames.contains(name); name = baseName + "_" + i++)
			;
		this.name = name;
		this.pcs.firePropertyChange("name", oldValue, name);
	}

	/**
	 * Sets a new original name for this schema. It checks with the mart first,
	 * and renames it if is not unique.
	 * 
	 * @param name
	 *            the new original name for the schema.
	 */
	protected void setOriginalName(String name) {
		Log.debug("Renaming original schema " + this + " to " + name);
		// Work out all used names.
		final Set usedNames = new HashSet();
		for (final Iterator i = this.mart.getSchemas().values().iterator(); i
				.hasNext();)
			usedNames.add(((Schema) i.next()).getOriginalName());
		// Make new name unique.
		final String baseName = name;
		for (int i = 1; usedNames.contains(name); name = baseName + "_" + i++)
			;
		this.originalName = name;
	}

	/**
	 * Sets whether this schema is masked or not.
	 * 
	 * @param masked
	 *            <tt>true</tt> if it is, <tt>false</tt> if it isn't.
	 */
	public void setMasked(final boolean masked) {
		Log.debug("Setting masked schema on " + this + " to " + masked);
		final boolean oldValue = this.masked;
		if (this.masked == masked)
			return;
		this.masked = masked;
		this.pcs.firePropertyChange("masked", oldValue, masked);
	}

	/**
	 * Checks whether this schema uses key-guessing or not.
	 * 
	 * @return <tt>true</tt> if it does, <tt>false</tt> if it doesn't.
	 */
	public boolean isKeyGuessing() {
		return this.keyGuessing;
	}

	/**
	 * Sets whether this schema uses key-guessing or not.
	 * 
	 * @param keyGuessing
	 *            <tt>true</tt> if it does, <tt>false</tt> if it doesn't.
	 */
	public void setKeyGuessing(final boolean keyGuessing) {
		Log.debug("Setting key guessing on " + this + " to " + keyGuessing);
		final boolean oldValue = this.keyGuessing;
		if (this.keyGuessing == keyGuessing)
			return;
		this.keyGuessing = keyGuessing;
		this.needsFullSync = true;
		this.pcs.firePropertyChange("keyGuessing", oldValue, keyGuessing);
	}

	public int hashCode() {
		return 0; // Because Schemas can be used as keys in maps.
	}

	public boolean equals(final Object o) {
		if (o == this)
			return true;
		else if (o == null)
			return false;
		else if (o instanceof Schema) {
			final Schema t = (Schema) o;
			return (this.mart.getUniqueId() + "_" + this.originalName)
					.equals(t.mart.getUniqueId() + "_" + t.originalName);
		} else
			return false;
	}

	public int compareTo(final Object obj) {
		final Schema t = (Schema) obj;
		return (this.mart.getUniqueId() + "_" + this.originalName)
				.compareTo(t.mart.getUniqueId() + "_" + t.originalName);
	}

	public String toString() {
		return this.name;
	}

	/**
	 * Synchronise this schema with the data source that is providing its
	 * tables. Synchronisation means checking the list of tables available and
	 * drop/add any that have changed, then check each column. and key and
	 * relation and update those too.
	 * <p>
	 * This method should set {@link #progress} to 0.0 and update it
	 * periodically until syncing is complete, when {@link #progress} should be
	 * greater than or equal to 100.0.
	 * 
	 * @throws SQLException
	 *             if there was a problem connecting to the data source.
	 * @throws DataModelException
	 *             if there was any other kind of logical problem.
	 */
	public void synchronise() throws SQLException, DataModelException {
		this.clearPartitionCache();
		this.needsFullSync = false;
		this.progress = 0.0;
		// Extend as required.
	}

	/**
	 * Call this method if you want the settings from this schema to be stored
	 * in the history file for later user.
	 */
	public void storeInHistory() {
		// Default implementation does nothing.
	}

	public boolean canCohabit(final DataLink dataLink) {
		// We're not connected to anything, so we can never cohabit.
		return false;
	}

	public boolean test() throws SQLException {
		// We're not connected to anything, so we always work.
		return true;
	}

	/**
	 * If this schema is identical across multiple source schemas, and the user
	 * wants to process each of those sequentially using the same schema
	 * settings, then the map returned by this call should be used to set up
	 * those partitions.
	 * <p>
	 * Note that the schema itself does not necessarily have to appear in the
	 * partition map - it is only a template by which each partition will be
	 * created.
	 * <p>
	 * The keys of the maps are strings - they can mean different things
	 * according to whether this is a JDBC schema, an XML schema, etc. The
	 * values are the prefix to stick on table names in datasets generated from
	 * this schema.
	 * <p>
	 * The entries in the map are the result of applying a combination of
	 * {@link #getPartitionRegex()} and {@link #getPartitionNameExpression()} to
	 * the list of available schemas in the database, as determined by the
	 * appropriate database driver.
	 * <p>
	 * This is NOT a bean. The contents are NOT mutable. If you want to change
	 * them, use {@link #setPartitionRegex(String)} and
	 * {@link #setPartitionNameExpression(String)} to alter the matching and
	 * transformation regexes to change the content of the map.
	 * 
	 * @return the map of partitions. If empty, then partitioning is not
	 *         required. It will never be <tt>null</tt>.
	 * @throws SQLException
	 *             if the partitions could not be retrieved.
	 */
	public Map getPartitions() throws SQLException {
		if (this.partitionCache.isEmpty() && this.partitionRegex != null
				&& this.partitionNameExpression != null)
			this.populatePartitionCache(this.partitionCache);
		return Collections.unmodifiableMap(this.partitionCache);
	}

	/**
	 * Return a collection (unordered) of all the partition schema prefixes
	 * currently in use by any table in this schema.
	 * 
	 * @return the collection of prefixes.
	 */
	public Collection getReferencedPartitions() {
		final Collection prefixes = new HashSet();
		for (final Iterator i = this.tables.values().iterator(); i.hasNext();)
			prefixes.addAll(((Table) i.next()).getSchemaPartitions());
		return prefixes;
	}

	/**
	 * This method is for subclasses to use {@link #getPartitionRegex()} and
	 * {@link #getPartitionNameExpression()}, both of which are guaranteed to
	 * be non-null when this method is called, to recalculate the set of
	 * partition values available. The map should have keys which are actual
	 * database schema names, and the values should be prefixes to use for those
	 * schemas when modifying table names to be unique to each partition.
	 * 
	 * @param partitionCache
	 *            the cache to populate. It will already be empty.
	 * @throws SQLException
	 *             if the population went wrong.
	 */
	protected void populatePartitionCache(final Map partitionCache)
			throws SQLException {
		// Do nothing.
	}

	/**
	 * Return the first n rows for a table.
	 * 
	 * @param schemaPrefix
	 *            the schema to use.
	 * @param table
	 *            the table to get rows from.
	 * @param count
	 *            the number of rows to select.
	 * @return the rows. The list will be empty if the operation is not
	 *         possible.
	 * @throws SQLException
	 *             if anything goes wrong.
	 */
	public List getRows(final String schemaPrefix, final Table table,
			final int count) throws SQLException {
		// Default implementation does nothing.
		return Collections.EMPTY_LIST;
	}

	/**
	 * Retrieve the regex used to work out schema partitions. If this regex is
	 * <tt>null</tt> then no partitioning will be done.
	 * 
	 * @return the regex used. Groups from this regex will be used to populate
	 *         values in the name expression. See
	 *         {@link #getPartitionNameExpression()}.
	 */
	public String getPartitionRegex() {
		return this.partitionRegex;
	}

	/**
	 * Retrieve the expression used to reformat groups from the partition regex
	 * into schema partition names.
	 * 
	 * @return the expression used. See also {@link #getPartitionRegex()}.
	 */
	public String getPartitionNameExpression() {
		return this.partitionNameExpression;
	}

	/**
	 * Set the regex used to work out schema partitions. If this regex is
	 * <tt>null</tt> then no partitioning will be done.
	 * 
	 * @param partitionRegex
	 *            the regex used. Groups from this regex will be used to
	 *            populate values in the name expression. See
	 *            {@link #setPartitionNameExpression(String)}.
	 */
	public void setPartitionRegex(final String partitionRegex) {
		Log.debug("Setting partition regex on " + this + " to "
				+ partitionRegex);
		final String oldValue = this.partitionRegex;
		// Extended check for null values.
		if (this.partitionRegex == partitionRegex
				|| this.partitionRegex != null
				&& this.partitionRegex.equals(partitionRegex))
			return;
		this.partitionRegex = partitionRegex;
		this.needsFullSync = true;
		this.partitionCache.clear();
		this.pcs.firePropertyChange("partitionRegex", oldValue, partitionRegex);
	}

	/**
	 * Clears the partition cache.
	 */
	public void clearPartitionCache() {
		this.partitionCache.clear();
	}

	/**
	 * Set the expression used to reformat groups from the partition regex into
	 * schema partition names.
	 * 
	 * @param partitionNameExpression
	 *            the expression used. See also
	 *            {@link #setPartitionRegex(String)}.
	 */
	public void setPartitionNameExpression(final String partitionNameExpression) {
		Log.debug("Setting partition name expression on " + this + " to "
				+ partitionNameExpression);
		final String oldValue = this.partitionNameExpression;
		// Extended check for null values.
		if (this.partitionNameExpression == partitionNameExpression
				|| this.partitionNameExpression != null
				&& this.partitionNameExpression.equals(partitionNameExpression))
			return;
		this.partitionNameExpression = partitionNameExpression;
		this.needsFullSync = true;
		this.clearPartitionCache();
		this.pcs.firePropertyChange("partitionNameExpression", oldValue,
				partitionNameExpression);
	}

	/**
	 * This implementation of the {@link Schema} interface connects to a JDBC
	 * data source and loads tables, keys and relations using database metadata.
	 * <p>
	 * If key-guessing is enabled, foreign keys are guessed instead of being
	 * read from the database. Guessing works by iterating through known primary
	 * keys, where the first column of the key matches the name of the table
	 * (optionally with '_id' appended), then iterating through all other tables
	 * looking for sets of columns with identical names, or names that have had
	 * '_key' appended. If it finds a matching set, then it assumes that it has
	 * found a foreign key, and establishes a relation between the two.
	 * <p>
	 * When using keyguessing, primary keys are read from database metadata, but
	 * if this method returns no results, then each table is searched for a
	 * column with the same name as the table, optionally with '_id' appended.
	 * If one is found, then it is assumed that that column is the primary key
	 * for the table.
	 * <p>
	 * This implementation is very careful not to override any hand-made
	 * relations or keys, or to reinstate any that have previously been marked
	 * as incorrect.
	 */
	/**
	 * @author vgu
	 * 
	 */
	public static class JDBCSchema extends Schema implements JDBCDataLink {
		private static final long serialVersionUID = 1L;

		private Connection connection;

		private String driverClassName;

		private String password;

		private String url;

		private String username;

		private String realSchemaName;

		/**
		 * <p>
		 * Establishes a JDBC connection from the information provided, and
		 * remembers it. Nothing is read yet - if you want to read the schema
		 * data, you must use the {@link #synchronise()} method to do so.
		 * 
		 * @param mart
		 *            the mart we belong to.
		 * @param driverClassName
		 *            the class name of the JDBC driver, eg.
		 *            <tt>com.mysql.jdbc.Driver</tt>.
		 * @param url
		 *            the JDBC URL of the database server to connect to.
		 * @param dataLinkDatabase
		 *            the database to read tables from.
		 * @param dataLinkSchema
		 *            the database schema name to read tables from. In MySQL
		 *            this should be the same as the database name specified in
		 *            the JDBC URL. In Oracle and PostgreSQL, it is a distinct
		 *            entity.
		 * @param username
		 *            the username to connect as.
		 * @param password
		 *            the password to connect as. Defaults to no password if the
		 *            empty string is passed in.
		 * @param name
		 *            the name to give this schema after it has been created.
		 * @param keyGuessing
		 *            <tt>true</tt> if you want keyguessing enabled,
		 *            <tt>false</tt> otherwise.
		 * @param partitionRegex
		 *            partition stuff.
		 * @param partitionNameExpression
		 *            partition stuff.
		 */
		public JDBCSchema(final Mart mart, final String driverClassName,
				final String url, final String dataLinkDatabase,
				final String dataLinkSchema, final String username,
				final String password, final String name,
				final boolean keyGuessing, final String partitionRegex,
				final String partitionNameExpression) {
			// Call the Schema constructor first, to set up our name,
			// and set up keyguessing.
			super(mart, name, keyGuessing, dataLinkDatabase, dataLinkSchema,
					partitionRegex, partitionNameExpression);

			Log.debug("Creating JDBC schema");

			// Remember the settings.
			this.setDriverClassName(driverClassName);
			this.setUrl(url);
			this.setUsername(username);
			this.setPassword(password);

			this.addPropertyChangeListener("driverClassName", this.listener);
			this.addPropertyChangeListener("url", this.listener);
			this.addPropertyChangeListener("username", this.listener);
			this.addPropertyChangeListener("password", this.listener);
		}

		protected void finalize() throws Throwable {
			try {
				this.closeConnection();
			} finally {
				super.finalize();
			}
		}

		public Collection getUniqueValues(final String schemaPrefix,
				final Column column) throws SQLException {
			// Do the select.
			final List results = new ArrayList();
			final String schemaName = schemaPrefix == null ? this
					.getDataLinkSchema() : (!this.getPartitions()
					.containsValue(schemaPrefix) ? this.getDataLinkSchema()
					: (String) new InverseMap(this.getPartitions())
							.get(schemaPrefix));
			final Connection conn = this.getConnection(null);
			final String sql = DatabaseDialect.getDialect(this)
					.getUniqueValuesSQL(schemaName, column);
			Log.debug("About to run query: " + sql);
			final ResultSet rs = conn.prepareStatement(sql).executeQuery();
			while (rs.next())
				results.add(rs.getString(1));
			rs.close();

			// Return the results.
			return results;
		}

		public List getRows(final String schemaPrefix, final Table table,
				final int count) throws SQLException {
			// Do the select.
			final List results = new ArrayList();
			final String schemaName = schemaPrefix == null ? this
					.getDataLinkSchema() : (String) new InverseMap(this
					.getPartitions()).get(schemaPrefix);
			final Connection conn = this.getConnection(null);
			final String sql = DatabaseDialect.getDialect(this)
					.getSimpleRowsSQL(schemaName, table);
			Log.debug("About to run query: " + sql);
			final ResultSet rs = conn.prepareStatement(sql).executeQuery();
			int rowCount = 0;
			while (rs.next() && rowCount++ < count) {
				final List values = new ArrayList();
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++)
					values.add(rs.getObject(i));
				results.add(values);
			}
			rs.close();

			// Return the results.
			return results;
		}

		public void populatePartitionCache(final Map partitions)
				throws SQLException {
			Log.debug("Populating partition columns on " + this);
			// Valid regex?
			Pattern p;
			try {
				p = Pattern.compile(this.getPartitionRegex());
			} catch (final PatternSyntaxException e) {
				// Ignore and return if invalid.
				return;
			}
			// Use regex and friends to work out partitions.
			final Connection conn = this.getConnection(null);
			// List out all catalogs available.
			Log.debug("Looking up JDBC catalogs");
			final DatabaseMetaData dmd = conn.getMetaData();
			final ResultSet rs = "".equals(dmd.getSchemaTerm()) ? dmd
					.getCatalogs() : dmd.getSchemas();
			try {
				while (rs.next()) {
					final String schema = rs.getString(1);
					// Match them against the regex, retaining those
					// that match and using the name expression to name them.
					final Matcher m = p.matcher(schema);
					if (m.matches())
						try {
							partitions.put(schema, m.replaceAll(this
									.getPartitionNameExpression()));
						} catch (final IndexOutOfBoundsException e) {
							// We don't care if the expression is invalid.
						}
				}
			} catch (final SQLException e) {
				throw e;
			} finally {
				rs.close();
			}
		}

		/**
		 * {@inheritDoc}
		 * <p>
		 * In our case, cohabitation means that the partner link is also a
		 * {@link JDBCDataLink} and that its connection is connected to the same
		 * database server listening on the same port and connected with the
		 * same username.
		 */
		public boolean canCohabit(final DataLink partner) {
			Log.debug("Testing " + this + " against " + partner
					+ " for cohabitation");
			// We can't cohabit with non-JDBCDataLink partners.
			if (!(partner instanceof JDBCDataLink))
				return false;
			final JDBCDataLink partnerLink = (JDBCDataLink) partner;

			// Work out the partner's catalogs and schemas.
			final Collection partnerSchemas = new HashSet();
			try {
				final DatabaseMetaData dmd = partnerLink.getConnection(null)
						.getMetaData();
				// We need to compare by catalog only.
				final ResultSet catalogs = dmd.getCatalogs();
				while (catalogs.next())
					partnerSchemas.add(catalogs.getString("TABLE_CAT"));
				return partnerSchemas.contains(this.getConnection(null)
						.getCatalog());
			} catch (final Throwable t) {
				// If get an error, assume can't find anything, thus assume
				// incompatible.
				return false;
			}
		}

		public Connection getConnection(final String overrideDataLinkSchema)
				throws SQLException {
			// If we are already connected, test to see if we are
			// still connected. If not, reset our connection.
			if (this.connection != null && this.connection.isClosed())
				try {
					Log.debug("Closing dead JDBC connection");
					this.connection.close();
				} catch (final SQLException e) {
					// We don't care. Ignore it.
				} finally {
					this.connection = null;
				}

			// If we are not connected, we should attempt to (re)connect now.
			if (this.connection == null) {
				Log.debug("Establishing JDBC connection");
				// Start out with no driver at all.
				Class loadedDriverClass = null;

				// Try the system class loader instead.
				try {
					loadedDriverClass = Class.forName(this.driverClassName);
				} catch (final ClassNotFoundException e) {
					final SQLException e2 = new SQLException();
					e2.initCause(e);
					throw e2;
				}

				// Check it really is an instance of Driver.
				if (!Driver.class.isAssignableFrom(loadedDriverClass))
					throw new ClassCastException(Resources
							.get("driverClassNotJDBCDriver"));

				// Connect!
				final Properties properties = new Properties();
				properties.setProperty("user", this.username);
				if (!this.password.equals(""))
					properties.setProperty("password", this.password);
				properties.setProperty("nullCatalogMeansCurrent", "false");
				this.connection = DriverManager.getConnection(
						overrideDataLinkSchema == null ? this.url : this.url
								.replaceAll(this.getDataLinkSchema(),
										overrideDataLinkSchema), properties);

				// Check the schema name.
				final DatabaseMetaData dmd = this.connection.getMetaData();
				final String catalog = this.connection.getCatalog();
				this.realSchemaName = this.getDataLinkSchema();
				ResultSet rs = dmd.getTables(catalog, this.realSchemaName, "%",
						null);
				if (!rs.isBeforeFirst()) {
					rs = dmd.getTables(catalog, this.realSchemaName
							.toUpperCase(), "%", null);
					if (rs.isBeforeFirst())
						this.realSchemaName = this.realSchemaName.toUpperCase();
				}
				if (!rs.isBeforeFirst()) {
					rs = dmd.getTables(catalog, this.realSchemaName
							.toLowerCase(), "%", null);
					if (rs.isBeforeFirst())
						this.realSchemaName = this.realSchemaName.toLowerCase();
				}
				rs.close();
			}

			// Return the connection.
			return this.connection;
		}

		public void setDataLinkDatabase(final String databaseName) {
			super.setDataLinkDatabase(databaseName);
			// Reset the cached database connection.
			try {
				this.closeConnection();
			} catch (final SQLException e) {
				// We don't care.
			}
		}

		public void setDataLinkSchema(final String schemaName) {
			super.setDataLinkSchema(schemaName);
			// Reset the cached database connection.
			try {
				this.closeConnection();
			} catch (final SQLException e) {
				// We don't care.
			}
		}

		private void closeConnection() throws SQLException {
			Log.debug("Closing JDBC connection");
			if (this.connection != null)
				try {
					this.connection.close();
				} finally {
					this.connection = null;
				}
		}

		public String getDriverClassName() {
			return this.driverClassName;
		}

		public String getUrl() {
			return this.url;
		}

		public String getPassword() {
			return this.password;
		}

		public String getUsername() {
			return this.username;
		}

		public void setDriverClassName(final String driverClassName) {
			Log.debug("Setting driver class name on " + this + " to "
					+ driverClassName);
			final String oldValue = this.driverClassName;
			if (this.driverClassName == driverClassName
					|| this.driverClassName != null
					&& this.driverClassName.equals(this.getDriverClassName()))
				return;
			this.driverClassName = driverClassName;
			try {
				this.closeConnection();
			} catch (final SQLException e) {
				// We don't care.
			}
			this.pcs.firePropertyChange("driverClassName", oldValue,
					driverClassName);
		}

		public void setUrl(final String url) {
			Log.debug("Setting JDBC URL on " + this + " to " + url);
			final String oldValue = this.url;
			if (this.url == url || this.url != null && this.url.equals(url))
				return;
			this.url = url;
			try {
				this.closeConnection();
			} catch (final SQLException e) {
				// We don't care.
			}
			this.pcs.firePropertyChange("url", oldValue, url);
		}

		public void setPassword(final String password) {
			Log.debug("Setting new password on " + this);
			final String oldValue = this.password;
			if (this.password == password || this.password != null
					&& this.password.equals(password))
				return;
			this.password = password;
			try {
				this.closeConnection();
			} catch (final SQLException e) {
				// We don't care.
			}
			this.pcs.firePropertyChange("password", oldValue, password);
		}

		public void setUsername(final String username) {
			Log.debug("Setting username on " + this + " to " + username);
			final String oldValue = this.username;
			if (this.username == username || this.username != null
					&& this.username.equals(username))
				return;
			this.username = username;
			try {
				this.closeConnection();
			} catch (final SQLException e) {
				// We don't care.
			}
			this.pcs.firePropertyChange("username", oldValue, username);
		}

		public void storeInHistory() {
			// Store the schema settings in the history file.
			final Properties history = new Properties();
			history.setProperty("driverClass", this.getDriverClassName());
			history.setProperty("jdbcURL", this.getUrl());
			history.setProperty("username", this.getUsername());
			history.setProperty("password", this.getPassword() == null ? ""
					: this.getPassword());
			history.setProperty("schema", this.getDataLinkSchema());
			history.setProperty("partitionRegex",
					this.getPartitionRegex() == null ? "" : this
							.getPartitionRegex());
			history.setProperty("partitionNameExpression", this
					.getPartitionNameExpression() == null ? "" : this
					.getPartitionNameExpression());
			history.setProperty("keyguessing", "" + this.isKeyGuessing());
			Settings.saveHistoryProperties(JDBCSchema.class, this.getName(),
					history);
		}

		public boolean test() throws SQLException {
			Log.debug("Testing connection for " + this);
			// Establish the JDBC connection. May throw an exception of its own,
			// which is fine, just let it go.
			final Connection connection = this.getConnection(null);
			// If we have no connection, we can't test it!
			if (connection == null)
				return false;

			// Get the metadata.
			final DatabaseMetaData dmd = connection.getMetaData();

			// By opening, executing, then closing a DMD query we will test
			// the connection fully without actually having to read anything
			// from it. 
			// modified by yong liang for checking the lowercase/uppercase schema
			final String catalog = connection.getCatalog();
//			ResultSet rs = dmd.getTables(
//					"".equals(dmd.getSchemaTerm()) ? this.realSchemaName
//							: catalog, this.realSchemaName, "%", null);
			//FIXME: It should use the same format as getConnection and synchronize in the future
			ResultSet rs = dmd.getTables(catalog, this.realSchemaName, "%", null);
			
			final boolean worked = rs.isBeforeFirst();
			rs.close();

			// If we get here, it worked.
			return worked;
		}

		public void synchronise() throws SQLException, DataModelException {
			Log.info("Synchronising " + this);
			super.synchronise();
			// Get database metadata, catalog, and schema details.
			final DatabaseMetaData dmd = this.getConnection(null).getMetaData();
			final String catalog = this.getConnection(null).getCatalog();
			
			// List of objects storing orphan key column and its table name
			// The list may have duplicated key
			List orphanFKList = new ArrayList();
			StringBuffer orphanSearch = new StringBuffer();
			boolean orphanBool = false;
		
			try {
				orphanBool = findOrphanKeysFromDB(orphanFKList, orphanSearch);

				if (orphanBool) {
					// force return
					// return;

					// ViewTextDialog.displayText("Orphan Key Found",
					// orphanSearch);
					// WarningOptionViewTextDialog.displayText(orphanSearch,
					// "Schema Synchronization Warning");

					Frame frame = new Frame();

					Object[] options = {Resources.get("detailButton"), Resources.get("cancelButton")};
					int n = JOptionPane
							.showOptionDialog(
									frame, Resources.get("orphanRelationWarningMessage"),
									Resources.get("orphanRelationWarningTitle"),
									JOptionPane.YES_NO_OPTION,
									JOptionPane.WARNING_MESSAGE, null, // do not use a custom Icon
									options, // the titles of buttons
									options[1]); // default button title

					// int n = JOptionPane.showConfirmDialog(
					// frame, orphanSearch + "\n", "Update Operation Will
					// Corrupt Schema. Do you still want to proceed?",
					// JOptionPane.YES_NO_OPTION);
					if (n != JOptionPane.YES_OPTION) {
						//CorruptSchemaTextDialog.displayText("Orphan Foreign Key", orphanSearch);
						return;						
					}
					else{
						SaveOrphanKeyDialog sokd = new SaveOrphanKeyDialog(Resources.get("orphanKeyDialogTitle"), orphanSearch.toString());
						sokd.setVisible(true);
						//user abort it
						if(!sokd.checkSaved())
							return;
					}
					


/*
 * 
 * Frame f = new Frame();
 * 
 * final JOptionPane optionPane = new JOptionPane( orphanSearch + "\n" + "Update
 * Operation Will Corrupt Schema. Do you still want to proceed?",
 * JOptionPane.WARNING_MESSAGE, JOptionPane.YES_NO_OPTION);
 * 
 * optionPane .addPropertyChangeListener(new PropertyChangeListener() { public
 * void propertyChange(PropertyChangeEvent e) { //String prop =
 * e.getPropertyName();
 * 
 * if (e.getSource() == optionPane){
 * 
 * int value = ((Integer) optionPane .getValue()).intValue(); if (value ==
 * JOptionPane.NO_OPTION) { return; } } } });
 * 
 * final JDialog dialog = new JDialog(f, "Data Integrity Warning", true);
 * dialog.setContentPane(optionPane); dialog.pack(); dialog.setVisible(true);
 */
				}
				// force return
				// return;
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Now that user decides to sync GUI model to DB schema, remove orphan key
			if(orphanBool)
				clearOrphanKey(orphanFKList);

			// Create a list of existing tables. During this method, we remove
			// from this list all tables that still exist in the database. At
			// the end of the method, the list contains only those tables
			// which no longer exist, so they will be dropped.
			final Collection tablesToBeDropped = new HashSet(this.getTables()
					.values());

			// Clear the existing schema partition information.
			for (final Iterator i = this.getTables().values().iterator(); i
					.hasNext();)
				((Table) i.next()).getSchemaPartitions().clear();

			// Load tables and views from database, then loop over them.
			ResultSet dbTables;
			if (this.getPartitions().isEmpty())
				dbTables = dmd.getTables(catalog, this.realSchemaName, "%",
						new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
			else
				dbTables = dmd.getTables("".equals(dmd.getSchemaTerm()) ? null
						: catalog, null, "%", new String[] { "TABLE", "VIEW",
						"ALIAS", "SYNONYM" });

			// Do the loop.
			final Collection tablesToBeKept = new HashSet();
			while (dbTables.next()) {
				// Check schema and catalog.
				final String catalogName = dbTables.getString("TABLE_CAT");
				final String schemaName = dbTables.getString("TABLE_SCHEM");
				String schemaPrefix = null;
				// No prefix if partitions are empty;
				if (!this.getPartitions().isEmpty()) {
					if ("".equals(dmd.getSchemaTerm()))
						// Use catalog name to get prefix.
						schemaPrefix = (String) this.getPartitions().get(
								catalogName);
					else
						// Use schema name to get prefix.
						schemaPrefix = (String) this.getPartitions().get(
								schemaName);
					// Don't want to include if prefix is still null.
					if (schemaPrefix == null)
						continue;
				}
				
				// What is the table called?
				final String dbTableName = dbTables.getString("TABLE_NAME");
				Log.debug("Processing table " + dbTableName);

				//this is hardcode for oracle, check if this table is from recyclebin
				if(this.driverClassName.equals("oracle.jdbc.driver.OracleDriver") && dbTableName.indexOf("BIN$")==0)
					continue;

				// Look to see if we already have a table by this name defined.
				// If we do, reuse it. If not, create a new table.
				Table dbTable = (Table) this.getTables().get(dbTableName);
				if (dbTable == null)
					try {
						dbTable = new Table(this, dbTableName);
						this.getTables().put(dbTableName, dbTable);
					} catch (final Throwable t) {
						throw new BioMartError(t);
					}
				// Add schema prefix to list.
				if (schemaPrefix != null)
					dbTable.getSchemaPartitions().add(schemaPrefix);

				// Table exists, so remove it from our list of tables to be
				// dropped at the end of the method.
				tablesToBeDropped.remove(dbTable);
				tablesToBeKept.add(dbTable);
			}
			dbTables.close();

			// Work out progress increment step size.
			double stepSize = 100.0 / (double) tablesToBeKept.size();
			// Divide by 2 - columns then relations.
			stepSize /= 2.0;

			// Loop over all columns.
			for (final Iterator i = tablesToBeKept.iterator(); i.hasNext();) {
				final Table dbTable = (Table) i.next();
				final String dbTableName = dbTable.getName();
				// Make a list of all the columns in the table. Any columns
				// remaining in this list by the end of the loop will be
				// dropped.
				final Collection colsToBeDropped = new HashSet(dbTable
						.getColumns().values());

				// Clear out the existing schema partition info on all cols.
				for (final Iterator j = dbTable.getColumns().values()
						.iterator(); j.hasNext();)
					((Column) j.next()).getSchemaPartitions().clear();

				// Load the table columns from the database, then loop over
				// them.
				Log.debug("Loading table column list for " + dbTableName);
				ResultSet dbTblCols;
				if (this.getPartitions().isEmpty())
					dbTblCols = dmd.getColumns(catalog, this.realSchemaName,
							dbTableName, "%");
				else
					dbTblCols = dmd.getColumns(
							"".equals(dmd.getSchemaTerm()) ? null : catalog,
							null, dbTableName, "%");
				// FIXME: When using Oracle, if the table is a synonym then the
				// above call returns no results.
				while (dbTblCols.next()) {
					// Check schema and catalog.
					final String catalogName = dbTblCols.getString("TABLE_CAT");
					final String schemaName = dbTblCols
							.getString("TABLE_SCHEM");
					String schemaPrefix = null;
					// No prefix if partitions are empty;
					if (!this.getPartitions().isEmpty()) {
						if ("".equals(dmd.getSchemaTerm()))
							// Use catalog name to get prefix.
							schemaPrefix = (String) this.getPartitions().get(
									catalogName);
						else
							// Use schema name to get prefix.
							schemaPrefix = (String) this.getPartitions().get(
									schemaName);
						// Don't want to include if prefix is still null.
						if (schemaPrefix == null)
							continue;
					}

					// What is the column called, and is it nullable?
					final String dbTblColName = dbTblCols
							.getString("COLUMN_NAME");
					Log.debug("Processing column " + dbTblColName);

					// Look to see if the column already exists on this table.
					// If it does, reuse it. Else, create it.
					Column dbTblCol = (Column) dbTable.getColumns().get(
							dbTblColName);
					if (dbTblCol == null)
						try {
							dbTblCol = new Column(dbTable, dbTblColName);
							dbTable.getColumns().put(dbTblCol.getName(),
									dbTblCol);
						} catch (final Throwable t) {
							throw new BioMartError(t);
						}

					// Column exists, so remove it from our list of columns to
					// be dropped at the end of the loop.
					colsToBeDropped.remove(dbTblCol);
					if (schemaPrefix != null)
						dbTblCol.getSchemaPartitions().add(schemaPrefix);
				}
				dbTblCols.close();

				// Drop all columns that are left in the list, as they no longer
				// exist in the database.
				for (final Iterator j = colsToBeDropped.iterator(); j.hasNext();) {
					final Column column = (Column) j.next();
					Log.debug("Dropping redundant column " + column.getName());
					dbTable.getColumns().remove(column.getName());
				}

				// Update progress;
				this.progress += stepSize;
			}

			// Remove from schema all tables not found in the database, using
			// the list we constructed above.
			for (final Iterator i = tablesToBeDropped.iterator(); i.hasNext();) {
				final Table existingTable = (Table) i.next();
				Log.debug("Dropping redundant table " + existingTable);
				final String tableName = existingTable.getName();
				// By clearing its keys we will also clear its relations.
				for (final Iterator j = existingTable.getKeys().iterator(); j
						.hasNext();) {
					// Deref to prevent concurrent mods.
					final Collection rels = new ArrayList(((Key) j.next())
							.getRelations());
					for (final Iterator r = rels.iterator(); r.hasNext();) {
						final Relation rel = (Relation) r.next();
						rel.getFirstKey().getRelations().remove(rel);
						rel.getSecondKey().getRelations().remove(rel);
					}
				}
				existingTable.setPrimaryKey(null);
				existingTable.getForeignKeys().clear();
				this.getTables().remove(tableName);
			}

			// Get and create primary keys.
			// Work out a list of all foreign keys currently existing.
			// Any remaining in this list later will be dropped.
			final Collection fksToBeDropped = new HashSet();
			for (final Iterator i = this.getTables().values().iterator(); i
					.hasNext();) {
				final Table t = (Table) i.next();
				fksToBeDropped.addAll(t.getForeignKeys());

				// Obtain the primary key from the database. Even in databases
				// without referential integrity, the primary key is still
				// defined and can be obtained from the metadata.
				Log.debug("Loading table primary keys");
				String searchCatalog = catalog;
				String searchSchema = this.realSchemaName;
				if (!t.getSchemaPartitions().isEmpty()) {
					// Locate partition with first prefix.
					final String prefix = (String) t.getSchemaPartitions()
							.iterator().next();
					String schemaName = (String) new InverseMap(this
							.getPartitions()).get(prefix);
					if (schemaName == null) // Should never happen.
						throw new BioMartError();
					if ("".equals(dmd.getSchemaTerm()))
						searchCatalog = schemaName;
					searchSchema = schemaName;
				}
				final ResultSet dbTblPKCols = dmd.getPrimaryKeys(searchCatalog,
						searchSchema, t.getName());

				// Load the primary key columns into a map keyed by column
				// position.
				// In other words, the first column in the key has a map key of
				// 1, and so on. We do this because we can't guarantee we'll
				// read the key columns from the database in the correct order.
				// We keep the map sorted, so that when we iterate over it later
				// we get back the columns in the correct order.
				final Map pkCols = new TreeMap();
				while (dbTblPKCols.next()) {
					final String pkColName = dbTblPKCols
							.getString("COLUMN_NAME");
					final Short pkColPosition = new Short(dbTblPKCols
							.getShort("KEY_SEQ"));
					pkCols.put(pkColPosition, t.getColumns().get(pkColName));
				}
				dbTblPKCols.close();

				// Did DMD find a PK? If not, which is really unusual but
				// potentially may happen, attempt to find one by looking for a
				// single column with the same name as the table or with '_id'
				// appended.
				// Only do this if we are using key-guessing.
				if (pkCols.isEmpty() && this.isKeyGuessing()) {
					Log
							.debug("Found no primary key, so attempting to guess one");
					// Plain version first.
					Column candidateCol = (Column) t.getColumns().get(
							t.getName());
					// Try with '_id' appended if plain version turned up
					// nothing.
					if (candidateCol == null)
						candidateCol = (Column) t
								.getColumns()
								.get(
										t.getName()
												+ Resources
														.get("primaryKeySuffix"));
					// Found something? Add it to the primary key columns map,
					// with a dummy key of 1. (Use Short for the key because
					// that
					// is what DMD would have used had it found anything
					// itself).
					if (candidateCol != null)
						pkCols.put(Short.valueOf("1"), candidateCol);
				}

				// Obtain the existing primary key on the table, if the table
				// previously existed and even had one in the first place.
				final PrimaryKey existingPK = t.getPrimaryKey();

				// Did we find a PK on the database copy of the table?
				if (!pkCols.isEmpty()) {

					// Yes, we found a PK on the database copy of the table. So,
					// create a new key based around the columns we identified.
					PrimaryKey candidatePK;
					try {
						candidatePK = new PrimaryKey((Column[]) pkCols.values()
								.toArray(new Column[0]));
					} catch (final Throwable th) {
						throw new BioMartError(th);
					}

					// If the existing table has no PK, or has a PK which
					// matches and is not incorrect, or has a PK which does not
					// match
					// and is not handmade, replace that PK with the one we
					// found.
					// This way we preserve any existing handmade PKs, and don't
					// override any marked as incorrect.
					try {
						if (existingPK == null)
							t.setPrimaryKey(candidatePK);
						else if (existingPK.equals(candidatePK)
								&& existingPK.getStatus().equals(
										ComponentStatus.HANDMADE))
							existingPK.setStatus(ComponentStatus.INFERRED);
						else if (!existingPK.equals(candidatePK)
								&& !existingPK.getStatus().equals(
										ComponentStatus.HANDMADE))
							t.setPrimaryKey(candidatePK);
					} catch (final Throwable th) {
						throw new BioMartError(th);
					}
				} else // No, we did not find a PK on the database copy of the
				// table, so that table should not have a PK at all. So if the
				// existing table has a PK which is not handmade, remove it.
				// the orphan PK is already cleaned by clearOrphanKey();
				if (existingPK != null
						&& !existingPK.getStatus().equals(
								ComponentStatus.HANDMADE))
					try {
						t.setPrimaryKey(null);
					} catch (final Throwable th) {
						throw new BioMartError(th);
					}
			}

			// Are we key-guessing? Key guess the foreign keys, passing in a
			// reference to the list of existing foreign keys. After this call
			// has completed, the list will contain all those foreign keys which
			// no longer exist, and can safely be dropped.
			if (this.isKeyGuessing())
				this.synchroniseKeysUsingKeyGuessing(fksToBeDropped, stepSize);
			// Otherwise, use DMD to do the same, also passing in the list of
			// existing foreign keys to be updated as the call progresses. Also
			// pass in the DMD details so it doesn't have to work them out for
			// itself.
			else
				this.synchroniseKeysUsingDMD(fksToBeDropped, dmd,
						this.realSchemaName, catalog, stepSize);

			// Drop any foreign keys that are left over (but not handmade ones).
			// the orphan FK is already cleaned by clearOrphanKey();
			for (final Iterator i = fksToBeDropped.iterator(); i.hasNext();) {
				final Key k = (Key) i.next();
				if (k.getStatus().equals(ComponentStatus.HANDMADE))
					continue;
				Log.debug("Dropping redundant foreign key " + k);
				for (final Iterator r = k.getRelations().iterator(); r
						.hasNext();) {
					final Relation rel = (Relation) r.next();
					rel.getFirstKey().getRelations().remove(rel);
					rel.getSecondKey().getRelations().remove(rel);
				}
				k.getTable().getForeignKeys().remove(k);
			}

			Log.info("Done synchronising");
		}

		
		private ResultSet getTablesFromDB() throws SQLException {

			// Get database metadata, catalog, and schema details.
			final DatabaseMetaData dmd = this.getConnection(null).getMetaData();
			final String catalog = this.getConnection(null).getCatalog();

			// Load tables and views from database, then loop over them.
			ResultSet dbTables = null;
			if (this.getPartitions().isEmpty())
				dbTables = dmd.getTables(catalog, this.realSchemaName, "%",
						new String[] { "TABLE", "VIEW", "ALIAS", "SYNONYM" });
			else
				dbTables = dmd.getTables("".equals(dmd.getSchemaTerm()) ? null
						: catalog, null, "%", new String[] { "TABLE", "VIEW",
						"ALIAS", "SYNONYM" });

			return dbTables;

		}

		private ResultSet getTableColumnsFromDB(String dbTableName)
				throws SQLException {

			// Get database metadata, catalog, and schema details.
			final DatabaseMetaData dmd = this.getConnection(null).getMetaData();
			final String catalog = this.getConnection(null).getCatalog();

			ResultSet dbTblCols;
			if (this.getPartitions().isEmpty())
				dbTblCols = dmd.getColumns(catalog, this.realSchemaName,
						dbTableName, "%");
			else
				dbTblCols = dmd.getColumns(
						"".equals(dmd.getSchemaTerm()) ? null : catalog, null,
						dbTableName, "%");

			return dbTblCols;

		}

		private HashMap getDBTableColumnCollection(ResultSet dbTableSet)
				throws SQLException {
			ResultSet dbTableColSet;
			HashMap tableColMap = new HashMap();
			while (dbTableSet.next()) {
				String tableName = dbTableSet.getString("TABLE_NAME");
				dbTableColSet = getTableColumnsFromDB(tableName);

				HashSet cols = new HashSet();
				while (dbTableColSet.next()) {
					cols.add(dbTableColSet.getString("COLUMN_NAME"));
				}
				dbTableColSet.close();
				tableColMap.put(tableName, cols);
			}

			return tableColMap;

		}

		 
		/** 
		 * Pass in a list object to hold table and column with orphan FK and PK
		 * Get PK, FK and the corresponding relations if they have. (Some PK, FK may not have relations)
		 *  
		 * modified by @author yliang
		 */
		private boolean findOrphanKeysFromDB(List orphanKeyList, StringBuffer orphanSearch) throws Exception {

			HashSet dbcols;
			boolean foundOrphanKey = false;
			StringBuffer result = orphanSearch;

			//List missTableList = new ArrayList();
			

			ResultSet dbTableSet = getTablesFromDB();
			HashMap tableColMap = getDBTableColumnCollection(dbTableSet);
			dbTableSet.close();

			// Loop through each key in the GUI model tables
			for (final Iterator i = this.getTables().values().iterator(); i
					.hasNext();) {

				final Table t = (Table) i.next();
				// Find the hashset of columns in corresponding DB table
				dbcols = (HashSet) tableColMap.get(t.getName());
				// Tables dropped or renamed is handled inside sync process
/*				if (dbcols == null) {
					//missTableList.add(t.getName());
					
					
					boolean foundRel = addTableKeysToOrphanList(t, orphanFK);
					if (foundRel){
						foundOrphanFK = true;
					}
					continue;
				
				}
*/
				//handle both PK and FK
				for (final Iterator j = t.getKeys().iterator(); j.hasNext();) {
					final Key k = (Key) j.next();
					for (int kcl = 0; kcl < k.getColumns().length; kcl++)

						// If there is no matching column in the DB table, the key is orphan
						// If dbcols is null, all columns are dropped and the key is orphan
						if (dbcols==null || !dbcols.contains(k.getColumns()[kcl].getName())) {

							foundOrphanKey = true;
							orphanKeyList.add(k);
							
							String msg = Resources.get("orphanFound")+" "+k+"; "+Resources.get("columnMissed")+" "+k.getColumns()[kcl].getName();
							if(k.getRelations()!=null && k.getRelations().size()>0)
								msg = msg + "; " + Resources.get("incorrectRelations")+ " "+ k.getRelations().toString() + "\n";

							result.append(msg);
							Log.warn(msg);
						}
				}
				
			}
			return foundOrphanKey;
		}

		/**
		 * clear Orphan Key
		 * @param orphanFKList
		 * @author yliang
		 */
		private void clearOrphanKey(List orphanFKList){
			
			
			for (final Iterator i = orphanFKList.iterator(); i.hasNext();) {
				final Key k = (Key) i.next();

				// Remove the relations for this key, it may happen that both PK and FK are orphen keys
				while(k.getRelations().size()>0)
				{
					final Relation rel = (Relation)k.getRelations().iterator().next();
					rel.getFirstKey().getRelations().remove(rel);
					rel.getSecondKey().getRelations().remove(rel);
				}
				// Remove the key from the table
				if(k instanceof PrimaryKey)
					k.getTable().setPrimaryKey(null);
				else
					k.getTable().getForeignKeys().remove(k);
				
				k.getTable().getKeys().remove(k);
			}
		}

	

		/**
		 * Establish foreign keys based purely on database metadata.
		 * 
		 * @param fksToBeDropped
		 *            the list of foreign keys to update as we go along. By the
		 *            end of the method, the only keys left in this list should
		 *            be ones that no longer exist in the database and may be
		 *            dropped.
		 * @param dmd
		 *            the database metadata to obtain the foreign keys from.
		 * @param schema
		 *            the database schema to read metadata from.
		 * @param catalog
		 *            the database catalog to read metadata from.
		 * @param stepSize
		 *            the progress step size to increment by.
		 * @throws SQLException
		 *             if there was a problem talking to the database.
		 * @throws DataModelException
		 *             if there was a logical problem during construction of the
		 *             set of foreign keys.
		 */
		/**
		 * @param fksToBeDropped
		 * @param dmd
		 * @param schema
		 * @param catalog
		 * @param stepSize
		 * @throws SQLException
		 * @throws DataModelException
		 */
		private void synchroniseKeysUsingDMD(final Collection fksToBeDropped,
				final DatabaseMetaData dmd, final String schema,
				final String catalog, final double stepSize)
				throws SQLException, DataModelException {
			Log.debug("Running DMD key synchronisation");
			// Loop through all the tables in the database, which is the same
			// as looping through all the primary keys.
			Log.debug("Finding tables");
			for (final Iterator i = this.getTables().values().iterator(); i
					.hasNext();) {
				// Update progress;
				this.progress += stepSize;

				// Obtain the table and its primary key.
				final Table pkTable = (Table) i.next();
				final PrimaryKey pk = pkTable.getPrimaryKey();
				// Skip all tables which have no primary key.
				if (pk == null)
					continue;

				Log.debug("Processing primary key " + pk);

				// Make a list of relations that already exist in this schema,
				// from some previous run. Any relations that are left in this
				// list by the end of the loop for this table no longer exist in
				// the database, and will be dropped.
				final Collection relationsToBeDropped = new HashSet(pk
						.getRelations());

				// Identify all foreign keys in the database metadata that refer
				// to the current primary key.
				Log.debug("Finding referring foreign keys");
				String searchCatalog = catalog;
				String searchSchema = this.realSchemaName;
				if (!pkTable.getSchemaPartitions().isEmpty()) {
					// Locate partition with first prefix.
					final String prefix = (String) pkTable
							.getSchemaPartitions().iterator().next();
					String schemaName = (String) new InverseMap(this
							.getPartitions()).get(prefix);
					if (schemaName == null) // Should never happen.
						throw new BioMartError();
					if ("".equals(dmd.getSchemaTerm()))
						searchCatalog = schemaName;
					searchSchema = schemaName;
				}
				final ResultSet dbTblFKCols = dmd.getExportedKeys(
						searchCatalog, searchSchema, pkTable.getName());

				// Loop through the results. There will be one result row per
				// column per key, so we need to build up a set of key columns
				// in a map.
				// The map keys represent the column position within a key. Each
				// map value is a list of columns. In essence the map is a 2-D
				// representation of the foreign keys which refer to this PK,
				// with the keys of the map (Y-axis) representing the column
				// position in the FK, and the values of the map (X-axis)
				// representing each individual FK. In all cases, FK columns are
				// assumed to be in the same order as the PK columns. The map is
				// sorted by key column position.
				// An assumption is made that the query will return columns from
				// the FK in the same order as all other FKs, ie. all column 1s
				// will be returned before any 2s, and then all 2s will be
				// returned
				// in the same order as the 1s they are associated with, etc.
				final TreeMap dbFKs = new TreeMap();
				while (dbTblFKCols.next()) {
					final String fkTblName = dbTblFKCols
							.getString("FKTABLE_NAME");
					final String fkColName = dbTblFKCols
							.getString("FKCOLUMN_NAME");
					final Short fkColSeq = new Short(dbTblFKCols
							.getShort("KEY_SEQ"));
					// Note the column.
					if (!dbFKs.containsKey(fkColSeq))
						dbFKs.put(fkColSeq, new ArrayList());
					// In some dbs, FKs can be invalid, so we need to check
					// them.
					final Table fkTbl = (Table) this.getTables().get(fkTblName);
					if (fkTbl != null) {
						final Column fkCol = (Column) fkTbl.getColumns().get(
								fkColName);
						if (fkCol != null)
							((List) dbFKs.get(fkColSeq)).add(fkCol);
					}
				}
				dbTblFKCols.close();

				// Only construct FKs if we actually found any.
				if (!dbFKs.isEmpty()) {
					// Identify the sequence of the first column, which may be 0
					// or 1, depending on database implementation.
					final int firstColSeq = ((Short) dbFKs.firstKey())
							.intValue();

					// How many columns are in the PK?
					final int pkColCount = pkTable.getPrimaryKey().getColumns().length;

					// How many FKs do we have?
					final int fkCount = ((List) dbFKs.get(dbFKs.firstKey()))
							.size();

					// Loop through the FKs, and construct each one at a time.
					for (int j = 0; j < fkCount; j++) {
						// Set up an array to hold the FK columns.
						final Column[] candidateFKColumns = new Column[pkColCount];

						// For each FK column name, look up the actual column in
						// the table.
						for (final Iterator k = dbFKs.entrySet().iterator(); k
								.hasNext();) {
							final Map.Entry entry = (Map.Entry) k.next();
							final Short keySeq = (Short) entry.getKey();
							// Convert the db-specific column index to a
							// 0-indexed figure for the array of fk columns.
							final int fkColSeq = keySeq.intValue()
									- firstColSeq;
							candidateFKColumns[fkColSeq] = (Column) ((List) entry
									.getValue()).get(j);
						}

						// Create a template foreign key based around the set
						// of candidate columns we found.
						ForeignKey fk;
						try {
							fk = new ForeignKey(candidateFKColumns);
						} catch (final Throwable t) {
							throw new BioMartError(t);
						}
						final Table fkTable = fk.getTable();

						// If any FK already exists on the target table with the
						// same columns in the same order, then reuse it.
						boolean fkAlreadyExists = false;
						for (final Iterator f = fkTable.getForeignKeys()
								.iterator(); f.hasNext() && !fkAlreadyExists;) {
							final ForeignKey candidateFK = (ForeignKey) f
									.next();
							if (candidateFK.equals(fk)) {
								// Found one. Reuse it!
								fk = candidateFK;
								// Update the status to indicate that the FK is
								// backed by the database, if previously it was
								// handmade.
								if (fk.getStatus().equals(
										ComponentStatus.HANDMADE))
									fk.setStatus(ComponentStatus.INFERRED);
								// Remove the FK from the list to be dropped
								// later, as it definitely exists now.
								fksToBeDropped.remove(candidateFK);
								// Flag the key as existing.
								fkAlreadyExists = true;
							}
						}

						// Has the key been reused, or is it a new one?
						if (!fkAlreadyExists)
							try {
								fkTable.getForeignKeys().add(fk);
							} catch (final Throwable t) {
								throw new BioMartError(t);
							}

						// Work out whether the relation from the FK to
						// the PK should be 1:M or 1:1. The rule is that
						// it will be 1:M in all cases except where the
						// FK table has a PK with identical columns to
						// the FK, in which case it is 1:1, as the FK
						// is unique.
						Cardinality card = Cardinality.MANY_A;
						final PrimaryKey fkPK = fkTable.getPrimaryKey();
						if (fkPK != null
								&& fk.getColumns().equals(fkPK.getColumns()))
							card = Cardinality.ONE;

						// Check to see if it already has a relation.
						boolean relationExists = false;
						for (final Iterator f = fk.getRelations().iterator(); f
								.hasNext();) {
							// Obtain the next relation.
							final Relation candidateRel = (Relation) f.next();

							// a) a relation already exists between the FK
							// and the PK.
							if (candidateRel.getOtherKey(fk).equals(pk)) {
								// If cardinality matches, make it
								// inferred. If doesn't match, make it
								// modified and update original cardinality.
								try {
									if (card.equals(candidateRel
											.getCardinality())) {
										if (!candidateRel
												.getStatus()
												.equals(
														ComponentStatus.INFERRED_INCORRECT))
											candidateRel
													.setStatus(ComponentStatus.INFERRED);
									} else {
										if (!candidateRel
												.getStatus()
												.equals(
														ComponentStatus.INFERRED_INCORRECT))
											candidateRel
													.setStatus(ComponentStatus.MODIFIED);
										candidateRel
												.setOriginalCardinality(card);
									}
								} catch (final AssociationException ae) {
									throw new BioMartError(ae);
								}
								// Don't drop it at the end of the loop.
								relationsToBeDropped.remove(candidateRel);
								// Say we've found it.
								relationExists = true;
							}

							// b) a handmade relation exists elsewhere which
							// should not be dropped. All other relations
							// elsewhere will be dropped.
							else if (candidateRel.getStatus().equals(
									ComponentStatus.HANDMADE))
								// Don't drop it at the end of the loop.
								relationsToBeDropped.remove(candidateRel);
						}

						// If relation did not already exist, create it.
						if (!relationExists) {
							// Establish the relation.
							try {
								final Relation rel = new Relation(pk, fk, card);
								pk.getRelations().add(rel);
								fk.getRelations().add(rel);
							} catch (final Throwable t) {
								throw new BioMartError(t);
							}
						}
					}
				}

				// Remove any relations that we didn't find in the database (but
				// leave the handmade ones behind).
				for (final Iterator j = relationsToBeDropped.iterator(); j
						.hasNext();) {
					final Relation r = (Relation) j.next();
					if (r.getStatus().equals(ComponentStatus.HANDMADE))
						continue;
					r.getFirstKey().getRelations().remove(r);
					r.getSecondKey().getRelations().remove(r);
				}
			}
		}

		/**
		 * This method implements the key-guessing algorithm for foreign keys.
		 * Basically, it iterates through all known primary keys, and looks for
		 * sets of matching columns in other tables, either with the same names
		 * or with '_key' appended. Any matching sets found are assumed to be
		 * foreign keys with relations to the current primary key.
		 * <p>
		 * Relations are 1:M, except when the table at the FK end has a PK with
		 * identical column to the FK. In this case, the FK is forced to be
		 * unique, which implies that it can only partake in a 1:1 relation, so
		 * the relation is marked as such.
		 * 
		 * @param fksToBeDropped
		 *            the list of foreign keys to update as we go along. By the
		 *            end of the method, the only keys left in this list should
		 *            be ones that no longer exist in the database and may be
		 *            dropped.
		 * @param stepSize
		 *            the progress step size to increment by.
		 * @throws SQLException
		 *             if there was a problem talking to the database.
		 * @throws DataModelException
		 *             if there was a logical problem during construction of the
		 *             set of foreign keys.
		 */
		private void synchroniseKeysUsingKeyGuessing(
				final Collection fksToBeDropped, final double stepSize)
				throws SQLException, DataModelException {
			Log.debug("Running non-DMD key synchronisation");
			// Loop through all the tables in the database, which is the same
			// as looping through all the primary keys.
			Log.debug("Finding tables");
			for (final Iterator i = this.getTables().values().iterator(); i
					.hasNext();) {
				// Update progress;
				this.progress += stepSize;

				// Obtain the table and its primary key.
				final Table pkTable = (Table) i.next();
				final PrimaryKey pk = pkTable.getPrimaryKey();
				// Skip all tables which have no primary key.
				if (pk == null)
					continue;

				Log.debug("Processing primary key " + pk);

				// If an FK exists on the PK table with the same columns as the
				// PK, then we cannot use this PK to make relations to other
				// tables.
				// This is because the FK shows that this table is not the
				// original source of the data in those columns. Some other
				// table is the original source, so we assume that relations
				// will have been established from that other table instead. So,
				// we skip this table.
				boolean pkIsAlsoAnFK = false;
				for (final Iterator j = pkTable.getForeignKeys().iterator(); j
						.hasNext()
						&& !pkIsAlsoAnFK;) {
					final Key fk = (Key) j.next();
					if (fk.getColumns().equals(pk.getColumns()))
						pkIsAlsoAnFK = true;
				}
				if (pkIsAlsoAnFK)
					continue;

				// To maintain some degree of sanity here, we assume that a PK
				// is the original source of data (and not a copy of data
				// sourced from some other table) if the first column in the PK
				// has the same name as the table it is in, or with '_id'
				// appended, or is just 'id' on its own. Any PK which does not
				// have this property is skipped.
				final Column firstPKCol = pk.getColumns()[0];
				String firstPKColName = firstPKCol.getName();
				int idPrefixIndex = firstPKColName.indexOf(Resources
						.get("primaryKeySuffix"));
				//then try uppercase, in Oracle, names are uppercase
				if(idPrefixIndex<0) 
					idPrefixIndex = firstPKColName.toUpperCase().indexOf(Resources.get("primaryKeySuffix").toUpperCase());
				if (idPrefixIndex >= 0)
					firstPKColName = firstPKColName.substring(0, idPrefixIndex);
				if (!firstPKColName.equals(pkTable.getName())
						&& !firstPKColName.equals(Resources.get("idCol")))
					continue;

				// Make a list of relations that already exist in this schema,
				// from some previous run. Any relations that are left in this
				// list by the end of the loop for this table no longer exist in
				// the database, and will be dropped.
				final Collection relationsToBeDropped = new HashSet(pk
						.getRelations());

				// Now we know that we can use this PK for certain, look for all
				// other tables (other than the one the PK itself belongs to),
				// for sets of columns with identical names, or with '_key'
				// appended. Any set that we find is going to be an FK with a
				// relation back to this PK.
				Log.debug("Searching for possible referring foreign keys");
				for (final Iterator l = this.getTables().values().iterator(); l
						.hasNext();) {
					// Obtain the next table to look at.
					final Table fkTable = (Table) l.next();

					// Make sure the table is not the same as the PK table.
					if (fkTable.equals(pkTable))
						continue;

					// Set up an empty list for the matching columns.
					final Column[] candidateFKColumns = new Column[pk
							.getColumns().length];
					int matchingColumnCount = 0;

					// Iterate through the PK columns and find a column in the
					// target FK table with the same name, or with '_key'
					// appended,
					// or with the PK table name and an underscore prepended.
					// If found, add that target column to the candidate FK
					// column
					// set.
					for (int columnIndex = 0; columnIndex < pk.getColumns().length; columnIndex++) {
						final String pkColumnName = pk.getColumns()[columnIndex]
								.getName();
						// Start out by assuming no match.
						Column candidateFKColumn = null;
						// Don't try to find 'id' or 'id_key' columns as that
						// would be silly and would probably match far too much.
						if (!pkColumnName.equals(Resources.get("idCol"))) {
							// Try equivalent name first.
							candidateFKColumn = (Column) fkTable.getColumns()
									.get(pkColumnName);
							// Then try with '_key' appended, if not found.
							if (candidateFKColumn == null)
								candidateFKColumn = (Column) fkTable
										.getColumns()
										.get(
												pkColumnName
														+ Resources
																.get("foreignKeySuffix"));
						}
						// Then try with PK tablename+'_' prepended, if not
						// found.
						if (candidateFKColumn == null)
							candidateFKColumn = (Column) fkTable
									.getColumns()
									.get(pkTable.getName() + "_" + pkColumnName);
						// Found it? Add it to the candidate list.
						if (candidateFKColumn != null) {
							candidateFKColumns[columnIndex] = candidateFKColumn;
							matchingColumnCount++;
						}
					}

					// We found a matching set, so create a FK on it!
					if (matchingColumnCount == pk.getColumns().length) {
						// Create a template foreign key based around the set
						// of candidate columns we found.
						ForeignKey fk;
						try {
							fk = new ForeignKey(candidateFKColumns);
						} catch (final Throwable t) {
							throw new BioMartError(t);
						}

						// If any FK already exists on the target table with the
						// same columns in the same order, then reuse it.
						boolean fkAlreadyExists = false;
						for (final Iterator f = fkTable.getForeignKeys()
								.iterator(); f.hasNext() && !fkAlreadyExists;) {
							final ForeignKey candidateFK = (ForeignKey) f
									.next();
							if (candidateFK.equals(fk)) {
								// Found one. Reuse it!
								fk = candidateFK;
								// Update the status to indicate that the FK is
								// backed by the database, if previously it was
								// handmade.
								if (fk.getStatus().equals(
										ComponentStatus.HANDMADE))
									fk.setStatus(ComponentStatus.INFERRED);
								// Remove the FK from the list to be dropped
								// later, as it definitely exists now.
								fksToBeDropped.remove(candidateFK);
								// Flag the key as existing.
								fkAlreadyExists = true;
							}
						}

						// Has the key been reused, or is it a new one?
						if (!fkAlreadyExists)
							try {
								fkTable.getForeignKeys().add(fk);
							} catch (final Throwable t) {
								throw new BioMartError(t);
							}

						// Work out whether the relation from the FK to
						// the PK should be 1:M or 1:1. The rule is that
						// it will be 1:M in all cases except where the
						// FK table has a PK with identical columns to
						// the FK, in which case it is 1:1, as the FK
						// is unique.
						Cardinality card = Cardinality.MANY_A;
						final PrimaryKey fkPK = fkTable.getPrimaryKey();
						if (fkPK != null
								&& fk.getColumns().equals(fkPK.getColumns()))
							card = Cardinality.ONE;

						// Check to see if it already has a relation.
						boolean relationExists = false;
						for (final Iterator f = fk.getRelations().iterator(); f
								.hasNext();) {
							// Obtain the next relation.
							final Relation candidateRel = (Relation) f.next();

							// a) a relation already exists between the FK
							// and the PK.
							if (candidateRel.getOtherKey(fk).equals(pk)) {
								// If cardinality matches, make it
								// inferred. If doesn't match, make it
								// modified and update original cardinality.
								try {
									if (card.equals(candidateRel
											.getCardinality())) {
										if (!candidateRel
												.getStatus()
												.equals(
														ComponentStatus.INFERRED_INCORRECT))
											candidateRel
													.setStatus(ComponentStatus.INFERRED);
									} else {
										if (!candidateRel
												.getStatus()
												.equals(
														ComponentStatus.INFERRED_INCORRECT))
											candidateRel
													.setStatus(ComponentStatus.MODIFIED);
										candidateRel
												.setOriginalCardinality(card);
									}
								} catch (final AssociationException ae) {
									throw new BioMartError(ae);
								}
								// Don't drop it at the end of the loop.
								relationsToBeDropped.remove(candidateRel);
								// Say we've found it.
								relationExists = true;
							}

							// b) a handmade relation exists elsewhere which
							// should not be dropped. All other relations
							// elsewhere will be dropped.
							else if (candidateRel.getStatus().equals(
									ComponentStatus.HANDMADE))
								// Don't drop it at the end of the loop.
								relationsToBeDropped.remove(candidateRel);
						}

						// If relation did not already exist, create it.
						if (!relationExists) {
							// Establish the relation.
							try {
								final Relation rel = new Relation(pk, fk, card);
								pk.getRelations().add(rel);
								fk.getRelations().add(rel);
							} catch (final Throwable t) {
								throw new BioMartError(t);
							}
						}
					}
				}

				// Remove any relations that we didn't find in the database (but
				// leave the handmade ones behind).
				for (final Iterator j = relationsToBeDropped.iterator(); j
						.hasNext();) {
					final Relation r = (Relation) j.next();
					if (r.getStatus().equals(ComponentStatus.HANDMADE))
						continue;
					r.getFirstKey().getRelations().remove(r);
					r.getSecondKey().getRelations().remove(r);
				}
			}
		}
	}
}
