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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.biomart.builder.exceptions.PartitionException;
import org.biomart.builder.exceptions.ValidationException;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.DataSet.DataSetColumn.UnrolledColumn;
import org.biomart.builder.model.DataSet.DataSetColumn.WrappedColumn;
import org.biomart.builder.model.Key.ForeignKey;
import org.biomart.builder.model.Key.PrimaryKey;
import org.biomart.builder.model.PartitionTable.PartitionTableApplication;
import org.biomart.builder.model.Relation.Cardinality;
import org.biomart.builder.model.Relation.UnrolledRelationDefinition;
import org.biomart.common.exceptions.AssociationException;
import org.biomart.common.exceptions.DataModelException;
import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.biomart.common.utils.BeanMap;
import org.biomart.common.utils.Transaction;
import org.biomart.common.utils.WeakPropertyChangeSupport;
import org.biomart.common.utils.Transaction.TransactionEvent;
import org.biomart.common.utils.Transaction.TransactionListener;

/**
 * The mart contains the set of all schemas that are providing data to this
 * mart. It also has zero or more datasets based around these.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.90 $, $Date: 2008-03-03 12:16:15 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class Mart implements TransactionListener {
	private static final long serialVersionUID = 1L;

	/**
	 * Subclasses use this field to fire events of their own.
	 */
	protected final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
			this);

	private final BeanMap datasets;

	private final BeanMap schemas;

	private String outputDatabase = null;

	private String outputSchema = null;

	private String outputHost = null;

	private String outputPort = null;

	private String overrideHost = null;

	private String overridePort = null;

	private boolean directModified = false;

	private boolean hideMaskedDataSets = false;

	private boolean hideMaskedSchemas = false;

	/**
	 * Constant referring to table and column name conversion.
	 */
	public static final int USE_MIXED_CASE = 0;

	/**
	 * Constant referring to table and column name conversion.
	 */
	public static final int USE_UPPER_CASE = 1;

	/**
	 * Constant referring to table and column name conversion.
	 */
	public static final int USE_LOWER_CASE = 2;

	private int nameCase = Mart.USE_MIXED_CASE;

	// For use in hash code and equals to prevent dups in prop change.
	private static int ID_SERIES = 0;

	private final int uniqueID = Mart.ID_SERIES++;

	private Collection schemaCache;

	private Collection datasetCache;

	// All changes to us make us modified.
	private final PropertyChangeListener listener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			Mart.this.setDirectModified(true);
		}
	};

	private final PropertyChangeListener schemaCacheBuilder = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			final Collection newSchs = new HashSet(Mart.this.schemas.values());
			if (!newSchs.equals(Mart.this.schemaCache)) {
				Mart.this.setDirectModified(true);
				// Identify dropped ones.
				final Collection dropped = new HashSet(Mart.this.schemaCache);
				dropped.removeAll(newSchs);
				// Identify new ones.
				newSchs.removeAll(Mart.this.schemaCache);
				// Drop dropped ones.
				for (final Iterator i = dropped.iterator(); i.hasNext();)
					Mart.this.schemaCache.remove(i.next());
				// Add added ones.
				for (final Iterator i = newSchs.iterator(); i.hasNext();) {
					final Schema sch = (Schema) i.next();
					Mart.this.schemaCache.add(sch);
					sch.addPropertyChangeListener("directModified",
							Mart.this.listener);
					sch.addPropertyChangeListener("hideMasked",
							Mart.this.listener);
				}
			}
		}
	};

	private final PropertyChangeListener datasetCacheBuilder = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			final Collection newDss = new HashSet(Mart.this.datasets.values());
			if (!newDss.equals(Mart.this.datasetCache)) {
				Mart.this.setDirectModified(true);
				// Identify dropped ones.
				final Collection dropped = new HashSet(Mart.this.datasetCache);
				dropped.removeAll(newDss);
				// Identify new ones.
				newDss.removeAll(Mart.this.datasetCache);
				// Drop dropped ones.
				for (final Iterator i = dropped.iterator(); i.hasNext();) {
					final DataSet deadDS = (DataSet) i.next();
					try {
						deadDS.setPartitionTable(false);
					} catch (final PartitionException pe) {
						// Ignore.
					}
					// Also remove all related mods in rels and tbls.
					for (final Iterator j = Mart.this.schemas.values()
							.iterator(); j.hasNext();) {
						final Schema sch = (Schema) j.next();
						for (final Iterator k = sch.getTables().values()
								.iterator(); k.hasNext();)
							((Table) k.next()).dropMods(deadDS, null);
						for (final Iterator k = sch.getRelations().iterator(); k
								.hasNext();)
							((Relation) k.next()).dropMods(deadDS, null);
					}
					// Remove all partition table applications.
					PartitionTableApplication pta = deadDS
							.getPartitionTableApplication();
					if (pta != null)
						pta.getPartitionTable().removeFrom(deadDS, null);
					for (final Iterator j = deadDS.getTables().values()
							.iterator(); j.hasNext();) {
						final DataSetTable dsTable = (DataSetTable) j.next();
						pta = dsTable.getPartitionTableApplication();
						if (pta != null)
							pta.getPartitionTable().removeFrom(deadDS,
									dsTable.getName());
					}
					// Remove from cache.
					Mart.this.datasetCache.remove(deadDS);
				}
				// Add added ones.
				for (final Iterator i = newDss.iterator(); i.hasNext();) {
					final DataSet ds = (DataSet) i.next();
					Mart.this.datasetCache.add(ds);
					ds.addPropertyChangeListener("directModified",
							Mart.this.listener);
					ds.addPropertyChangeListener("hideMasked",
							Mart.this.listener);
				}
			}
		}
	};

	/**
	 * Construct a new, empty, mart.
	 */
	public Mart() {
		Log.debug("Creating new mart");
		this.datasets = new BeanMap(new TreeMap());
		this.schemas = new BeanMap(new TreeMap());

		Transaction.addTransactionListener(this);

		this.addPropertyChangeListener("case", this.listener);
		this.addPropertyChangeListener("outputHost", this.listener);
		this.addPropertyChangeListener("outputPort", this.listener);
		this.addPropertyChangeListener("outputSchema", this.listener);
		this.addPropertyChangeListener("overrideHost", this.listener);
		this.addPropertyChangeListener("hideMaskedSchemas", this.listener);
		this.addPropertyChangeListener("hideMaskedDataSets", this.listener);

		// Listeners on schema and dataset additions to spot
		// and handle renames.
		this.schemaCache = new HashSet();
		this.schemas.addPropertyChangeListener(this.schemaCacheBuilder);
		this.datasetCache = new HashSet();
		this.datasets.addPropertyChangeListener(this.datasetCacheBuilder);
	}

	/**
	 * Obtain the next unique ID to use for a schema.
	 * 
	 * @return the next ID.
	 */
	public int getNextUniqueId() {
		int x = 0;
		for (final Iterator i = this.schemaCache.iterator(); i.hasNext();)
			x = Math.max(x, ((Schema) i.next()).getUniqueId());
		return x + 1;
	}

	/**
	 * Obtain the unique series number for this mart.
	 * 
	 * @return the unique Id.
	 */
	public int getUniqueId() {
		return this.uniqueID;
	}

	public int hashCode() {
		return 0; // All marts go in one big bucket!
	}

	public boolean equals(final Object obj) {
		if (obj == this)
			return true;
		else if (obj == null)
			return false;
		else if (obj instanceof Mart)
			return this.uniqueID == ((Mart) obj).uniqueID;
		else
			return false;
	}

	/**
	 * Is this mart hiding masked datasets?
	 * 
	 * @param hideMaskedDataSets
	 *            true if it is.
	 */
	public void setHideMaskedDataSets(final boolean hideMaskedDataSets) {
		final boolean oldValue = this.hideMaskedDataSets;
		if (this.hideMaskedDataSets == hideMaskedDataSets)
			return;
		this.hideMaskedDataSets = hideMaskedDataSets;
		this.pcs.firePropertyChange("hideMaskedDataSets", oldValue,
				hideMaskedDataSets);
	}

	/**
	 * Is this mart hiding masked datasets?
	 * 
	 * @return true if it is.
	 */
	public boolean isHideMaskedDataSets() {
		return this.hideMaskedDataSets;
	}

	/**
	 * Is this mart hiding masked schemas?
	 * 
	 * @param hideMaskedSchemas
	 *            true if it is.
	 */
	public void setHideMaskedSchemas(final boolean hideMaskedSchemas) {
		final boolean oldValue = this.hideMaskedSchemas;
		if (this.hideMaskedSchemas == hideMaskedSchemas)
			return;
		this.hideMaskedSchemas = hideMaskedSchemas;
		this.pcs.firePropertyChange("hideMaskedSchemas", oldValue,
				hideMaskedSchemas);
	}

	/**
	 * Is this mart hiding masked schemas?
	 * 
	 * @return true if it is.
	 */
	public boolean isHideMaskedSchemas() {
		return this.hideMaskedSchemas;
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
		// Ignore, for now.
	}

	public void transactionResetVisibleModified() {
		// Ignore, for now.
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
	 * What case to use for table and column names? Mixed is default.
	 * 
	 * @return one of {@link #USE_LOWER_CASE}, {@link #USE_UPPER_CASE}, or
	 *         {@link #USE_MIXED_CASE}.
	 */
	public int getCase() {
		return this.nameCase;
	}

	/**
	 * What case to use for table and column names? Mixed is default.
	 * 
	 * @param nameCase
	 *            one of {@link #USE_LOWER_CASE}, {@link #USE_UPPER_CASE}, or
	 *            {@link #USE_MIXED_CASE}.
	 */
	public void setCase(final int nameCase) {
		Log.debug("Changing case for " + this + " to " + nameCase);
		final int oldValue = this.nameCase;
		if (this.nameCase == nameCase)
			return;
		// Make the change.
		this.nameCase = nameCase;
		this.pcs.firePropertyChange("nameCase", oldValue, nameCase);
	}

	/**
	 * Optional, sets the default target schema this mart will output dataset
	 * DDL to later.
	 * 
	 * @param outputSchema
	 *            the target schema.
	 */
	public void setOutputSchema(final String outputSchema) {
		Log.debug("Changing outputSchema for " + this + " to " + outputSchema);
		final String oldValue = this.outputSchema;
		if (this.outputSchema == outputSchema || this.outputSchema != null
				&& this.outputSchema.equals(outputSchema))
			return;
		// Make the change.
		this.outputSchema = outputSchema;
		this.pcs.firePropertyChange("outputSchema", oldValue, outputSchema);
	}

	/**
	 * Optional, gets the default target schema this mart will output dataset
	 * DDL to later.
	 * 
	 * @return the target schema.
	 */
	public String getOutputSchema() {
		return this.outputSchema;
	}

	/**
	 * Optional, sets the default target database this mart will output dataset
	 * DDL to later.
	 * 
	 * @param outputDatabase
	 *            the target database.
	 */
	public void setOutputDatabase(final String outputDatabase) {
		Log.debug("Changing outputDatabase for " + this + " to "
				+ outputDatabase);
		final String oldValue = this.outputDatabase;
		if (this.outputDatabase == outputDatabase
				|| this.outputDatabase != null
				&& this.outputDatabase.equals(outputDatabase))
			return;
		// Make the change.
		this.outputDatabase = outputDatabase;
		this.pcs.firePropertyChange("outputDatabase", oldValue, outputDatabase);
	}

	/**
	 * Optional, gets the default target database this mart will output dataset
	 * DDL to later.
	 * 
	 * @return the target schema.
	 */
	public String getOutputDatabase() {
		return this.outputDatabase;
	}

	/**
	 * Optional, sets the default target host this mart will output dataset DDL
	 * to later.
	 * 
	 * @param outputHost
	 *            the target host.
	 */
	public void setOutputHost(final String outputHost) {
		Log.debug("Changing outputHost for " + this + " to " + outputHost);
		final String oldValue = this.outputHost;
		if (this.outputHost == outputHost || this.outputHost != null
				&& this.outputHost.equals(outputHost))
			return;
		// Make the change.
		this.outputHost = outputHost;
		this.pcs.firePropertyChange("outputHost", oldValue, outputHost);
	}

	/**
	 * Optional, gets the default target host this mart will output dataset DDL
	 * to later.
	 * 
	 * @return the target host.
	 */
	public String getOutputHost() {
		return this.outputHost;
	}

	/**
	 * Optional, sets the default target port this mart will output dataset DDL
	 * to later.
	 * 
	 * @param outputPort
	 *            the target port.
	 */
	public void setOutputPort(final String outputPort) {
		Log.debug("Changing outputPort for " + this + " to " + outputPort);
		final String oldValue = this.outputPort;
		if (this.outputPort == outputPort || this.outputPort != null
				&& this.outputPort.equals(outputPort))
			return;
		// Make the change.
		this.outputPort = outputPort;
		this.pcs.firePropertyChange("outputPort", oldValue, outputPort);
	}

	/**
	 * Optional, gets the default target port this mart will output dataset DDL
	 * to later.
	 * 
	 * @return the target port.
	 */
	public String getOutputPort() {
		return this.outputPort;
	}

	/**
	 * Optional, sets the default target JDBC host this mart will output dataset
	 * DDL to later.
	 * 
	 * @param overrideHost
	 *            the target host.
	 */
	public void setOverrideHost(final String overrideHost) {
		Log.debug("Changing overrideHost for " + this + " to " + overrideHost);
		final String oldValue = this.overrideHost;
		if (this.overrideHost == overrideHost || this.overrideHost != null
				&& this.overrideHost.equals(overrideHost))
			return;
		// Make the change.
		this.overrideHost = overrideHost;
		this.pcs.firePropertyChange("overrideHost", oldValue, overrideHost);
	}

	/**
	 * Optional, gets the default target JDBC host this mart will output dataset
	 * DDL to later.
	 * 
	 * @return the target host.
	 */
	public String getOverrideHost() {
		return this.overrideHost;
	}

	/**
	 * Optional, sets the default target JDBC port this mart will output dataset
	 * DDL to later.
	 * 
	 * @param overridePort
	 *            the target port.
	 */
	public void setOverridePort(final String overridePort) {
		Log.debug("Changing overridePort for " + this + " to " + overridePort);
		final String oldValue = this.overridePort;
		if (this.overridePort == overridePort || this.overridePort != null
				&& this.overridePort.equals(overridePort))
			return;
		// Make the change.
		this.overridePort = overridePort;
		this.pcs.firePropertyChange("overridePort", oldValue, overridePort);
	}

	/**
	 * Optional, gets the default target JDBC port this mart will output dataset
	 * DDL to later.
	 * 
	 * @return the target port.
	 */
	public String getOverridePort() {
		return this.overridePort;
	}

	/**
	 * Returns the set of dataset objects which this mart includes. The set may
	 * be empty but it is never <tt>null</tt>.
	 * 
	 * @return a set of dataset objects. Keys are names, values are datasets.
	 */
	public BeanMap getDataSets() {
		return this.datasets;
	}

	/**
	 * Returns the set of partition column names which this mart includes. The
	 * set may be empty but it is never <tt>null</tt>.
	 * 
	 * @return a set of partition column names (as strings).
	 */
	public Collection getPartitionColumnNames() {
		final List colNames = new ArrayList();
		for (final Iterator i = this.getPartitionTables().iterator(); i
				.hasNext();) {
			final PartitionTable pt = (PartitionTable) i.next();
			for (final Iterator j = pt.getSelectedColumnNames().iterator(); j
					.hasNext();) {
				final String col = (String) j.next();
				if (!col.equals(PartitionTable.DIV_COLUMN))
					colNames.add(pt.getName() + "." + col);
			}
		}
		Collections.sort(colNames);
		return Collections.unmodifiableCollection(colNames);
	}

	/**
	 * Returns the set of partition table names which this mart includes. The
	 * set may be empty but it is never <tt>null</tt>.
	 * 
	 * @return a set of partition table names (as strings).
	 */
	public Collection getPartitionTables() {
		final List tbls = new ArrayList();
		for (final Iterator i = this.datasets.values().iterator(); i.hasNext();) {
			final DataSet ds = (DataSet) i.next();
			if (ds.isPartitionTable())
				tbls.add(ds.asPartitionTable());
		}
		Collections.sort(tbls);
		return Collections.unmodifiableCollection(tbls);
	}

	/**
	 * Returns the set of schema objects which this mart includes. The set may
	 * be empty but it is never <tt>null</tt>.
	 * 
	 * @return a set of schema objects. Keys are names, values are actual
	 *         schemas.
	 */
	public BeanMap getSchemas() {
		return this.schemas;
	}

	/**
	 * Given a set of tables, produce the minimal set of datasets which include
	 * all the specified tables. Tables can be included in the same dataset if
	 * they are linked by 1:M relations (1:M, 1:M in a chain), or if the table
	 * is the last in the chain and is linked to the previous table by a pair of
	 * 1:M and M:1 relations via a third table, simulating a M:M relation.
	 * <p>
	 * If the chains of tables fork, then one dataset is generated for each
	 * branch of the fork.
	 * <p>
	 * Every suggested dataset is synchronised before being returned.
	 * <p>
	 * Datasets will be named after their central tables. If a dataset with that
	 * name already exists, a '_' and sequence number will be appended to make
	 * the new dataset name unique.
	 * <p>
	 * See also
	 * {@link #continueSubclassing(Collection, Collection, DataSet, Table)}.
	 * 
	 * @param includeTables
	 *            the tables that must appear in the final set of datasets.
	 * @return the collection of datasets generated.
	 * @throws SQLException
	 *             if there is any problem talking to the source database whilst
	 *             generating the dataset.
	 * @throws DataModelException
	 *             if synchronisation fails.
	 */
	public Collection suggestDataSets(final Collection includeTables)
			throws SQLException, DataModelException {
		Log.debug("Suggesting datasets for " + includeTables);
		// The root tables are all those which do not have a M:1 relation
		// to another one of the initial set of tables. This means that
		// extra datasets will be created for each table at the end of
		// 1:M:1 relation, so that any further tables past it will still
		// be included.
		Log.debug("Finding root tables");
		final Collection rootTables = new HashSet(includeTables);
		for (final Iterator i = includeTables.iterator(); i.hasNext();) {
			final Table candidate = (Table) i.next();
			for (final Iterator j = candidate.getRelations().iterator(); j
					.hasNext();) {
				final Relation rel = (Relation) j.next();
				if (rel.getStatus().equals(ComponentStatus.INFERRED_INCORRECT))
					continue;
				if (!rel.isOneToMany())
					continue;
				if (!rel.getManyKey().getTable().equals(candidate))
					continue;
				if (includeTables.contains(rel.getOneKey().getTable()))
					rootTables.remove(candidate);
			}
		}
		// We construct one dataset per root table.
		final Set suggestedDataSets = new TreeSet();
		for (final Iterator i = rootTables.iterator(); i.hasNext();) {
			final Table rootTable = (Table) i.next();
			Log.debug("Constructing dataset for root table " + rootTable);
			final DataSet dataset;
			try {
				dataset = new DataSet(this, rootTable, rootTable.getName());
			} catch (final ValidationException e) {
				// Skip this one.
				continue;
			}
			this.datasets.put(dataset.getOriginalName(), dataset);
			// Process it.
			final Collection tablesIncluded = new HashSet();
			tablesIncluded.add(rootTable);
			Log.debug("Attempting to find subclass datasets");
			suggestedDataSets.addAll(this.continueSubclassing(includeTables,
					tablesIncluded, dataset, rootTable));
		}

		// Synchronise them all.
		Log.debug("Synchronising constructed datasets");
		for (final Iterator i = suggestedDataSets.iterator(); i.hasNext();)
			((DataSet) i.next()).synchronise();

		// Do any of the resulting datasets contain all the tables
		// exactly with subclass relations between each?
		// If so, just use that one dataset and forget the rest.
		Log.debug("Finding perfect candidate");
		DataSet perfectDS = null;
		for (final Iterator i = suggestedDataSets.iterator(); i.hasNext()
				&& perfectDS == null;) {
			final DataSet candidate = (DataSet) i.next();

			// A candidate is a perfect match if the set of tables
			// covered by the subclass relations is the same as the
			// original set of tables requested.
			final Collection scTables = new HashSet();
			for (final Iterator j = candidate.getIncludedRelations().iterator(); j
					.hasNext();) {
				final Relation r = (Relation) j.next();
				if (!r.isSubclassRelation(candidate))
					continue;
				scTables.add(r.getFirstKey().getTable());
				scTables.add(r.getSecondKey().getTable());
			}
			// Finally perform the check to see if we have them all.
			if (scTables.containsAll(includeTables))
				perfectDS = candidate;
		}
		if (perfectDS != null) {
			Log.debug("Perfect candidate found - dropping others");
			// Drop the others.
			for (final Iterator i = suggestedDataSets.iterator(); i.hasNext();) {
				final DataSet candidate = (DataSet) i.next();
				if (!candidate.equals(perfectDS)) {
					this.datasets.remove(candidate.getOriginalName());
					i.remove();
				}
			}
			// Rename it to lose any extension it may have gained.
			perfectDS.setName(perfectDS.getCentralTable().getName());
		} else
			Log.debug("No perfect candidate found - retaining all");

		// Return the final set of suggested datasets.
		return suggestedDataSets;
	}

	/**
	 * This internal method takes a bunch of tables that the user would like to
	 * see as subclass or main tables in a single dataset, and attempts to find
	 * a subclass path between them. For each subclass path it can build, it
	 * produces one dataset based on that path. Each path contains as many
	 * tables as possible. The paths do not overlap. If there is a choice, the
	 * one chosen is arbitrary.
	 * 
	 * @param includeTables
	 *            the tables we want to include as main or subclass tables.
	 * @param tablesIncluded
	 *            the tables we have managed to include in a path so far.
	 * @param dataset
	 *            the dataset we started out from which contains just the main
	 *            table on its own with no subclassing.
	 * @param table
	 *            the real table we are looking at to see if there is a subclass
	 *            path between any of the include tables and any of the existing
	 *            subclassed or main tables via this real table.
	 * @return the datasets we have created - one per subclass path, or if there
	 *         were none, then a singleton collection containing the dataset
	 *         originally passed in.
	 */
	private Collection continueSubclassing(final Collection includeTables,
			final Collection tablesIncluded, final DataSet dataset,
			final Table table) {
		// Check table has a primary key.
		final Key pk = table.getPrimaryKey();

		// Make a unique set to hold all the resulting datasets. It
		// is initially empty.
		final Collection suggestedDataSets = new HashSet();
		// Make a set to contain relations to subclass.
		final Collection subclassedRelations = new HashSet();
		// Make a map to hold tables included for each relation.
		final Map relationTablesIncluded = new HashMap();
		// Make a list to hold all tables included at this level.
		final Collection localTablesIncluded = new HashSet(tablesIncluded);

		// Find all 1:M relations starting from the given table that point
		// to another interesting table.
		if (pk != null)
			for (final Iterator i = pk.getRelations().iterator(); i.hasNext();) {
				final Relation r = (Relation) i.next();
				if (!r.isOneToMany())
					continue;
				else if (r.getStatus().equals(
						ComponentStatus.INFERRED_INCORRECT))
					continue;

				// For each relation, if it points to another included
				// table via 1:M we should subclass the relation.
				final Table target = r.getManyKey().getTable();
				if (includeTables.contains(target)
						&& !localTablesIncluded.contains(target)) {
					subclassedRelations.add(r);
					final Collection newRelationTablesIncluded = new HashSet(
							tablesIncluded);
					relationTablesIncluded.put(r, newRelationTablesIncluded);
					newRelationTablesIncluded.add(target);
					localTablesIncluded.add(target);
				}
			}

		// Find all 1:M:1 relations starting from the given table that point
		// to another interesting table.
		if (pk != null)
			for (final Iterator i = pk.getRelations().iterator(); i.hasNext();) {
				final Relation firstRel = (Relation) i.next();
				if (!firstRel.isOneToMany())
					continue;
				else if (firstRel.getStatus().equals(
						ComponentStatus.INFERRED_INCORRECT))
					continue;

				final Table intermediate = firstRel.getManyKey().getTable();
				for (final Iterator j = intermediate.getForeignKeys()
						.iterator(); j.hasNext();) {
					final Key fk = (Key) j.next();
					if (fk.getStatus().equals(
							ComponentStatus.INFERRED_INCORRECT))
						continue;
					for (final Iterator k = fk.getRelations().iterator(); k
							.hasNext();) {
						final Relation secondRel = (Relation) k.next();
						if (secondRel.equals(firstRel))
							continue;
						else if (!secondRel.isOneToMany())
							continue;
						else if (secondRel.getStatus().equals(
								ComponentStatus.INFERRED_INCORRECT))
							continue;
						// For each relation, if it points to another included
						// table via M:1 we should subclass the relation.
						final Table target = secondRel.getOneKey().getTable();
						if (includeTables.contains(target)
								&& !localTablesIncluded.contains(target)) {
							subclassedRelations.add(firstRel);
							final Collection newRelationTablesIncluded = new HashSet(
									tablesIncluded);
							relationTablesIncluded.put(firstRel,
									newRelationTablesIncluded);
							newRelationTablesIncluded.add(target);
							localTablesIncluded.add(target);
						}
					}
				}
			}

		// No subclassing? Return a singleton.
		if (subclassedRelations.isEmpty())
			return Collections.singleton(dataset);

		// Iterate through the relations we found and recurse.
		// If not the last one, we copy the original dataset and
		// work on the copy, otherwise we work on the original.
		for (final Iterator i = subclassedRelations.iterator(); i.hasNext();) {
			final Relation r = (Relation) i.next();
			DataSet suggestedDataSet = dataset;
			try {
				if (i.hasNext()) {
					suggestedDataSet = new DataSet(this, dataset
							.getCentralTable(), dataset.getCentralTable()
							.getName());
					this.datasets.put(suggestedDataSet.getOriginalName(),
							suggestedDataSet);
					// Copy subclassed relations from existing dataset.
					for (final Iterator j = dataset.getIncludedRelations()
							.iterator(); j.hasNext();)
						((Relation) j.next()).setSubclassRelation(
								suggestedDataSet, true);
				}
				r.setSubclassRelation(suggestedDataSet, true);
			} catch (final ValidationException e) {
				// Not valid? OK, ignore this one.
				continue;
			}
			suggestedDataSets.addAll(this.continueSubclassing(includeTables,
					(Collection) relationTablesIncluded.get(r),
					suggestedDataSet, r.getManyKey().getTable()));
		}

		// Return the resulting datasets.
		return suggestedDataSets;
	}

	/**
	 * Given a dataset and a set of columns from one table upon which a table of
	 * that dataset is based, find all other tables which have similar columns,
	 * and create a new dataset for each one.
	 * <p>
	 * This method will not create datasets around tables which have already
	 * been used as the underlying table in any dataset table in the existing
	 * dataset. Neither will it create a dataset around the table from which the
	 * original columns came.
	 * <p>
	 * There may be no datasets resulting from this, if the columns do not
	 * appear elsewhere.
	 * <p>
	 * Datasets are synchronised before being returned.
	 * <p>
	 * Datasets will be named after their central tables. If a dataset with that
	 * name already exists, a '_' and sequence number will be appended to make
	 * the new dataset name unique.
	 * 
	 * @param dataset
	 *            the dataset the columns were selected from.
	 * @param columns
	 *            the columns to search across.
	 * @return the resulting set of datasets.
	 * @throws SQLException
	 *             if there is any problem talking to the source database whilst
	 *             generating the dataset.
	 * @throws DataModelException
	 *             if synchronisation fails.
	 */
	public Collection suggestInvisibleDataSets(final DataSet dataset,
			final Collection columns) throws SQLException, DataModelException {
		Log.debug("Suggesting invisible datasets for " + dataset + " columns "
				+ columns);
		final Collection invisibleDataSets = new HashSet();
		final Table sourceTable = ((Column) columns.iterator().next())
				.getTable();
		// Find all tables which mention the columns specified.
		Log.debug("Finding candidate tables");
		final Collection candidates = new HashSet();
		for (final Iterator i = this.schemas.values().iterator(); i.hasNext();)
			for (final Iterator j = ((Schema) i.next()).getTables().values()
					.iterator(); j.hasNext();) {
				final Table table = (Table) j.next();
				int matchingColCount = 0;
				for (final Iterator k = columns.iterator(); k.hasNext();) {
					final Column col = (Column) k.next();
					if (table.getColumns().containsKey(col.getName())
							|| table
									.getColumns()
									.containsKey(
											col.getName()
													+ Resources
															.get("foreignKeySuffix")))
						matchingColCount++;
				}
				if (matchingColCount == columns.size())
					candidates.add(table);
			}
		// Remove from the found tables all those which are already
		// used, and the one from which the original columns came.
		Log.debug("Removing candidates that are already used in this dataset");
		candidates.remove(sourceTable);
		for (final Iterator i = dataset.getTables().values().iterator(); i
				.hasNext();)
			candidates.remove(((DataSetTable) i.next()).getFocusTable());
		// Generate the dataset for each.
		Log.debug("Creating datasets for remaining candidates");
		for (final Iterator i = candidates.iterator(); i.hasNext();) {
			final Table table = (Table) i.next();
			final DataSet inv;
			try {
				inv = new DataSet(this, table, table.getName());
			} catch (final ValidationException e) {
				// Skip this one.
				continue;
			}
			this.datasets.put(inv.getOriginalName(), inv);
			invisibleDataSets.add(inv);
		}
		// Synchronise them all and make them all invisible.
		Log.debug("Synchronising suggested datasets");
		for (final Iterator i = invisibleDataSets.iterator(); i.hasNext();) {
			final DataSet ds = (DataSet) i.next();
			ds.setInvisible(true);
			ds.synchronise();
		}
		// Return the results.
		return invisibleDataSets;
	}

	/**
	 * Given a pair of tables, construct an unrolled dataset with all defaults
	 * in place for a useful ontology structure.
	 * <p>
	 * Datasets are synchronised before being returned. It will always return
	 * exactly one dataset.
	 * 
	 * @param nTable
	 *            the vocabulary definition table.
	 * @param nIDCol
	 *            the unique ID column for each vocab term.
	 * @param nNamingCol
	 *            the human readable version of each vocab term.
	 * @param nrTable
	 *            the relationship table.
	 * @param nrParentIDCol
	 *            the ID of the parent term in the relationship.
	 * @param nrChildIDCol
	 *            the ID of the child term in the relationship.
	 * @param reversed
	 *            <tt>true</tt> if the unrolling goes in the opposite sense of
	 *            the data in the table, e.g. the table goes parent to child but
	 *            we want to unroll child to parent.
	 * @return the resulting dataset.
	 * @throws SQLException
	 *             if there is any problem talking to the source database whilst
	 *             generating the dataset.
	 * @throws AssociationException
	 *             if some logic problem occurs.
	 * @throws ValidationException
	 *             if some logic problem occurs.
	 * @throws DataModelException
	 *             if synchronisation fails.
	 */
	public DataSet suggestUnrolledDataSets(final Table nTable,
			final Column nIDCol, final Column nNamingCol, final Table nrTable,
			final Column nrParentIDCol, final Column nrChildIDCol,
			final boolean reversed) throws SQLException, DataModelException,
			AssociationException, ValidationException {
		// Create PK on nTable.nIDCol (or reuse).
		PrimaryKey pk = new PrimaryKey(new Column[] { nIDCol });
		nTable.setPrimaryKey(pk);
		pk = nTable.getPrimaryKey();
		pk.setStatus(ComponentStatus.HANDMADE);
		pk.getTable().setMasked(false);
		// Create FKs on nrTable.nrParent/ChildIDCol (or reuse).
		ForeignKey parentFk = new ForeignKey(new Column[] { nrParentIDCol });
		ForeignKey childFk = new ForeignKey(new Column[] { nrChildIDCol });
		if (!nrTable.getForeignKeys().add(parentFk)) {
			// Reuse.
			ForeignKey reuse = null;
			for (final Iterator i = nrTable.getForeignKeys().iterator(); i
					.hasNext()
					&& reuse == null;) {
				final ForeignKey cand = (ForeignKey) i.next();
				if (cand.equals(parentFk))
					reuse = cand;
			}
			parentFk = reuse;
		}
		parentFk.setStatus(ComponentStatus.HANDMADE);
		parentFk.getTable().setMasked(false);
		if (!nrTable.getForeignKeys().add(childFk)) {
			// Reuse.
			ForeignKey reuse = null;
			for (final Iterator i = nrTable.getForeignKeys().iterator(); i
					.hasNext()
					&& reuse == null;) {
				final ForeignKey cand = (ForeignKey) i.next();
				if (cand.equals(childFk))
					reuse = cand;
			}
			childFk = reuse;
		}
		childFk.setStatus(ComponentStatus.HANDMADE);
		childFk.getTable().setMasked(false);
		// Create or reuse relations between PK and each FK.
		Relation parentRel = null;
		try {
			parentRel = new Relation(pk, parentFk, Cardinality.MANY_A);
			pk.getRelations().add(parentRel);
			parentFk.getRelations().add(parentRel);
		} catch (final AssociationException e) {
			// Reuse.
			Relation reuse = null;
			for (final Iterator i = pk.getRelations().iterator(); i.hasNext()
					&& reuse == null;) {
				final Relation cand = (Relation) i.next();
				if (cand.getFirstKey().equals(parentFk)
						&& cand.getSecondKey().equals(pk)
						|| cand.getFirstKey().equals(pk)
						&& cand.getSecondKey().equals(parentFk))
					reuse = cand;
			}
			parentRel = reuse;
		} finally {
			parentRel.setStatus(ComponentStatus.HANDMADE);
		}
		Relation childRel = null;
		try {
			childRel = new Relation(pk, childFk, Cardinality.MANY_A);
			pk.getRelations().add(childRel);
			childFk.getRelations().add(childRel);
		} catch (final AssociationException e) {
			// Reuse.
			Relation reuse = null;
			for (final Iterator i = pk.getRelations().iterator(); i.hasNext()
					&& reuse == null;) {
				final Relation cand = (Relation) i.next();
				if (cand.getFirstKey().equals(childFk)
						&& cand.getSecondKey().equals(pk)
						|| cand.getFirstKey().equals(pk)
						&& cand.getSecondKey().equals(childFk))
					reuse = cand;
			}
			childRel = reuse;
		} finally {
			childRel.setStatus(ComponentStatus.HANDMADE);
		}

		// Now swap parent and child if reversed.
		if (reversed) {
			final Relation otherRel = parentRel;
			parentRel = childRel;
			childRel = otherRel;
		}

		// Don't make dataset itself green.
		if (Transaction.getCurrentTransaction() != null)
			Transaction.getCurrentTransaction().setAllowVisModChange(false);

		// Create a simple dataset based on the selected table.
		final DataSet ds = new DataSet(this, nTable, nTable.getName());
		ds.synchronise(); // Must do now in order to locate dimensions.
		final DataSetTable mainTable = ds.getMainTable();
		// Locate the merge dimension based on parent rel and merge it.
		// Locate the unroll dimension based on child rel and unroll it
		for (final Iterator i = ds.getTables().values().iterator(); i.hasNext();) {
			final DataSetTable dst = (DataSetTable) i.next();
			if (dst.getFocusRelation() != null)
				if (dst.getFocusRelation().equals(parentRel))
					dst.getFocusRelation().setMergeRelation(ds, true);
				else if (dst.getFocusRelation().equals(childRel))
					dst.getFocusRelation()
							.setUnrolledRelation(
									ds,
									new UnrolledRelationDefinition(nNamingCol,
											reversed));
				else
					dst.setDimensionMasked(true);
		}
		ds.synchronise(); // Must do again to update dimensions.
		// Locate all unimportant relations and mask them.
		// Force the child rel.
		for (final Iterator i = mainTable.getIncludedRelations().iterator(); i
				.hasNext();) {
			final Relation cand = (Relation) i.next();
			if (cand.equals(parentRel))
				continue;
			else if (cand.equals(childRel))
				cand.setForceRelation(ds, mainTable.getName(), true);
			else
				cand.setMaskRelation(ds, mainTable.getName(), true);
		}
		ds.synchronise(); // Must do again to update relations.
		// Auto-mask all DS cols on main table which are not wrappers of
		// nIDCol or nNamingCol.
		for (final Iterator i = mainTable.getColumns().values().iterator(); i
				.hasNext();) {
			final DataSetColumn dsCol = (DataSetColumn) i.next();
			if (dsCol instanceof UnrolledColumn)
				continue;
			else if (dsCol instanceof WrappedColumn) {
				final WrappedColumn wcol = (WrappedColumn) dsCol;
				if (wcol.getWrappedColumn().equals(nIDCol))
					continue;
				else if (wcol.getWrappedColumn().equals(nNamingCol))
					continue;
			}
			dsCol.setColumnMasked(true);
		}
		// Locate all unimportant dimensions and mask them.
		for (final Iterator i = ds.getTables().values().iterator(); i.hasNext();) {
			final DataSetTable dst = (DataSetTable) i.next();
			if (dst.getFocusRelation() != null
					&& !dst.getFocusRelation().equals(parentRel)
					&& !dst.getFocusRelation().equals(childRel))
				dst.setDimensionMasked(true);
		}

		// All done!
		return ds;
	}
}
