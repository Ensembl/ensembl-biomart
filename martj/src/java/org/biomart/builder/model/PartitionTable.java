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
import java.lang.ref.WeakReference;
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
import java.util.WeakHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.biomart.builder.exceptions.PartitionException;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.Schema.JDBCSchema;
import org.biomart.common.exceptions.BioMartError;
import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.biomart.common.utils.BeanList;
import org.biomart.common.utils.BeanMap;
import org.biomart.common.utils.Transaction;
import org.biomart.common.utils.WeakPropertyChangeSupport;
import org.biomart.common.utils.Transaction.TransactionEvent;
import org.biomart.common.utils.Transaction.TransactionListener;

/**
 * The partition table interface allows lists of values to be stored, with those
 * lists broken into sub-lists if required. Each entry in the list can consist
 * of multiple columns each labelled with a unique name. The partition table
 * itself also has a unique name.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.32 $, $Date: 2007-12-17 09:34:01 $, modified by
 *          $Author: rh4 $
 * @since 0.7
 */
public abstract class PartitionTable implements TransactionListener, Comparable {
	/**
	 * Subclasses use this field to fire events of their own.
	 */
	protected final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
			this);

	private boolean visibleModified = true;

	private boolean directModified = false;

	private final PropertyChangeListener listener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			PartitionTable.this.setDirectModified(true);
		}
	};

	/**
	 * Use this constant to pass to methods which require a number of rows as a
	 * parameter.
	 */
	public static final int UNLIMITED_ROWS = -1;

	/**
	 * Use this marker in the selected column list to indicate the start of a
	 * subdivision.
	 */
	public static final String DIV_COLUMN = "__SUBDIVISION_BOUNDARY__";

	/**
	 * Use this marker to indicate that the partitoin is applied to the whole
	 * dataset, not just a dimension in it.
	 */
	public static final String NO_DIMENSION = "";

	/**
	 * Internal use only, by anonymous subclass. Sorted by column name.
	 */
	protected BeanMap columnMap = new BeanMap(new TreeMap());

	private int rowIterator = -1;

	private PartitionRow currentRow = null;

	private List rows = null;

	/**
	 * Internal use only, for subdivision tables only.
	 */
	protected final List subRows = new ArrayList();

	/**
	 * Internal use only, by anonymous subclass.
	 */
	protected List selectedColumnNames = new ArrayList();

	/**
	 * Internal use only, by anonymous subclass.
	 */
	protected List groupCols = new ArrayList();

	private PartitionTable subdivision = null;

	private final Map dmApplications = new WeakHashMap();

	// Unique IDs for avoiding listener probs.
	private static int ID_SERIES = 0;

	private final int uniqueId = PartitionTable.ID_SERIES++;

	/**
	 * Create a new, empty, partition table.
	 */
	public PartitionTable() {
		// Set up listening for property changes.
		Transaction.addTransactionListener(this);

		// All changes to us make us modified.
		this.addPropertyChangeListener(this.listener);
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
		// We don't care as this gets set internally.
	}

	public void transactionResetVisibleModified() {
		this.visibleModified = false;
	}

	public void transactionResetDirectModified() {
		this.directModified = false;
	}

	public void transactionStarted(final TransactionEvent evt) {
		// Don't really care for now.
	}

	public void transactionEnded(final TransactionEvent evt) {
		// Do nothing.
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
	 * Return all applications of this table. Keys are datasets, values are
	 * nested maps of dimension names to applications. Note that the
	 * applications are wrapped in WeakReferences and when resolved may be null.
	 * 
	 * @return the map.
	 */
	public Map getAllApplications() {
		return this.dmApplications;
	}

	/**
	 * What is our name?
	 * 
	 * @return the name.
	 */
	public abstract String getName();

	/**
	 * What is our original name?
	 * 
	 * @return the original name.
	 */
	public abstract String getOriginalName();

	/**
	 * What columns can we list and include?
	 * 
	 * @return the list of includable columns.
	 */
	public abstract Collection getAvailableColumnNames();

	/**
	 * Get ready to iterate over the rows in this table. After calling this, a
	 * call to {@link #nextRow()} will return the first row.
	 * 
	 * @param schemaPrefix
	 *            the partition of the schema we are getting rows from, if the
	 *            table needs it (<tt>null</tt> otherwise). This value is
	 *            used when establishing a connection to the schema. See
	 *            {@link JDBCSchema#getConnection(String)}. If <tt>null</tt>
	 *            is passed when it needs a non-null value then it should use a
	 *            sensible default.
	 * @param limit
	 *            the maximum number of rows to return, or
	 *            {@link #UNLIMITED_ROWS} for no limit.
	 * @throws PartitionException
	 *             if anything went wrong.
	 */
	public void prepareRows(final String schemaPrefix, final int limit)
			throws PartitionException {
		Log.debug("Preparing rows");
		this.currentRow = null;
		this.rows = new ArrayList(this.getRows(schemaPrefix));
		// Iterate over rows, apply transforms, drop duplicates.
		final Set seen = new HashSet();
		for (final Iterator i = this.rows.iterator(); i.hasNext();) {
			final PartitionRow row = (PartitionRow) i.next();
			final StringBuffer buf = new StringBuffer();
			for (final Iterator j = this.columnMap.values().iterator(); j
					.hasNext();) {
				final PartitionColumn pcol = (PartitionColumn) j.next();
				buf.append(pcol.getValueForRow(row));
				buf.append(',');
			}
			final String result = buf.toString();
			if (!seen.contains(result))
				seen.add(result);
			else
				i.remove();
		}
		Collections.sort(this.rows);
		if (limit != PartitionTable.UNLIMITED_ROWS && this.rows.size() > limit)
			this.rows = this.rows.subList(0, limit);
		this.rowIterator = 0;
	}

	/**
	 * How many rows do we have?
	 * 
	 * @return the number of rows.
	 */
	public int countRows() {
		return this.rows.size();
	}

	/**
	 * Move to the next row, or the first row if not yet called. This will skip
	 * over all rows with an identical set of values used to define any
	 * subdivision.
	 * 
	 * @return <tt>true</tt> if it could, or <tt>false</tt> if there are no
	 *         more.
	 * @throws PartitionException
	 *             if anything went wrong.
	 */
	public boolean nextRow() throws PartitionException {
		return this.getNextRow(false);
	}

	/**
	 * Move to the next row, or the first row if not yet called. This will not
	 * skip over multiple rows with the same subdivision-defining values.
	 * 
	 * @return <tt>true</tt> if it could, or <tt>false</tt> if there are no
	 *         more.
	 * @throws PartitionException
	 *             if anything went wrong.
	 */
	public boolean nudgeRow() throws PartitionException {
		return this.getNextRow(true);
	}

	/**
	 * Return the current row. If {@link #nextRow()} has not been called since
	 * {@link #prepareRows(String, int)} was called, or you are calling this
	 * after a failed call to {@link #nextRow()} then you will get an exception.
	 * 
	 * @return the current row.
	 * @throws PartitionException
	 *             if anything went wrong, or there is no current row.
	 */
	public PartitionRow currentRow() throws PartitionException {
		// Exception if currentRow is null.
		if (this.currentRow == null)
			throw new PartitionException(Resources
					.get("partitionCurrentBeforeNext"));
		return this.currentRow;
	}

	/**
	 * What columns do we have?
	 * 
	 * @return the column names in keys and the columns themselves in values.
	 */
	public BeanMap getColumns() {
		return this.columnMap;
	}

	/**
	 * What columns did the user select? (This may contain entries which are
	 * equal to {@link #DIV_COLUMN} which indicate the location of subdivision
	 * boundaries.)
	 * 
	 * @return the ordered list of selected columns.
	 */
	public List getSelectedColumnNames() {
		return this.selectedColumnNames;
	}

	/**
	 * What columns did the user select? (This may contain entries which are
	 * equal to {@link #DIV_COLUMN} which indicate the location of subdivision
	 * boundaries.)
	 * 
	 * @param selectedColumnNames
	 *            the ordered list of selected column names.
	 * @throws PartitionException
	 *             if any of them are invalid.
	 */
	public void setSelectedColumnNames(final List selectedColumnNames)
			throws PartitionException {
		final List oldValue = this.selectedColumnNames;
		if (oldValue.equals(selectedColumnNames))
			return;

		// Preserve any existing regexes.
		final Map regexStore = new HashMap(this.columnMap);

		// Clear-out.
		this.selectedColumnNames.clear();
		this.groupCols.clear();
		this.columnMap.clear();

		// Construct new table hierarchy.
		String previous = "";
		for (final Iterator i = selectedColumnNames.iterator(); i.hasNext();) {
			final String col = (String) i.next();
			if (col.equals(PartitionTable.DIV_COLUMN))
				// Don't allow back-to-back divs.
				if (previous.equals(col))
					continue;
				// Don't allow div-at-end.
				else if (!i.hasNext())
					continue;
				// Don't allow div-at-start.
				else if ("".equals(previous))
					continue;
			this.selectedColumnNames.add(col);
			previous = col;
		}

		// Column groupings into subdivisions.
		final List currentGroupCols = new ArrayList();
		int groupPos = 0;
		while (groupPos < this.selectedColumnNames.size()
				&& !this.selectedColumnNames.get(groupPos).equals(
						PartitionTable.DIV_COLUMN))
			currentGroupCols.add(this.selectedColumnNames.get(groupPos++));
		// Set up initial table.
		for (final Iterator i = currentGroupCols.iterator(); i.hasNext();) {
			final String col = (String) i.next();
			this.groupCols.add(col);
			final PartitionColumn pcol = new PartitionColumn(this, col);
			pcol.addPropertyChangeListener(this.listener);
			final PartitionColumn regexStored = (PartitionColumn) regexStore
					.get(pcol.getName());
			if (regexStored != null) {
				pcol.setRegexMatch(regexStored.getRegexMatch());
				pcol.setRegexReplace(regexStored.getRegexReplace());
			}
			this.columnMap.put(col, pcol);
		}
		PartitionTable currentPT = this;
		while (groupPos < this.selectedColumnNames.size()) {
			// Skip DIV itself.
			if (groupPos < this.selectedColumnNames.size())
				groupPos++;
			// Extend group cols to next DIV
			final List newGroupCols = new ArrayList();
			while (groupPos < this.selectedColumnNames.size()
					&& !this.selectedColumnNames.get(groupPos).equals(
							PartitionTable.DIV_COLUMN)) {
				final String col = (String) this.selectedColumnNames
						.get(groupPos++);
				currentGroupCols.add(col);
				newGroupCols.add(col);
			}
			// Create subdiv PT with extended group cols
			final PartitionTable parent = this;
			final PartitionTable subdiv = new PartitionTable() {
				{
					// Subdiv column map = pointer this column map.
					this.columnMap = parent.columnMap;
					// Subdiv selected cols = pointer this selected cols.
					this.selectedColumnNames = parent.selectedColumnNames;
					// Subdiv group cols = copy currentGroupCols
					this.groupCols = new ArrayList(currentGroupCols);
				}

				protected List getRows(String schemaPartition)
						throws PartitionException {
					return this.subRows;
				}

				public Collection getAvailableColumnNames() {
					return parent.getAvailableColumnNames();
				}

				public String getName() {
					return parent.getName();
				}

				public String getOriginalName() {
					return parent.getOriginalName();
				}

			};
			// Assign subdiv to current PT
			currentPT.subdivision = subdiv;
			// Create column objects for each new group col
			for (final Iterator i = newGroupCols.iterator(); i.hasNext();) {
				final String col = (String) i.next();
				// Assign column objects to new subdiv
				final PartitionColumn pcol = new PartitionColumn(subdiv, col);
				pcol.addPropertyChangeListener(this.listener);
				final PartitionColumn regexStored = (PartitionColumn) regexStore
						.get(pcol.getName());
				if (regexStored != null) {
					pcol.setRegexMatch(regexStored.getRegexMatch());
					pcol.setRegexReplace(regexStored.getRegexReplace());
				}
				this.columnMap.put(col, pcol);
			}
			// Set current PT = new subdiv
			currentPT = subdiv;
		}
		this.pcs.firePropertyChange("selectedColumnNames", oldValue,
				selectedColumnNames);
	}

	/**
	 * Apply this partition table to the given dimension using the given
	 * definition. If the definition is null, apply using defaults.
	 * 
	 * @param ds
	 *            the dataset.
	 * @param dimension
	 *            the dimension.
	 * @param appl
	 *            the application definition (null for default).
	 */
	public void applyTo(final DataSet ds, String dimension,
			PartitionTableApplication appl) {
		if (dimension == null || dimension.equals(PartitionTable.NO_DIMENSION))
			dimension = PartitionTable.NO_DIMENSION;
		if (!this.dmApplications.containsKey(ds))
			this.dmApplications.put(ds, new HashMap());
		if (appl == null)
			appl = PartitionTableApplication.createDefault(this, ds, dimension);
		((Map) this.dmApplications.get(ds)).put(dimension, new WeakReference(
				appl));
		if (!dimension.equals(PartitionTable.NO_DIMENSION))
			((DataSetTable) ds.getTables().get(dimension))
					.setPartitionTableApplication(appl);
		else
			ds.setPartitionTableApplication(appl);
		// Listen to the applied rows.
		if (appl != null)
			appl.addPropertyChangeListener("directModified", this.listener);
		// Fire event - we have no before/after, so a simple event will do.
		this.pcs.firePropertyChange("partitionTableApplication", null, appl);
	}

	/**
	 * Remove this partition table from a dimension.
	 * 
	 * @param ds
	 *            the dataset.
	 * @param dimension
	 *            the dimension.
	 */
	public void removeFrom(final DataSet ds, String dimension) {
		if (dimension == null || dimension.equals(PartitionTable.NO_DIMENSION))
			dimension = PartitionTable.NO_DIMENSION;
		if (!this.dmApplications.containsKey(ds))
			return;
		((Map) this.dmApplications.get(ds)).remove(dimension);
		if (((Map) this.dmApplications.get(ds)).isEmpty())
			this.dmApplications.remove(ds);
		if (!dimension.equals(PartitionTable.NO_DIMENSION)) {
			if (ds.getTables().containsKey(dimension))
				((DataSetTable) ds.getTables().get(dimension))
						.setPartitionTableApplication(null);
		} else
			ds.setPartitionTableApplication(null);
		// Fire event - we have no before/after, so a simple event will do.
		this.pcs.firePropertyChange("partitionTableApplication", null, null);
	}

	private boolean getNextRow(final boolean nudge) throws PartitionException {
		// If row iterator is negative, throw exception.
		if (this.rowIterator < 0)
			throw new PartitionException(Resources
					.get("partitionIterateBeforePopulate"));
		// Exception if doesn't have a next, and set currentRow to
		// null.
		if (this.rowIterator >= this.rows.size())
			return false;
		// Update current row.
		this.currentRow = (PartitionRow) this.rows.get(this.rowIterator++);
		// Set up the sub-division tables.
		if (this.subdivision != null) {
			this.subdivision.subRows.clear();
			this.subdivision.subRows.add(this.currentRow);
			// Keep adding rows till find one not same.
			boolean keepGoing = !nudge;
			while (keepGoing && this.rowIterator < this.rows.size()) {
				final PartitionRow subRow = (PartitionRow) this.rows
						.get(this.rowIterator);
				for (final Iterator i = this.groupCols.iterator(); i.hasNext()
						&& keepGoing;) {
					final String subColName = (String) i.next();
					final PartitionColumn pcol = (PartitionColumn) this
							.getColumns().get(subColName);
					final String parentValue = pcol
							.getValueForRow(this.currentRow);
					final String subValue = pcol.getValueForRow(subRow);
					keepGoing &= parentValue.equals(subValue);
				}
				if (keepGoing) {
					this.subdivision.subRows.add(subRow);
					this.rowIterator++;
				}
			}
		}
		return true;
	}

	/**
	 * Implementing methods should use this to build a list of rows and return
	 * it. Iteration will be handled by the parent. Duplicated rows, if any,
	 * will be handled by the parent, as will any regexing or special row
	 * manipulation.
	 * 
	 * @param schemaPrefix
	 *            the partition to get rows for, or <tt>null</tt> if not to
	 *            bother.
	 * @return the rows. Never <tt>null</tt> but may be empty.
	 * @throws PartitionException
	 *             if the rows couldn't be obtained.
	 */
	protected abstract List getRows(final String schemaPrefix)
			throws PartitionException;

	public boolean equals(final Object obj) {
		if (obj == this)
			return true;
		else if (obj == null)
			return false;
		else if (obj instanceof PartitionTable) {
			final PartitionTable pt = (PartitionTable) obj;
			return (this.uniqueId + "_" + this.getOriginalName())
					.equals(pt.uniqueId + "_" + pt.getOriginalName());
		} else
			return false;
	}

	public int compareTo(final Object obj) {
		final PartitionTable pt = (PartitionTable) obj;
		return (this.uniqueId + "_" + this.getOriginalName())
				.compareTo(pt.uniqueId + "_" + pt.getOriginalName());
	}

	public String toString() {
		return this.getName();
	}

	/**
	 * A column knows its name.
	 */
	public static class PartitionColumn implements TransactionListener {
		private final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
				this);

		private boolean visibleModified = true;

		private boolean directModified = false;

		private final PropertyChangeListener listener = new PropertyChangeListener() {
			public void propertyChange(final PropertyChangeEvent evt) {
				PartitionColumn.this.setDirectModified(true);
			}
		};

		private final PartitionTable table;

		private String name;

		private String regexMatch = null;

		private String regexReplace = null;

		private Pattern compiled = null;

		/**
		 * Construct a new column that is going to be added to this table (but
		 * don't actually add it yet).
		 * 
		 * @param table
		 *            the table.
		 * @param name
		 *            the name.
		 */
		public PartitionColumn(final PartitionTable table, final String name) {
			// Set up listening for property changes.
			Transaction.addTransactionListener(this);

			// All changes to us make us modified.
			this.addPropertyChangeListener(this.listener);

			this.table = table;
			this.name = name;
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
			// We don't care as this gets set internally.
		}

		public void transactionResetVisibleModified() {
			this.visibleModified = false;
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

		/**
		 * Find out which table this column belongs to.
		 * 
		 * @return the table.
		 */
		public PartitionTable getPartitionTable() {
			return this.table;
		}

		/**
		 * Find out the column name.
		 * 
		 * @return the column name.
		 */
		public String getName() {
			return this.name;
		}

		/**
		 * Get the value in this column for the specified row.
		 * 
		 * @param row
		 *            the row.
		 * @return the value.
		 * @throws PartitionException
		 *             if there were problems getting the value.
		 */
		public String getValueForRow(final PartitionRow row)
				throws PartitionException {
			if (this.compiled == null && this.regexMatch != null)
				try {
					this.compiled = Pattern.compile(this.regexMatch);
				} catch (final PatternSyntaxException pe) {
					this.compiled = null;
				}
			if (this.compiled != null && this.regexReplace != null)
				return this.compiled.matcher(row.getValue(this.getName()))
						.replaceAll(this.regexReplace);
			else
				return row.getValue(this.getName());
		}

		/**
		 * Get the value in this column for the specified row.
		 * 
		 * @param row
		 *            the row.
		 * @return the value.
		 * @throws PartitionException
		 *             if there were problems getting the value.
		 */
		public String getRawValueForRow(final PartitionRow row)
				throws PartitionException {
			return row.getValue(this.getName());
		}

		/**
		 * Set the regex to use to match values.
		 * 
		 * @param regexMatch
		 *            the regex.
		 */
		public void setRegexMatch(final String regexMatch) {
			final String oldValue = this.regexMatch;
			if (oldValue == regexMatch || oldValue != null
					&& oldValue.equals(regexMatch))
				return;
			this.regexMatch = regexMatch;
			this.pcs.firePropertyChange("regexMatch", oldValue, regexMatch);
			this.compiled = null;
		}

		/**
		 * What regex are we using to match values?
		 * 
		 * @return the regex.
		 */
		public String getRegexMatch() {
			return this.regexMatch;
		}

		/**
		 * Set the regex to use to replace values.
		 * 
		 * @param regexReplace
		 *            the regex.
		 */
		public void setRegexReplace(final String regexReplace) {
			final String oldValue = this.regexReplace;
			if (oldValue == regexReplace || oldValue != null
					&& oldValue.equals(regexReplace))
				return;
			this.regexReplace = regexReplace;
			this.pcs.firePropertyChange("regexReplace", oldValue, regexReplace);
		}

		/**
		 * What regex are we using to replace values?
		 * 
		 * @return the regex.
		 */
		public String getRegexReplace() {
			return this.regexReplace;
		}

		/**
		 * A fake column for use when partitioning is not applied, but the
		 * algorithm requires a partition column object.
		 */
		public static class FakeColumn extends PartitionColumn {
			/**
			 * Construct a fake column that belongs to a fake table which only
			 * has one row, with no columns.
			 */
			public FakeColumn() {
				super(new PartitionTable() {
					public String getName() {
						return "__FAKE__TABLE__";
					}

					public String getOriginalName() {
						return "__FAKE__TABLE__";
					}

					public Collection getAvailableColumnNames() {
						return Collections.EMPTY_SET;
					}

					public BeanMap getColumns() {
						return new BeanMap(Collections.EMPTY_MAP);
					}

					protected List getRows(final String schemaPartition)
							throws PartitionException {
						final List rows = new ArrayList();
						rows.add(new PartitionRow(this) {
							public String getValue(final String columnName)
									throws PartitionException {
								// Should never get called. If it does,
								// then the empty string should suffice.
								return "";
							}
						});
						return rows;
					}
				}, null);
			}
		}
	}

	/**
	 * This class defines how rows of the table will behave.
	 */
	public static abstract class PartitionRow implements Comparable {

		private final PartitionTable table;

		/**
		 * Use this constructor to make a new numbered row. The numbers are not
		 * checked so use with care.
		 * 
		 * @param table
		 *            the table this row belongs to.
		 */
		protected PartitionRow(final PartitionTable table) {
			this.table = table;
		}

		/**
		 * Find out which table this row belongs to.
		 * 
		 * @return the table.
		 */
		public PartitionTable getPartitionTable() {
			return this.table;
		}

		/**
		 * Return the value in the given column. If null, returns "null".
		 * 
		 * @param columnName
		 *            the column.
		 * @return the value.
		 * @throws PartitionException
		 *             if there was a problem, or the column does not exist.
		 */
		public abstract String getValue(final String columnName)
				throws PartitionException;

		public String toString() {
			final StringBuffer sbuff = new StringBuffer();
			for (final Iterator i = this.getPartitionTable()
					.getSelectedColumnNames().iterator(); i.hasNext();) {
				final String colName = (String) i.next();
				if (colName.equals(PartitionTable.DIV_COLUMN))
					continue;
				try {
					final PartitionColumn col = (PartitionColumn) this
							.getPartitionTable().getColumns().get(colName);
					sbuff.append(col.getValueForRow(this));
				} catch (final PartitionException pe) {
					throw new BioMartError(pe);
				}
			}
			return sbuff.toString();
		}

		public int compareTo(final Object obj) {
			final PartitionRow them = (PartitionRow) obj;
			for (final Iterator i = this.getPartitionTable()
					.getSelectedColumnNames().iterator(); i.hasNext();) {
				final String colName = (String) i.next();
				if (colName.equals(PartitionTable.DIV_COLUMN))
					continue;
				try {
					final PartitionColumn col = (PartitionColumn) this
							.getPartitionTable().getColumns().get(colName);
					if (!col.getValueForRow(this).equals(
							col.getValueForRow(them)))
						return col.getValueForRow(this).compareTo(
								col.getValueForRow(them));
				} catch (final PartitionException pe) {
					throw new BioMartError(pe);
				}
			}
			return 0;
		}
	}

	/**
	 * Defines how a partition table is applied in real life.
	 */
	public static class PartitionTableApplication implements
			TransactionListener {
		private final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
				this);

		private boolean visibleModified = true;

		private boolean directModified = false;

		private final PropertyChangeListener listener = new PropertyChangeListener() {
			public void propertyChange(final PropertyChangeEvent evt) {
				PartitionTableApplication.this.setDirectModified(true);
			}
		};

		private final PropertyChangeListener rowsListener = new PropertyChangeListener() {
			public void propertyChange(final PropertyChangeEvent evt) {
				final Collection newRows = new HashSet(
						PartitionTableApplication.this.partitionAppliedRows);
				newRows.removeAll(PartitionTableApplication.this.rowCache);
				for (final Iterator i = newRows.iterator(); i.hasNext();) {
					final PartitionAppliedRow row = (PartitionAppliedRow) i
							.next();
					row
							.addPropertyChangeListener(PartitionTableApplication.this.listener);
				}
				PartitionTableApplication.this.rowCache.clear();
				PartitionTableApplication.this.rowCache
						.addAll(PartitionTableApplication.this.partitionAppliedRows);
				PartitionTableApplication.this.setDirectModified(true);
			}
		};

		private final PartitionTable pt;

		private final BeanList partitionAppliedRows = new BeanList(
				new ArrayList());

		private final Collection rowCache = new HashSet();

		/**
		 * Construct a new, empty, partition table application.
		 * 
		 * @param pt
		 *            the partition table.
		 */
		public PartitionTableApplication(final PartitionTable pt) {
			// Set up listening for property changes.
			Transaction.addTransactionListener(this);

			// All changes to us make us modified.
			this.addPropertyChangeListener(this.listener);

			this.pt = pt;

			// Listen to partitionAppliedRows
			// Also listen to each new row added.
			this.partitionAppliedRows.addPropertyChangeListener(this.listener);
			this.partitionAppliedRows
					.addPropertyChangeListener(this.rowsListener);
		}

		/**
		 * Replicate ourselves.
		 * 
		 * @return the copy.
		 */
		public PartitionTableApplication replicate() {
			final PartitionTableApplication appl = new PartitionTableApplication(
					this.pt);
			for (final Iterator i = this.partitionAppliedRows.iterator(); i
					.hasNext();)
				appl.partitionAppliedRows.add(((PartitionAppliedRow) i.next())
						.replicate());
			return appl;
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
			// We don't care as this gets set internally.
		}

		public void transactionResetVisibleModified() {
			this.visibleModified = false;
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

		/**
		 * For a given relation, obtain the row that applies it.
		 * 
		 * @param rel
		 *            the relation.
		 * @return the applied row.
		 */
		public PartitionAppliedRow getAppliedRowForRelation(final Relation rel) {
			for (final Iterator i = this.partitionAppliedRows.iterator(); i
					.hasNext();) {
				final PartitionAppliedRow row = (PartitionAppliedRow) i.next();
				if (row.getRelation() != null && row.getRelation().equals(rel))
					return row;
			}
			return null;
		}

		/**
		 * What table is this applying?
		 * 
		 * @return the table.
		 */
		public PartitionTable getPartitionTable() {
			return this.pt;
		}

		/**
		 * Obtain a list of all partition rows.
		 * 
		 * @return the list.
		 */
		public BeanList getPartitionAppliedRows() {
			return this.partitionAppliedRows;
		}

		/**
		 * Convenience method to get the column to use to provide the name for
		 * the first entry in the applied rows.
		 * 
		 * @return the real naming column.
		 * @throws PartitionException
		 *             if it cannot.
		 */
		public PartitionColumn getNamePartitionCol() throws PartitionException {
			return (PartitionColumn) this.getPartitionTable().getColumns().get(
					((PartitionAppliedRow) this.partitionAppliedRows.get(0))
							.getNamePartitionCol());
		}

		/**
		 * Update the compound relation counts internally.
		 * 
		 * @throws PartitionException
		 *             if it goes wrong.
		 */
		public void syncCounts() throws PartitionException {
			// Get real partition table for each alias and count rows.
			for (int i = 0; i < this.partitionAppliedRows.size(); i++) {
				final PartitionAppliedRow prow = (PartitionAppliedRow) this.partitionAppliedRows
						.get(i);
				final String partitionCol = prow.getPartitionCol();
				final PartitionTable realPT = ((PartitionColumn) this.pt
						.getColumns().get(partitionCol)).getPartitionTable();
				int compound = 0;
				realPT.prepareRows(null, PartitionTable.UNLIMITED_ROWS);
				while (realPT.nextRow())
					compound++;
				prow.setCompound(compound);
			}
		}

		/**
		 * Create a default application based on the given dataset.
		 * 
		 * @param pt
		 *            the partition table.
		 * @param ds
		 *            the dataset.
		 * @return the default application.
		 */
		public static PartitionTableApplication createDefault(
				final PartitionTable pt, final DataSet ds) {
			final PartitionTableApplication pa = new PartitionTableApplication(
					pt);
			final String ptCol = (String) pt.getSelectedColumnNames()
					.iterator().next();
			pa.getPartitionAppliedRows().add(
					new PartitionAppliedRow(ptCol, (String) ds.getMainTable()
							.getColumns().keySet().iterator().next(), ptCol,
							null));
			return pa;
		}

		/**
		 * Create a default application based on the given dimension.
		 * 
		 * @param pt
		 *            the partition table.
		 * @param ds
		 *            the dataset.
		 * @param dimension
		 *            the dimension.
		 * @return the default application.
		 */
		public static PartitionTableApplication createDefault(
				final PartitionTable pt, final DataSet ds,
				final String dimension) {
			final PartitionTableApplication pa = new PartitionTableApplication(
					pt);
			if (pt.getSelectedColumnNames().size() < 1)
				return pa;
			final String ptCol = (String) pt.getSelectedColumnNames()
					.iterator().next();
			pa.getPartitionAppliedRows().add(
					new PartitionAppliedRow(ptCol, (String) ((DataSetTable) ds
							.getTables().get(dimension)).getColumns().keySet()
							.iterator().next(), ptCol, null));
			return pa;
		}

		/**
		 * Details of how a partition table is broken down into a particular
		 * row.
		 */
		public static class PartitionAppliedRow implements TransactionListener {
			private final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
					this);

			private boolean visibleModified = true;

			private boolean directModified = false;

			private final PropertyChangeListener listener = new PropertyChangeListener() {
				public void propertyChange(final PropertyChangeEvent evt) {
					PartitionAppliedRow.this.setDirectModified(true);
				}
			};

			private int compound = 1;

			private String partitionCol;

			private String rootDataSetCol;

			private String namePartitionCol;

			private Relation relation;

			/**
			 * Construct a row of data from a single partition table.
			 * 
			 * @param partitionCol
			 *            the column providing unique values.
			 * @param rootDataSetCol
			 *            the data set column the values are applied to. This is
			 *            a root name (not including the {0}*__ prefix).
			 * @param namePartitionCol
			 *            the column providing data to be used in the prefix.
			 * @param relation
			 *            the relation that provides the dataset column this
			 *            refers to.
			 */
			public PartitionAppliedRow(final String partitionCol,
					final String rootDataSetCol, final String namePartitionCol,
					final Relation relation) {
				this.compound = 1;
				this.partitionCol = partitionCol;
				this.rootDataSetCol = rootDataSetCol;
				this.namePartitionCol = namePartitionCol;
				this.relation = relation;

				// Set up listening for property changes.
				Transaction.addTransactionListener(this);

				// All changes to us make us modified.
				this.addPropertyChangeListener(this.listener);
			}

			/**
			 * Replicate ourselves.
			 * 
			 * @return the replica.
			 */
			public PartitionAppliedRow replicate() {
				final PartitionAppliedRow row = new PartitionAppliedRow(
						this.partitionCol, this.rootDataSetCol,
						this.namePartitionCol, this.relation);
				row.compound = this.compound;
				return row;
			}

			public boolean isDirectModified() {
				return this.directModified;
			}

			public void setDirectModified(final boolean modified) {
				if (modified == this.directModified)
					return;
				final boolean oldValue = this.directModified;
				this.directModified = modified;
				this.pcs.firePropertyChange("directModified", oldValue,
						modified);
			}

			public boolean isVisibleModified() {
				return this.visibleModified;
			}

			public void setVisibleModified(final boolean modified) {
				// We don't care as this gets set internally.
			}

			public void transactionResetVisibleModified() {
				this.visibleModified = false;
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

			/**
			 * @return the compound
			 */
			public int getCompound() {
				return this.compound;
			}

			/**
			 * @param compound
			 *            the compound to set
			 */
			public void setCompound(final int compound) {
				final int oldValue = this.compound;
				if (oldValue == compound)
					return;
				this.compound = compound;
				this.pcs.firePropertyChange("compound", oldValue, compound);
			}

			/**
			 * @return the namePartitionCol
			 */
			public String getNamePartitionCol() {
				return this.namePartitionCol;
			}

			/**
			 * @return the partitionCol
			 */
			public String getPartitionCol() {
				return this.partitionCol;
			}

			/**
			 * @return the rootDataSetCol
			 */
			public String getRootDataSetCol() {
				return this.rootDataSetCol;
			}

			/**
			 * @return the relation
			 */
			public Relation getRelation() {
				return this.relation;
			}

			/**
			 * @param namePartitionCol
			 *            the namePartitionCol to set
			 */
			public void setNamePartitionCol(final String namePartitionCol) {
				final String oldValue = this.namePartitionCol;
				if (oldValue == namePartitionCol || oldValue != null
						&& oldValue.equals(namePartitionCol))
					return;
				this.namePartitionCol = namePartitionCol;
				this.pcs.firePropertyChange("namePartitionCol", oldValue,
						namePartitionCol);
			}

			/**
			 * @param partitionCol
			 *            the partitionCol to set
			 */
			public void setPartitionCol(final String partitionCol) {
				final String oldValue = this.partitionCol;
				if (oldValue == partitionCol || oldValue != null
						&& oldValue.equals(partitionCol))
					return;
				this.partitionCol = partitionCol;
				this.pcs.firePropertyChange("partitionCol", oldValue,
						partitionCol);
			}

			/**
			 * @param rootDataSetCol
			 *            the rootDataSetCol to set
			 */
			public void setRootDataSetCol(final String rootDataSetCol) {
				final String oldValue = this.rootDataSetCol;
				if (oldValue == rootDataSetCol || oldValue != null
						&& oldValue.equals(rootDataSetCol))
					return;
				this.rootDataSetCol = rootDataSetCol;
				this.pcs.firePropertyChange("rootDataSetCol", oldValue,
						rootDataSetCol);
			}

			/**
			 * @param relation
			 *            the relation to set
			 */
			public void setRelation(final Relation relation) {
				final Relation oldValue = this.relation;
				if (oldValue == relation || oldValue != null
						&& oldValue.equals(relation))
					return;
				this.relation = relation;
				this.pcs.firePropertyChange("relation", oldValue, relation);

			}

			public int hashCode() {
				return (this.namePartitionCol == null ? 1
						: this.namePartitionCol.hashCode())
						* (this.partitionCol == null ? 1 : this.partitionCol
								.hashCode())
						* (this.rootDataSetCol == null ? 1
								: this.rootDataSetCol.hashCode())
						* (this.relation == null ? 1 : this.relation.hashCode());
			}

			public boolean equals(final Object o) {
				if (!(o instanceof PartitionAppliedRow))
					return false;
				final PartitionAppliedRow them = (PartitionAppliedRow) o;
				return (this.namePartitionCol == them.namePartitionCol || this.namePartitionCol != null
						&& this.namePartitionCol.equals(them.namePartitionCol))
						&& (this.partitionCol == them.partitionCol || this.partitionCol != null
								&& this.partitionCol.equals(them.partitionCol))
						&& (this.rootDataSetCol == them.rootDataSetCol || this.rootDataSetCol != null
								&& this.rootDataSetCol
										.equals(them.rootDataSetCol))
						&& (this.relation == them.relation || this.relation != null
								&& this.relation.equals(them.relation));
			}
		}
	}
}
