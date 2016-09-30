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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.DataSet.ExpressionColumnDefinition;
import org.biomart.builder.model.DataSet.DataSetColumn.ExpressionColumn;
import org.biomart.builder.model.DataSet.DataSetColumn.InheritedColumn;
import org.biomart.builder.model.DataSet.DataSetColumn.WrappedColumn;
import org.biomart.builder.model.Relation.UnrolledRelationDefinition;
import org.biomart.common.exceptions.BioMartError;

/**
 * This interface defines a unit of transformation for mart construction.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.29 $, $Date: 2008-02-19 13:27:29 $, modified by
 *          $Author: rh4 $
 * @since 0.6
 */
public abstract class TransformationUnit {
	/**
	 * A map of source schema column names to dataset column objects.
	 */
	private final Map newColumnNameMap;

	private TransformationUnit previousUnit;

	/**
	 * Constructs a new transformation unit that follows on from a given
	 * previous unit (optional, can be <tt>null</tt>).
	 * 
	 * @param previousUnit
	 *            the unit this one comes after.
	 */
	public TransformationUnit(final TransformationUnit previousUnit) {
		this.newColumnNameMap = new HashMap();
		this.previousUnit = previousUnit;
	}

	/**
	 * Does this unit apply to the given schema prefix?
	 * 
	 * @param schemaPrefix
	 *            the prefix.
	 * @return <tt>true</tt> if it does.
	 */
	public boolean appliesToPartition(final String schemaPrefix) {
		return this.previousUnit == null ? true : this.previousUnit
				.appliesToPartition(schemaPrefix);
	}

	/**
	 * Find out what unit came before this one.
	 * 
	 * @return the previous unit. May be <tt>null</tt>.
	 */
	public TransformationUnit getPreviousUnit() {
		return this.previousUnit;
	}

	/**
	 * Change the previous unit to this one.
	 * 
	 * @param previousUnit
	 *            the new previous unit. <tt>null</tt> to remove it.
	 */
	public void setPreviousUnit(final TransformationUnit previousUnit) {
		this.previousUnit = previousUnit;
	}

	/**
	 * Obtain a map of columns defined in this unit. The keys are schema
	 * columns. The values are the dataset column names used for those columns
	 * after this unit has been applied.
	 * 
	 * @return the map of columns. Potentially empty but never <tt>null</tt>.
	 */
	public Map getNewColumnNameMap() {
		return this.newColumnNameMap;
	}

	/**
	 * Given a schema column, work out which dataset column in the
	 * transformation so far refers to it. If the column was not adopted in this
	 * particular unit it will go back until it finds the unit that adopted it,
	 * and interrogate that and return the results.
	 * 
	 * @param column
	 *            the column to look for.
	 * @return the matching dataset column. May be <tt>null</tt> if the column
	 *         is not in this dataset table at all.
	 */
	public abstract DataSetColumn getDataSetColumnFor(final Column column);

	/**
	 * This type of transformation selects columns from a single table.
	 */
	public static class SelectFromTable extends TransformationUnit {
		private static final long serialVersionUID = 1L;

		private final Table table;

		private SelectFromTable(final TransformationUnit previousUnit,
				final Table table) {
			super(previousUnit);
			this.table = table;
		}

		/**
		 * Instantiate a unit that selects from the given schema table.
		 * 
		 * @param table
		 *            the table this unit selects from.
		 */
		public SelectFromTable(final Table table) {
			super(null);
			this.table = table;
		}

		public boolean appliesToPartition(final String schemaPrefix) {
			return this.table.existsForPartition(schemaPrefix)
					&& super.appliesToPartition(schemaPrefix);
		}

		/**
		 * Find out which schema table this unit selects from.
		 * 
		 * @return the schema table this unit selects from.
		 */
		public Table getTable() {
			return this.table;
		}

		private boolean columnMatches(final Column column, DataSetColumn dsCol) {
			if (dsCol == null)
				return false;
			while (dsCol instanceof InheritedColumn)
				dsCol = ((InheritedColumn) dsCol).getInheritedColumn();
			if (dsCol instanceof WrappedColumn)
				return ((WrappedColumn) dsCol).getWrappedColumn()
						.equals(column);
			return false;
		}

		public DataSetColumn getDataSetColumnFor(final Column column) {
			DataSetColumn candidate = (DataSetColumn) this
					.getNewColumnNameMap().get(column);
			if (candidate == null)
				// We need to check each of our columns to see if they
				// are dataset columns, and if so, if they point to
				// the appropriate real column.
				for (final Iterator i = this.getNewColumnNameMap().values()
						.iterator(); i.hasNext() && candidate == null;) {
					candidate = (DataSetColumn) i.next();
					if (!this.columnMatches(column, candidate))
						candidate = null;
				}
			return candidate;
		}
	}

	/**
	 * This unit joins an existing dataset table to a schema table.
	 */
	public static class JoinTable extends SelectFromTable {
		private static final long serialVersionUID = 1L;

		private List sourceDataSetColumns;

		private Key schemaSourceKey;

		private Relation schemaRelation;

		private int schemaRelationIteration;

		/**
		 * Construct a new join unit.
		 * 
		 * @param previousUnit
		 *            the unit that precedes this one.
		 * @param table
		 *            the table we are joining to.
		 * @param sourceDataSetColumns
		 *            the columns in the existing dataset table that are used to
		 *            make the join.
		 * @param schemaSourceKey
		 *            the key in the schema table that we are joining to.
		 * @param schemaRelation
		 *            the relation we are following to make the join.
		 * @param schemaRelationIteration
		 *            the number of the compound relation, if it is compound.
		 *            Use 0 if it is not.
		 */
		public JoinTable(final TransformationUnit previousUnit,
				final Table table, final List sourceDataSetColumns,
				final Key schemaSourceKey, final Relation schemaRelation,
				final int schemaRelationIteration) {
			super(previousUnit, table);
			this.sourceDataSetColumns = sourceDataSetColumns;
			this.schemaSourceKey = schemaSourceKey;
			this.schemaRelation = schemaRelation;
			this.schemaRelationIteration = schemaRelationIteration;
		}

		public boolean appliesToPartition(final String schemaPrefix) {
			for (final Iterator i = this.sourceDataSetColumns.iterator(); i
					.hasNext();) {
				final DataSetColumn dsCol = (DataSetColumn) i.next();
				if (!dsCol.existsForPartition(schemaPrefix)) {
					return false;
				}
			}
			return super.appliesToPartition(schemaPrefix);
		}

		/**
		 * Get the dataset columns this transformation starts from.
		 * 
		 * @return the columns.
		 */
		public List getSourceDataSetColumns() {
			return this.sourceDataSetColumns;
		}

		/**
		 * Get the schema table key this transformation joins to.
		 * 
		 * @return the key we are joining to.
		 */
		public Key getSchemaSourceKey() {
			return this.schemaSourceKey;
		}

		/**
		 * Get the schema relation used to make the join.
		 * 
		 * @return the relation.
		 */
		public Relation getSchemaRelation() {
			return this.schemaRelation;
		}

		/**
		 * Get the number of the compound relation used, or 0 if it is not
		 * compound.
		 * 
		 * @return the compound relation number.
		 */
		public int getSchemaRelationIteration() {
			return this.schemaRelationIteration;
		}

		public DataSetColumn getDataSetColumnFor(final Column column) {
			DataSetColumn candidate = (DataSetColumn) this
					.getNewColumnNameMap().get(column);
			if (candidate == null && this.getPreviousUnit() != null) {
				final Key ourKey = Arrays.asList(
						this.schemaRelation.getFirstKey().getColumns())
						.contains(column) ? this.schemaRelation.getFirstKey()
						: this.schemaRelation.getSecondKey();
				final Key parentKey = this.schemaRelation.getOtherKey(ourKey);
				final int pos = Arrays.asList(ourKey.getColumns()).indexOf(
						column);
				if (pos >= 0)
					candidate = this.getPreviousUnit().getDataSetColumnFor(
							parentKey.getColumns()[pos]);
				if (candidate == null)
					candidate = this.getPreviousUnit().getDataSetColumnFor(
							column);
			}
			return candidate;
		}
	}

	/**
	 * This unit adds expression columns to a dataset table. The expressions are
	 * already defined in the table as {@link ExpressionColumn}s.
	 */
	public static class Expression extends TransformationUnit {
		private static final long serialVersionUID = 1L;

		private DataSetTable dsTable;

		/**
		 * Construct an expression unit.
		 * 
		 * @param previousUnit
		 *            the previous unit in the chain.
		 * @param dsTable
		 *            the table we are adding expressions to.
		 */
		public Expression(final TransformationUnit previousUnit,
				final DataSetTable dsTable) {
			super(previousUnit);
			this.dsTable = dsTable;
		}

		public DataSetColumn getDataSetColumnFor(final Column column) {
			if (this.getPreviousUnit() != null)
				return this.getPreviousUnit().getDataSetColumnFor(column);
			else
				// Should never happen.
				throw new BioMartError();
		}

		/**
		 * Get the dataset table that will receive the expressions.
		 * 
		 * @return the table.
		 */
		public DataSetTable getDataSetTable() {
			return this.dsTable;
		}

		/**
		 * Get an ordered collection where each member is a collection of
		 * expression columns that can be added in a single step.
		 * 
		 * @return the ordered collection of collections of expressions.
		 */
		public Collection getOrderedExpressionGroups() {
			final List groups = new ArrayList();
			final Collection entries = new TreeSet(new Comparator() {
				public int compare(Object a, Object b) {
					final Map.Entry entryA = (Map.Entry) a;
					final Map.Entry entryB = (Map.Entry) b;
					final String colNameA = ((Column) entryA.getKey())
							.getName();
					final ExpressionColumnDefinition exprA = ((ExpressionColumn) entryA
							.getValue()).getDefinition();
					final ExpressionColumnDefinition exprB = ((ExpressionColumn) entryB
							.getValue()).getDefinition();
					return exprB.getAliases().keySet().contains(colNameA) ? -1
							: exprA.isGroupBy() == exprB.isGroupBy() ? 1 : -1;
				}
			});
			entries.addAll(this.getNewColumnNameMap().entrySet());
			// Iterator over entries and sort into groups.
			Map.Entry previousEntry = null;
			Collection currentGroup = new HashSet();
			groups.add(currentGroup);
			for (final Iterator i = entries.iterator(); i.hasNext();) {
				final Map.Entry entry = (Map.Entry) i.next();
				if (previousEntry != null) {
					final String colNameA = ((Column) entry.getKey()).getName();
					final ExpressionColumnDefinition exprA = ((ExpressionColumn) entry
							.getValue()).getDefinition();
					final ExpressionColumnDefinition exprB = ((ExpressionColumn) previousEntry
							.getValue()).getDefinition();
					if (exprB.getAliases().keySet().contains(colNameA)
							|| !(exprA.isGroupBy() == exprB.isGroupBy())) {
						currentGroup = new HashSet();
						groups.add(currentGroup);
					}
				}
				currentGroup.add(entry.getValue());
				previousEntry = entry;
			}
			return groups;
		}
	}

	/**
	 * This unit joins an existing dataset table to a schema table, or at least
	 * would do that if the join were ever to be made, which it won't.
	 */
	public static class SkipTable extends JoinTable {
		private static final long serialVersionUID = 1L;

		/**
		 * Construct a new join unit.
		 * 
		 * @param previousUnit
		 *            the unit that precedes this one.
		 * @param table
		 *            the table we are joining to.
		 * @param sourceDataSetColumns
		 *            the columns in the existing dataset table that are used to
		 *            make the join.
		 * @param schemaSourceKey
		 *            the key in the schema table that we are joining to.
		 * @param schemaRelation
		 *            the relation we are following to make the join.
		 * @param schemaRelationIteration
		 *            the number of the compound relation, if it is compound.
		 *            Use 0 if it is not.
		 */
		public SkipTable(final TransformationUnit previousUnit,
				final Table table, final List sourceDataSetColumns,
				final Key schemaSourceKey, final Relation schemaRelation,
				final int schemaRelationIteration) {
			super(previousUnit, table, sourceDataSetColumns, schemaSourceKey,
					schemaRelation, schemaRelationIteration);
		}
	}

	/**
	 * This type of transformation unrolls tables.
	 */
	public static class UnrollTable extends TransformationUnit {
		private static final long serialVersionUID = 1L;

		private final Relation relation;

		private final List sourceDataSetColumns;

		private final UnrolledRelationDefinition unrolledDef;

		private final DataSetColumn unrolledIDColumn;

		private final DataSetColumn unrolledNameColumn;

		/**
		 * Instantiate a unit that selects from the given schema table.
		 * 
		 * @param previousUnit
		 *            the unit that precedes this one.
		 * @param relation
		 *            the relation we are unrolling.
		 * @param sourceDataSetColumns
		 *            the columns in the existing dataset table that are used to
		 *            make the join.
		 * @param unrolledDef
		 *            the unrolled relation definition.
		 * @param unrolledIDColumn
		 *            the unrolled ID column.
		 * @param unrolledNameColumn
		 *            the unrolled name column.
		 */
		public UnrollTable(final TransformationUnit previousUnit,
				final Relation relation, final List sourceDataSetColumns,
				final UnrolledRelationDefinition unrolledDef,
				final DataSetColumn unrolledIDColumn,
				final DataSetColumn unrolledNameColumn) {
			super(previousUnit);
			this.relation = relation;
			this.sourceDataSetColumns = sourceDataSetColumns;
			this.unrolledDef = unrolledDef;
			this.unrolledIDColumn = unrolledIDColumn;
			this.unrolledNameColumn = unrolledNameColumn;
		}

		/**
		 * @return the relation
		 */
		public Relation getRelation() {
			return this.relation;
		}

		/**
		 * @return the sourceDataSetColumns
		 */
		public List getSourceDataSetColumns() {
			return this.sourceDataSetColumns;
		}

		/**
		 * @return the unrolledDef
		 */
		public UnrolledRelationDefinition getUnrolledDef() {
			return this.unrolledDef;
		}

		/**
		 * @return the unrolledIDColumn
		 */
		public DataSetColumn getUnrolledIDColumn() {
			return this.unrolledIDColumn;
		}

		/**
		 * @return the unrolledNameColumn
		 */
		public DataSetColumn getUnrolledNameColumn() {
			return this.unrolledNameColumn;
		}

		public DataSetColumn getDataSetColumnFor(final Column column) {
			if (this.getPreviousUnit() != null)
				return this.getPreviousUnit().getDataSetColumnFor(column);
			else
				return null;
		}
	}
}
