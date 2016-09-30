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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.biomart.builder.exceptions.ValidationException;
import org.biomart.builder.model.Key.ForeignKey;
import org.biomart.builder.model.Key.PrimaryKey;
import org.biomart.common.exceptions.AssociationException;
import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.biomart.common.utils.BeanMap;
import org.biomart.common.utils.Transaction;
import org.biomart.common.utils.WeakPropertyChangeSupport;
import org.biomart.common.utils.Transaction.TransactionEvent;
import org.biomart.common.utils.Transaction.TransactionListener;

/**
 * A relation represents the association between two keys. Relations between two
 * primary keys are always 1:1. Relations between two foreign keys are either
 * 1:1 or M:M. Relations between a foreign key and a primary key can either be
 * 1:1 or 1:M.
 * <p>
 * Both keys must have the same number of columns, and the related columns
 * should appear in the same order in both keys. If they do not, then results
 * may be unpredictable.
 * <p>
 * A {@link Relation} class forms the basic functionality outlined above.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.58 $, $Date: 2008-03-06 11:32:30 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class Relation implements Comparable, TransactionListener {

	private static final long serialVersionUID = 1L;

	private Cardinality cardinality;

	private Cardinality originalCardinality;

	private final Key firstKey;

	private final Key secondKey;

	private Key oneKey;

	private Key manyKey;

	private boolean oneToManyAAllowed;

	private boolean oneToManyBAllowed;

	private boolean oneToOne;

	private boolean oneToManyA;

	private boolean oneToManyB;

	private boolean external;

	private ComponentStatus status;

	private boolean visibleModified = Transaction.getCurrentTransaction() == null ? false
			: Transaction.getCurrentTransaction().isAllowVisModChange();

	private boolean directModified = false;

	private final Map mods = new HashMap();

	private static final String DATASET_WIDE = "__DATASET_WIDE__";

	/**
	 * Subclasses use this field to fire events of their own.
	 */
	protected final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
			this);

	// All changes to us make us modified.
	private final PropertyChangeListener listener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			Relation.this.setDirectModified(true);
		}
	};

	// Add listeners to keys such that if the number of columns
	// no longer match, the relation will be removed.
	private final PropertyChangeListener keyColListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			if (Relation.this.firstKey.getColumns().length != Relation.this.secondKey
					.getColumns().length) {
				Relation.this.firstKey.getRelations().remove(Relation.this);
				Relation.this.secondKey.getRelations().remove(Relation.this);
			}
		}
	};

	/**
	 * This constructor tests that both ends of the relation have keys with the
	 * same number of columns. The default constructor sets the status to
	 * {@link ComponentStatus#INFERRED}.
	 * 
	 * @param firstKey
	 *            the first key.
	 * @param secondKey
	 *            the second key.
	 * @param cardinality
	 *            the cardinality of the foreign key end of this relation. If
	 *            both keys are primary keys, then this is ignored and defaults
	 *            to 1 (meaning 1:1). If they are a mixture, then this
	 *            differentiates between 1:1 and 1:M. If they are both foreign
	 *            keys, then this differentiates between 1:1 and M:M. See
	 *            {@link #setCardinality(Cardinality)}.
	 * @throws AssociationException
	 *             if the number of columns in the keys don't match, or if the
	 *             relation already exists.
	 */
	public Relation(Key firstKey, Key secondKey, final Cardinality cardinality)
			throws AssociationException {
		Log.debug("Creating relation between " + firstKey + " and " + secondKey
				+ " with cardinality " + cardinality);

		// Remember the keys etc.
		this.firstKey = firstKey;
		this.secondKey = secondKey;
		this.setOriginalCardinality(cardinality);
		this.setCardinality(cardinality);
		this.setStatus(ComponentStatus.INFERRED);

		// Check the keys have the same number of columns.
		if (firstKey.getColumns().length != secondKey.getColumns().length)
			throw new AssociationException(Resources
					.get("keyColumnCountMismatch"));
		// Check the relation doesn't already exist.
		if (firstKey.getRelations().contains(this))
			throw new AssociationException(Resources
					.get("relationAlreadyExists"));
		// Cannot place a relation on an FK to this table if it
		// already has relations.
		if (firstKey.getTable().equals(secondKey.getTable())
				&& (firstKey instanceof ForeignKey
						&& firstKey.getRelations().size() > 0 || secondKey instanceof ForeignKey
						&& secondKey.getRelations().size() > 0))
			throw new AssociationException(Resources
					.get("fkToThisOnceOrOthers"));
		// Cannot place a relation on an FK to another table if
		// it already has a relation to this table (it will have
		// only one due to previous check).
		if (!firstKey.getTable().equals(secondKey.getTable())
				&& ((firstKey instanceof ForeignKey
						&& firstKey.getRelations().size() == 1 && ((Relation) firstKey
						.getRelations().iterator().next())
						.getOtherKey(firstKey).getTable().equals(
								firstKey.getTable())) || (secondKey instanceof ForeignKey
						&& secondKey.getRelations().size() == 1 && ((Relation) secondKey
						.getRelations().iterator().next()).getOtherKey(
						secondKey).getTable().equals(secondKey.getTable()))))
			throw new AssociationException(Resources
					.get("fkToThisOnceOrOthers"));

		// Update flags.
		this.oneToManyAAllowed = this.secondKey instanceof ForeignKey;
		this.oneToManyBAllowed = this.firstKey instanceof ForeignKey;
		this.external = !this.firstKey.getTable().getSchema().equals(
				this.secondKey.getTable().getSchema());

		Transaction.addTransactionListener(this);

		this.firstKey.addPropertyChangeListener("columns", this.keyColListener);
		this.secondKey
				.addPropertyChangeListener("columns", this.keyColListener);

		this.addPropertyChangeListener("cardinality", this.listener);
		this.addPropertyChangeListener("originalCardinality", this.listener);
		this.addPropertyChangeListener("status", this.listener);
		this.addPropertyChangeListener("compoundRelation", this.listener);
		this.addPropertyChangeListener("unrolledRelation", this.listener);
		this.addPropertyChangeListener("forceRelation", this.listener);
		this.addPropertyChangeListener("loopbackRelation", this.listener);
		this.addPropertyChangeListener("maskRelation", this.listener);
		this.addPropertyChangeListener("mergeRelation", this.listener);
		this.addPropertyChangeListener("restrictRelation", this.listener);
		this.addPropertyChangeListener("subclassRelation", this.listener);
		this.addPropertyChangeListener("alternativeJoin", this.listener);
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
		// Don't care for now.
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
	 * Returns the cardinality of the foreign key end of this relation, in a 1:M
	 * relation. In 1:1 relations this will always return 1, and in M:M
	 * relations it will always return M.
	 * 
	 * @return the cardinality of the foreign key end of this relation, in 1:M
	 *         relations only. Otherwise determined by the relation type.
	 */
	public Cardinality getCardinality() {
		return this.cardinality;
	}

	/**
	 * Returns the original cardinality of the foreign key end of this relation,
	 * in a 1:M relation. In 1:1 relations this will always return 1, and in M:M
	 * relations it will always return M.
	 * 
	 * @return the original cardinality of the foreign key end of this relation,
	 *         in 1:M relations only. Otherwise determined by the relation type.
	 */
	public Cardinality getOriginalCardinality() {
		return this.originalCardinality;
	}

	/**
	 * Returns the first key of this relation. The concept of which key is first
	 * and which is second depends merely on the order they were passed to the
	 * constructor.
	 * 
	 * @return the first key.
	 */
	public Key getFirstKey() {
		return this.firstKey;
	}

	/**
	 * In a 1:M relation, this will return the M end of the relation. In all
	 * other relation types, this will return <tt>null</tt>.
	 * 
	 * @return the key at the many end of the relation, or <tt>null</tt> if
	 *         this is not a 1:M relation.
	 */
	public Key getManyKey() {
		return this.manyKey;
	}

	/**
	 * In a 1:M relation, this will return the 1 end of the relation. In all
	 * other relation types, this will return <tt>null</tt>.
	 * 
	 * @return the key at the one end of the relation, or <tt>null</tt> if
	 *         this is not a 1:M relation.
	 */
	public Key getOneKey() {
		return this.oneKey;
	}

	/**
	 * Given a key that is in this relationship, return the other key.
	 * 
	 * @param key
	 *            the key we know is in this relationship.
	 * @return the other key in this relationship, or <tt>null</tt> if the key
	 *         specified is not in this relationship.
	 */
	public Key getOtherKey(final Key key) {
		return this.firstKey.equals(key) ? this.secondKey : this.firstKey;
	}

	/**
	 * Returns the second key of this relation. The concept of which key is
	 * first and which is second depends merely on the order they were passed to
	 * the constructor.
	 * 
	 * @return the second key.
	 */
	public Key getSecondKey() {
		return this.secondKey;
	}

	/**
	 * Returns the status of this relation. The default value, unless otherwise
	 * specified, is {@link ComponentStatus#INFERRED}.
	 * 
	 * @return the status of this relation.
	 */
	public ComponentStatus getStatus() {
		return this.status;
	}

	/**
	 * Returns <tt>true</tt> if this relation involves keys in two separate
	 * schemas. Those that do are external, those that don't are not.
	 * 
	 * @return <tt>true</tt> if this is external, <tt>false</tt> otherwise.
	 */
	public boolean isExternal() {
		return this.external;
	}

	/**
	 * Returns <tt>true</tt> if this is a 1:M(a) relation.
	 * 
	 * @return <tt>true</tt> if this is a 1:M(a) relation, <tt>false</tt>
	 *         otherwise.
	 */
	public boolean isOneToManyA() {
		return this.oneToManyA;
	}

	/**
	 * Returns <tt>true</tt> if this is a 1:M(b) relation.
	 * 
	 * @return <tt>true</tt> if this is a 1:M(b) relation, <tt>false</tt>
	 *         otherwise.
	 */
	public boolean isOneToManyB() {
		return this.oneToManyB;
	}

	/**
	 * Returns <tt>true</tt> if this is either kind of 1:M relation.
	 * 
	 * @return <tt>true</tt> if this is either kind of 1:M relation,
	 *         <tt>false</tt> otherwise.
	 */
	public boolean isOneToMany() {
		return this.oneToManyA || this.oneToManyB;
	}

	/**
	 * Can this relation be 1:M(a)? Returns <tt>true</tt> in all cases where
	 * the two keys are of different types.
	 * 
	 * @return <tt>true</tt> if this can be 1:M(a), <tt>false</tt> if not.
	 */
	public boolean isOneToManyAAllowed() {
		return this.oneToManyAAllowed;
	}

	/**
	 * Can this relation be 1:M(b)? Returns <tt>true</tt> in all cases where
	 * the two keys are of different types.
	 * 
	 * @return <tt>true</tt> if this can be 1:M(b), <tt>false</tt> if not.
	 */
	public boolean isOneToManyBAllowed() {
		return this.oneToManyBAllowed;
	}

	/**
	 * Returns <tt>true</tt> if this is a 1:1 relation.
	 * 
	 * @return <tt>true</tt> if this is a 1:1 relation, <tt>false</tt>
	 *         otherwise.
	 */
	public boolean isOneToOne() {
		return this.oneToOne;
	}

	/**
	 * Returns the key in this relation associated with the given table. If both
	 * keys are on that table, returns the one that is a PK, or the first one if
	 * both are FKs.
	 * 
	 * @param table
	 *            the table to get the key for.
	 * @return the key for that table. <tt>null</tt> if neither key is from
	 *         that table.
	 */
	public Key getKeyForTable(final Table table) {
		return this.firstKey.getTable().equals(table) ? this.firstKey
				: this.secondKey;
	}

	/**
	 * Returns the key in this relation associated with the given schema. If
	 * both keys are on tables in that schema, returns the first one.
	 * 
	 * @param schema
	 *            the schema to get the key for.
	 * @return the key for that schema. <tt>null</tt> if neither key is from
	 *         that schema.
	 */
	public Key getKeyForSchema(final Schema schema) {
		return this.firstKey.getTable().getSchema().equals(schema) ? this.firstKey
				: this.secondKey;
	}

	/**
	 * Sets the cardinality of the foreign key end of this relation, in a 1:M
	 * relation. If used on a 1:1 or M:M relation, then specifying M makes it
	 * M:M and specifying 1 makes it 1:1.
	 * 
	 * @param cardinality
	 *            the cardinality.
	 */
	public void setCardinality(Cardinality cardinality) {
		Log.debug("Changing cardinality of " + this + " to " + cardinality);
		if (this.firstKey instanceof PrimaryKey
				&& this.secondKey instanceof PrimaryKey) {
			Log.debug("Overriding cardinality change to ONE");
			cardinality = Cardinality.ONE;
		}

		// TODO This is a backwards-compatibility clause that needs to
		// stay in throughout the 0.7 release. It can be removed in 0.8.
		if (cardinality == Cardinality.MANY
				&& this.secondKey instanceof PrimaryKey) {
			cardinality = Cardinality.MANY_B;
		} else if (cardinality == Cardinality.MANY
				&& this.firstKey instanceof PrimaryKey) {
			cardinality = Cardinality.MANY_A;
		}
		// End fudge-mode.

		final Cardinality oldValue = this.cardinality;
		if (this.cardinality == cardinality || this.cardinality != null
				&& this.cardinality.equals(cardinality))
			return;
		this.cardinality = cardinality;

		if (this.cardinality.equals(Cardinality.ONE)) {
			this.oneToOne = true;
			this.oneToManyA = false;
			this.oneToManyB = false;
			this.oneKey = null;
			this.manyKey = null;
		} else if (this.cardinality.equals(Cardinality.MANY_A)) {
			this.oneToOne = false;
			this.oneToManyA = true;
			this.oneToManyB = false;
			this.oneKey = this.firstKey;
			this.manyKey = this.secondKey;
		} else if (this.cardinality.equals(Cardinality.MANY_B)) {
			this.oneToOne = false;
			this.oneToManyA = false;
			this.oneToManyB = true;
			this.oneKey = this.secondKey;
			this.manyKey = this.firstKey;
		} else {
			// TODO This is a backwards-compatibility clause that needs to
			// stay in throughout the 0.7 release. It can be removed in 0.8.
			this.oneToOne = false;
			this.oneToManyA = false;
			this.oneToManyB = false;
			this.oneKey = null;
			this.manyKey = null;
			// End fudge-mode.
		}
		if (Transaction.getCurrentTransaction() != null
				&& Transaction.getCurrentTransaction().isAllowVisModChange())
			this.setVisibleModified(true);
		this.pcs.firePropertyChange("cardinality", oldValue, cardinality);
	}

	/**
	 * Sets the original cardinality of the foreign key end of this relation, in
	 * a 1:M relation. If used on a 1:1 or M:M relation, then specifying M makes
	 * it M:M and specifying 1 makes it 1:1.
	 * 
	 * @param originalCardinality
	 *            the originalCardinality.
	 */
	public void setOriginalCardinality(Cardinality originalCardinality) {
		Log.debug("Changing original cardinality of " + this + " to "
				+ originalCardinality);
		final Cardinality oldValue = this.originalCardinality;
		if (this.originalCardinality == originalCardinality
				|| this.originalCardinality != null
				&& this.originalCardinality.equals(originalCardinality))
			return;
		this.originalCardinality = originalCardinality;

		// TODO This is a backwards-compatibility clause that needs to
		// stay in throughout the 0.7 release. It can be removed in 0.8.
		if (oldValue == Cardinality.MANY
				&& !(this.firstKey instanceof PrimaryKey || this.secondKey instanceof PrimaryKey))
			return;
		// End fudge-mode.

		this.pcs.firePropertyChange("originalCardinality", oldValue,
				originalCardinality);
	}

	/**
	 * Sets the status of this relation.
	 * 
	 * @param status
	 *            the new status of this relation.
	 * @throws AssociationException
	 *             if the keys at either end of the relation are incompatible
	 *             upon attempting to mark an
	 *             {@link ComponentStatus#INFERRED_INCORRECT} relation as
	 *             anything else.
	 */
	public void setStatus(final ComponentStatus status)
			throws AssociationException {
		Log.debug("Changing status of " + this + " to " + status);
		// If the new status is not incorrect, we need to make sure we
		// can legally do this, ie. the two keys have the same number of
		// columns each.
		if (!status.equals(ComponentStatus.INFERRED_INCORRECT))
			// Check both keys have same cardinality.
			if (this.firstKey.getColumns().length != this.secondKey
					.getColumns().length)
				throw new AssociationException(Resources
						.get("keyColumnCountMismatch"));
		final ComponentStatus oldValue = this.status;
		if (this.status == status || this.status != null
				&& this.status.equals(status))
			return;
		// Make the change.
		this.status = status;
		this.pcs.firePropertyChange("status", oldValue, status);
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
			tableKey = Relation.DATASET_WIDE;
		if (!this.mods.containsKey(dataset))
			this.mods.put(dataset, new HashMap());
		final Map dsMap = (Map) this.mods.get(dataset);
		if (!dsMap.containsKey(tableKey))
			dsMap.put(tableKey.intern(), new HashMap());
		return (Map) dsMap.get(tableKey);
	}

	/**
	 * Is this relation subclassed?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isSubclassRelation(final DataSet dataset) {
		return this.getMods(dataset, null).containsKey("subclassRelation");
	}

	/**
	 * Subclass this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param subclass
	 *            <tt>true</tt> to subclass it, <tt>false</tt> to not.
	 * @throws ValidationException
	 *             if it cannot make the change.
	 */
	public void setSubclassRelation(final DataSet dataset,
			final boolean subclass) throws ValidationException {
		final boolean oldValue = this.isSubclassRelation(dataset);
		if (subclass == oldValue)
			return;
		if (subclass) {
			// Work out the child end of the relation - the M end. The parent is
			// the 1 end.
			final Table parentTable = this.getOneKey().getTable();
			final Table childTable = this.getManyKey().getTable();
			if (parentTable.equals(childTable))
				throw new ValidationException(Resources
						.get("subclassNotBetweenTwoTables"));
			if (parentTable.getPrimaryKey() == null
					|| childTable.getPrimaryKey() == null)
				throw new ValidationException(Resources
						.get("subclassTargetNoPK"));

			// We need to test if the selected relation links to
			// a table which itself has subclass relations, or
			// is the central table, and has not got an
			// existing subclass relation in the direction we
			// are working in.
			boolean hasConflict = false;
			final Set combinedRels = new HashSet();
			combinedRels.addAll(parentTable.getRelations());
			combinedRels.addAll(childTable.getRelations());
			for (final Iterator i = combinedRels.iterator(); i.hasNext()
					&& !hasConflict;) {
				final Relation rel = (Relation) i.next();
				if (!rel.isSubclassRelation(dataset))
					continue;
				else if (rel.getOneKey().getTable().equals(parentTable)
						|| rel.getManyKey().getTable().equals(childTable))
					hasConflict = true;
			}
			// If child has M:1 or parent has 1:M, we cannot do this.
			if (hasConflict)
				throw new ValidationException(Resources
						.get("mixedCardinalitySubclasses"));

			// Now do it.
			this.getMods(dataset, null).put("subclassRelation", null);
			this.pcs.firePropertyChange("subclassRelation", null, dataset);
		} else {
			// Break the chain first.
			final Key key = this.getManyKey();
			if (key != null) {
				final Table target = key.getTable();
				if (!target.equals(dataset.getCentralTable()))
					if (target.getPrimaryKey() != null)
						for (final Iterator i = target.getPrimaryKey()
								.getRelations().iterator(); i.hasNext();) {
							final Relation rel = (Relation) i.next();
							if (rel.isOneToMany())
								rel.setSubclassRelation(dataset, false);
						}
			}

			// Now do it.
			this.getMods(dataset, null).remove("subclassRelation");
			this.pcs.firePropertyChange("subclassRelation", dataset, null);
		}
	}

	/**
	 * Is this relation merged?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isMergeRelation(final DataSet dataset) {
		return this.getMods(dataset, null).containsKey("mergeRelation");
	}

	/**
	 * Merge this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param merge
	 *            <tt>true</tt> to merge it, <tt>false</tt> to not.
	 */
	public void setMergeRelation(final DataSet dataset, final boolean merge) {
		final boolean oldValue = this.isMergeRelation(dataset);
		if (merge == oldValue)
			return;
		if (merge) {
			this.getMods(dataset, null).put("mergeRelation", null);
			this.pcs.firePropertyChange("mergeRelation", null, dataset);
		} else {
			this.getMods(dataset, null).remove("mergeRelation");
			this.pcs.firePropertyChange("mergeRelation", dataset, null);
		}
	}

	/**
	 * Is this relation masked?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isMaskRelation(final DataSet dataset) {
		return this.getMods(dataset, null).containsKey("maskRelation");
	}

	/**
	 * Is this relation masked?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isMaskRelation(final DataSet dataset, final String tableKey) {
		return this.isMaskRelation(dataset)
				|| this.getMods(dataset, tableKey).containsKey("maskRelation");
	}

	/**
	 * Mask this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param mask
	 *            <tt>true</tt> to mask it, <tt>false</tt> to not.
	 */
	public void setMaskRelation(final DataSet dataset, final boolean mask) {
		final boolean oldValue = this.isMaskRelation(dataset);
		if (mask == oldValue)
			return;
		if (mask) {
			this.getMods(dataset, null).put("maskRelation", null);
			this.pcs.firePropertyChange("maskRelation", null, dataset);
		} else {
			this.getMods(dataset, null).remove("maskRelation");
			this.pcs.firePropertyChange("maskRelation", dataset, null);
		}
	}

	/**
	 * Mask this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param mask
	 *            <tt>true</tt> to mask it, <tt>false</tt> to not.
	 */
	public void setMaskRelation(final DataSet dataset, final String tableKey,
			final boolean mask) {
		final boolean oldValue = this.isMaskRelation(dataset, tableKey);
		if (mask == oldValue)
			return;
		if (mask) {
			this.getMods(dataset, tableKey).put("maskRelation", null);
			this.pcs.firePropertyChange("maskRelation", null, tableKey);
		} else {
			this.getMods(dataset, tableKey).remove("maskRelation");
			this.pcs.firePropertyChange("maskRelation", tableKey, null);
		}
	}

	/**
	 * Is this relation alternative-joined?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isAlternativeJoin(final DataSet dataset,
			final String tableKey) {
		return this.getMods(dataset, tableKey).containsKey("alternativeJoin");
	}

	/**
	 * Alternative-join this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param join
	 *            <tt>true</tt> to do it, <tt>false</tt> to not.
	 */
	public void setAlternativeJoin(final DataSet dataset,
			final String tableKey, final boolean join) {
		final boolean oldValue = this.isAlternativeJoin(dataset, tableKey);
		if (join == oldValue)
			return;
		if (join) {
			this.getMods(dataset, tableKey).put("alternativeJoin", null);
			this.pcs.firePropertyChange("alternativeJoin", null, tableKey);
		} else {
			this.getMods(dataset, tableKey).remove("alternativeJoin");
			this.pcs.firePropertyChange("alternativeJoin", tableKey, null);
		}
	}

	/**
	 * Is this relation forced?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isForceRelation(final DataSet dataset) {
		return this.getMods(dataset, null).containsKey("forceRelation");
	}

	/**
	 * Is this relation forced?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isForceRelation(final DataSet dataset, final String tableKey) {
		return this.isForceRelation(dataset)
				|| this.getMods(dataset, tableKey).containsKey("forceRelation");
	}

	/**
	 * Force this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param forced
	 *            <tt>true</tt> to force it, <tt>false</tt> to not.
	 */
	public void setForceRelation(final DataSet dataset, final boolean forced) {
		final boolean oldValue = this.isForceRelation(dataset);
		if (forced == oldValue)
			return;
		if (forced) {
			this.getMods(dataset, null).put("forceRelation", null);
			this.pcs.firePropertyChange("forceRelation", null, dataset);
		} else {
			this.getMods(dataset, null).remove("forceRelation");
			this.pcs.firePropertyChange("forceRelation", dataset, null);
		}
	}

	/**
	 * Force this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param forced
	 *            <tt>true</tt> to force it, <tt>false</tt> to not.
	 */
	public void setForceRelation(final DataSet dataset, final String tableKey,
			final boolean forced) {
		final boolean oldValue = this.isForceRelation(dataset, tableKey);
		if (forced == oldValue)
			return;
		if (forced) {
			this.getMods(dataset, tableKey).put("forceRelation", null);
			this.pcs.firePropertyChange("forceRelation", null, tableKey);
		} else {
			this.getMods(dataset, tableKey).remove("forceRelation");
			this.pcs.firePropertyChange("forceRelation", tableKey, null);
		}
	}

	/**
	 * Is this relation loopbacked?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return true if it is, false otherwise.
	 */
	public boolean isLoopbackRelation(final DataSet dataset) {
		return this.getMods(dataset, null).containsKey("loopbackRelation");
	}

	/**
	 * Is this relation loopbacked?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return the column to use if it is, null otherwise.
	 */
	public Column getLoopbackRelation(final DataSet dataset) {
		return (Column) this.getMods(dataset, null).get("loopbackRelation");
	}

	/**
	 * Is this relation loopbacked?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return true if it is, false otherwise.
	 */
	public boolean isLoopbackRelation(final DataSet dataset,
			final String tableKey) {
		return this.getMods(dataset, tableKey).containsKey("loopbackRelation");
	}

	/**
	 * Is this relation loopbacked?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return the column to use if it is, null otherwise.
	 */
	public Column getLoopbackRelation(final DataSet dataset,
			final String tableKey) {
		Column result = (Column) this.getMods(dataset, tableKey).get(
				"loopbackRelation");
		if (result == null)
			result = this.getLoopbackRelation(dataset);
		return result;
	}

	/**
	 * Loopback this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param loopback
	 *            the column to set - if null, it undoes it.
	 * @throws ValidationException
	 *             if it cannot be done.
	 */
	public void setLoopbackRelation(final DataSet dataset, final Column loopback)
			throws ValidationException {
		final Column oldValue = this.getLoopbackRelation(dataset);
		if (loopback == oldValue || oldValue != null
				&& oldValue.equals(loopback))
			return;

		// Check that the relation is a 1:M relation.
		if (!this.isOneToMany())
			throw new ValidationException(Resources.get("loopbackNotOneMany"));

		if (loopback != null) {
			this.getMods(dataset, null).put("loopbackRelation", loopback);
			this.pcs.firePropertyChange("loopbackRelation", null, dataset);
		} else {
			this.getMods(dataset, null).remove("loopbackRelation");
			this.pcs.firePropertyChange("loopbackRelation", dataset, null);
		}
	}

	/**
	 * Loopback this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param loopback
	 *            the column to set - if null, it undoes it.
	 * @throws ValidationException
	 *             if it cannot be done.
	 */
	public void setLoopbackRelation(final DataSet dataset,
			final String tableKey, final Column loopback)
			throws ValidationException {
		final Column oldValue = this.getLoopbackRelation(dataset, tableKey);
		if (loopback == oldValue || oldValue != null
				&& oldValue.equals(loopback))
			return;

		// Check that the relation is a 1:M relation.
		if (!this.isOneToMany())
			throw new ValidationException(Resources.get("loopbackNotOneMany"));

		if (loopback != null) {
			this.getMods(dataset, tableKey).put("loopbackRelation", loopback);
			this.pcs.firePropertyChange("loopbackRelation", null, tableKey);
		} else {
			this.getMods(dataset, tableKey).remove("loopbackRelation");
			this.pcs.firePropertyChange("loopbackRelation", tableKey, null);
		}
	}

	/**
	 * Is this relation restricted, anywhere (regardless of iteration)?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isRestrictRelation(final DataSet dataset,
			final String tableKey) {
		return this.getMods(dataset, tableKey).containsKey("restrictRelation")
				&& !((Map) this.getMods(dataset, tableKey).get(
						"restrictRelation")).isEmpty();
	}

	/**
	 * Is this relation compounded?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @param n
	 *            the index to lookup. 0 is first.
	 * @return the def to use if it is, null otherwise.
	 */
	public RestrictedRelationDefinition getRestrictRelation(
			final DataSet dataset, final String tableKey, final int n) {
		return !this.getMods(dataset, tableKey).containsKey("restrictRelation") ? null
				: (RestrictedRelationDefinition) ((Map) this.getMods(dataset,
						tableKey).get("restrictRelation")).get(new Integer(n));
	}

	/**
	 * Restrict this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param n
	 *            the index of the relation to restrict - 0 is first.
	 * @param def
	 *            the definition to set - if null, it undoes it.
	 */
	public void setRestrictRelation(final DataSet dataset,
			final String tableKey, final RestrictedRelationDefinition def,
			final int n) {
		final RestrictedRelationDefinition oldValue = this.getRestrictRelation(
				dataset, tableKey, n);
		if (def == oldValue || oldValue != null && oldValue.equals(def))
			return;

		if (def != null) {
			if (!this.getMods(dataset, tableKey)
					.containsKey("restrictRelation"))
				this.getMods(dataset, tableKey).put("restrictRelation",
						new HashMap());
			((Map) this.getMods(dataset, tableKey).get("restrictRelation"))
					.put(new Integer(n), def);
			def.addPropertyChangeListener("directModified", this.listener);
			this.pcs.firePropertyChange("restrictRelation", null, tableKey);
		} else {
			if (this.getMods(dataset, tableKey).containsKey("restrictRelation"))
				((Map) this.getMods(dataset, tableKey).get("restrictRelation"))
						.remove(new Integer(n));
			this.pcs.firePropertyChange("restrictRelation", tableKey, null);
		}
	}

	/**
	 * Is this relation unrolled?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return the def to use if it is, null otherwise.
	 */
	public UnrolledRelationDefinition getUnrolledRelation(final DataSet dataset) {
		return (UnrolledRelationDefinition) this.getMods(dataset, null).get(
				"unrolledRelation");
	}

	/**
	 * Unroll this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param def
	 *            the definition to set - if null, it undoes it.
	 */
	public void setUnrolledRelation(final DataSet dataset,
			final UnrolledRelationDefinition def) {
		final UnrolledRelationDefinition oldValue = this
				.getUnrolledRelation(dataset);
		if (def == oldValue || oldValue != null && oldValue.equals(def))
			return;

		if (def != null) {
			this.getMods(dataset, null).put("unrolledRelation", def);
			def.addPropertyChangeListener("directModified", this.listener);
			this.pcs.firePropertyChange("unrolledRelation", null, dataset);
		} else {
			this.getMods(dataset, null).remove("unrolledRelation");
			this.pcs.firePropertyChange("unrolledRelation", dataset, null);
		}
	}

	/**
	 * Is this relation compounded?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return the def to use if it is, null otherwise.
	 */
	public CompoundRelationDefinition getCompoundRelation(final DataSet dataset) {
		return (CompoundRelationDefinition) this.getMods(dataset, null).get(
				"compoundRelation");
	}

	/**
	 * Is this relation compounded?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @return true if it is, false otherwise.
	 */
	public boolean isCompoundRelation(final DataSet dataset) {
		return this.getMods(dataset, null).containsKey("compoundRelation");
	}

	/**
	 * Is this relation compounded?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return the def to use if it is, null otherwise.
	 */
	public CompoundRelationDefinition getCompoundRelation(
			final DataSet dataset, final String tableKey) {
		CompoundRelationDefinition result = (CompoundRelationDefinition) this
				.getMods(dataset, tableKey).get("compoundRelation");
		if (result == null)
			result = this.getCompoundRelation(dataset);
		return result;
	}

	/**
	 * Is this relation compounded?
	 * 
	 * @param dataset
	 *            the dataset to check for.
	 * @param tableKey
	 *            the table to check for.
	 * @return true if it is, false otherwise.
	 */
	public boolean isCompoundRelation(final DataSet dataset,
			final String tableKey) {
		return this.getMods(dataset, tableKey).containsKey("compoundRelation");
	}

	/**
	 * Compound this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param def
	 *            the definition to set - if null, it undoes it.
	 */
	public void setCompoundRelation(final DataSet dataset,
			final CompoundRelationDefinition def) {
		final CompoundRelationDefinition oldValue = this
				.getCompoundRelation(dataset);
		if (def == oldValue || oldValue != null && oldValue.equals(def))
			return;

		if (def != null) {
			this.getMods(dataset, null).put("compoundRelation", def);
			def.addPropertyChangeListener("directModified", this.listener);
			this.pcs.firePropertyChange("compoundRelation", null, dataset);
		} else {
			this.getMods(dataset, null).remove("compoundRelation");
			this.pcs.firePropertyChange("compoundRelation", dataset, null);
		}
	}

	/**
	 * Compound this relation.
	 * 
	 * @param dataset
	 *            the dataset to set for.
	 * @param tableKey
	 *            the dataset table to set for.
	 * @param def
	 *            the definition to set - if null, it undoes it.
	 */
	public void setCompoundRelation(final DataSet dataset,
			final String tableKey, final CompoundRelationDefinition def) {
		final CompoundRelationDefinition oldValue = this.getCompoundRelation(
				dataset, tableKey);
		if (def == oldValue || oldValue != null && oldValue.equals(def))
			return;

		if (def != null) {
			this.getMods(dataset, tableKey).put("compoundRelation", def);
			def.addPropertyChangeListener("directModified", this.listener);
			this.pcs.firePropertyChange("compoundRelation", null, tableKey);
		} else {
			this.getMods(dataset, tableKey).remove("compoundRelation");
			this.pcs.firePropertyChange("compoundRelation", tableKey, null);
		}
	}

	public int compareTo(final Object o) throws ClassCastException {
		final Relation r = (Relation) o;
		if (this.firstKey.equals(r.firstKey))
			return this.secondKey.compareTo(r.secondKey);
		else if (this.firstKey.equals(r.secondKey))
			return this.secondKey.compareTo(r.firstKey);
		else if (this.secondKey.equals(r.firstKey))
			return this.firstKey.compareTo(r.secondKey);
		else
			return this.firstKey.compareTo(r.firstKey);
	}

	public int hashCode() {
		final int firstHash = this.firstKey.hashCode();
		final int secondHash = this.secondKey.hashCode();
		// So that two rels between same keys always match.
		return (Math.min(firstHash, secondHash) + "_" + Math.max(firstHash,
				secondHash)).hashCode();
	}

	public boolean equals(final Object o) {
		if (o == this)
			return true;
		else if (o == null)
			return false;
		else if (o instanceof Relation) {
			final Relation r = (Relation) o;
			// Check that the same keys are involved.
			return r.firstKey.equals(this.secondKey)
					&& r.secondKey.equals(this.firstKey)
					|| r.firstKey.equals(this.firstKey)
					&& r.secondKey.equals(this.secondKey);
		} else
			return false;
	}

	public String toString() {
		final StringBuffer sb = new StringBuffer();
		sb.append(this.firstKey == null ? "<undef>" : this.firstKey.toString());
		sb.append(" -> ");
		sb.append(this.secondKey == null ? "<undef>" : this.secondKey
				.toString());
		return sb.toString();
	}

	/**
	 * This internal singleton class represents the cardinality of a relation.
	 * Note that the names of cardinality objects are case-sensitive.
	 */
	public static class Cardinality implements Comparable {
		private static final long serialVersionUID = 1L;

		private static final Map singletons = new HashMap();

		/**
		 * Use this constant to refer to a relation with many values at the
		 * second key end.
		 */
		public static final Cardinality MANY_B = Cardinality.get("M(b)");

		/**
		 * Use this constant to refer to a relation with many values at the
		 * first key end.
		 */
		public static final Cardinality MANY_A = Cardinality.get("M(a)");

		// TODO This is a backwards-compatibility clause that needs to
		// stay in throughout the 0.7 release. It can be removed in 0.8.
		/**
		 * Use this constant to refer to a relation with many values at the
		 * first key end.
		 */
		public static final Cardinality MANY = Cardinality.get("M");

		// End fudge-mode.

		/**
		 * Use this constant to refer to a 1:1 relation.
		 */
		public static final Cardinality ONE = Cardinality.get("1");

		/**
		 * The static factory method creates and returns a cardinality with the
		 * given name. It ensures the object returned is a singleton. Note that
		 * the names of cardinality objects are case-sensitive.
		 * 
		 * @param name
		 *            the name of the cardinality object.
		 * @return the cardinality object or null if null was passed in.
		 */
		public static Cardinality get(String name) {
			// Return null for null name.
			if (name == null)
				return null;

			// Do we already have this one?
			// If so, then return it.
			if (Cardinality.singletons.containsKey(name))
				return (Cardinality) Cardinality.singletons.get(name);

			// Otherwise, create it, remember it.
			final Cardinality c = new Cardinality(name);
			Cardinality.singletons.put(name, c);

			// Return it.
			return c;
		}

		private final String name;

		/**
		 * The private constructor takes a single parameter, which defines the
		 * name this cardinality object will display when printed.
		 * 
		 * @param name
		 *            the name of the cardinality.
		 */
		private Cardinality(final String name) {
			this.name = name;
		}

		public int compareTo(final Object o) throws ClassCastException {
			final Cardinality c = (Cardinality) o;
			return this.toString().compareTo(c.toString());
		}

		public boolean equals(final Object o) {
			// We are dealing with singletons so can use == happily.
			return o == this;
		}

		/**
		 * Displays the name of this cardinality object.
		 * 
		 * @return the name of this cardinality object.
		 */
		public String getName() {
			return this.name;
		}

		public int hashCode() {
			return this.toString().hashCode();
		}

		/**
		 * {@inheritDoc}
		 * <p>
		 * Always returns the name of this cardinality.
		 */
		public String toString() {
			return this.name;
		}
	}

	/**
	 * Defines a compound relation.
	 */
	public static class CompoundRelationDefinition implements
			TransactionListener {
		private static final long serialVersionUID = 1L;

		private int n;

		private boolean parallel;

		private boolean directModified = false;

		private final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
				this);

		private final PropertyChangeListener listener = new PropertyChangeListener() {
			public void propertyChange(final PropertyChangeEvent e) {
				CompoundRelationDefinition.this.setDirectModified(true);
			}
		};

		/**
		 * This constructor gives the compound relation an arity and a flag
		 * indicating whether to follow the multiple copies in parallel or
		 * serial.
		 * 
		 * @param n
		 *            the number of times this relation has been compounded (the
		 *            arity).
		 * @param parallel
		 *            whether this is a parallel (<tt>true</tt>) or serial (<tt>false</tt>)
		 *            compounding.
		 */
		public CompoundRelationDefinition(final int n, final boolean parallel) {
			// Remember the settings.
			this.n = n;
			this.parallel = parallel;

			Transaction.addTransactionListener(this);

			this.addPropertyChangeListener("n", this.listener);
			this.addPropertyChangeListener("parallel", this.listener);
		}

		/**
		 * Replicate ourselves.
		 * 
		 * @return the copy.
		 */
		public CompoundRelationDefinition replicate() {
			return new CompoundRelationDefinition(this.n, this.parallel);
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
		 * Get the arity of this compound relation.
		 * 
		 * @return the arity.
		 */
		public int getN() {
			return this.n;
		}

		/**
		 * Set the arity of this compound relation.
		 * 
		 * @param n
		 *            the new arity.
		 */
		public void setN(final int n) {
			if (n == this.n)
				return;
			final int oldValue = this.n;
			this.n = n;
			this.pcs.firePropertyChange("n", oldValue, n);
		}

		/**
		 * Is this compound relation parallel?
		 * 
		 * @return <tt>true</tt> if it is.
		 */
		public boolean isParallel() {
			return this.parallel;
		}

		/**
		 * Is this compound relation parallel?
		 * 
		 * @param parallel
		 *            <tt>true</tt> if it is.
		 */
		public void setParallel(final boolean parallel) {
			if (parallel == this.parallel)
				return;
			final boolean oldValue = this.parallel;
			this.parallel = parallel;
			this.pcs.firePropertyChange("parallel", oldValue, parallel);
		}
	}

	/**
	 * Defines an unrolled relation.
	 */
	public static class UnrolledRelationDefinition implements
			TransactionListener {
		private static final long serialVersionUID = 1L;

		private Column nameColumn;

		private boolean reversed;

		private boolean directModified = false;

		private final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
				this);

		private final PropertyChangeListener listener = new PropertyChangeListener() {
			public void propertyChange(final PropertyChangeEvent e) {
				UnrolledRelationDefinition.this.setDirectModified(true);
			}
		};

		/**
		 * This constructor gives the unrolled relation a name column and a flag
		 * indicating whether to reverse the sense of the unrolling.
		 * 
		 * @param nameColumn
		 *            the column to name each unrolled record with.
		 * @param reversed
		 *            whether this is a reversed (<tt>true</tt>) unrolling.
		 */
		public UnrolledRelationDefinition(final Column nameColumn,
				final boolean reversed) {
			// Remember the settings.
			this.nameColumn = nameColumn;
			this.reversed = reversed;

			Transaction.addTransactionListener(this);

			this.addPropertyChangeListener("nameColumn", this.listener);
			this.addPropertyChangeListener("reversed", this.listener);
		}

		/**
		 * Replicate ourselves.
		 * 
		 * @return the copy.
		 */
		public UnrolledRelationDefinition replicate() {
			return new UnrolledRelationDefinition(this.nameColumn,
					this.reversed);
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
		 * Get the name column of this unrolled relation.
		 * 
		 * @return the column.
		 */
		public Column getNameColumn() {
			return this.nameColumn;
		}

		/**
		 * Set the name column of this unrolled relation.
		 * 
		 * @param nameColumn
		 *            the column.
		 */
		public void setNameColumn(final Column nameColumn) {
			final Column oldValue = this.nameColumn;
			if (nameColumn == oldValue || oldValue != null
					&& oldValue.equals(nameColumn))
				return;
			this.nameColumn = nameColumn;
			this.pcs.firePropertyChange("nameColumn", oldValue, nameColumn);
		}

		/**
		 * Is this unrolled relation reversed?
		 * 
		 * @return <tt>true</tt> if it is.
		 */
		public boolean isReversed() {
			return this.reversed;
		}

		/**
		 * Is this unrolled relation reversed?
		 * 
		 * @param reversed
		 *            <tt>true</tt> if it is.
		 */
		public void setReversed(final boolean reversed) {
			if (reversed == this.reversed)
				return;
			final boolean oldValue = this.reversed;
			this.reversed = reversed;
			this.pcs.firePropertyChange("reversed", oldValue, reversed);
		}
	}

	/**
	 * Defines the restriction on a table, ie. a where-clause.
	 */
	public static class RestrictedRelationDefinition implements
			TransactionListener {
		private static final long serialVersionUID = 1L;

		private BeanMap leftAliases;

		private BeanMap rightAliases;

		private String expr;

		private boolean directModified = false;

		private final WeakPropertyChangeSupport pcs = new WeakPropertyChangeSupport(
				this);

		private final PropertyChangeListener listener = new PropertyChangeListener() {
			public void propertyChange(final PropertyChangeEvent e) {
				RestrictedRelationDefinition.this.setDirectModified(true);
			}
		};

		/**
		 * This constructor gives the restriction an initial expression and a
		 * set of aliases. The expression may not be empty, and neither can the
		 * alias map.
		 * 
		 * @param expr
		 *            the expression to define for this restriction.
		 * @param leftAliases
		 *            the aliases to use for columns on the LHS of the join.
		 * @param rightAliases
		 *            the aliases to use for columns on the RHS of the join.
		 */
		public RestrictedRelationDefinition(final String expr, Map leftAliases,
				Map rightAliases) {
			// Test for good arguments.
			if (expr == null || expr.trim().length() == 0)
				throw new IllegalArgumentException(Resources
						.get("relRestrictMissingExpression"));
			if (leftAliases == null)
				leftAliases = new HashMap();
			if (rightAliases == null)
				rightAliases = new HashMap();
			if (leftAliases.isEmpty() && rightAliases.isEmpty())
				throw new IllegalArgumentException(Resources
						.get("relRestrictMissingAliases"));

			// Remember the settings.
			this.leftAliases = new BeanMap(new HashMap(leftAliases));
			this.rightAliases = new BeanMap(new HashMap(rightAliases));
			this.expr = expr;

			Transaction.addTransactionListener(this);

			this.addPropertyChangeListener(this.listener);
			this.leftAliases.addPropertyChangeListener(this.listener);
			this.rightAliases.addPropertyChangeListener(this.listener);
		}

		/**
		 * Replicate ourselves.
		 * 
		 * @return the copy.
		 */
		public RestrictedRelationDefinition replicate() {
			return new RestrictedRelationDefinition(this.expr,
					this.leftAliases, this.rightAliases);
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
		 * Retrieves the map used for setting up aliases.
		 * 
		 * @return the aliases map. Keys must be {@link Column} instances, and
		 *         values are aliases used in the expression.
		 */
		public BeanMap getLeftAliases() {
			return this.leftAliases;
		}

		/**
		 * Retrieves the map used for setting up aliases.
		 * 
		 * @return the aliases map. Keys must be {@link Column} instances, and
		 *         values are aliases used in the expression.
		 */
		public BeanMap getRightAliases() {
			return this.rightAliases;
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

		/**
		 * Returns the expression, <i>with</i> substitution. This value is
		 * RDBMS-specific. The prefix map must contain two entries. Each entry
		 * relates to one of the keys of a relation. The key of the map is the
		 * key of the relation, and the value is the prefix to use in the
		 * substituion, eg. "a" if columns for the table for that key should be
		 * prefixed as "a.mycolumn".
		 * 
		 * @param schemaPrefix
		 *            the value to substitute for ':schemaPrefix'.
		 * @param leftTablePrefix
		 *            the prefix to use for the LHS table in the expression.
		 * @param rightTablePrefix
		 *            the prefix to use for the LHS table in the expression.
		 * @param leftIsDataSet
		 *            if the LHS side is a dataset table.
		 * @param rightIsDataSet
		 *            if the RHS side is a dataset table.
		 * @param mappingUnit
		 *            the transformation unit this restriction will use to
		 *            translate columns into dataset column equivalents.
		 * @return the substituted expression.
		 */
		public String getSubstitutedExpression(final String schemaPrefix,
				final String leftTablePrefix, final String rightTablePrefix,
				final boolean leftIsDataSet, final boolean rightIsDataSet,
				final TransformationUnit mappingUnit) {
			Log.debug("Calculating restricted table expression");
			String sub = this.expr;
			for (final Iterator i = this.leftAliases.entrySet().iterator(); i
					.hasNext();) {
				final Map.Entry entry = (Map.Entry) i.next();
				final Column col = (Column) entry.getKey();
				final String alias = ":" + (String) entry.getValue();
				sub = sub.replaceAll(alias, leftTablePrefix
						+ "."
						+ (leftIsDataSet ? mappingUnit.getDataSetColumnFor(col)
								.getModifiedName() : col.getName()));
			}
			for (final Iterator i = this.rightAliases.entrySet().iterator(); i
					.hasNext();) {
				final Map.Entry entry = (Map.Entry) i.next();
				final Column col = (Column) entry.getKey();
				final String alias = ":" + (String) entry.getValue();
				sub = sub.replaceAll(alias, rightTablePrefix
						+ "."
						+ (rightIsDataSet ? mappingUnit
								.getDataSetColumnFor(col).getModifiedName()
								: col.getName()));
			}
			sub = sub.replaceAll(":" + Resources.get("schemaPrefix"),
					schemaPrefix == null ? "null" : schemaPrefix);
			Log.debug("Expression is: " + sub);
			return sub;
		}
	}
}
