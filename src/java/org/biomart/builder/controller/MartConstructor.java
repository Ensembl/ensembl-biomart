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

package org.biomart.builder.controller;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.biomart.builder.controller.dialects.DatabaseDialect;
import org.biomart.builder.exceptions.ConstructorException;
import org.biomart.builder.exceptions.ListenerException;
import org.biomart.builder.exceptions.PartitionException;
import org.biomart.builder.exceptions.ValidationException;
import org.biomart.builder.model.Column;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Key;
import org.biomart.builder.model.Mart;
import org.biomart.builder.model.MartConstructorAction;
import org.biomart.builder.model.PartitionTable;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Table;
import org.biomart.builder.model.TransformationUnit;
import org.biomart.builder.model.DataLink.JDBCDataLink;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetOptimiserType;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.DataSet.DataSetTableType;
import org.biomart.builder.model.DataSet.ExpressionColumnDefinition;
import org.biomart.builder.model.DataSet.SplitOptimiserColumnDef;
import org.biomart.builder.model.DataSet.DataSetColumn.ExpressionColumn;
import org.biomart.builder.model.DataSet.DataSetColumn.InheritedColumn;
import org.biomart.builder.model.DataSet.DataSetColumn.WrappedColumn;
import org.biomart.builder.model.MartConstructorAction.AddExpression;
import org.biomart.builder.model.MartConstructorAction.CopyOptimiser;
import org.biomart.builder.model.MartConstructorAction.CreateOptimiser;
import org.biomart.builder.model.MartConstructorAction.Distinct;
import org.biomart.builder.model.MartConstructorAction.Drop;
import org.biomart.builder.model.MartConstructorAction.DropColumns;
import org.biomart.builder.model.MartConstructorAction.ExpandUnroll;
import org.biomart.builder.model.MartConstructorAction.Index;
import org.biomart.builder.model.MartConstructorAction.InitialUnroll;
import org.biomart.builder.model.MartConstructorAction.Join;
import org.biomart.builder.model.MartConstructorAction.LeftJoin;
import org.biomart.builder.model.MartConstructorAction.Rename;
import org.biomart.builder.model.MartConstructorAction.Select;
import org.biomart.builder.model.MartConstructorAction.UpdateOptimiser;
import org.biomart.builder.model.PartitionTable.PartitionColumn;
import org.biomart.builder.model.PartitionTable.PartitionTableApplication;
import org.biomart.builder.model.PartitionTable.PartitionTableApplication.PartitionAppliedRow;
import org.biomart.builder.model.Relation.RestrictedRelationDefinition;
import org.biomart.builder.model.Relation.UnrolledRelationDefinition;
import org.biomart.builder.model.Table.RestrictedTableDefinition;
import org.biomart.builder.model.TransformationUnit.Expression;
import org.biomart.builder.model.TransformationUnit.JoinTable;
import org.biomart.builder.model.TransformationUnit.SelectFromTable;
import org.biomart.builder.model.TransformationUnit.SkipTable;
import org.biomart.builder.model.TransformationUnit.UnrollTable;
import org.biomart.common.exceptions.BioMartError;
import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.biomart.common.utils.InverseMap;

/**
 * This interface defines the behaviour expected from an object which can take a
 * dataset and actually construct a mart based on this information. Whether it
 * carries out the task or just writes some DDL to be run by the user later is
 * up to the implementor.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.113.2.1 $, $Date: 2009-06-02 15:43:12 $, modified by
 *          $Author: syed $
 * @since 0.5
 */
public interface MartConstructor {

	/**
	 * This method takes a dataset and generates a {@link Runnable} which when
	 * run will construct a graph of actions describing how to construct the
	 * mart, then emit those actions as events to whatever may be listening.
	 * <p>
	 * The {@link Runnable} can be started by calling {@link Runnable#run()} on
	 * it. Ideally this should be done within it's own {@link Thread}, so that
	 * the thread can do the work in the background.
	 * <p>
	 * Once started, the {@link Runnable} can be monitored using the methods
	 * available in the {@link ConstructorRunnable} interface.
	 * 
	 * @param targetDatabaseName
	 *            the name of the database to create the dataset tables in.
	 * @param targetSchemaName
	 *            the name of the schema to create the dataset tables in.
	 * @param datasets
	 *            a set of datasets to construct. An empty set means nothing
	 *            will get constructed.
	 * @param prefixes
	 *            the schema prefixes to construct datasets for. Datasets not
	 *            valid for these prefixes will not get built. If the list is
	 *            empty, all possible prefixes will be used.
	 * @return the {@link Runnable} object that when run will construct the
	 *         action graph and start emitting action events.
	 * @throws Exception
	 *             if there was any problem creating the {@link Runnable}
	 *             object.
	 */
	public ConstructorRunnable getConstructorRunnable(
			String targetDatabaseName, String targetSchemaName,
			Collection datasets, Collection prefixes) throws Exception;

	/**
	 * This interface defines a class which does the actual construction work.
	 * It should keep its status up-to-date, as these will be displayed
	 * regularly to the user. You should probably provide a constructor which
	 * takes a dataset as a parameter.
	 */
	public interface ConstructorRunnable extends Runnable {
		/**
		 * Is this thread still going?
		 * 
		 * @return <tt>true</tt> if it is.
		 */
		public boolean isAlive();

		/**
		 * This method adds a listener which will listen out for events emitted
		 * by the constructor.
		 * 
		 * @param listener
		 *            the listener to add.
		 */
		public void addMartConstructorListener(MartConstructorListener listener);

		/**
		 * This method will be called if the user wants the thread to stop work
		 * straight away. It should set an exception for
		 * {@link #getFailureException()} to return saying that it was
		 * cancelled, so that the user knows it was so, and doesn't think it
		 * just finished successfully without any warnings.
		 */
		public void cancel();

		/**
		 * If the thread failed or was cancelled, this method should return an
		 * exception describing the failure. If it succeeded, or is still in
		 * progress and hasn't failed yet, it should return <tt>null</tt>.
		 * 
		 * @return the exception that caused the thread to fail, if any, or
		 *         <tt>null</tt> otherwise.
		 */
		public Exception getFailureException();

		/**
		 * This method should return a value between 0 and 100 indicating how
		 * the thread is getting along in the general scheme of things. 0
		 * indicates just starting, 100 indicates complete.
		 * 
		 * @return a percentage indicating how far the thread has got.
		 */
		public int getPercentComplete();

		/**
		 * This method should return a message describing what the thread is
		 * currently doing.
		 * 
		 * @return a message describing current activity.
		 */
		public String getStatusMessage();
	}

	/**
	 * Defines the generic way of constructing a mart. Generates a graph of
	 * actions then iterates through that graph in an ordered manner, ensuring
	 * that no action is reached before all actions it depends on have been
	 * reached. Each action it iterates over fires an action event to all
	 * listeners registered with it.
	 */
	public static class GenericConstructorRunnable implements
			ConstructorRunnable {
		private boolean cancelled = false;

		private Collection datasets;

		private Collection schemaPrefixes;

		private String datasetSchemaName;

		private Exception failure = null;

		private Collection martConstructorListeners;

		private double percentComplete = 0.0;

		private final Map uniqueOptCols = new HashMap();

		private final Map finalNameCache = new HashMap();

		private final Map indexOptCols = new HashMap();

		private String statusMessage = Resources.get("mcCreatingGraph");

		private int tempNameCount = 0;

		private boolean alive = true;

		/**
		 * Constructs a builder object that will construct an action graph
		 * containing all actions necessary to build the given dataset, then
		 * emit events related to those actions.
		 * <p>
		 * The helper specified will interface between the builder object and
		 * the data source, providing it with bits of data it may need in order
		 * to construct the graph.
		 * 
		 * @param datasetSchemaName
		 *            the name of the database schema into which the transformed
		 *            dataset should be put.
		 * @param datasets
		 *            the dataset(s) to transform into a mart.
		 * @param schemaPrefixes
		 *            only process datasets that exist in this list of
		 *            partitions.
		 */
		public GenericConstructorRunnable(final String datasetSchemaName,
				final Collection datasets, final Collection schemaPrefixes) {
			super();
			Log.debug("Created generic constructor runnable");
			this.datasets = datasets;
			this.schemaPrefixes = schemaPrefixes;
			this.martConstructorListeners = new ArrayList();
			this.datasetSchemaName = datasetSchemaName;
		}

		private void checkCancelled() throws ConstructorException {
			if (this.cancelled)
				throw new ConstructorException(Resources.get("mcCancelled"));
		}

		/**
		 * This is the starting point for the conversion of a dataset into a set
		 * of actions. Internally, it constructs a graph of actions specific to
		 * this dataset, populates the graph, then iterates over the graph at
		 * the end emitting those actions as events in the correct order, so
		 * that any action that depends on another action is guaranteed to be
		 * emitted after the action it depends on.
		 * 
		 * @param dataset
		 *            the dataset to build an action graph for and then emit
		 *            actions from that graph.
		 * @param totalDataSetCount
		 *            a counter informing this method how many datasets in total
		 *            there are to process. It is used to work out percentage
		 *            process.
		 * @throws Exception
		 *             if anything goes wrong at all during the transformation
		 *             process.
		 */
		private void makeActionsForDataset(final DataSet dataset,
				final int totalDataSetCount) throws ListenerException,
				ValidationException, ConstructorException, SQLException,
				PartitionException {
			Log.debug("Making actions for dataset " + dataset);
			// Check not cancelled.
			this.checkCancelled();

			// Start with a fresh set of final names.
			this.finalNameCache.clear();

			// Find out the main table source schema.
			final Schema templateSchema = dataset.getCentralTable().getSchema();
			final PartitionTableApplication dsPta = dataset
					.getPartitionTableApplication();

			// Is it partitioned?
			Collection schemaPartitions = new ArrayList(templateSchema
					.getPartitions().entrySet());
			if (schemaPartitions.isEmpty()) {
				Log.debug("Using dummy empty partition");
				schemaPartitions = new ArrayList();
				schemaPartitions.add(new Map.Entry() {
					public Object getKey() {
						return templateSchema.getDataLinkSchema();
					}

					public Object getValue() {
						return null;
					}

					public Object setValue(final Object value) {
						return null;
					}
				});
			} else
				for (final Iterator i = schemaPartitions.iterator(); i
						.hasNext();) {
					final Map.Entry entry = (Map.Entry) i.next();
					if (!this.schemaPrefixes.isEmpty()
							&& !this.schemaPrefixes.contains(entry.getValue()))
						i.remove();
				}

			// Work out the progress step size : 1 step = 1 table per source
			// schema partition.
			final Collection tablesToProcess = this.getTablesToProcess(dataset);
			double stepPercent = 100.0 / totalDataSetCount;
			stepPercent /= tablesToProcess.size();
			stepPercent /= schemaPartitions.size();
			if (dsPta != null)
				stepPercent /= ((PartitionAppliedRow) dsPta
						.getPartitionAppliedRows().get(0)).getCompound();

			// Process the tables.
			for (final Iterator s = schemaPartitions.iterator(); s.hasNext();) {
				final Map.Entry schemaPartition = (Map.Entry) s.next();
				final Set droppedTables = new HashSet();
				// Clear out optimiser col names so that they start
				// again on this partition.
				this.uniqueOptCols.clear();
				this.indexOptCols.clear();

				Log.debug("Starting schema partition " + schemaPartition);
				this.issueListenerEvent(
						MartConstructorListener.PARTITION_STARTED,
						schemaPartition.getKey());

				// Loop over dataset partitions.
				boolean fakeDSPartition = dsPta == null;
				if (!fakeDSPartition)
					dsPta.getPartitionTable().prepareRows(
							(String) schemaPartition.getValue(),
							PartitionTable.UNLIMITED_ROWS);
				while (fakeDSPartition ? true : dsPta != null
						&& dsPta.getPartitionTable().nextRow()) {
					//added by Yong Liang, clear the optimiser before dataset partition started
					this.uniqueOptCols.clear();
					this.indexOptCols.clear();

					fakeDSPartition = false;
					// Make more specific.
					String partitionedDataSetName = dataset.getName();
					if (dsPta != null)
						partitionedDataSetName = dsPta.getNamePartitionCol()
								.getValueForRow(
										dsPta.getNamePartitionCol()
												.getPartitionTable()
												.currentRow())
								+ Resources.get("tablenameSubSep")
								+ partitionedDataSetName;
					this.issueListenerEvent(
							MartConstructorListener.DATASET_STARTED,
							partitionedDataSetName);
					final Map bigParents = new HashMap();
					for (final Iterator i = tablesToProcess.iterator(); i
							.hasNext();) {
						final DataSetTable dsTable = (DataSetTable) i.next();
						if (!droppedTables.contains(dsTable.getParent())) {
							// Loop over dataset table partitions.
							final PartitionTableApplication dmPta = dsTable
									.getPartitionTableApplication();
							boolean fakeDMPartition = dmPta == null;
							if (!fakeDMPartition)
								dmPta.getPartitionTable().prepareRows(
										(String) schemaPartition.getValue(),
										PartitionTable.UNLIMITED_ROWS);
							final double subStepPercent = dmPta == null ? stepPercent
									: stepPercent
											/ dmPta.getPartitionTable()
													.countRows();
							while (fakeDMPartition ? true : dmPta != null
									&& dmPta.getPartitionTable().nextRow()) {
								fakeDMPartition = false;
								final double targetPercent = this.percentComplete
										+ subStepPercent;
								if (!this.makeActionsForDatasetTable(
										bigParents, subStepPercent,
										templateSchema,
										(String) schemaPartition.getKey(),
										(String) schemaPartition.getValue(),
										dsPta, dmPta, dataset, dsTable))
									droppedTables.add(dsTable);
								// In case the construction didn't do all the
								// steps.
								this.percentComplete = targetPercent;
							}
						}

						// Check not cancelled.
						this.checkCancelled();
					}
					this.issueListenerEvent(
							MartConstructorListener.DATASET_ENDED,
							partitionedDataSetName);
				}

				this.issueListenerEvent(
						MartConstructorListener.PARTITION_ENDED,
						schemaPartition.getKey());
			}
			Log.debug("Finished dataset " + dataset);
		}

		private List getTablesToProcess(final DataSet dataset)
				throws ValidationException {
			Log.debug("Creating ordered list of tables for dataset " + dataset);
			// Create a list in the order by which we want to process tables.
			final List tablesToProcess = new ArrayList();
			// Main table first.
			tablesToProcess.add(dataset.getMainTable());
			// Now recursively expand the table list.
			for (int i = 0; i < tablesToProcess.size(); i++) {
				final DataSetTable tbl = (DataSetTable) tablesToProcess.get(i);
				// Expand the table.
				final Collection nextSCs = new ArrayList();
				final Collection nextDims = new ArrayList();
				if (tbl.getPrimaryKey() != null)
					for (final Iterator j = tbl.getPrimaryKey().getRelations()
							.iterator(); j.hasNext();) {
						final Relation r = (Relation) j.next();
						final DataSetTable dsTab = (DataSetTable) r
								.getManyKey().getTable();
						if (!dsTab.isDimensionMasked()
								&& dsTab.getFocusRelation() != null
								&& !dsTab.getFocusRelation().isMergeRelation(
										dataset)
								&& dsTab.getFocusRelation()
										.getUnrolledRelation(dataset) == null)
							if (dsTab.getType().equals(
									DataSetTableType.DIMENSION))
								nextDims.add(dsTab);
							else
								nextSCs.add(dsTab);
					}
				// We need to insert each dimension directly
				// after its parent table and before any subsequent
				// subclass table. This ensures that by the time the subclass
				// table is created, the parent table will have all its
				// columns in place and complete already.
				tablesToProcess.addAll(i + 1, nextSCs);
				tablesToProcess.addAll(i + 1, nextDims);
			}
			return tablesToProcess;
		}

		private boolean makeActionsForDatasetTable(final Map bigParents,
				double stepPercent, final Schema templateSchema,
				final String schemaPartition, final String schemaPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta, final DataSet dataset,
				final DataSetTable dsTable) throws ListenerException,
				SQLException, PartitionException, ConstructorException {
			Log.debug("Creating actions for table " + dsTable);
			final String finalCombinedName = this.getFinalName(schemaPrefix,
					dsPta, dmPta, dsTable);
			final String tempName = "TEMP";
			String previousTempTable = null;
			boolean firstJoin = true;
			boolean requiresFinalLeftJoin = false;
			boolean requiresDistinct = dsTable.isDistinctTable();
			final Set droppedCols = new HashSet();
			int bigness = dsTable.getType().equals(DataSetTableType.MAIN) ? 0
					: ((Integer) bigParents.get(dsTable.getParent()))
							.intValue();

			// Skip immediately if not applicable to current schema partition.
			if (!(schemaPartition == null
					|| dsTable.getFocusTable().getSchemaPartitions().isEmpty() || dsTable
					.getFocusTable().getSchemaPartitions().contains(
							schemaPrefix)))
				return false;

			// Use the transformation units to create the basic table.
			// FIXME
			final Collection units = dsTable.getTransformationUnits();
			stepPercent /= units.size();
			Relation firstJoinRel = null;
			for (final Iterator j = units.iterator(); j.hasNext(); this.percentComplete += stepPercent) {
				this.checkCancelled();

				// Check if TU actually applies to us. If not, skip it.
				final TransformationUnit tu = (TransformationUnit) j.next();
				if (!tu.appliesToPartition(schemaPrefix))
					continue;
				final String tempTable = tempName + this.tempNameCount++;

				// Translate TU to Action.
				// Expression?
				if (tu instanceof Expression) {
					if (!this.doExpression(schemaPrefix, dsPta, dmPta, dataset,
							dsTable, (Expression) tu, previousTempTable,
							tempTable, droppedCols, finalCombinedName))
						// Skip to next action to prevent non-existent
						// new temp table from getting dropped.
						continue;
				}
				// Unroll?
				else if (tu instanceof UnrollTable) {
					this.doUnrollTable(templateSchema, schemaPartition,
							schemaPrefix, dsPta, dmPta, dataset, dsTable,
							(UnrollTable) tu, previousTempTable, tempTable,
							droppedCols, bigness, finalCombinedName);
					requiresDistinct = true;
				}
				// Skip?
				else if (tu instanceof SkipTable)
					// Ignore.
					continue;
				else if (tu instanceof JoinTable) {
					if (firstJoinRel == null)
						firstJoinRel = ((JoinTable) tu).getSchemaRelation();
					bigness = Math.max(bigness, ((JoinTable) tu).getTable()
							.getBigTable(dataset, dsTable.getName()));
					requiresFinalLeftJoin |= this.doJoinTable(templateSchema,
							schemaPartition, schemaPrefix, dsPta, dmPta,
							dataset, dsTable, (JoinTable) tu, firstJoinRel,
							previousTempTable, tempTable, droppedCols, bigness,
							finalCombinedName);
				}

				// Select-from?
				else if (tu instanceof SelectFromTable) {
					bigness = Math
							.max(bigness, ((SelectFromTable) tu).getTable()
									.getBigTable(dataset, dsTable.getName()));
					this.doSelectFromTable(templateSchema, schemaPartition,
							schemaPrefix, dsPta, dmPta, dataset, dsTable,
							(SelectFromTable) tu, tempTable, bigness,
							finalCombinedName);
				} else
					throw new BioMartError();

				if (previousTempTable != null) {
					final Drop action = new Drop(this.datasetSchemaName,
							finalCombinedName);
					action.setTable(previousTempTable);
					this.issueAction(action);
				}

				if (tu instanceof JoinTable && firstJoin) {
					if (droppedCols.size() == tu.getNewColumnNameMap().size()
							&& !dsTable.getType().equals(DataSetTableType.MAIN)) {
						// If first join of non-MAIN table dropped all cols
						// then the target table does not exist and the entire
						// non-MAIN table can be dropped. This also means that
						// if this is SUBCLASS then all its DMS and further
						// SUBCLASS tables can be ignored.
						final Drop action = new Drop(this.datasetSchemaName,
								finalCombinedName);
						action.setTable(tempTable);
						this.issueAction(action);
						return false;
					}
					// Don't repeat this check.
					firstJoin = false;
				}

				// Update the previous table.
				previousTempTable = tempTable;
			} //end of for loop

			// Do a final left-join against the parent to reinstate
			// any potentially missing rows.
			if (requiresFinalLeftJoin
					&& !dsTable.getType().equals(DataSetTableType.MAIN)
					&& !dsTable.isNoFinalLeftJoin()) {
				final String tempTable = tempName + this.tempNameCount++;
				bigness = Math.max(bigness, ((Integer) bigParents.get(dsTable
						.getParent())).intValue());
				this.doParentLeftJoin(schemaPrefix, dsPta, dmPta, dataset,
						dsTable, finalCombinedName, previousTempTable,
						tempTable, droppedCols, bigness);
				previousTempTable = tempTable;
			}

			// Drop masked dependencies and create column indices.
			final List dropCols = new ArrayList();
			final List keepCols = new ArrayList();
			for (final Iterator x = dsTable.getColumns().values().iterator(); x
					.hasNext();) {
				final DataSetColumn col = (DataSetColumn) x.next();
				if (col.existsForPartition(schemaPrefix)
						&& !droppedCols.contains(col.getPartitionedName()))
					if (col.isRequiredInterim() && !col.isRequiredFinal())
						dropCols.add(col.getPartitionedName());
					else if (col.isRequiredFinal())
						keepCols.add(col);
			}

			// Does it need a final distinct?
			if (requiresDistinct) {
				final String tempTable = tempName + this.tempNameCount++;
				final Set keepColNames = new HashSet();
				for (final Iterator i = keepCols.iterator(); i.hasNext();)
					keepColNames.add(((DataSetColumn) i.next())
							.getPartitionedName());
				this.doDistinct(dataset, dsTable, finalCombinedName,
						previousTempTable, tempTable, keepColNames, bigness);
				previousTempTable = tempTable;
			} else if (!dropCols.isEmpty()) {
				final DropColumns dropcol = new DropColumns(
						this.datasetSchemaName, finalCombinedName);
				dropcol.setTable(previousTempTable);
				dropcol.setColumns(dropCols);
				this.issueAction(dropcol);
			}

			// Indexing.
			for (final Iterator i = keepCols.iterator(); i.hasNext();) {
				final DataSetColumn col = (DataSetColumn) i.next();
				if (col.isColumnIndexed()) {
					final Index index = new Index(this.datasetSchemaName,
							finalCombinedName);
					index.setTable(previousTempTable);
					index.setColumns(Collections.singletonList(col
							.getPartitionedName()));
					this.issueAction(index);
				}
			}

			// Add a rename action to produce the final table.
			final Rename action = new Rename(this.datasetSchemaName,
					finalCombinedName);
			action.setFrom(previousTempTable);
			action.setTo(finalCombinedName);
			this.issueAction(action);

			// Create indexes on all keys on the final table.
			for (final Iterator j = dsTable.getKeys().iterator(); j.hasNext();) {
				final Key key = (Key) j.next();
				final List keyCols = new ArrayList();
				for (int k = 0; k < key.getColumns().length; k++)
					keyCols.add(((DataSetColumn) key.getColumns()[k])
							.getPartitionedName());
				final Index index = new Index(this.datasetSchemaName,
						finalCombinedName);
				index.setTable(finalCombinedName);
				index.setColumns(keyCols);
				this.issueAction(index);
			}

			// Create optimiser columns - either count or bool,
			// or none if not required.
			DataSetOptimiserType oType = dataset.getDataSetOptimiserType();
			if (dsTable.getType().equals(DataSetTableType.MAIN_SUBCLASS))
				oType = oType.isTable() ? DataSetOptimiserType.TABLE_INHERIT
						: DataSetOptimiserType.COLUMN_INHERIT;
			if (!oType.equals(DataSetOptimiserType.NONE)
					&& !dsTable.isSkipOptimiser())
				this.doOptimiseTable(schemaPrefix, dsPta, dmPta, dataset,
						dsTable, oType, !dsTable.getType().equals(
								DataSetTableType.DIMENSION)
								&& dataset.getDataSetOptimiserType().isTable(),
						bigness, finalCombinedName);

			// Optimiser indexing.
			if (dataset.isIndexOptimiser()
					&& this.indexOptCols.containsKey(dsTable))
				for (final Iterator i = ((Collection) this.indexOptCols
						.get(dsTable)).iterator(); i.hasNext();) {
					final String col = (String) i.next();
					final Index index = new Index(this.datasetSchemaName,
							finalCombinedName);
					index.setTable(finalCombinedName);
					index.setColumns(Collections.singletonList(col));
					this.issueAction(index);
				}

			// Remember size for children.
			bigParents.put(dsTable, new Integer(bigness));

			// Return success.
			return true;
		}

		private void doParentLeftJoin(final String schemaPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta, final DataSet dataset,
				final DataSetTable dsTable, final String finalCombinedName,
				final String previousTempTable, final String tempTable,
				final Set droppedCols, final int bigness)
				throws ListenerException, PartitionException {
			// Work out the parent table.
			final DataSetTable parent = dsTable.getParent();
			// Work out what columns to take from each side.
			final List leftJoinCols = new ArrayList();
			final List leftSelectCols = leftJoinCols;
			final List rightJoinCols = leftJoinCols;
			final List rightSelectCols = new ArrayList();
			for (int x = 0; x < parent.getPrimaryKey().getColumns().length; x++)
				leftJoinCols.add(((DataSetColumn) parent.getPrimaryKey()
						.getColumns()[x]).getPartitionedName());
			for (final Iterator x = dsTable.getColumns().values().iterator(); x
					.hasNext();) {
				final DataSetColumn col = (DataSetColumn) x.next();
				if (col.existsForPartition(schemaPrefix)
						&& col.isRequiredInterim())
					rightSelectCols.add(col.getPartitionedName());
			}
			rightSelectCols.removeAll(rightJoinCols);
			rightSelectCols.removeAll(droppedCols);
			// Add to rightSelectCols all the has columns for this table.
			final Collection hasCols = dataset.getDataSetOptimiserType()
					.isTable() ? null : (Collection) this.uniqueOptCols
					.get(dsTable);
			if (hasCols != null)
				rightSelectCols.addAll(hasCols);
			// Index the left-hand side of the join.
			final Index index = new Index(this.datasetSchemaName,
					finalCombinedName);
			index.setTable(previousTempTable);
			index.setColumns(leftJoinCols);
			this.issueAction(index);
			// Make the join.
			final LeftJoin action = new LeftJoin(this.datasetSchemaName,
					finalCombinedName);
			action.setLeftTable(this.getFinalName(schemaPrefix, dsPta, dmPta,
					parent));
			action.setRightSchema(this.datasetSchemaName);
			action.setRightTable(previousTempTable);
			action.setLeftJoinColumns(leftJoinCols);
			action.setRightJoinColumns(rightJoinCols);
			action.setLeftSelectColumns(leftSelectCols);
			action.setRightSelectColumns(rightSelectCols);
			action.setResultTable(tempTable);
			action.setBigTable(bigness);
			this.issueAction(action);
			// Drop the old one.
			final Drop drop = new Drop(this.datasetSchemaName,
					finalCombinedName);
			drop.setTable(previousTempTable);
			this.issueAction(drop);
		}

		private void doDistinct(final DataSet dataset,
				final DataSetTable dsTable, final String finalCombinedName,
				final String previousTempTable, final String tempTable,
				final Collection keepCols, final int bigness)
				throws ListenerException {
			// Add to keepCols all the has columns for this table.
			final Collection distinctCols = new HashSet(keepCols);
			final Collection hasCols = dataset.getDataSetOptimiserType()
					.isTable() ? null : (Collection) this.uniqueOptCols
					.get(dsTable);
			if (hasCols != null)
				distinctCols.addAll(hasCols);
			// Make the join.
			final Distinct action = new Distinct(this.datasetSchemaName,
					finalCombinedName);
			action.setSchema(this.datasetSchemaName);
			action.setTable(previousTempTable);
			action.setResultTable(tempTable);
			action.setKeepCols(distinctCols);
			action.setBigTable(bigness);
			this.issueAction(action);
			// Drop the old one.
			final Drop drop = new Drop(this.datasetSchemaName,
					finalCombinedName);
			drop.setTable(previousTempTable);
			this.issueAction(drop);
		}

		private void doOptimiseTable(final String schemaPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta, final DataSet dataset,
				final DataSetTable dsTable, final DataSetOptimiserType oType,
				final boolean createTable, final int bigness,
				final String finalCombinedName) throws ListenerException,
				PartitionException {
			if (createTable) {
				// Tables are same name, but use 'bool' or 'count'
				// instead of 'main'
				final String optTable = this.getOptimiserTableName(
						schemaPrefix, dsPta, dmPta, dsTable, dataset
								.getDataSetOptimiserType());
				// The key cols are those from the primary key.
				//TODO add <String> for keyCols
				final List keyCols = new ArrayList();
				for (int y = 0; y < dsTable.getPrimaryKey().getColumns().length; y++)
					keyCols.add(((DataSetColumn) dsTable.getPrimaryKey()
							.getColumns()[y]).getPartitionedName());

				// Create the table by selecting the pk.
				final CreateOptimiser create = new CreateOptimiser(
						this.datasetSchemaName, finalCombinedName);
				create.setKeyColumns(keyCols);
				create.setOptTableName(optTable);
				create.setBigTable(bigness);
				this.issueAction(create);

				// Index the pk on the new table.
				final Index index = new Index(this.datasetSchemaName,
						finalCombinedName);
				index.setTable(optTable);
				index.setColumns(keyCols);
				this.issueAction(index);
			}
			if (!dsTable.getType().equals(DataSetTableType.MAIN)) {
				// Work out the dimension/subclass parent.
				final DataSetTable parent = dsTable.getParent();
				// Set up the column on the dimension parent.
				String optTable = this
						.getOptimiserTableName(schemaPrefix, dsPta, dmPta,
								parent, dataset.getDataSetOptimiserType());

				// Key columns are primary key cols from parent.
				// Do a left-join update. We're looking for rows
				// where at least one child non-key col is non-null.
				final List keyCols = new ArrayList();
				for (int y = 0; y < parent.getPrimaryKey().getColumns().length; y++)
					keyCols.add(((DataSetColumn) parent.getPrimaryKey()
							.getColumns()[y]).getPartitionedName());

				// Work out what to count.
				final List nonNullCols = new ArrayList();
				for (final Iterator y = dsTable.getColumns().values()
						.iterator(); y.hasNext();) {
					final DataSetColumn col = (DataSetColumn) y.next();
					// We won't select masked cols as they won't be in
					// the final table, and we won't select expression
					// columns as they can genuinely be null.
					if (col.existsForPartition(schemaPrefix)
							&& col.isRequiredFinal() && !col.isColumnMasked()
							&& !(col instanceof ExpressionColumn))
						nonNullCols.add(col.getPartitionedName());
				}
				nonNullCols.removeAll(keyCols);

				// Loop rest of this block once per unique value
				// in column, using SQL to get those values, and
				// inserting them into each optimiser column name.
				final Map restrictCols = new HashMap();
				for (final Iterator i = dsTable.getColumns().values()
						.iterator(); i.hasNext();) {
					final DataSetColumn cand = (DataSetColumn) i.next();
					if (cand.getSplitOptimiserColumn() != null
							&& !cand.isColumnMasked())
						restrictCols.put(cand, cand.getSplitOptimiserColumn());
				}
				if (restrictCols.isEmpty())
					restrictCols.put("", null);
				for (final Iterator i = restrictCols.entrySet().iterator(); i
						.hasNext();) {
					final Map.Entry entry = (Map.Entry) i.next();
					DataSetColumn restrictCol = entry.getKey().equals("") ? null
							: (DataSetColumn) entry.getKey();
					final SplitOptimiserColumnDef splitOptDef = (SplitOptimiserColumnDef) entry
							.getValue();
					final DataSetColumn splitContentCol = splitOptDef == null ? null
							: (DataSetColumn) dsTable.getColumns().get(
									splitOptDef.getContentCol());
					final Collection subNonNullCols = new ArrayList(nonNullCols);
					final List restrictValues = new ArrayList();
					if (restrictCol != null) {
						subNonNullCols.remove(restrictCol.getPartitionedName());
						// Disambiguate inherited columns.
						while (restrictCol instanceof InheritedColumn)
							restrictCol = ((InheritedColumn) restrictCol)
									.getInheritedColumn();
						// Can only restrict on wrapped columns.
						if (restrictCol instanceof WrappedColumn) {
							// Populate restrict values.
							final Column dataCol = ((WrappedColumn) restrictCol)
									.getWrappedColumn();
							try {
								restrictValues.addAll(dataCol.getTable()
										.getSchema().getUniqueValues(
												schemaPrefix, dataCol));
							} catch (final SQLException e) {
								throw new PartitionException(e);
							}
						}
					} else
						restrictValues.add(null);
					for (final Iterator j = restrictValues.iterator(); j
							.hasNext();) {
						final String restrictValue = (String) j.next();
						// Columns are dimension table names with '_bool' or
						// '_count' appended.
						final String optCol = this.getOptimiserColumnName(
								dsPta, dmPta, parent, dsTable, oType,
								restrictCol, restrictValue, splitOptDef == null
										|| splitOptDef.isPrefix(),
								splitOptDef == null || splitOptDef.isSuffix());

						// Do the bool/count update.
						final UpdateOptimiser update = new UpdateOptimiser(
								this.datasetSchemaName, finalCombinedName);
						update.setKeyColumns(keyCols);
						update.setNonNullColumns(subNonNullCols);
						update.setSourceTableName(finalCombinedName);
						update.setOptTableName(optTable);
						update.setOptColumnName(optCol);
						update.setCountNotBool(!oType.isBool());
						update.setNullNotZero(oType.isUseNull());
						update.setOptRestrictColumn(restrictCol == null ? null
								: restrictCol.getPartitionedName());
						update.setOptRestrictValue(restrictValue);
						update
								.setValueColumnName(splitContentCol == null ? null
										: splitContentCol.getPartitionedName());
						update
								.setValueColumnSeparator(splitContentCol == null ? null
										: splitOptDef.getSeparator());
						update.setValueColumnSize(splitContentCol == null ? 255
								: splitOptDef.getSize());
						this.issueAction(update);

						// Store the reference for later.
						if (!this.uniqueOptCols.containsKey(parent))
							this.uniqueOptCols.put(parent, new HashSet());
						((Collection) this.uniqueOptCols.get(parent))
								.add(optCol);
						if (!this.indexOptCols.containsKey(parent))
							this.indexOptCols.put(parent, new HashSet());
						if (!dsTable.isSkipIndexOptimiser()) {
							((Collection) this.indexOptCols.get(parent))
									.add(optCol);
							// Index the column.
							final Index index = new Index(
									this.datasetSchemaName, finalCombinedName);
							index.setTable(optTable);
							index.setColumns(Collections.singletonList(optCol));
							this.issueAction(index);
						}

						// Subclass tables need the column copied down if
						// they are column based.
						if (dsTable.getType().equals(
								DataSetTableType.MAIN_SUBCLASS)
								&& !oType.isTable()) {
							// Set up the column on the subclass itself. Because
							// we are not using tables, this will always be the
							// finished name of the subclass table itself.
							final String scOptTable = this
									.getOptimiserTableName(schemaPrefix, dsPta,
											dmPta, dsTable, dataset
													.getDataSetOptimiserType());

							// If this is a subclass table, copy the optimiser
							// column down to us as well and add it to our own
							// set.
							final CopyOptimiser copy = new CopyOptimiser(
									this.datasetSchemaName, finalCombinedName);
							copy.setKeyColumns(keyCols);
							copy.setOptTableName(scOptTable);
							copy.setOptColumnName(optCol);
							copy.setParentOptTableName(optTable);
							this.issueAction(copy);

							// Store the reference for later.
							if (!this.uniqueOptCols.containsKey(dsTable))
								this.uniqueOptCols.put(dsTable, new HashSet());
							((Collection) this.uniqueOptCols.get(dsTable))
									.add(optCol);
							if (!this.indexOptCols.containsKey(dsTable))
								this.indexOptCols.put(dsTable, new HashSet());
							if (!dsTable.isSkipIndexOptimiser()) {
								((Collection) this.indexOptCols.get(dsTable))
										.add(optCol);
								// Index the column.
								final Index index = new Index(
										this.datasetSchemaName,
										finalCombinedName);
								index.setTable(scOptTable);
								index.setColumns(Collections
										.singletonList(optCol));
								this.issueAction(index);
							}
						} // end of if (dsTable.getType().equals(...)
					} //for (final Iterator j = restrictValues.iterator() 
				} //end of for (final Iterator i = restrictCols.entrySet().iterator()
			}//end of if (!dsTable.getType().equals(DataSetTableType.MAIN))
		}

		private void doSelectFromTable(final Schema templateSchema,
				final String schemaPartition, final String schemaPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta, final DataSet dataset,
				final DataSetTable dsTable, final SelectFromTable stu,
				final String tempTable, final int bigness,
				final String finalCombinedName) throws SQLException,
				ListenerException, PartitionException {

			final Select action = new Select(this.datasetSchemaName,
					finalCombinedName);
			action.setBigTable(bigness);
			action.setSchemaPrefix(schemaPrefix);

			// If this is a dimension, look up DM PT,
			// otherwise if this is the main table, look up DS PT,
			// otherwise don't do it at all.
			PartitionTableApplication pta = null;
			if (dsTable.getType().equals(DataSetTableType.DIMENSION)
					&& dmPta != null)
				pta = dmPta;
			else if (dsTable.getType().equals(DataSetTableType.MAIN)
					&& dsPta != null)
				pta = dsPta;
			if (pta != null) {
				// This is a select, so we are dealing with the first row
				// only.
				final PartitionAppliedRow prow = (PartitionAppliedRow) pta
						.getPartitionAppliedRows().get(0);
				// The naming column will also always be the first row,
				// which will be on pta itself, so we don't need to
				// initialise the table as it has already been done.
				final PartitionColumn pcol = (PartitionColumn) pta
						.getPartitionTable().getColumns().get(
								prow.getPartitionCol());
				// For each of the getNewColumnNameMap cols that are in the
				// current ptable application, add a restriction for that col
				// using current ptable column value.
				for (final Iterator i = stu.getNewColumnNameMap().entrySet()
						.iterator(); i.hasNext();) {
					final Map.Entry entry = (Map.Entry) i.next();
					final DataSetColumn dsCol = (DataSetColumn) entry
							.getValue();
					// Only apply this to the dsCol which matches
					// the partition row's ds col.
					if (dsCol.existsForPartition(schemaPrefix)
							&& (dsCol.getName()
									.equals(prow.getRootDataSetCol()) || dsCol
									.getName().endsWith(
											Resources.get("columnnameSep")
													+ prow.getRootDataSetCol())))
						// Apply restriction.
						action.getPartitionRestrictions().put(
								((Column) entry.getKey()).getName(),
								pcol.getRawValueForRow(pcol.getPartitionTable()
										.currentRow()));
				}
				// PrepareRow on subdivision, if any.
				if (pta.getPartitionAppliedRows().size() > 1) {
					final PartitionAppliedRow subprow = (PartitionAppliedRow) pta
							.getPartitionAppliedRows().get(1);
					((PartitionColumn) pta.getPartitionTable().getColumns()
							.get(subprow.getPartitionCol()))
							.getPartitionTable().prepareRows(schemaPrefix,
									PartitionTable.UNLIMITED_ROWS);
				}
			}

			final Table sourceTable = stu.getTable();
			// Make sure that we use the same partition on the RHS
			// if it exists, otherwise use the default partition.
			String schema = null;
			if (sourceTable instanceof DataSetTable)
				schema = this.datasetSchemaName;
			else if (stu.getTable().getSchema() == templateSchema)
				schema = schemaPartition;
			else {
				final Collection schemaParts = stu.getTable()
						.getSchemaPartitions();
				if (!schemaParts.isEmpty()) {
					if (schemaParts.contains(schemaPrefix))
						schema = (String) new InverseMap(stu.getTable()
								.getSchema().getPartitions()).get(schemaPrefix);
				} else
					schema = stu.getTable().getSchema().getDataLinkSchema();
			}
			if (schema == null) // Can never happen.
				throw new BioMartError();

			// Source tables are always main or subclass and
			// therefore are never partitioned.
			final String table = sourceTable instanceof DataSetTable ? this
					.getFinalName(schemaPrefix, dsPta, dmPta,
							(DataSetTable) sourceTable) : stu.getTable()
					.getName();
			final Map selectCols = new HashMap();
			// Select columns from parent table.
			for (final Iterator k = stu.getNewColumnNameMap().entrySet()
					.iterator(); k.hasNext();) {
				final Map.Entry entry = (Map.Entry) k.next();
				final DataSetColumn col = (DataSetColumn) entry.getValue();
				if (pta != null)
					col.fixPartitionedName(pta);
				if (col.existsForPartition(schemaPrefix)
						&& col.isRequiredInterim())
					selectCols
							.put(
									sourceTable instanceof DataSetTable ? ((DataSetColumn) entry
											.getKey()).getPartitionedName()
											: ((Column) entry.getKey())
													.getName(), col
											.getPartitionedName());

			}
			// Add to selectCols all the inherited has columns, if
			// this is not a dimension table and the optimiser type is not a
			// table one.
			DataSetOptimiserType oType = dataset.getDataSetOptimiserType();
			if (dsTable.getType().equals(DataSetTableType.MAIN_SUBCLASS))
				oType = oType.isTable() ? DataSetOptimiserType.TABLE_INHERIT
						: DataSetOptimiserType.COLUMN_INHERIT;
			if (!oType.isTable() && sourceTable instanceof DataSetTable
					&& !dsTable.getType().equals(DataSetTableType.DIMENSION)) {
				final Collection hasCols = (Collection) this.uniqueOptCols
						.get(sourceTable);
				if (hasCols != null) {
					for (final Iterator k = hasCols.iterator(); k.hasNext();) {
						final String hasCol = (String) k.next();
						selectCols.put(hasCol, hasCol);
					}
					// Make inherited copies.
					this.uniqueOptCols.put(dsTable, new HashSet(hasCols));
				}
				// Inherited indexed optimiser cols.
				final Collection indCols = (Collection) this.indexOptCols
						.get(sourceTable);
				if (indCols != null)
					this.indexOptCols.put(dsTable, new HashSet(indCols));
			}
			// Do the select.
			action.setSchema(schema);
			action.setTable(table);
			action.setSelectColumns(selectCols);
			action.setResultTable(tempTable);

			// Table restriction.
			final RestrictedTableDefinition def = stu.getTable()
					.getRestrictTable(dataset, dsTable.getName());
			if (def != null)
				action.setTableRestriction(def);
			this.issueAction(action);
		}

		private boolean doJoinTable(final Schema templateSchema,
				final String schemaPartition, final String schemaPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta, final DataSet dataset,
				final DataSetTable dsTable, final JoinTable ljtu,
				final Relation firstJoinRel, final String previousTempTable,
				final String tempTable, final Set droppedCols,
				final int bigness, final String finalCombinedName)
				throws SQLException, ListenerException, PartitionException {

			// Left join whenever we have a double-level partition table
			// on the dataset, or whenever main/sc table + not alternative join,
			// or dimension table + alternative join.
			boolean useLeftJoin = (dsTable.getType().equals(
					DataSetTableType.MAIN)
					&& dsPta != null && dsPta.getPartitionAppliedRows().size() > 1)
					|| (dsTable.getType().equals(DataSetTableType.DIMENSION) ? ljtu
							.getSchemaRelation().isAlternativeJoin(dataset,
									dsTable.getName())
							: !ljtu.getSchemaRelation().isAlternativeJoin(
									dataset, dsTable.getName()));
			boolean requiresFinalLeftJoin = !useLeftJoin;
			final Join action = new Join(this.datasetSchemaName,
					finalCombinedName);
			action.setLeftJoin(useLeftJoin);
			action.setBigTable(bigness);
			action.setSchemaPrefix(schemaPrefix);

			PartitionTableApplication pta = null;
			if (dsTable.getType().equals(DataSetTableType.DIMENSION)
					&& dmPta != null)
				pta = dmPta;
			else if (dsTable.getType().equals(DataSetTableType.MAIN)
					&& dsPta != null)
				pta = dsPta;
			if (pta != null) {
				// If this is first relation after select table
				// (note first relation, not first join) then apply
				// next row to any subdiv table present.
				// Use a test to see if this is the first relation
				// after the select (regardless of how many times this
				// relation has been seen).
				final boolean nextRow = firstJoinRel.equals(ljtu
						.getSchemaRelation());
				if (nextRow && pta.getPartitionAppliedRows().size() > 1)
					((PartitionColumn) pta.getPartitionTable().getColumns()
							.get(
									((PartitionAppliedRow) pta
											.getPartitionAppliedRows().get(1))
											.getPartitionCol()))
							.getPartitionTable().nextRow();
				// For all relations, if this is the one
				// that some subdiv partition applies to, then apply it.
				// This is a join, so we look up row by relation.
				final PartitionAppliedRow prow = pta
						.getAppliedRowForRelation(ljtu.getSchemaRelation());
				// It might not have one after all.
				if (prow != null) {
					// Look up the table that the naming column is on. It
					// will be a subtable which needs initialising on the
					// first pass, and next rowing on all passes.
					final PartitionColumn pcol = (PartitionColumn) pta
							.getPartitionTable().getColumns().get(
									prow.getPartitionCol());
					final PartitionTable ptbl = pcol.getPartitionTable();
					// For each of the getNewColumnNameMap cols that are in the
					// current ptable application, add a restriction for that
					// col using current ptable column value.
					for (final Iterator i = ljtu.getNewColumnNameMap()
							.entrySet().iterator(); i.hasNext();) {
						final Map.Entry entry = (Map.Entry) i.next();
						final DataSetColumn dsCol = (DataSetColumn) entry
								.getValue();
						// Only apply this to the dsCol which matches
						// the partition row's ds col.
						if (dsCol.existsForPartition(schemaPrefix)
								&& dsCol.getName().split("\\.")[3].equals(prow
										.getRootDataSetCol().split("\\.")[3])) {
							// Apply restriction.
							action.getPartitionRestrictions().put(
									((Column) entry.getKey()).getName(),
									pcol.getRawValueForRow(ptbl.currentRow()));
							// Make this an inner join if we are NOT dealing
							// with a two-level dataset partition.
							if (!(dsTable.getType().equals(
									DataSetTableType.MAIN)
									&& dsPta != null && dsPta
									.getPartitionAppliedRows().size() > 1)
									&& !useLeftJoin) {
								// We'll need a final left join.
								requiresFinalLeftJoin = true;
								useLeftJoin = false;
								action.setLeftJoin(false);
							}
						}
					}
				}
			}

			// Make sure that we use the same partition on the RHS
			// if it exists, otherwise use the default partition.
			String rightSchema = null;
			if (ljtu.getTable() instanceof DataSetTable)
				rightSchema = this.datasetSchemaName;
			else {
				if (ljtu.getTable().getSchema() == templateSchema)
					rightSchema = schemaPartition;
				else {
					final Collection rightSchemaParts = ljtu.getTable()
							.getSchemaPartitions();
					if (!rightSchemaParts.isEmpty()) {
						if (rightSchemaParts.contains(schemaPrefix))
							rightSchema = (String) new InverseMap(ljtu
									.getTable().getSchema().getPartitions())
									.get(schemaPrefix);
					} else
						rightSchema = ljtu.getTable().getSchema()
								.getDataLinkSchema();
				}
			}
			if (rightSchema == null) {
				droppedCols.addAll(ljtu.getNewColumnNameMap().values());
				return false;
			}

			final String rightTable = ljtu.getTable() instanceof DataSetTable ? this
					.getFinalName(schemaPrefix, dsPta, dmPta,
							(DataSetTable) ljtu.getTable())
					: ljtu.getTable().getName();
			final List leftJoinCols = new ArrayList();
			final List rightJoinCols = new ArrayList();
			for (int i = 0; i < ljtu.getSchemaRelation().getOtherKey(
					ljtu.getSchemaSourceKey()).getColumns().length; i++) {
				final Column rightCol = ljtu.getSchemaRelation().getOtherKey(
						ljtu.getSchemaSourceKey()).getColumns()[i];
				if (ljtu.getTable() instanceof DataSetTable)
					rightJoinCols.add(((DataSetColumn) ljtu
							.getSchemaSourceKey().getColumns()[i])
							.getPartitionedName());
				else
					rightJoinCols.add(rightCol.getName());
			}
			final Map selectCols = new HashMap();
			// Populate vars.
			for (final Iterator k = ljtu.getSourceDataSetColumns().iterator(); k
					.hasNext();) {
				final String joinCol = ((DataSetColumn) k.next())
						.getPartitionedName();
				if (droppedCols.contains(joinCol)) {
					droppedCols.addAll(ljtu.getNewColumnNameMap().values());
					return false;
				} else
					leftJoinCols.add(joinCol);
			}
			for (final Iterator k = ljtu.getNewColumnNameMap().entrySet()
					.iterator(); k.hasNext();) {
				final Map.Entry entry = (Map.Entry) k.next();
				final DataSetColumn col = (DataSetColumn) entry.getValue();
				if (pta != null)
					col.fixPartitionedName(pta);
				if (col.existsForPartition(schemaPrefix)
						&& col.isRequiredInterim()) {
					if (entry.getKey() instanceof DataSetColumn)
						selectCols.put(((DataSetColumn) entry.getKey())
								.getModifiedName(), col.getPartitionedName());
					else
						selectCols.put(((Column) entry.getKey()).getName(), col
								.getPartitionedName());
				}
			}
			// Index the left-hand side of the join.
			final Index index = new Index(this.datasetSchemaName,
					finalCombinedName);
			index.setTable(previousTempTable);
			index.setColumns(leftJoinCols);
			this.issueAction(index);
			// Make the join.
			action.setLeftTable(previousTempTable);
			action.setRightSchema(rightSchema);
			action.setRightTable(rightTable);
			action.setLeftJoinColumns(leftJoinCols);
			action.setRightJoinColumns(rightJoinCols);
			action.setSelectColumns(selectCols);
			action.setResultTable(tempTable);

			// Table restriction.
			final RestrictedTableDefinition rdef = ljtu.getTable()
					.getRestrictTable(dataset, dsTable.getName());
			if (rdef != null)
				action.setTableRestriction(rdef);

			// Don't add restriction if loopback relation from M end.
			final boolean loopbackManyEnd = ljtu.getSchemaRelation()
					.getLoopbackRelation(dataset, dsTable.getName()) != null
					&& ljtu.getSchemaSourceKey().equals(
							ljtu.getSchemaRelation().getManyKey());
			final RestrictedRelationDefinition def = ljtu.getSchemaRelation()
					.getRestrictRelation(dataset, dsTable.getName(),
							ljtu.getSchemaRelationIteration());
			if (!loopbackManyEnd && def != null) {
				// Add the restriction.
				action.setRelationRestrictionPreviousUnit(ljtu
						.getPreviousUnit());
				action.setRelationRestrictionLeftIsFirst(ljtu
						.getSchemaRelation().getFirstKey().equals(
								ljtu.getSchemaSourceKey()));
				action.setRelationRestriction(def);
			}
			// If this is a loopback from the one end, add the optional
			// differentiation column only if we have previously
			// traversed this same relation.
			final boolean loopbackOneEnd = ljtu.getSchemaRelation()
					.getLoopbackRelation(dataset, dsTable.getName()) != null
					&& ljtu.getSchemaSourceKey().equals(
							ljtu.getSchemaRelation().getOneKey());
			if (loopbackOneEnd) {
				// Identify the differentiation column.
				final Column diffCol = ljtu.getSchemaRelation()
						.getLoopbackRelation(dataset, dsTable.getName());
				if (diffCol != null) {
					// Identify the differentiation column from the
					// previous unit's selected columns.
					DataSetColumn prevDSCol = ljtu.getPreviousUnit()
							.getDataSetColumnFor(diffCol);
					if (prevDSCol == null
							&& ljtu.getSchemaRelationIteration() > 0) {
						// Hunt for it in alternative fork.
						for (final Iterator i = dsTable
								.getTransformationUnits().iterator(); i
								.hasNext()
								&& prevDSCol == null; i.next())
							prevDSCol = ((TransformationUnit) i.next())
									.getDataSetColumnFor(diffCol);
					}
					if (prevDSCol != null) {
						final String prevDsColName = prevDSCol
								.getPartitionedName();
						// Add both to the transformation unit to become
						// a new restriction later.
						action.setLoopbackDiffSource(prevDsColName);
						action.setLoopbackDiffTarget(diffCol.getName());
					}
				}
			}
			this.issueAction(action);
			return requiresFinalLeftJoin;
		}

		private void doUnrollTable(final Schema templateSchema,
				final String schemaPartition, final String schemaPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta, final DataSet dataset,
				final DataSetTable dsTable, final UnrollTable utu,
				final String previousTempTable, final String tempTable,
				final Set droppedCols, final int bigness,
				final String finalCombinedName) throws SQLException,
				ListenerException, PartitionException, ConstructorException {

			// Set parentRel = utu.getRelation, childRel = otherRel
			// Replace references to utu.getRelation with parentRel
			// Replace references to otherRel with childRel
			// If 'inverted sense' flag set on UnrollDefinition, swap
			// the two around. Flag is set on definition by wizard, which
			// merges child rel instead of parent rel and unrolls parent rel
			// when chosen. This swap action restores this - so that the
			// child records are merged but the correct path of unrolling
			// is still followed.
			// Proceed as normal.

			// Find other merged relation between these two tables, and
			// the many key is the parent key col.
			final UnrolledRelationDefinition unrollDef = utu.getUnrolledDef();
			Relation otherRel = null;
			for (final Iterator i = utu.getRelation().getOneKey()
					.getRelations().iterator(); i.hasNext() && otherRel == null;) {
				final Relation candRel = (Relation) i.next();
				if (candRel.equals(utu.getRelation()))
					continue;
				if (candRel.getManyKey().getTable().equals(
						utu.getRelation().getManyKey().getTable())
						&& candRel.isMergeRelation(dataset))
					otherRel = candRel;
			}
			if (otherRel == null)
				throw new BioMartError(); // Should never happen.
			final Relation parentRel = unrollDef.isReversed() ? otherRel : utu
					.getRelation();
			final Relation childRel = unrollDef.isReversed() ? utu
					.getRelation() : otherRel;

			// Make sure that we use the same partition on the RHS
			// if it exists, otherwise use the default partition.
			// Note that it will run unpredictably if compound keys are used.
			final String unrollFK = unrollDef.isReversed() ? utu
					.getDataSetColumnFor(childRel.getManyKey().getColumns()[0])
					.getPartitionedName() : utu.getDataSetColumnFor(
					parentRel.getManyKey().getColumns()[0])
					.getPartitionedName();
			final String unrollPK = utu.getDataSetColumnFor(
					parentRel.getOneKey().getColumns()[0]).getPartitionedName();
			final String unrollIDColName = utu.getUnrolledIDColumn()
					.getPartitionedName();
			final String unrollNameColName = utu.getUnrolledNameColumn()
					.getPartitionedName();
			final String unrollIterationColName = unrollIDColName + "__i";

			// Make n an efficient approximation of the
			// number of unroll cycles required to get longest path
			// covered.
			// = SPREADING RULE. Mark root = 0, descendants = 1,
			// next descendants = 2, etc. etc. until all nodes
			// marked. Don't renumber nodes already numbered.
			// Number used to mark last set of nodes is length of
			// longest path.
			// Open connection and query.
			final Connection conn = ((JDBCDataLink) templateSchema)
					.getConnection(schemaPartition);
			final String sqlStr = DatabaseDialect.getDialect(templateSchema)
					.getUnrollTableSQL(schemaPrefix, dataset, dsTable,
							parentRel, childRel, schemaPartition,
							templateSchema, utu);
			Log.debug("Executing unroll statement: " + sqlStr);
			final ResultSet rs = conn.prepareStatement(sqlStr).executeQuery();
			// Iterate over all pairs in db.
			// For each pair:
			// 1. if L not in allNodes, add.
			// 2. if R not in childNodes, add.
			// 3. get L from allNodes and add R to children.
			final Map allNodes = new HashMap();
			int max = 0;
			int min = 0;
			while (rs.next()) {
				final String L = rs.getString(1);
				final String R = rs.getString(2);
				final boolean Lnew = !allNodes.containsKey(L);
				final boolean Rnew = !allNodes.containsKey(R);
				if (Rnew) {
					if (Lnew) {
						// R new L new
						allNodes.put(L, new Integer(0));
						allNodes.put(R, new Integer(1));
					} else
						// R new L old
						allNodes.put(R, new Integer(((Integer) allNodes.get(L))
								.intValue() + 1));
				} else if (Lnew)
					// R old L new
					allNodes.put(L, new Integer(((Integer) allNodes.get(R))
							.intValue() - 1));
				else
					// R old L old
					allNodes.put(L, new Integer(Math.min(((Integer) allNodes
							.get(L)).intValue(), ((Integer) allNodes.get(R))
							.intValue() - 1)));
				min = Math.min(min, ((Integer) allNodes.get(L)).intValue());
				max = Math.max(max, ((Integer) allNodes.get(R)).intValue());
				this.checkCancelled();
			}
			final int n = max - min;
			// Close query and connection.
			rs.close();
			conn.close();

			// Create initial select table with unrolled child cols and
			// iteration col.
			final InitialUnroll iaction = new InitialUnroll(
					this.datasetSchemaName, finalCombinedName);
			iaction.setSchema(this.datasetSchemaName);
			iaction.setSourceTable(previousTempTable);
			iaction.setUnrollPKCol(unrollPK);
			iaction.setUnrollIDCol(unrollIDColName);
			iaction.setUnrollNameCol(unrollNameColName);
			iaction.setUnrollIterationCol(unrollIterationColName);
			iaction.setNamingCol(utu.getDataSetColumnFor(
					unrollDef.getNameColumn()).getPartitionedName());
			iaction.setTable(tempTable);
			iaction.setBigTable(bigness);
			this.issueAction(iaction);
			// Index FK of relation in new table.
			Index index = new Index(this.datasetSchemaName, finalCombinedName);
			index.setTable(tempTable);
			index.setColumns(Collections.singletonList(unrollFK));
			this.issueAction(index);
			// Index unrolled child id + iteration column.
			index = new Index(this.datasetSchemaName, finalCombinedName);
			index.setTable(tempTable);
			index.setColumns(Arrays.asList(new String[] { unrollIDColName,
					unrollIterationColName }));
			this.issueAction(index);
			// Build a list of cols to include from the parent.
			final List parentCols = new ArrayList();
			TransformationUnit tu = utu;
			while ((tu = tu.getPreviousUnit()) != null)
				for (final Iterator i = tu.getNewColumnNameMap().values()
						.iterator(); i.hasNext();) {
					final DataSetColumn dsCol = (DataSetColumn) i.next();
					if (dsCol.existsForPartition(schemaPrefix)
							&& dsCol.isRequiredInterim())
						parentCols.add(dsCol.getPartitionedName());
				}
			parentCols.removeAll(droppedCols);

			// Do n expansion insert+update pairs.
			// We start at 1 because InitialUnroll inserts using 1.
			for (int i = 1; i <= n; i++) {
				final ExpandUnroll eaction = new ExpandUnroll(
						this.datasetSchemaName, finalCombinedName);
				eaction.setSchema(this.datasetSchemaName);
				eaction.setSourceTable(tempTable);
				eaction.setParentCols(parentCols);
				eaction.setUnrollPKCol(unrollPK);
				eaction.setUnrollIteration(i);
				eaction.setUnrollFKCol(unrollFK);
				eaction.setUnrollIDCol(unrollIDColName);
				eaction.setUnrollNameCol(unrollNameColName);
				eaction.setUnrollIterationCol(unrollIterationColName);
				eaction.setNamingCol(utu.getDataSetColumnFor(
						unrollDef.getNameColumn()).getPartitionedName());
				eaction.setBigTable(bigness);
				eaction.setReversed(unrollDef.isReversed());
				this.issueAction(eaction);
			}

			// Drop iteration column.
			final DropColumns dropcol = new DropColumns(this.datasetSchemaName,
					finalCombinedName);
			dropcol.setTable(tempTable);
			dropcol.setColumns(Collections
					.singletonList(unrollIterationColName));
			this.issueAction(dropcol);
		}

		private boolean doExpression(final String schemaPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta, final DataSet dataset,
				final DataSetTable dsTable, final Expression etu,
				final String previousTempTable, final String tempTable,
				final Set droppedCols, final String finalCombinedName)
				throws ListenerException, PartitionException {

			// Some useful stuff.
			boolean useXTable = false;
			final String xTableName = tempTable + "X";

			// Work out what columns we can select in the first group.
			final Collection selectCols = new HashSet();
			for (final Iterator z = dsTable.getColumns().values().iterator(); z
					.hasNext();) {
				final DataSetColumn col = (DataSetColumn) z.next();
				final String colName = col.getPartitionedName();
				if (col.existsForPartition(schemaPrefix)
						&& col.isRequiredInterim()
						&& !droppedCols.contains(colName)
						&& !(col instanceof ExpressionColumn))
					selectCols.add(colName);
			}
			// Add to selectCols all the has columns for this table.
			final Collection hasCols = dataset.getDataSetOptimiserType()
					.isTable() ? null : (Collection) this.uniqueOptCols
					.get(dsTable);
			if (hasCols != null)
				selectCols.addAll(hasCols);

			// Do each group of similar expressions in a single statement.
			for (final Iterator i = etu.getOrderedExpressionGroups().iterator(); i
					.hasNext();) {
				final Collection expGroup = (Collection) i.next();

				// Work out all group by and expression columns.
				final Collection groupByCols = new HashSet();
				final Map exprCols = new HashMap();
				for (final Iterator j = expGroup.iterator(); j.hasNext();) {
					final ExpressionColumn expCol = (ExpressionColumn) j.next();
					final ExpressionColumnDefinition expr = expCol
							.getDefinition();
					// If it refers to any dropped columns, drop the expression
					// column too.
					boolean usesDroppedCols = false;
					for (final Iterator k = expr.getAliases().keySet()
							.iterator(); k.hasNext() && !usesDroppedCols;) {
						final String exprAlias = (String) k.next();
						if (droppedCols.contains(exprAlias)) {
							droppedCols.add(expCol.getPartitionedName());
							usesDroppedCols = true;
						}
					}
					if (usesDroppedCols)
						continue;
					// Otherwise, work out group-by stuff.
					if (expr.isGroupBy()) {
						for (final Iterator x = dsTable.getColumns().values()
								.iterator(); x.hasNext();) {
							final DataSetColumn col = (DataSetColumn) x.next();
							final String colName = col.getPartitionedName();
							if (col.existsForPartition(schemaPrefix)
									&& col.isRequiredInterim()
									&& (expr.getAliases().keySet().contains(
											col.getName()) || !selectCols
											.contains(colName)))
								continue;
							groupByCols.add(colName);
						}
						// Make sure group-by doesn't drop the has cols.
						if (hasCols != null)
							groupByCols.addAll(hasCols);
					}
					// Add the column to the list to be generated.
					exprCols.put(expCol.getModifiedName(), expr
							.getSubstitutedExpression(schemaPrefix, dsTable,
									null));
				}

				// None left to do here? Don't do any then!
				if (exprCols.isEmpty())
					continue;

				// Rename temp to X table if required.
				if (useXTable) {
					final Rename rename = new Rename(this.datasetSchemaName,
							finalCombinedName);
					rename.setFrom(tempTable);
					rename.setTo(xTableName);
					this.issueAction(rename);
				}

				// Select only columns from all group bys in this group.
				if (!groupByCols.isEmpty())
					selectCols.retainAll(groupByCols);

				// Issue an AddExpression for the group.
				final AddExpression action = new AddExpression(
						this.datasetSchemaName, finalCombinedName);
				action.setTable(useXTable ? xTableName : previousTempTable);
				action.setSchemaPrefix(schemaPrefix);
				// We use a set to prevent post-modification problems.
				action.setSelectColumns(new HashSet(selectCols));
				action.setExpressionColumns(exprCols);
				if (!groupByCols.isEmpty())
					action.setGroupByColumns(groupByCols);
				action.setResultTable(tempTable);
				this.issueAction(action);

				// Update select cols for next time.
				for (final Iterator j = expGroup.iterator(); j.hasNext();)
					selectCols.add(((ExpressionColumn) j.next())
							.getModifiedName());

				// Drop the X table if it was used.
				if (useXTable) {
					final Drop drop = new Drop(this.datasetSchemaName,
							finalCombinedName);
					drop.setTable(xTableName);
					this.issueAction(drop);
				} else
					useXTable = true;
			}

			return useXTable;
		}

		private void issueAction(final MartConstructorAction action)
				throws ListenerException {
			// Execute the action.
			this.statusMessage = action.getStatusMessage();
			this.issueListenerEvent(MartConstructorListener.ACTION_EVENT, null,
					action);
		}

		private String getOptimiserTableName(
				final String schemaPartitionPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta,
				final DataSetTable dsTable, final DataSetOptimiserType oType)
				throws PartitionException {
			final StringBuffer finalName = new StringBuffer();
			if (schemaPartitionPrefix != null) {
				finalName.append(schemaPartitionPrefix);
				finalName.append(Resources.get("tablenameSubSep"));
			}
			finalName.append(dsTable.getSchema().getName());
			if (dsPta != null) {
				finalName.append(Resources.get("tablenameSubSep"));
				final PartitionColumn pcol = dsPta.getNamePartitionCol();
				finalName.append(pcol.getValueForRow(pcol.getPartitionTable()
						.currentRow()));
			}
			finalName.append(Resources.get("tablenameSep"));
			finalName.append(dsTable.getModifiedName());
			if (oType.equals(DataSetOptimiserType.TABLE_INHERIT)) {
				finalName.append(Resources.get("tablenameSubSep"));
				finalName.append(Resources.get("countTblPartition"));
				finalName.append(Resources.get("tablenameSep"));
				finalName.append(Resources.get("dimensionSuffix"));
			} else if (oType.equals(DataSetOptimiserType.TABLE_BOOL_INHERIT)) {
				finalName.append(Resources.get("tablenameSubSep"));
				finalName.append(Resources.get("boolTblPartition"));
				finalName.append(Resources.get("tablenameSep"));
				finalName.append(Resources.get("dimensionSuffix"));
			} else if (dsTable.getType().equals(DataSetTableType.MAIN)) {
				finalName.append(Resources.get("tablenameSep"));
				finalName.append(Resources.get("mainSuffix"));
			} else if (dsTable.getType().equals(DataSetTableType.MAIN_SUBCLASS)) {
				finalName.append(Resources.get("tablenameSep"));
				finalName.append(Resources.get("subclassSuffix"));
			} else if (dsTable.getType().equals(DataSetTableType.DIMENSION)) {
				if (dmPta != null) {
					finalName.append(Resources.get("tablenameSubSep"));
					final PartitionColumn pcol = dmPta.getNamePartitionCol();
					finalName.append(pcol.getValueForRow(pcol
							.getPartitionTable().currentRow()));
				}
				finalName.append(Resources.get("tablenameSep"));
				finalName.append(Resources.get("dimensionSuffix"));
			} else
				throw new BioMartError();
			final String name = finalName.toString().replaceAll("\\W+", "");
			// UC/LC/Mixed?
			switch (((DataSet) dsTable.getSchema()).getMart().getCase()) {
			case Mart.USE_LOWER_CASE:
				return name.toLowerCase();
			case Mart.USE_UPPER_CASE:
				return name.toUpperCase();
			default:
				return name;
			}
		}

		private String getOptimiserColumnName(
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta,
				final DataSetTable parent, final DataSetTable dsTable,
				final DataSetOptimiserType oType,
				final DataSetColumn restrictCol, final String restrictValue,
				final boolean prefix, final boolean suffix)
				throws PartitionException {
			// Set up storage for unique names if required.
			if (!this.uniqueOptCols.containsKey(parent))
				this.uniqueOptCols.put(parent, new HashSet());
			if (!this.indexOptCols.containsKey(parent))
				this.indexOptCols.put(parent, new HashSet());
			// Make a unique name.
			int counter = -1;
			String name;
			do {
				final StringBuffer sb = new StringBuffer();
				if (prefix) {
					sb.append(dsTable.getModifiedName());
					if (dmPta != null) {
						final PartitionColumn pcol = dmPta
								.getNamePartitionCol();
						sb.append(Resources.get("tablenameSubSep"));
						sb.append(pcol.getValueForRow(pcol.getPartitionTable()
								.currentRow()));
					}
					if (++counter > 0) {
						sb.append(Resources.get("tablenameSubSep"));
						sb.append("" + counter);
					}
				}
				if (restrictCol != null) {
					if (prefix) {
						sb.append(Resources.get("tablenameSubSep"));
						sb.append(restrictCol.getModifiedName());
						sb.append(Resources.get("tablenameSubSep"));
					}
					sb.append(restrictValue);
					if (!prefix && ++counter > 0) {
						sb.append(Resources.get("tablenameSubSep"));
						sb.append("" + counter);
					}
				}
				if (suffix) {
					sb.append(Resources.get("tablenameSubSep"));
					sb.append(oType.isBool() ? Resources.get("boolColSuffix")
							: Resources.get("countColSuffix"));
				}
				name = sb.toString();
			} while (((Collection) this.uniqueOptCols.get(parent))
					.contains(name));
			name = name.replaceAll("\\W+", "");
			// UC/LC/Mixed?
			switch (dsTable.getDataSet().getMart().getCase()) {
			case Mart.USE_LOWER_CASE:
				name = name.toLowerCase();
				break;
			case Mart.USE_UPPER_CASE:
				name = name.toUpperCase();
				break;
			}
			// Store the name above in the unique list for the parent.
			((Collection) this.uniqueOptCols.get(parent)).add(name);
			if (!dsTable.isSkipIndexOptimiser())
				((Collection) this.indexOptCols.get(parent)).add(name);
			return name;
		}

		private String getFinalName(final String schemaPartitionPrefix,
				final PartitionTableApplication dsPta,
				final PartitionTableApplication dmPta,
				final DataSetTable dsTable) throws PartitionException {
			final Object[] finalNameCacheKey = new Object[] {
					schemaPartitionPrefix, dsPta, dmPta, dsTable };
			if (dsTable.getType().equals(DataSetTableType.DIMENSION)
					&& this.finalNameCache.containsKey(finalNameCacheKey))
				return (String) this.finalNameCache.get(finalNameCacheKey);
			final StringBuffer finalName = new StringBuffer();
			if (schemaPartitionPrefix != null) {
				finalName.append(schemaPartitionPrefix);
				finalName.append(Resources.get("tablenameSubSep"));
			}
			finalName.append(dsTable.getSchema().getName());
			if (dsPta != null) {
				finalName.append(Resources.get("tablenameSubSep"));
				final PartitionColumn pcol = dsPta.getNamePartitionCol();
				finalName.append(pcol.getValueForRow(pcol.getPartitionTable()
						.currentRow()));
			}
			finalName.append(Resources.get("tablenameSep"));
			finalName.append(dsTable.getModifiedName());
			String finalSuffix;
			if (dsTable.getType().equals(DataSetTableType.MAIN))
				finalSuffix = Resources.get("mainSuffix");
			else if (dsTable.getType().equals(DataSetTableType.MAIN_SUBCLASS))
				finalSuffix = Resources.get("subclassSuffix");
			else if (dsTable.getType().equals(DataSetTableType.DIMENSION)) {
				if (dmPta != null) {
					finalName.append(Resources.get("tablenameSubSep"));
					final PartitionColumn pcol = dmPta.getNamePartitionCol();
					finalName.append(pcol.getValueForRow(pcol
							.getPartitionTable().currentRow()));
				}
				finalSuffix = Resources.get("dimensionSuffix");
			} else
				throw new BioMartError();
			finalName.append(Resources.get("tablenameSep"));
			finalName.append(finalSuffix);
			// Remove any non-[char/number/underscore] symbols.
			String name = finalName.toString().replaceAll("\\W+", "");
			// UC/LC/Mixed?
			switch (dsTable.getDataSet().getMart().getCase()) {
			case Mart.USE_LOWER_CASE:
				name = name.toLowerCase();
				break;
			case Mart.USE_UPPER_CASE:
				name = name.toUpperCase();
				break;
			default:
				break;
			}
			if (dsTable.getType().equals(DataSetTableType.DIMENSION)) {
				final String firstBit = name.substring(0, name.length()
						- (Resources.get("tablenameSep") + finalSuffix)
								.length());
				final String lastBit = name.substring(name.length()
						- (Resources.get("tablenameSep") + finalSuffix)
								.length());
				int i = 1;
				final DecimalFormat formatter = new DecimalFormat("000");
				while (this.finalNameCache.containsValue(name))
					// Clash! Rename the table to avoid it.
					name = firstBit + Resources.get("tablenameSubSep")
							+ Resources.get("clashSuffix")
							+ formatter.format(i++) + lastBit;
				this.finalNameCache.put(finalNameCacheKey, name);
			}
			return name;
		}

		private void issueListenerEvent(final int event)
				throws ListenerException {
			this.issueListenerEvent(event, null);
		}

		private void issueListenerEvent(final int event, final Object data)
				throws ListenerException {
			this.issueListenerEvent(event, data, null);
		}

		private void issueListenerEvent(final int event, final Object data,
				final MartConstructorAction action) throws ListenerException {
			Log.debug("Event issued: event:" + event + " data:" + data
					+ " action:" + action);
			for (final Iterator i = this.martConstructorListeners.iterator(); i
					.hasNext();)
				((MartConstructorListener) i.next())
						.martConstructorEventOccurred(event, data, action);
		}

		public void addMartConstructorListener(
				final MartConstructorListener listener) {
			Log.debug("Listener added to constructor runnable");
			this.martConstructorListeners.add(listener);
		}

		public void cancel() {
			Log.debug("Constructor runnable cancelled");
			this.cancelled = true;
		}

		public Exception getFailureException() {
			return this.failure;
		}

		public int getPercentComplete() {
			return (int) this.percentComplete;
		}

		public String getStatusMessage() {
			return this.statusMessage;
		}

		public void run() {
			try {
				// Begin.
				Log.debug("Construction started");
				this.issueListenerEvent(MartConstructorListener.CONSTRUCTION_STARTED);

				// Work out how many datasets we have.
				final int totalDataSetCount = this.datasets.size();

				for (final Iterator j = this.datasets.iterator(); j.hasNext();) {
					// Loop over all the datasets we want included from this
					// mart. Build actions for each one.
					final DataSet ds = (DataSet) j.next();
					try {
						this.makeActionsForDataset(ds, totalDataSetCount);
					} catch (final Throwable t) {
						throw t;
					}
				}
				this.issueListenerEvent(MartConstructorListener.CONSTRUCTION_ENDED);
				Log.info("Construction ended");
			} catch (final ConstructorException e) {
				// This is so the users see a nice message straight away.
				this.failure = e;
			} catch (final Throwable t) {
				this.failure = new ConstructorException(t);
			} finally {
				this.alive = false;
			}
		}

		public boolean isAlive() {
			return this.alive;
		}

		public void finalize() {
			this.alive = false;
		}
	}

	/**
	 * This interface defines a listener which hears events about mart
	 * construction. The events are defined as constants in this interface. The
	 * listener will take these events and either build scripts for later
	 * execution, or will execute them directly in order to physically construct
	 * the mart.
	 */
	public interface MartConstructorListener {

		/**
		 * This event will occur when an action needs performing, and will be
		 * accompanied by a {@link MartConstructorAction} object describing what
		 * needs doing.
		 */
		public static final int ACTION_EVENT = 0;

		/**
		 * This event will occur when mart construction ends.
		 */
		public static final int CONSTRUCTION_ENDED = 1;

		/**
		 * This event will occur when mart construction begins.
		 */
		public static final int CONSTRUCTION_STARTED = 2;

		/**
		 * This event will occur when an individual dataset ends.
		 */
		public static final int DATASET_ENDED = 3;

		/**
		 * This event will occur when an individual dataset begins.
		 */
		public static final int DATASET_STARTED = 4;

		/**
		 * This event will occur when an individual schema partition ends.
		 */
		public static final int PARTITION_ENDED = 5;

		/**
		 * This event will occur when an individual schema partition begins.
		 */
		public static final int PARTITION_STARTED = 6;

		/**
		 * This method will be called when an event occurs.
		 * 
		 * @param event
		 *            the event that occurred. See the constants defined
		 *            elsewhere in this interface for possible events.
		 * @param data
		 *            ancilliary data, may be null.
		 * @param action
		 *            an action object that belongs to this event. Will be
		 *            <tt>null</tt> in all cases except where the event is
		 *            {@link #ACTION_EVENT}.
		 * @throws ListenerException
		 *             if anything goes wrong whilst handling the event.
		 */
		public void martConstructorEventOccurred(int event, Object data,
				MartConstructorAction action) throws ListenerException;
	}
}
