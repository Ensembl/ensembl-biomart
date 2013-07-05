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
package org.biomart.builder.controller.dialects;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.biomart.builder.exceptions.ConstructorException;
import org.biomart.builder.exceptions.PartitionException;
import org.biomart.builder.model.Column;
import org.biomart.builder.model.DataLink;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Key;
import org.biomart.builder.model.MartConstructorAction;
import org.biomart.builder.model.PartitionTable;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Table;
import org.biomart.builder.model.TransformationUnit;
import org.biomart.builder.model.DataLink.JDBCDataLink;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetTable;
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
import org.biomart.builder.model.Relation.RestrictedRelationDefinition;
import org.biomart.builder.model.Table.RestrictedTableDefinition;
import org.biomart.builder.model.TransformationUnit.JoinTable;
import org.biomart.builder.model.TransformationUnit.SelectFromTable;
import org.biomart.builder.model.TransformationUnit.SkipTable;
import org.biomart.builder.model.TransformationUnit.UnrollTable;
import org.biomart.common.exceptions.BioMartError;
import org.biomart.common.resources.Resources;

/**
 * Understands how to create SQL and DDL for a MySQL database.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.102 $, $Date: 2008-03-12 14:22:38 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class MySQLDialect extends DatabaseDialect {

	private int indexCount;

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doInitialUnroll(final InitialUnroll action,
			final List statements) throws Exception {
		final String schemaName = action.getDataSetSchemaName();

		this.checkColumnName(action.getUnrollIDCol());
		this.checkColumnName(action.getUnrollNameCol());
		this.checkColumnName(action.getUnrollIterationCol());

		final StringBuffer sb = new StringBuffer();
		sb.append("create table ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getTable());
		if (action.getBigTable() > 0) {
			sb.append(" max_rows=");
			sb.append(action.getBigTable());
		}
		sb.append(" as select parent.*, parent.");
		sb.append(action.getUnrollPKCol());
		sb.append(" as ");
		sb.append(action.getUnrollIDCol());
		sb.append(", parent.");
		sb.append(action.getNamingCol());
		sb.append(" as ");
		sb.append(action.getUnrollNameCol());
		sb.append(", 1 as ");
		sb.append(action.getUnrollIterationCol());
		sb.append(" from ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append(" as parent");

		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doExpandUnroll(final ExpandUnroll action, final List statements)
			throws Exception {
		final String schemaName = action.getDataSetSchemaName();

		final boolean reversed = action.isReversed();

		final StringBuffer sb = new StringBuffer();
		sb.append("insert into ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append('(');
		for (final Iterator i = action.getParentCols().iterator(); i.hasNext();) {
			sb.append((String) i.next());
			sb.append(',');
		}
		sb.append(action.getUnrollIDCol());
		sb.append(',');
		sb.append(action.getUnrollNameCol());
		sb.append(',');
		sb.append(action.getUnrollIterationCol());
		sb.append(") select distinct");
		for (final Iterator i = action.getParentCols().iterator(); i.hasNext();) {
			final String parentCol = (String) i.next();
			if (reversed) {
				if (parentCol.equals(action.getUnrollPKCol())) {
					sb.append(" child.");
					sb.append(action.getUnrollFKCol());
				} else {
					sb.append(" parent.");
					sb.append(parentCol);
				}
			} else {
				if (parentCol.equals(action.getUnrollFKCol()))
					sb.append(" child.");
				else
					sb.append(" parent.");
				sb.append(parentCol);
			}
			sb.append(',');
		}
		sb.append(" child.");
		sb.append(reversed ? action.getUnrollIDCol() : action.getUnrollPKCol());
		sb.append(" as ");
		sb.append(action.getUnrollIDCol());
		sb.append(", child.");
		sb.append(reversed ? action.getUnrollNameCol() : action.getNamingCol());
		sb.append(" as ");
		sb.append(action.getUnrollNameCol());
		sb.append(", ");
		sb.append(action.getUnrollIteration() + 1);
		sb.append(" as ");
		sb.append(action.getUnrollIterationCol());
		sb.append(" from ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append(" as parent inner join ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append(" as child on parent.");
		sb.append(reversed ? action.getUnrollPKCol() : action.getUnrollFKCol());
		sb.append("=child.");
		sb.append(reversed ? action.getUnrollFKCol() : action.getUnrollPKCol());
		sb.append(" and parent.");
		sb.append(action.getUnrollIterationCol());
		sb.append('=');
		sb.append(action.getUnrollIteration());

		statements.add(sb.toString());

		// MySQL 4 has no 'delete from where' which can reference
		// itself. Therefore we need a temp table workaround.
		sb.setLength(0);
		sb.append("create table ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append('_');
		sb.append(action.getUnrollIteration());
		sb.append('t');
		if (action.getBigTable() > 0) {
			sb.append(" max_rows=");
			sb.append(action.getBigTable());
		}
		sb.append(" as select distinct ");
		sb.append(action.getUnrollPKCol());
		sb.append(',');
		sb.append(action.getUnrollIDCol());
		sb.append(" from ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append(" where ");
		sb.append(action.getUnrollIterationCol());
		sb.append('<');
		sb.append(action.getUnrollIteration() + 1);

		statements.add(sb.toString());

		sb.setLength(0);
		sb.append("create index ");
		sb.append(action.getSourceTable());
		sb.append('_');
		sb.append(action.getUnrollIteration());
		sb.append("ti on ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append('_');
		sb.append(action.getUnrollIteration());
		sb.append("t(");
		sb.append(action.getUnrollPKCol());
		sb.append(',');
		sb.append(action.getUnrollIDCol());
		sb.append(')');

		statements.add(sb.toString());

		sb.setLength(0);
		sb.append("delete from ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append(" where ");
		sb.append(action.getUnrollIterationCol());
		sb.append('=');
		sb.append(action.getUnrollIteration() + 1);
		sb.append(" and (");
		sb.append(action.getUnrollPKCol());
		sb.append(',');
		sb.append(action.getUnrollIDCol());
		sb.append(") in (select ");
		sb.append(action.getUnrollPKCol());
		sb.append(',');
		sb.append(action.getUnrollIDCol());
		sb.append(" from ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append('_');
		sb.append(action.getUnrollIteration());
		sb.append("t)");

		statements.add(sb.toString());

		sb.setLength(0);

		sb.append("drop table ");
		sb.append(schemaName);
		sb.append('.');
		sb.append(action.getSourceTable());
		sb.append('_');
		sb.append(action.getUnrollIteration());
		sb.append('t');

		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doRename(final Rename action, final List statements)
			throws Exception {
		final String schemaName = action.getDataSetSchemaName();
		final String oldTableName = action.getFrom();
		final String newTableName = action.getTo();

		this.checkTableName(newTableName);

		statements.add("rename table " + schemaName + "." + oldTableName
				+ " to " + schemaName + "." + newTableName + "");
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doSelect(final Select action, final List statements)
			throws Exception {
		final String createTableSchema = action.getDataSetSchemaName();
		final String createTableName = action.getResultTable();
		final String fromTableSchema = action.getSchema();
		final String fromTableName = action.getTable();

		final StringBuffer sb = new StringBuffer();
		sb.append("create table " + createTableSchema + "." + createTableName);
		if (action.getBigTable() > 0) {
			sb.append(" max_rows=");
			sb.append(action.getBigTable());
		}
		sb.append(" as select ");
		for (final Iterator i = action.getSelectColumns().entrySet().iterator(); i
				.hasNext();) {
			final Map.Entry entry = (Map.Entry) i.next();
			sb.append("a.");
			sb.append(entry.getKey());
			if (!entry.getKey().equals(entry.getValue())) {
				this.checkColumnName((String) entry.getValue());
				sb.append(" as ");
				sb.append(entry.getValue());
			}
			if (i.hasNext())
				sb.append(',');
		}
		sb.append(" from " + fromTableSchema + "." + fromTableName + " as a");
		if (action.getTableRestriction() != null
				|| !action.getPartitionRestrictions().isEmpty())
			sb.append(" where ");
		for (final Iterator i = action.getPartitionRestrictions().entrySet()
				.iterator(); i.hasNext();) {
			final Map.Entry entry = (Map.Entry) i.next();
			sb.append("a.");
			sb.append((String) entry.getKey());
			sb.append("='");
			sb.append((String) entry.getValue());
			sb.append('\'');
			if (i.hasNext() || action.getTableRestriction() != null)
				sb.append(" and ");
		}
		if (action.getTableRestriction() != null)
			sb.append(action.getTableRestriction().getSubstitutedExpression(
					action.getSchemaPrefix(), "a"));

		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doDistinct(final Distinct action, final List statements)
			throws Exception {
		final String createTableSchema = action.getDataSetSchemaName();
		final String createTableName = action.getResultTable();
		final String fromTableSchema = action.getSchema();
		final String fromTableName = action.getTable();

		final StringBuffer cols = new StringBuffer();
		for (final Iterator i = action.getKeepCols().iterator(); i.hasNext();) {
			cols.append(i.next());
			if (i.hasNext())
				cols.append(',');
		}

		final StringBuffer sb = new StringBuffer();
		sb.append("create table " + createTableSchema + "." + createTableName);
		if (action.getBigTable() > 0) {
			sb.append(" max_rows=");
			sb.append(action.getBigTable());
		}
		sb.append(" as select distinct " + cols + " from " + fromTableSchema
				+ "." + fromTableName);

		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doAddExpression(final AddExpression action,
			final List statements) throws Exception {
		final String createTableSchema = action.getDataSetSchemaName();
		final String createTableName = action.getResultTable();
		final String fromTableSchema = action.getDataSetSchemaName();
		final String fromTableName = action.getTable();

		final StringBuffer sb = new StringBuffer();
		sb.append("create table " + createTableSchema + "." + createTableName
				+ " as select ");
		for (final Iterator i = action.getSelectColumns().iterator(); i
				.hasNext();) {
			final String entry = (String) i.next();
			sb.append(entry);
			if (i.hasNext())
				sb.append(',');
		}
		for (final Iterator i = action.getExpressionColumns().entrySet()
				.iterator(); i.hasNext();) {
			final Map.Entry entry = (Map.Entry) i.next();
			sb.append(',');
			this.checkColumnName((String) entry.getKey());
			sb.append((String) entry.getValue());
			sb.append(" as ");
			sb.append((String) entry.getKey());
		}
		sb.append(" from " + fromTableSchema + "." + fromTableName);
		if (action.getGroupByColumns() != null) {
			sb.append(" group by ");
			for (final Iterator i = action.getGroupByColumns().iterator(); i
					.hasNext();) {
				final String entry = (String) i.next();
				sb.append(entry);
				if (i.hasNext())
					sb.append(',');
			}
		}
		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doIndex(final Index action, final List statements)
			throws Exception {
		final String schemaName = action.getDataSetSchemaName();
		final String tableName = action.getTable();
		final StringBuffer sb = new StringBuffer();

		sb.append("create index I_" + this.indexCount++ + " on " + schemaName
				+ "." + tableName + "(");
		for (final Iterator i = action.getColumns().iterator(); i.hasNext();) {
			sb.append(i.next());
			if (i.hasNext())
				sb.append(',');
		}
		sb.append(")");

		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doJoin(final Join action, final List statements)
			throws Exception {
		final String srcSchemaName = action.getDataSetSchemaName();
		final String srcTableName = action.getLeftTable();
		final String trgtSchemaName = action.getRightSchema();
		final String trgtTableName = action.getRightTable();
		final String mergeTableName = action.getResultTable();

		final String joinType = action.isLeftJoin() ? "left" : "inner";

		final StringBuffer sb = new StringBuffer();
		sb.append("create table " + action.getDataSetSchemaName() + "."
				+ mergeTableName);
		if (action.getBigTable() > 0) {
			sb.append(" max_rows=");
			sb.append(action.getBigTable());
		}
		sb.append(" as select a.*");
		for (final Iterator i = action.getSelectColumns().entrySet().iterator(); i
				.hasNext();) {
			final Map.Entry entry = (Map.Entry) i.next();
			sb.append(",b.");
			sb.append(entry.getKey());
			if (!entry.getKey().equals(entry.getValue())) {
				this.checkColumnName((String) entry.getValue());
				sb.append(" as ");
				sb.append(entry.getValue());
			}
		}
		sb.append(" from " + srcSchemaName + "." + srcTableName + " as a "
				+ joinType + " join " + trgtSchemaName + "." + trgtTableName
				+ " as b on ");
		for (int i = 0; i < action.getLeftJoinColumns().size(); i++) {
			if (i > 0)
				sb.append(" and ");
			final String pkColName = (String) action.getLeftJoinColumns()
					.get(i);
			final String fkColName = (String) action.getRightJoinColumns().get(
					i);
			sb.append("a." + pkColName + "=b." + fkColName + "");
		}
		if (action.getRelationRestriction() != null) {
			sb.append(" and ");
			sb.append(action.getRelationRestriction().getSubstitutedExpression(
					action.getSchemaPrefix(),
					action.isRelationRestrictionLeftIsFirst() ? "a" : "b",
					action.isRelationRestrictionLeftIsFirst() ? "b" : "a",
					action.isRelationRestrictionLeftIsFirst(),
					!action.isRelationRestrictionLeftIsFirst(),
					action.getRelationRestrictionPreviousUnit()));
		}
		if (action.getTableRestriction() != null) {
			sb.append(" and (");
			sb.append(action.getTableRestriction().getSubstitutedExpression(
					action.getSchemaPrefix(), "b"));
			sb.append(')');
		}
		for (final Iterator i = action.getPartitionRestrictions().entrySet()
				.iterator(); i.hasNext();) {
			final Map.Entry entry = (Map.Entry) i.next();
			sb.append(" and b.");
			sb.append((String) entry.getKey());
			sb.append("='");
			sb.append((String) entry.getValue());
			sb.append('\'');
		}
		if (action.getLoopbackDiffSource() != null) {
			sb.append(" and a.");
			sb.append(action.getLoopbackDiffSource());
			sb.append("<>b.");
			sb.append(action.getLoopbackDiffTarget());
		}

		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doLeftJoin(final LeftJoin action, final List statements)
			throws Exception {
		final String srcSchemaName = action.getDataSetSchemaName();
		final String srcTableName = action.getLeftTable();
		final String trgtSchemaName = action.getRightSchema();
		final String trgtTableName = action.getRightTable();
		final String mergeTableName = action.getResultTable();

		final StringBuffer sb = new StringBuffer();
		sb.append("create table " + action.getDataSetSchemaName() + "."
				+ mergeTableName);
		if (action.getBigTable() > 0) {
			sb.append(" max_rows=");
			sb.append(action.getBigTable());
		}
		sb.append(" as select ");
		for (final Iterator i = action.getLeftSelectColumns().iterator(); i
				.hasNext();) {
			final String entry = (String) i.next();
			sb.append("a.");
			sb.append(entry);
			sb.append(',');
		}
		for (final Iterator i = action.getRightSelectColumns().iterator(); i
				.hasNext();) {
			final String entry = (String) i.next();
			sb.append("b.");
			sb.append(entry);
			if (i.hasNext())
				sb.append(',');
		}
		sb.append(" from " + srcSchemaName + "." + srcTableName
				+ " as a left join " + trgtSchemaName + "." + trgtTableName
				+ " as b on ");
		for (int i = 0; i < action.getLeftJoinColumns().size(); i++) {
			if (i > 0)
				sb.append(" and ");
			final String pkColName = (String) action.getLeftJoinColumns()
					.get(i);
			final String fkColName = (String) action.getRightJoinColumns().get(
					i);
			sb.append("a." + pkColName + "=b." + fkColName + "");
		}

		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doDropColumns(final DropColumns action, final List statements)
			throws Exception {
		final String schemaName = action.getDataSetSchemaName();
		final String tableName = action.getTable();

		for (final Iterator i = action.getColumns().iterator(); i.hasNext();)
			statements.add("alter table " + schemaName + "." + tableName
					+ " drop column " + (String) i.next());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doDrop(final Drop action, final List statements)
			throws Exception {
		final String schemaName = action.getDataSetSchemaName();
		final String tableName = action.getTable();

		statements.add("drop table " + schemaName + "." + tableName + "");
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doCreateOptimiser(final CreateOptimiser action,
			final List statements) throws Exception {
		final String schemaName = action.getDataSetSchemaName();
		final String sourceTableName = action.getDataSetTableName();
		final String optTableName = action.getOptTableName();

		this.checkTableName(optTableName);

		final StringBuffer sb = new StringBuffer();
		sb.append("create table " + schemaName + "." + optTableName);
		if (action.getBigTable() > 0) {
			sb.append(" max_rows=");
			sb.append(action.getBigTable());
		}
		sb.append(" as select ");
		for (final Iterator i = action.getKeyColumns().iterator(); i.hasNext();) {
			sb.append((String) i.next());
			if (i.hasNext())
				sb.append(',');
		}
		sb.append(" from " + schemaName + "." + sourceTableName);
		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doCopyOptimiser(final CopyOptimiser action,
			final List statements) throws Exception {
		final String schemaName = action.getDataSetSchemaName();
		final String parentOptTableName = action.getParentOptTableName();
		final String optTableName = action.getOptTableName();
		final String optColName = action.getOptColumnName();

		this.checkColumnName(optColName);

		statements.add("alter table " + schemaName + "." + optTableName
				+ " add column (" + optColName + " integer default 0)");

		final StringBuffer sb = new StringBuffer();
		sb.append("update " + schemaName + "." + optTableName + " a set "
				+ optColName + "=(select max(" + optColName + ") from " + schemaName
				+ "." + parentOptTableName + " b where ");
		for (final Iterator i = action.getKeyColumns().iterator(); i.hasNext();) {
			final String keyCol = (String) i.next();
			sb.append("a.");
			sb.append(keyCol);
			sb.append("=b.");
			sb.append(keyCol);
			if (i.hasNext())
				sb.append(" and ");
		}
		sb.append(')');
		statements.add(sb.toString());
	}

	/**
	 * Performs an action.
	 * 
	 * @param action
	 *            the action to perform.
	 * @param statements
	 *            the list into which statements will be added.
	 * @throws Exception
	 *             if anything goes wrong.
	 */
	public void doUpdateOptimiser(final UpdateOptimiser action,
			final List statements) throws Exception {
		final String schemaName = action.getDataSetSchemaName();
		final String sourceTableName = action.getSourceTableName();
		final String optTableName = action.getOptTableName();
		final String optColName = action.getOptColumnName();
		final String optRestrictColName = action.getOptRestrictColumn();
		final String optRestrictValue = action.getOptRestrictValue();

		this.checkColumnName(optColName);

		final String colType = action.getValueColumnName() == null ? "integer default 0"
				: "varchar("+action.getValueColumnSize()+")";

		statements.add("alter table " + schemaName + "." + optTableName
				+ " add column (" + optColName + " " + colType + ")");

		final String countStmt = action.getValueColumnName() == null ? (action
				.isCountNotBool() ? "count(1)" : "case count(1) when 0 then "
				+ (action.isNullNotZero() ? "null" : "0") + " else 1 end")
				: "group_concat(b." + action.getValueColumnName()
						+ " separator '" + action.getValueColumnSeparator()
						+ "')";

		final StringBuffer sb = new StringBuffer();
		sb.append("update " + schemaName + "." + optTableName + " a set "
				+ optColName + "=(select " + countStmt + " from " + schemaName
				+ "." + sourceTableName + " b where ");
		for (final Iterator i = action.getKeyColumns().iterator(); i.hasNext();) {
			final String keyCol = (String) i.next();
			sb.append("a.");
			sb.append(keyCol);
			sb.append("=b.");
			sb.append(keyCol);
			sb.append(" and ");
		}
		if (optRestrictColName != null) {
			sb.append("b.");
			sb.append(optRestrictColName);
			if (optRestrictValue == null)
				sb.append(" is null and");
			else {
				sb.append("='");
				sb.append(optRestrictValue);
				sb.append("' and ");
			}
		}
		sb.append("not (");
		for (final Iterator i = action.getNonNullColumns().iterator(); i
				.hasNext();) {
			sb.append("b.");
			sb.append((String) i.next());
			sb.append(" is null");
			if (i.hasNext())
				sb.append(" and ");
		}
		sb.append("))");
		statements.add(sb.toString());
	}

	public String[] getStatementsForAction(final MartConstructorAction action)
			throws ConstructorException {

		final List statements = new ArrayList();

		try {
			final String className = action.getClass().getName();
			final String methodName = "do"
					+ className.substring(className.lastIndexOf('$') + 1);
			final Method method = this.getClass().getMethod(methodName,
					new Class[] { action.getClass(), List.class });
			method.invoke(this, new Object[] { action, statements });
		} catch (final InvocationTargetException ite) {
			final Throwable t = ite.getCause();
			if (t instanceof ConstructorException)
				throw (ConstructorException) t;
			else
				throw new ConstructorException(t);
		} catch (final Throwable t) {
			if (t instanceof ConstructorException)
				throw (ConstructorException) t;
			else
				throw new ConstructorException(t);
		}

		return (String[]) statements.toArray(new String[0]);
	}

	public void reset() {
		this.indexCount = 0;
	}

	public boolean understandsDataLink(final DataLink dataLink) {
		// Convert to JDBC version.
		if (!(dataLink instanceof JDBCDataLink))
			return false;
		final JDBCDataLink jddl = (JDBCDataLink) dataLink;

		try {
			return jddl.getConnection(null).getMetaData()
					.getDatabaseProductName().equals("MySQL");
		} catch (final SQLException e) {
			throw new BioMartError(e);
		}
	}

	public String getUnrollTableSQL(final String schemaPrefix,
			final DataSet dataset, final DataSetTable dsTable,
			final Relation parentRel, final Relation childRel,
			final String schemaPartition, final Schema templateSchema,
			final UnrollTable utu) {
		final StringBuffer sql = new StringBuffer();
		// From lookup table joined with parent table,
		// find both parent ID col and child ID col.
		final Table parentTable = parentRel.getOneKey().getTable();
		final Table childTable = parentRel.getManyKey().getTable();
		sql.append("select child.");
		sql.append(childRel.getManyKey().getColumns()[0].getName());
		sql.append(", child.");
		sql.append(parentRel.getManyKey().getColumns()[0].getName());
		sql.append(" from ");
		sql.append(schemaPartition == null ? ((JDBCDataLink) templateSchema)
				.getDataLinkSchema() : schemaPartition);
		sql.append('.');
		sql.append(parentTable.getName());
		sql.append(" as parent, ");
		sql.append(schemaPartition == null ? ((JDBCDataLink) templateSchema)
				.getDataLinkSchema() : schemaPartition);
		sql.append('.');
		sql.append(childTable.getName());
		sql.append(" as child where parent.");
		sql.append(parentRel.getOneKey().getColumns()[0].getName());
		sql.append("=child.");
		sql.append(childRel.getManyKey().getColumns()[0].getName());
		if (parentTable.getRestrictTable(dataset, dsTable.getName()) != null) {
			sql.append(" and ");
			sql.append(parentTable.getRestrictTable(dataset, dsTable.getName())
					.getSubstitutedExpression(schemaPrefix, "parent"));
		}
		if (childTable.getRestrictTable(dataset, dsTable.getName()) != null) {
			sql.append(" and ");
			sql.append(childTable.getRestrictTable(dataset, dsTable.getName())
					.getSubstitutedExpression(schemaPrefix, "child"));
		}
		if (childRel.getRestrictRelation(dataset, dsTable.getName(), 0) != null) {
			sql.append(" and ");
			sql
					.append(childRel.getRestrictRelation(dataset,
							dsTable.getName(), 0)
							.getSubstitutedExpression(
									schemaPrefix,
									childRel.getFirstKey().equals(
											childRel.getOneKey()) ? "parent"
											: "child",
									childRel.getFirstKey().equals(
											childRel.getManyKey()) ? "parent"
											: "child", false, false, utu));
		}
		if (parentRel.getRestrictRelation(dataset, dsTable.getName(), 0) != null) {
			sql.append(" and ");
			sql
					.append(parentRel.getRestrictRelation(dataset,
							dsTable.getName(), 0)
							.getSubstitutedExpression(
									schemaPrefix,
									parentRel.getFirstKey().equals(
											parentRel.getOneKey()) ? "parent"
											: "child",
									parentRel.getFirstKey().equals(
											parentRel.getManyKey()) ? "parent"
											: "child", false, false, utu));
		}
		return sql.toString();
	}

	public String getPartitionTableRowsSQL(final String schemaPrefix,
			final Map positionMap, final PartitionTable pt, final DataSet ds,
			final Schema schema, final String usablePartition)
			throws PartitionException {
		final StringBuffer sql = new StringBuffer();

		// This is generic SQL and should not need any dialects.

		final List trueSelectedCols = new ArrayList();
		for (final Iterator i = pt.getSelectedColumnNames().iterator(); i
				.hasNext();) {
			final String col = (String) i.next();
			if (!col.equals(PartitionTable.DIV_COLUMN))
				trueSelectedCols.add(col);
		}

		// Make a map of columns in statement to
		// named columns in results. Use allCols to
		// map modified names back to real names in
		// order to track down dataset column objects.
		// Update position map with column modified names.
		// Keys are column names, values are integers.
		int nextCol = 1; // ResultSet is 1-indexed.
		final StringBuffer sqlSel = new StringBuffer();
		sqlSel.append("select distinct ");
		final StringBuffer sqlFrom = new StringBuffer();
		sqlFrom.append(" from ");
		final StringBuffer sqlWhere = new StringBuffer();
		sqlWhere.append(" where ");
		int currSuffix = 0;
		final Map prevSuffixes = new HashMap();
		for (final Iterator i = ds.getMainTable().getTransformationUnits()
				.iterator(); i.hasNext()
				&& positionMap.size() <= trueSelectedCols.size();) {
			final TransformationUnit tu = (TransformationUnit) i.next();
			if (tu instanceof SelectFromTable) {
				// JoinTable extends SelectFromTable.
				// Skip SkipTables and UnrollTables.
				if (tu instanceof SkipTable || tu instanceof UnrollTable)
					continue;
				// Add the unit to the from clause.
				final Table selTab = ((SelectFromTable) tu).getTable();
				final String selSch = selTab.getSchema().equals(schema) ? usablePartition
						: selTab.getSchema().getDataLinkSchema();
				Key prevKey = null;
				if (tu instanceof JoinTable) {
					prevKey = ((JoinTable) tu).getSchemaSourceKey();
					sqlFrom.append(',');
				}
				sqlFrom.append(selSch);
				sqlFrom.append('.');
				sqlFrom.append(selTab.getName());
				sqlFrom.append(" as a");
				sqlFrom.append(++currSuffix);
				prevSuffixes.put(tu, new Integer(currSuffix));
				if (tu instanceof JoinTable) {
					final JoinTable jtu = (JoinTable) tu;
					final int lhs;
					final int rhs;
					final TransformationUnit prevTu = jtu.getPreviousUnit();
					if (prevKey.equals(jtu.getSchemaRelation().getFirstKey())) {
						lhs = ((Integer) prevSuffixes.get(prevTu)).intValue();
						rhs = currSuffix;
					} else {
						rhs = ((Integer) prevSuffixes.get(prevTu)).intValue();
						lhs = currSuffix;
					}
					// Append join info to where clause.
					if (!sqlWhere.toString().equals(" where "))
						sqlWhere.append(" and ");
					for (int k = 0; k < prevKey.getColumns().length; k++) {
						if (k > 0)
							sqlWhere.append(" and ");
						sqlWhere.append('a');
						sqlWhere.append(prevKey.equals(jtu.getSchemaRelation()
								.getFirstKey()) ? lhs : rhs);
						sqlWhere.append('.');
						sqlWhere.append(((Column) prevKey.getColumns()[k])
								.getName());
						sqlWhere.append('=');
						sqlWhere.append('a');
						sqlWhere.append(prevKey.equals(jtu.getSchemaRelation()
								.getFirstKey()) ? rhs : lhs);
						sqlWhere.append('.');
						sqlWhere.append(((Column) jtu.getSchemaRelation()
								.getOtherKey(jtu.getSchemaSourceKey())
								.getColumns()[k]).getName());
					}
					// Add any rel restrictions to where clause.
					final RestrictedRelationDefinition rr = jtu
							.getSchemaRelation().getRestrictRelation(ds,
									ds.getMainTable().getName(),
									jtu.getSchemaRelationIteration());
					if (rr != null) {
						sqlWhere.append(" and ");
						sqlWhere.append(rr.getSubstitutedExpression(
								schemaPrefix, "a" + lhs, "a" + rhs, false,
								false, jtu));
					}
				}
				// Add any table restrictions to where clause.
				final RestrictedTableDefinition rt = selTab.getRestrictTable(
						ds, ds.getMainTable().getName());
				if (rt != null) {
					if (!sqlWhere.toString().equals(" where "))
						sqlWhere.append(" and ");
					sqlWhere.append(rt.getSubstitutedExpression(schemaPrefix,
							"a" + currSuffix));
				}
				// If any unit columns match selected columns,
				// add them to the select statement and their
				// position to the index map.
				for (final Iterator j = tu.getNewColumnNameMap().entrySet()
						.iterator(); j.hasNext();) {
					final Map.Entry entry = (Map.Entry) j.next();
					final DataSetColumn dsCol = (DataSetColumn) entry
							.getValue();
					if (trueSelectedCols.contains(dsCol.getModifiedName())) {
						final Column col = (Column) entry.getKey();
						if (nextCol > 1)
							sqlSel.append(',');
						sqlSel.append('a');
						sqlSel.append(currSuffix);
						sqlSel.append('.');
						sqlSel.append(col.getName());
						positionMap.put(new Integer(nextCol++), dsCol
								.getModifiedName());
					}
				}
			} else
				throw new PartitionException(Resources.get("cannotDoBasicSQL"));
		}

		// Build SQL.
		sql.append(sqlSel);
		sql.append(sqlFrom);
		if (!sqlWhere.toString().equals(" where "))
			sql.append(sqlWhere);

		return sql.toString();
	}

	public String getSimpleRowsSQL(final String schemaName, final Table table) {
		final StringBuffer sql = new StringBuffer();
		sql.append("select ");
		for (final Iterator i = table.getColumns().keySet().iterator(); i
				.hasNext();) {
			sql.append((String) i.next());
			if (i.hasNext())
				sql.append(',');
		}
		sql.append(" from ");
		sql.append(schemaName);
		sql.append('.');
		sql.append(table.getName());
		return sql.toString();
	}

	public String getUniqueValuesSQL(final String schemaName,
			final Column column) {
		final StringBuffer sql = new StringBuffer();
		sql.append("select distinct ");
		sql.append(column.getName());
		sql.append(" from ");
		sql.append(schemaName);
		sql.append('.');
		sql.append(column.getTable().getName());
		return sql.toString();
	}
}
