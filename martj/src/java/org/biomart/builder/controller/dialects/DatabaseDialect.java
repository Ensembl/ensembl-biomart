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

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.biomart.builder.exceptions.ConstructorException;
import org.biomart.builder.exceptions.PartitionException;
import org.biomart.builder.model.Column;
import org.biomart.builder.model.DataLink;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.MartConstructorAction;
import org.biomart.builder.model.PartitionTable;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Table;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.Schema.JDBCSchema;
import org.biomart.builder.model.TransformationUnit.UnrollTable;
import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;

/**
 * This class provides methods which generate atomic DDL or SQL statements. It
 * could be an interface, except for the static initializers which the
 * implementing classes use to register themselves. Once registered, the
 * {@link DatabaseDialect#getDialect(DataLink)} method will be able to identify
 * which dialect to use for a given {@link DataLink}.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.25 $, $Date: 2008-02-20 11:47:30 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public abstract class DatabaseDialect {

	private static final Set dialects = new HashSet();

	private int maxTableNameLength = Integer.MAX_VALUE;

	private int maxColumnNameLength = Integer.MAX_VALUE;

	/**
	 * Registers all known dialects for use with this system. Each implementing
	 * class should be added to this list.
	 */
	static {
		DatabaseDialect.dialects.add(new MySQLDialect());
		DatabaseDialect.dialects.add(new OracleDialect());
		DatabaseDialect.dialects.add(new PostgreSQLDialect());
	}

	/**
	 * Common constructor for subclasses does nothing except log that the
	 * subclass has been created and registered.
	 */
	protected DatabaseDialect() {
		Log.info("Registering dialect: " + this.getClass().getName());
	}

	/**
	 * Work out what kind of dialect to use for the given data link. It does
	 * this by checking each registered dialect to see if the
	 * {@link DatabaseDialect#understandsDataLink(DataLink)} method returns
	 * <tt>true</tt> for that data link. It returns the first one that returns
	 * <tt>true</tt>, ignoring any subsequent ones.
	 * <p>
	 * Note that it must be able to open the connection provided by
	 * <tt>dataLink</tt> in order to get some basic attributes from it that
	 * determine compatibility, such as maximum table and column name lengths.
	 * 
	 * @param dataLink
	 *            the data link to work out the dialect for.
	 * @return the appropriate DatabaseDialect, or <tt>null</tt> if none
	 *         found.
	 * @throws SQLException
	 *             if it was unable to determine basic attributes from the
	 *             connection provided.
	 */
	public static DatabaseDialect getDialect(final DataLink dataLink)
			throws SQLException {
		for (final Iterator i = DatabaseDialect.dialects.iterator(); i
				.hasNext();) {
			final DatabaseDialect d = (DatabaseDialect) i.next();
			if (d.understandsDataLink(dataLink)) {
				// Get maximum table/col name lengths.
				if (dataLink instanceof JDBCSchema) {
					final DatabaseMetaData dmd = ((JDBCSchema) dataLink)
							.getConnection(null).getMetaData();
					d.setMaxColumnNameLength(dmd.getMaxColumnNameLength());
					d.setMaxTableNameLength(dmd.getMaxTableNameLength());
				}
				return d;
			}
		}
		return null;
	}

	/**
	 * Given a particular action, return a SQL or DDL statement that will
	 * perform it. If multiple statements are required to perform the action,
	 * they are returned as an array. Each line of the array is considered to be
	 * a complete single statement which could, for example, be executed
	 * directly with a JDBC database function such as
	 * {@link PreparedStatement#execute()}.
	 * <p>
	 * Note that the statements returned should not be parameterised. They
	 * should contain all values hard-coded into them, as the user may choose to
	 * save them to file for later use, preventing parameterisation from
	 * working.
	 * 
	 * @param action
	 *            the action to translate into SQL or DDL.
	 * @return the statement(s) that represent the action.
	 * @throws ConstructorException
	 *             if the action was not able to be converted into one or more
	 *             SQL or DDL statements.
	 */
	public abstract String[] getStatementsForAction(MartConstructorAction action)
			throws ConstructorException;

	/**
	 * Call this method before using the dialect for anything. This is necessary
	 * in order to clear out any state it may be keeping track of.
	 */
	public abstract void reset();

	/**
	 * Test to see whether this particular dialect implementation can understand
	 * the data link given, ie. it knows how to interact with it and speak the
	 * appropriate version of SQL or DDL.
	 * 
	 * @param dataLink
	 *            the data link to test compatibility with.
	 * @return <tt>true</tt> if it understands it, <tt>false</tt> if not.
	 */
	public abstract boolean understandsDataLink(DataLink dataLink);

	private void setMaxTableNameLength(final int value) {
		this.maxTableNameLength = value;
	}

	private void setMaxColumnNameLength(final int value) {
		this.maxColumnNameLength = value;
	}

	/**
	 * Use this method to check if the given table name is acceptable to this
	 * database dialect. Throws an exception if it is not, otherwise does
	 * nothing.
	 * 
	 * @param tableName
	 *            the table name to check.
	 * @throws ConstructorException
	 *             if the name is not acceptable.
	 */
	protected void checkTableName(final String tableName)
			throws ConstructorException {
		if (tableName.length() > this.maxTableNameLength)
			throw new ConstructorException(Resources.get("nameTooLong",
					tableName));
	}

	/**
	 * Use this method to check if the given column name is acceptable to this
	 * database dialect. Throws an exception if it is not, otherwise does
	 * nothing.
	 * 
	 * @param columnName
	 *            the table name to check.
	 * @throws ConstructorException
	 *             if the name is not acceptable.
	 */
	protected void checkColumnName(final String columnName)
			throws ConstructorException {
		if (columnName.length() > this.maxColumnNameLength)
			throw new ConstructorException(Resources.get("nameTooLong",
					columnName));
	}

	/**
	 * Get the SQL for unrolling a table's rolled-up relations.
	 * 
	 * @param schemaPrefix
	 *            the value to substitute for ':schemaPrefix'.
	 * @param dataset
	 *            the dataset.
	 * @param dsTable
	 *            the dataset table.
	 * @param parentRel
	 *            the parent relation.
	 * @param childRel
	 *            the child relation.
	 * @param schemaPartition
	 *            the schema partition, or <tt>null</tt> for none.
	 * @param templateSchema
	 *            the template schema.
	 * @param utu
	 *            the unroll unit leading to this.
	 * @return the SQL.
	 */
	public abstract String getUnrollTableSQL(final String schemaPrefix,
			final DataSet dataset, final DataSetTable dsTable,
			final Relation parentRel, final Relation childRel,
			final String schemaPartition, final Schema templateSchema,
			final UnrollTable utu);

	/**
	 * Gets the SQL to return rows of a partition table.
	 * 
	 * @param schemaPrefix
	 *            the value to substitute for ':schemaPrefix'.
	 * @param positionMap
	 *            the map to populate with column names to column positions.
	 * @param pt
	 *            the partition table to get rows for.
	 * @param ds
	 *            the dataset.
	 * @param schema
	 *            the schema to connect to.
	 * @param usablePartition
	 *            the partition to connect to in the schema.
	 * @return the SQL for the rows. positionMap will also have been populated
	 *         at this stage.
	 * @throws PartitionException
	 *             if anything goes wrong.
	 */
	public abstract String getPartitionTableRowsSQL(final String schemaPrefix,
			final Map positionMap, final PartitionTable pt, final DataSet ds,
			final Schema schema, final String usablePartition)
			throws PartitionException;

	/**
	 * Get SQL to return rows from a table.
	 * 
	 * @param schemaName
	 *            the schema to use.
	 * @param table
	 *            the table to get rows from.
	 * @return the SQL.
	 */
	public abstract String getSimpleRowsSQL(final String schemaName,
			final Table table);

	/**
	 * Get SQL to return unique values from a column.
	 * 
	 * @param schemaName
	 *            the schema to use.
	 * @param column
	 *            the column to get values from.
	 * @return the SQL.
	 */
	public abstract String getUniqueValuesSQL(final String schemaName,
			final Column column);
}
