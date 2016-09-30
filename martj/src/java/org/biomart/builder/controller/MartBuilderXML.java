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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.biomart.builder.exceptions.PartitionException;
import org.biomart.builder.model.Column;
import org.biomart.builder.model.ComponentStatus;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Key;
import org.biomart.builder.model.Mart;
import org.biomart.builder.model.PartitionTable;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Table;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetOptimiserType;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.DataSet.DataSetTableType;
import org.biomart.builder.model.DataSet.ExpressionColumnDefinition;
import org.biomart.builder.model.DataSet.SplitOptimiserColumnDef;
import org.biomart.builder.model.DataSet.DataSetColumn.ExpressionColumn;
import org.biomart.builder.model.Key.ForeignKey;
import org.biomart.builder.model.Key.PrimaryKey;
import org.biomart.builder.model.PartitionTable.PartitionColumn;
import org.biomart.builder.model.PartitionTable.PartitionTableApplication;
import org.biomart.builder.model.PartitionTable.PartitionTableApplication.PartitionAppliedRow;
import org.biomart.builder.model.Relation.Cardinality;
import org.biomart.builder.model.Relation.CompoundRelationDefinition;
import org.biomart.builder.model.Relation.RestrictedRelationDefinition;
import org.biomart.builder.model.Relation.UnrolledRelationDefinition;
import org.biomart.builder.model.Schema.JDBCSchema;
import org.biomart.builder.model.Table.RestrictedTableDefinition;
import org.biomart.common.exceptions.AssociationException;
import org.biomart.common.exceptions.DataModelException;
import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * The MartBuilderXML class provides two static methods which serialize and
 * deserialize {@link Mart} objects to/from a basic XML format. A third method
 * saves a human-readable report based on the XML.
 * <p>
 * Writing is done by building up a map of objects to unique IDs. Where objects
 * cross-reference each other, they look up the unique ID in the map and
 * reference that instead. When reading, the reverse map is built up to achieve
 * the same effect. This system relies on objects being written out before they
 * are cross-referenced by other objects, so circular references are not
 * possible, and the file structure must be carefully planned to avoid other
 * situations where this may arise.
 * <p>
 * NOTE: The XML is version-specific. A formal DTD will be included with each
 * official release of MartBuilder. This DTD will be found in the
 * <tt>org.biomart.builder.resources</tt> package.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.147 $, $Date: 2008-03-12 14:22:37 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class MartBuilderXML extends DefaultHandler {

	private static final String CURRENT_DTD_VERSION = "0.7";

	private static final String[] SUPPORTED_DTD_VERSIONS = new String[] {
			"0.7", "0.6", "0.5" };

	private static final String DTD_PUBLIC_ID_START = "-//EBI//DTD MartBuilder ";

	private static final String DTD_PUBLIC_ID_END = "//EN";

	private static final String DTD_URL_START = "http://www.biomart.org/DTD/MartBuilder-";

	private static final String DTD_URL_END = ".dtd";

	private static String currentReadingDTDVersion;

	/**
	 * The load method takes a {@link File} and loads up a {@link Mart} object
	 * based on the XML contents of the file. This XML is usually generated by
	 * the {@link MartBuilderXML#save(Mart,File)} method.
	 * 
	 * @param file
	 *            the {@link File} to load the data from.
	 * @return a {@link Mart} object containing the data from the file.
	 * @throws IOException
	 *             if there was any problem reading the file.
	 * @throws DataModelException
	 *             if the content of the file is not valid {@link Mart} XML, or
	 *             has any logical problems.
	 */
	public static Mart load(final File file) throws IOException,
			DataModelException {
		Log.info("Loading XML from " + file.getPath());
		// Use the default (non-validating) parser
		final SAXParserFactory factory = SAXParserFactory.newInstance();
		// Parse the input
		final MartBuilderXML loader = new MartBuilderXML();
		try {
			final SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(file, loader);
		} catch (final ParserConfigurationException e) {
			throw new DataModelException(Resources.get("XMLConfigFailed"), e);
		} catch (final SAXException e) {
			throw new DataModelException(Resources.get("XMLUnparseable"), e);
		}
		// Get the constructed object.
		final Mart mart = loader.getConstructedMart();
		// Check that we got something useful.
		if (mart == null)
			throw new DataModelException(Resources.get("fileNotSchemaVersion",
					MartBuilderXML.CURRENT_DTD_VERSION));
		// Return.
		Log.info("Done loading XML from " + file.getPath());
		return mart;
	}

	/**
	 * The save method takes a {@link Mart} object and writes out XML describing
	 * it to the given {@link File}. This XML can be read by the
	 * {@link MartBuilderXML#load(File)} method.
	 * 
	 * @param mart
	 *            {@link Mart} object containing the data for the file.
	 * @param file
	 *            the {@link File} to save the data to.
	 * @throws IOException
	 *             if there was any problem writing the file.
	 * @throws DataModelException
	 *             if it encounters an object not writable under the current
	 *             DTD.
	 * @throws PartitionException
	 *             if any partition stuff broke.
	 */
	public static void save(final Mart mart, final File file)
			throws IOException, DataModelException, PartitionException {
		Log.info("Saving XML as " + file.getPath());
		// Open the file.
		final FileWriter fw = new FileWriter(file);
		try {
			// Write it out.
			(new MartBuilderXML()).writeXML(mart, fw, true);
		} catch (final IOException e) {
			throw e;
		} catch (final DataModelException e) {
			throw e;
		} finally {
			// Close the output stream.
			fw.close();
		}
		Log.info("Done saving XML as " + file.getPath());
	}

	private Mart constructedMart;

	private int currentElementID;

	private String currentOutputElement;

	private int currentOutputIndent;

	private Map mappedObjects;

	private Stack objectStack;

	private Map reverseMappedObjects;

	/**
	 * This class is intended to be used only in a static context. It creates
	 * its own instances internally as required.
	 */
	private MartBuilderXML() {
		this.constructedMart = null;
		this.currentOutputElement = null;
		this.currentOutputIndent = 0;
		this.currentElementID = 1;
	}

	private Mart getConstructedMart() {
		return this.constructedMart;
	}

	/**
	 * Internal method which closes a tag in the output stream.
	 * 
	 * @param name
	 *            the tag to close.
	 * @param xmlWriter
	 *            the writer we are writing to.
	 * @throws IOException
	 *             if it failed to write it.
	 */
	private void closeElement(final String name, final Writer xmlWriter)
			throws IOException {
		// Can we use the simple /> method?
		if (this.currentOutputElement != null
				&& name.equals(this.currentOutputElement)) {
			// Yes, so put closing angle bracket and newline on it.
			xmlWriter.write("/>");
			xmlWriter.write(System.getProperty("line.separator"));
		} else {
			// No, so use the full technique.
			// Decrease the indent.
			this.currentOutputIndent--;
			// Output any indent required.
			for (int i = this.currentOutputIndent; i > 0; i--)
				xmlWriter.write("\t");
			// Output the tag.
			xmlWriter.write("</");
			xmlWriter.write(name);
			xmlWriter.write(">\n");
		}
		// Reset the current tag.
		this.currentOutputElement = null;
	}

	/**
	 * Internal method which opens a tag in the output stream.
	 * 
	 * @param name
	 *            the tag to open.
	 * @param xmlWriter
	 *            the writer we are writing to.
	 * @throws IOException
	 *             if it failed to write it.
	 */
	private void openElement(final String name, final Writer xmlWriter)
			throws IOException {
		// Are we already partway through one?
		if (this.currentOutputElement != null) {
			// Yes, so put closing angle bracket and newline on it.
			xmlWriter.write(">");
			xmlWriter.write(System.getProperty("line.separator"));
			// Increase the indent.
			this.currentOutputIndent++;
		}

		// Write any indent required.
		for (int i = this.currentOutputIndent; i > 0; i--)
			xmlWriter.write("\t");
		// Open the tag.
		xmlWriter.write("<");
		// Write the tag.
		xmlWriter.write(name);

		// Update tag that we are currently writing.
		this.currentOutputElement = name;
	}

	/**
	 * Internal method which writes an attribute in the output stream.
	 * 
	 * @param name
	 *            the name of the attribute.
	 * @param value
	 *            the value of the attribute.
	 * @param xmlWriter
	 *            the writer we are writing to.
	 * @throws IOException
	 *             if it failed to write it.
	 */
	private void writeAttribute(final String name, final String value,
			final Writer xmlWriter) throws IOException {
		// Write it.
		if (value == null || "".equals(value))
			return;
		xmlWriter.write(" ");
		xmlWriter.write(name);
		xmlWriter.write("=\"");
		xmlWriter.write(value.replaceAll("&", "&amp;").replaceAll("\"",
				"&quot;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"));
		xmlWriter.write("\"");
	}

	/**
	 * Internal method which writes a comma-separated list of attributes in the
	 * output stream.
	 * 
	 * @param name
	 *            the name of the attribute.
	 * @param values
	 *            the values of the attribute.
	 * @param xmlWriter
	 *            the writer we are writing to.
	 * @throws IOException
	 *             if it failed to write it.
	 */
	private void writeListAttribute(final String name, final Object[] values,
			final Writer xmlWriter) throws IOException {
		// Write it.
		final StringBuffer sb = new StringBuffer();
		for (int i = 0; i < values.length; i++) {
			final String value = values[i] == null ? null : values[i]
					.toString();
			if (i > 0)
				sb.append(",");
			if (value != null)
				sb.append(value.replaceAll(",", "__COMMA__"));
		}
		this.writeAttribute(name, sb.length() == 0 ? null : sb.toString(),
				xmlWriter);
	}

	private String[] readListAttribute(final String string,
			final boolean blankIsSingleNull) {
		if (string == null || string.length() == 0)
			return blankIsSingleNull ? new String[] { null } : new String[0];
		final String[] values = string.split("\\s*,\\s*", -1);
		for (int i = 0; i < values.length; i++) {
			values[i] = values[i].replaceAll("__COMMA__", ",");
			if (values[i].length() == 0)
				values[i] = null;
		}
		return values;
	}

	/**
	 * Internal method which writes out a set of relations.
	 * 
	 * @param relations
	 *            the set of {@link Relation}s to write.
	 * @param xmlWriter
	 *            the writer to write to.
	 * @throws IOException
	 *             if there was a problem writing to file.
	 */
	private void writeRelations(final Collection relations,
			final boolean writeExternal, final Writer xmlWriter)
			throws IOException {
		// Write out each relation in turn.
		for (final Iterator i = relations.iterator(); i.hasNext();) {
			final Relation r = (Relation) i.next();
			if (writeExternal != r.isExternal())
				continue;
			Log.debug("Writing relation: " + r);

			// Assign the relation an ID.
			final String relMappedID = "" + this.currentElementID++;
			this.reverseMappedObjects.put(r, relMappedID);

			// Write the relation.
			this.openElement("relation", xmlWriter);
			this.writeAttribute("id", relMappedID, xmlWriter);
			this.writeAttribute("cardinality", r.getCardinality().getName(),
					xmlWriter);
			this.writeAttribute("originalCardinality", r
					.getOriginalCardinality().getName(), xmlWriter);
			this.writeAttribute("firstKeyId",
					(String) this.reverseMappedObjects.get(r.getFirstKey()),
					xmlWriter);
			this.writeAttribute("secondKeyId",
					(String) this.reverseMappedObjects.get(r.getSecondKey()),
					xmlWriter);
			this.writeAttribute("status", r.getStatus().toString(), xmlWriter);
			this.writeAttribute("visibleModified", Boolean.toString(r
					.isVisibleModified()), xmlWriter);
			this.closeElement("relation", xmlWriter);
		}
	}

	/**
	 * Internal method which writes an entire schema out to file.
	 * 
	 * @param schema
	 *            the schema to write.
	 * @param xmlWriter
	 *            the writer to write to.
	 * @throws IOException
	 *             in case there was any problem writing the file.
	 * @throws DataModelException
	 *             if there were any logical problems with the schema.
	 */
	private void writeSchema(final Schema schema, final Writer xmlWriter)
			throws IOException, DataModelException {
		Log.debug("Writing schema: " + schema);
		// What kind of schema is it?
		if (schema instanceof JDBCSchema) {
			Log.debug("Writing JDBC schema");
			// It's a JDBC schema.
			final JDBCSchema jdbcSchema = (JDBCSchema) schema;

			// Begin the schema element.
			this.openElement("jdbcSchema", xmlWriter);
			this.writeAttribute("uniqueId", "" + jdbcSchema.getUniqueId(),
					xmlWriter);

			this.writeAttribute("driverClassName", jdbcSchema
					.getDriverClassName(), xmlWriter);
			this.writeAttribute("url", jdbcSchema.getUrl(), xmlWriter);
			this.writeAttribute("databaseName", jdbcSchema
					.getDataLinkDatabase(), xmlWriter);
			this.writeAttribute("schemaName", jdbcSchema.getDataLinkSchema(),
					xmlWriter);
			this
					.writeAttribute("username", jdbcSchema.getUsername(),
							xmlWriter);
			if (jdbcSchema.getPassword() != null)
				this.writeAttribute("password", jdbcSchema.getPassword(),
						xmlWriter);
			this.writeAttribute("name", jdbcSchema.getName(), xmlWriter);
			this.writeAttribute("keyguessing", Boolean.toString(jdbcSchema
					.isKeyGuessing()), xmlWriter);
			this.writeAttribute("masked", Boolean.toString(jdbcSchema
					.isMasked()), xmlWriter);
			this.writeAttribute("hideMasked", Boolean.toString(jdbcSchema
					.isHideMasked()), xmlWriter);

			// Partitions.
			this.writeAttribute("partitionRegex", jdbcSchema
					.getPartitionRegex(), xmlWriter);
			this.writeAttribute("partitionExpression", jdbcSchema
					.getPartitionNameExpression(), xmlWriter);
		}
		// Other schema types are not recognised.
		else
			throw new DataModelException(Resources.get("unknownSchemaType",
					schema.getClass().getName()));

		// Write out the contents.
		this.writeSchemaContents(schema, xmlWriter);

		// Close the schema element.
		// What kind of schema was it?
		// JDBC?
		if (schema instanceof JDBCSchema)
			this.closeElement("jdbcSchema", xmlWriter);
		// Others?
		else
			throw new DataModelException(Resources.get("unknownSchemaType",
					schema.getClass().getName()));
	}

	/**
	 * Internal method which writes out the contents of a schema.
	 * 
	 * @param schema
	 *            the {@link Schema} to write out the tables of.
	 * @param xmlWriter
	 *            the writer to write to.
	 * @throws IOException
	 *             if there was a problem writing to file.
	 * @throws AssociationException
	 *             if an unwritable kind of object was found.
	 */
	private void writeSchemaContents(final Schema schema, final Writer xmlWriter)
			throws IOException, DataModelException {
		Log.debug("Writing schema contents for " + schema);

		// Write out tables inside each schema.
		for (final Iterator ti = schema.getTables().values().iterator(); ti
				.hasNext();) {
			final Table table = (Table) ti.next();
			Log.debug("Writing table: " + table);

			// Give the table an ID.
			final String tableMappedID = "" + this.currentElementID++;
			this.reverseMappedObjects.put(table, tableMappedID);

			// Start table.
			this.openElement("table", xmlWriter);
			this
					.writeAttribute("uniqueId", "" + table.getUniqueId(),
							xmlWriter);
			this.writeAttribute("id", tableMappedID, xmlWriter);
			this.writeAttribute("name", table.getName(), xmlWriter);
			this.writeAttribute("ignore", Boolean.toString(table.isMasked()),
					xmlWriter);
			this.writeListAttribute("inSchemaPartition", table
					.getSchemaPartitions().toArray(), xmlWriter);

			// Write out columns inside each table.
			for (final Iterator ci = table.getColumns().values().iterator(); ci
					.hasNext();) {
				final Column col = (Column) ci.next();
				Log.debug("Writing column: " + col);

				// Give the column an ID.
				final String colMappedID = "" + this.currentElementID++;
				this.reverseMappedObjects.put(col, colMappedID);

				// Start column.
				this.openElement("column", xmlWriter);
				this.writeAttribute("id", colMappedID, xmlWriter);
				this.writeAttribute("name", col.getName(), xmlWriter);
				this.writeAttribute("visibleModified", Boolean.toString(col
						.isVisibleModified()), xmlWriter);
				this.writeListAttribute("inSchemaPartition", col
						.getSchemaPartitions().toArray(), xmlWriter);
				this.closeElement("column", xmlWriter);
			}

			// Write out keys inside each table. Remember relations as
			// we go along.
			for (final Iterator ki = table.getKeys().iterator(); ki.hasNext();) {
				final Key key = (Key) ki.next();
				Log.debug("Writing key: " + key);

				// Give the key an ID.
				final String keyMappedID = "" + this.currentElementID++;
				this.reverseMappedObjects.put(key, keyMappedID);

				// What kind of key is it?
				String elem = null;
				if (key instanceof PrimaryKey)
					elem = "primaryKey";
				else if (key instanceof ForeignKey)
					elem = "foreignKey";
				else
					throw new DataModelException(Resources.get("unknownKey",
							key.getClass().getName()));

				// Write the key.
				this.openElement(elem, xmlWriter);
				this.writeAttribute("id", keyMappedID, xmlWriter);
				final List columnIds = new ArrayList();
				for (int kci = 0; kci < key.getColumns().length; kci++)
					columnIds.add(this.reverseMappedObjects.get(key
							.getColumns()[kci]));
				this.writeListAttribute("columnIds", (String[]) columnIds
						.toArray(new String[0]), xmlWriter);
				this.writeAttribute("status", key.getStatus().toString(),
						xmlWriter);
				this.writeAttribute("visibleModified", Boolean.toString(key
						.isVisibleModified()), xmlWriter);
				this.closeElement(elem, xmlWriter);
			}

			// Finish table.
			this.closeElement("table", xmlWriter);
		}

		// Write relations.
		this.writeRelations(schema.getRelations(), false, xmlWriter);
	}

	/**
	 * Internal method which does the work of writing out XML files and
	 * generating those funky ID tags you see in them.
	 * 
	 * @param mart
	 *            the mart to write.
	 * @param xmlWriter
	 *            the Writer to write the XML to.
	 * @param writeDTD
	 *            <tt>true</tt> if a DTD header line is to be included.
	 * @throws IOException
	 *             if a write error occurs.
	 * @throws DataModelException
	 *             if it encounters an object not writable under the current
	 *             DTD.
	 * @throws PartitionException
	 *             if it encounters an object that does not reference a valid
	 *             partition.
	 */
	private void writeXML(final Mart mart, final Writer xmlWriter,
			final boolean writeDTD) throws IOException, DataModelException,
			PartitionException {
		// Write the headers.
		xmlWriter.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		if (writeDTD)
			xmlWriter.write("<!DOCTYPE mart PUBLIC \""
					+ MartBuilderXML.DTD_PUBLIC_ID_START
					+ MartBuilderXML.CURRENT_DTD_VERSION
					+ MartBuilderXML.DTD_PUBLIC_ID_START + "\" \""
					+ MartBuilderXML.DTD_URL_START
					+ MartBuilderXML.CURRENT_DTD_VERSION
					+ MartBuilderXML.DTD_URL_END + "\">\n");

		// Initialise the ID counter.
		this.reverseMappedObjects = new HashMap();

		// Start by enclosing the whole lot in a <mart> tag.
		Log.debug("Writing mart: " + mart);
		this.openElement("mart", xmlWriter);
		this.writeAttribute("outputDatabase", mart.getOutputDatabase(),
				xmlWriter);
		this.writeAttribute("outputSchema", mart.getOutputSchema(), xmlWriter);
		this.writeAttribute("outputHost", mart.getOutputHost(), xmlWriter);
		this.writeAttribute("outputPort", mart.getOutputPort(), xmlWriter);
		this.writeAttribute("overrideHost", mart.getOverrideHost(), xmlWriter);
		this.writeAttribute("overridePort", mart.getOverridePort(), xmlWriter);
		this.writeAttribute("nameCase", "" + mart.getCase(), xmlWriter);
		this.writeAttribute("hideMaskedDataSets", Boolean.toString(mart
				.isHideMaskedDataSets()), xmlWriter);
		this.writeAttribute("hideMaskedSchemas", Boolean.toString(mart
				.isHideMaskedSchemas()), xmlWriter);

		// Write out each schema.
		final Set externalRelations = new HashSet();
		for (final Iterator i = mart.getSchemas().values().iterator(); i
				.hasNext();) {
			final Schema schema = (Schema) i.next();
			this.writeSchema(schema, xmlWriter);
			for (final Iterator j = schema.getRelations().iterator(); j
					.hasNext();) {
				final Relation rel = (Relation) j.next();
				if (rel.isExternal())
					externalRelations.add(rel);
			}
		}

		// Write out relations.
		this.writeRelations(externalRelations, true, xmlWriter);

		// Write out datasets.
		for (final Iterator dsi = mart.getDataSets().values().iterator(); dsi
				.hasNext();) {
			final DataSet ds = (DataSet) dsi.next();
			// Get schema and dataset mods.
			Log.debug("Writing dataset: " + ds);

			this.openElement("dataset", xmlWriter);
			this.writeAttribute("name", ds.getName(), xmlWriter);
			this.writeAttribute("centralTableId",
					(String) this.reverseMappedObjects
							.get(ds.getCentralTable()), xmlWriter);
			this.writeAttribute("optimiser", ds.getDataSetOptimiserType()
					.getName(), xmlWriter);
			this.writeAttribute("invisible",
					Boolean.toString(ds.isInvisible()), xmlWriter);
			this.writeAttribute("masked", Boolean.toString(ds.isMasked()),
					xmlWriter);
			this.writeAttribute("hideMasked", Boolean.toString(ds
					.isHideMasked()), xmlWriter);
			this.writeAttribute("indexOptimiser", Boolean.toString(ds
					.isIndexOptimiser()), xmlWriter);

			// Write out visibleModified keys (toString()) for
			// all vismod relations, keys, and columns.
			Log.debug("Writing visible modified keys/rels/cols");
			final Set vismodKeys = new HashSet();
			for (final Iterator i = ds.getRelations().iterator(); i.hasNext();) {
				final Relation rel = (Relation) i.next();
				if (rel.isVisibleModified())
					vismodKeys.add(rel.toString());
			}
			for (final Iterator i = ds.getTables().values().iterator(); i
					.hasNext();) {
				final Table tbl = (Table) i.next();
				for (final Iterator j = tbl.getKeys().iterator(); j.hasNext();) {
					final Key k = (Key) j.next();
					if (k.isVisibleModified())
						vismodKeys.add(k.toString());
				}
				for (final Iterator j = tbl.getColumns().values().iterator(); j
						.hasNext();) {
					final Column col = (Column) j.next();
					if (col.isVisibleModified())
						vismodKeys.add(col.toString());
				}
			}
			for (final Iterator i = vismodKeys.iterator(); i.hasNext();) {
				final String key = (String) i.next();
				this.openElement("visibleModified", xmlWriter);
				this.writeAttribute("key", key, xmlWriter);
				this.closeElement("visibleModified", xmlWriter);
			}

			Log.debug("Writing global modifications");
			for (final Iterator s = mart.getSchemas().values().iterator(); s
					.hasNext();) {
				final Schema sch = (Schema) s.next();
				// Write out relation special stuff.
				for (final Iterator i = sch.getRelations().iterator(); i
						.hasNext();) {
					final Relation r = (Relation) i.next();

					// Write out subclass relations inside dataset.
					if (r.isSubclassRelation(ds)) {
						this.openElement("subclassRelation", xmlWriter);
						this.writeAttribute("relationId",
								(String) this.reverseMappedObjects.get(r),
								xmlWriter);
						this.closeElement("subclassRelation", xmlWriter);
					}

					// Write out merged tables inside dataset.
					if (r.isMergeRelation(ds)) {
						this.openElement("mergedRelation", xmlWriter);
						this.writeAttribute("relationId",
								(String) this.reverseMappedObjects.get(r),
								xmlWriter);
						this.closeElement("mergedRelation", xmlWriter);
					}

					// Masked relations.
					if (r.isMaskRelation(ds)) {
						this.openElement("maskedRelation", xmlWriter);
						this.writeAttribute("relationId",
								(String) this.reverseMappedObjects.get(r),
								xmlWriter);
						this.closeElement("maskedRelation", xmlWriter);
					}

					// Loopback relations.
					if (r.isLoopbackRelation(ds)) {
						this.openElement("loopbackRelation", xmlWriter);
						this.writeAttribute("relationId",
								(String) this.reverseMappedObjects.get(r),
								xmlWriter);
						this.writeAttribute("diffColumnId",
								(String) this.reverseMappedObjects.get(r
										.getLoopbackRelation(ds)), xmlWriter);
						this.closeElement("loopbackRelation", xmlWriter);
					}

					// Force relations.
					if (r.isForceRelation(ds)) {
						this.openElement("forcedRelation", xmlWriter);
						this.writeAttribute("relationId",
								(String) this.reverseMappedObjects.get(r),
								xmlWriter);
						this.closeElement("forcedRelation", xmlWriter);
					}

					// Compound relations.
					if (r.isCompoundRelation(ds)) {
						final CompoundRelationDefinition def = r
								.getCompoundRelation(ds);
						this.openElement("compoundRelation", xmlWriter);
						this.writeAttribute("relationId",
								(String) this.reverseMappedObjects.get(r),
								xmlWriter);
						this.writeAttribute("n", "" + def.getN(), xmlWriter);
						this.writeAttribute("parallel", "" + def.isParallel(),
								xmlWriter);
						this.closeElement("compoundRelation", xmlWriter);
					}

					// Unrolled relations.
					if (r.getUnrolledRelation(ds) != null) {
						final UnrolledRelationDefinition def = r
								.getUnrolledRelation(ds);
						this.openElement("unrolledRelation", xmlWriter);
						this.writeAttribute("relationId",
								(String) this.reverseMappedObjects.get(r),
								xmlWriter);
						this.writeAttribute("columnId",
								(String) this.reverseMappedObjects.get(def
										.getNameColumn()), xmlWriter);
						this.writeAttribute("reversed", "" + def.isReversed(),
								xmlWriter);
						this.closeElement("unrolledRelation", xmlWriter);
					}
				}

				// Write out per-table mods.
				for (final Iterator i = sch.getTables().values().iterator(); i
						.hasNext();) {
					final Table tbl = (Table) i.next();

					// Big table.
					if (tbl.getBigTable(ds) > 0) {
						this.openElement("bigTable", xmlWriter);
						this.writeAttribute("tableId",
								(String) this.reverseMappedObjects.get(tbl),
								xmlWriter);
						this.writeAttribute("bigness",
								"" + tbl.getBigTable(ds), xmlWriter);
						this.closeElement("bigTable", xmlWriter);
					}
				}
			}

			// Write out dstable mods from each table.
			for (final Iterator i = ds.getTables().values().iterator(); i
					.hasNext();) {
				final DataSetTable dsTable = (DataSetTable) i.next();
				Log.debug("Writing modifications for " + dsTable);
				// Write out masked tables inside dataset.
				if (dsTable.isDimensionMasked()) {
					this.openElement("maskedTable", xmlWriter);
					this.writeAttribute("tableKey", dsTable.getName(),
							xmlWriter);
					this.closeElement("maskedTable", xmlWriter);
				}
				// Rename table.
				final String renameTbl = dsTable.getTableRename();
				if (renameTbl != null) {
					this.openElement("renamedTable", xmlWriter);
					this.writeAttribute("tableKey", dsTable.getName(),
							xmlWriter);
					this.writeAttribute("newName", renameTbl, xmlWriter);
					this.closeElement("renamedTable", xmlWriter);
				}
				// Explain-hide-masked table.
				if (dsTable.isExplainHideMasked()) {
					this.openElement("explainHideMasked", xmlWriter);
					this.writeAttribute("tableKey", dsTable.getName(),
							xmlWriter);
					this.closeElement("explainHideMasked", xmlWriter);
				}
				// Explain-hide-masked table.
				if (dsTable.isTableHideMasked()) {
					this.openElement("tableHideMasked", xmlWriter);
					this.writeAttribute("tableKey", dsTable.getName(),
							xmlWriter);
					this.closeElement("tableHideMasked", xmlWriter);
				}
				// Distinct table.
				if (dsTable.isDistinctTable()) {
					this.openElement("distinctRows", xmlWriter);
					this.writeAttribute("tableKey", dsTable.getName(),
							xmlWriter);
					this.closeElement("distinctRows", xmlWriter);
				}
				// No-left-join table.
				if (dsTable.isNoFinalLeftJoin()) {
					this.openElement("noFinalLeftJoin", xmlWriter);
					this.writeAttribute("tableKey", dsTable.getName(),
							xmlWriter);
					this.closeElement("noFinalLeftJoin", xmlWriter);
				}
				// No-optimiser table.
				if (dsTable.isSkipOptimiser()) {
					this.openElement("skipOptimiser", xmlWriter);
					this.writeAttribute("tableKey", dsTable.getName(),
							xmlWriter);
					this.closeElement("skipOptimiser", xmlWriter);
				}
				// No-index-optimiser table.
				if (dsTable.isSkipIndexOptimiser()) {
					this.openElement("skipIndexOptimiser", xmlWriter);
					this.writeAttribute("tableKey", dsTable.getName(),
							xmlWriter);
					this.closeElement("skipIndexOptimiser", xmlWriter);
				}
				// Write out dscol mods from each table col.
				for (final Iterator j = dsTable.getColumns().values()
						.iterator(); j.hasNext();) {
					final DataSetColumn dsCol = (DataSetColumn) j.next();
					if (dsCol.isColumnMasked()) {
						this.openElement("maskedColumn", xmlWriter);
						this.writeAttribute("tableKey", dsTable.getName(),
								xmlWriter);
						this.writeAttribute("colKey", dsCol.getName(),
								xmlWriter);
						this.closeElement("maskedColumn", xmlWriter);
					}
					// Write out indexed columns.
					if (dsCol.isColumnIndexed()) {
						this.openElement("indexedColumn", xmlWriter);
						this.writeAttribute("tableKey", dsTable.getName(),
								xmlWriter);
						this.writeAttribute("colKey", dsCol.getName(),
								xmlWriter);
						this.closeElement("indexedColumn", xmlWriter);
					}
					// Write out split optimiser columns.
					if (dsCol.getSplitOptimiserColumn() != null) {
						final SplitOptimiserColumnDef def = dsCol
								.getSplitOptimiserColumn();
						this.openElement("splitOptimiser", xmlWriter);
						this.writeAttribute("tableKey", dsTable.getName(),
								xmlWriter);
						this.writeAttribute("colKey", dsCol.getName(),
								xmlWriter);
						this.writeAttribute("contentCol", def.getContentCol(),
								xmlWriter);
						this.writeAttribute("separator", def.getSeparator(),
								xmlWriter);
						this.writeAttribute("prefix", "" + def.isPrefix(),
								xmlWriter);
						this.writeAttribute("suffix", "" + def.isSuffix(),
								xmlWriter);
						this.writeAttribute("size", "" + def.getSize(),
								xmlWriter);
						this.closeElement("splitOptimiser", xmlWriter);
					}
					// Expression column.
					if (dsCol instanceof ExpressionColumn) {
						final ExpressionColumnDefinition expcol = ((ExpressionColumn) dsCol)
								.getDefinition();
						this.openElement("expressionColumn", xmlWriter);
						this.writeAttribute("tableKey", dsTable.getName(),
								xmlWriter);
						this.writeAttribute("colKey", dsCol.getName(),
								xmlWriter);
						final StringBuffer cols = new StringBuffer();
						final StringBuffer names = new StringBuffer();
						for (final Iterator z = expcol.getAliases().entrySet()
								.iterator(); z.hasNext();) {
							final Map.Entry entry3 = (Map.Entry) z.next();
							cols.append((String) entry3.getKey());
							names.append((String) entry3.getValue());
							if (z.hasNext()) {
								cols.append(',');
								names.append(',');
							}
						}
						this.writeAttribute("aliasColumnNames",
								cols.toString(), xmlWriter);
						this.writeAttribute("aliasNames", names.toString(),
								xmlWriter);
						this.writeAttribute("expression", expcol
								.getExpression(), xmlWriter);
						this.writeAttribute("groupBy", "" + expcol.isGroupBy(),
								xmlWriter);
						this.closeElement("expressionColumn", xmlWriter);
					}
					// Rename column.
					final String renameCol = dsCol.getColumnRename();
					if (renameCol != null) {
						this.openElement("renamedColumn", xmlWriter);
						this.writeAttribute("tableKey", dsTable.getName(),
								xmlWriter);
						this.writeAttribute("colKey", dsCol.getName(),
								xmlWriter);
						this.writeAttribute("newName", renameCol, xmlWriter);
						this.closeElement("renamedColumn", xmlWriter);
					}

				}

				for (final Iterator s = mart.getSchemas().values().iterator(); s
						.hasNext();) {
					final Schema sch = (Schema) s.next();
					// Write out relation special stuff.
					for (final Iterator j = sch.getRelations().iterator(); j
							.hasNext();) {
						final Relation r = (Relation) j.next();

						// Alternative joins.
						if (r.isAlternativeJoin(ds, dsTable.getName())) {
							this.openElement("alternativeJoin", xmlWriter);
							this.writeAttribute("tableKey", dsTable.getName(),
									xmlWriter);
							this.writeAttribute("relationId",
									(String) this.reverseMappedObjects.get(r),
									xmlWriter);
							this.closeElement("alternativeJoin", xmlWriter);
						}

						// Mask relations.
						if (r.isMaskRelation(ds, dsTable.getName())
								&& !r.isMaskRelation(ds)) {
							this.openElement("maskedRelation", xmlWriter);
							this.writeAttribute("tableKey", dsTable.getName(),
									xmlWriter);
							this.writeAttribute("relationId",
									(String) this.reverseMappedObjects.get(r),
									xmlWriter);
							this.closeElement("maskedRelation", xmlWriter);
						}

						// Loopback relations.
						if (r.isLoopbackRelation(ds, dsTable.getName())) {
							this.openElement("loopbackRelation", xmlWriter);
							this.writeAttribute("tableKey", dsTable.getName(),
									xmlWriter);
							this.writeAttribute("relationId",
									(String) this.reverseMappedObjects.get(r),
									xmlWriter);
							this.writeAttribute("diffColumnId",
									(String) this.reverseMappedObjects.get(r
											.getLoopbackRelation(ds, dsTable
													.getName())), xmlWriter);
							this.closeElement("loopbackRelation", xmlWriter);
						}

						// Force relations.
						if (r.isForceRelation(ds, dsTable.getName())) {
							this.openElement("forcedRelation", xmlWriter);
							this.writeAttribute("tableKey", dsTable.getName(),
									xmlWriter);
							this.writeAttribute("relationId",
									(String) this.reverseMappedObjects.get(r),
									xmlWriter);
							this.closeElement("forcedRelation", xmlWriter);
						}

						// Compound relations.
						if (r.isCompoundRelation(ds, dsTable.getName())) {
							final CompoundRelationDefinition def = r
									.getCompoundRelation(ds, dsTable.getName());
							this.openElement("compoundRelation", xmlWriter);
							this.writeAttribute("tableKey", dsTable.getName(),
									xmlWriter);
							this.writeAttribute("relationId",
									(String) this.reverseMappedObjects.get(r),
									xmlWriter);
							this
									.writeAttribute("n", "" + def.getN(),
											xmlWriter);
							this.writeAttribute("parallel", ""
									+ def.isParallel(), xmlWriter);
							this.closeElement("compoundRelation", xmlWriter);
						}

						// Restrict relations.
						if (r.isRestrictRelation(ds, dsTable.getName())) {
							final CompoundRelationDefinition cdef = r
									.getCompoundRelation(ds, dsTable.getName());
							final int maxIteration = cdef == null ? 1 : cdef
									.getN();
							RestrictedRelationDefinition def = null;
							// Loop over known compound indices.
							for (int iteration = 0; iteration < maxIteration; iteration++) {
								def = r.getRestrictRelation(ds, dsTable
										.getName(), iteration);
								if (def == null)
									continue;
								this.openElement("restrictedRelation",
										xmlWriter);
								this.writeAttribute("tableKey", dsTable
										.getName(), xmlWriter);
								this.writeAttribute("relationId",
										(String) this.reverseMappedObjects
												.get(r), xmlWriter);
								this.writeAttribute("index", "" + iteration,
										xmlWriter);
								final StringBuffer lcols = new StringBuffer();
								final StringBuffer lnames = new StringBuffer();
								for (final Iterator a = def.getLeftAliases()
										.entrySet().iterator(); a.hasNext();) {
									final Map.Entry entry4 = (Map.Entry) a
											.next();
									lcols
											.append((String) this.reverseMappedObjects
													.get((Column) entry4
															.getKey()));
									lnames.append((String) entry4.getValue());
									if (a.hasNext()) {
										lcols.append(',');
										lnames.append(',');
									}
								}
								this.writeAttribute("leftAliasColumnIds", lcols
										.toString(), xmlWriter);
								this.writeAttribute("leftAliasNames", lnames
										.toString(), xmlWriter);
								final StringBuffer rcols = new StringBuffer();
								final StringBuffer rnames = new StringBuffer();
								for (final Iterator a = def.getRightAliases()
										.entrySet().iterator(); a.hasNext();) {
									final Map.Entry entry4 = (Map.Entry) a
											.next();
									rcols
											.append((String) this.reverseMappedObjects
													.get((Column) entry4
															.getKey()));
									rnames.append((String) entry4.getValue());
									if (a.hasNext()) {
										rcols.append(',');
										rnames.append(',');
									}
								}
								this.writeAttribute("rightAliasColumnIds",
										rcols.toString(), xmlWriter);
								this.writeAttribute("rightAliasNames", rnames
										.toString(), xmlWriter);
								this.writeAttribute("expression", def
										.getExpression(), xmlWriter);
								this.closeElement("restrictedRelation",
										xmlWriter);
							}
						}
					}

					// Write out per-table mods.
					for (final Iterator j = sch.getTables().values().iterator(); j
							.hasNext();) {
						final Table tbl = (Table) j.next();

						// Transform starts.
						if (tbl.isTransformStart(ds, dsTable.getName())) {
							this.openElement("transformStart", xmlWriter);
							this.writeAttribute("tableKey", dsTable.getName(),
									xmlWriter);
							this
									.writeAttribute("tableId",
											(String) this.reverseMappedObjects
													.get(tbl), xmlWriter);
							this.closeElement("transformStart", xmlWriter);
						}

						// Big table.
						if (tbl.getBigTable(ds, dsTable.getName()) > 0) {
							this.openElement("bigTable", xmlWriter);
							this.writeAttribute("tableKey", dsTable.getName(),
									xmlWriter);
							this
									.writeAttribute("tableId",
											(String) this.reverseMappedObjects
													.get(tbl), xmlWriter);
							this.writeAttribute("bigness", ""
									+ tbl.getBigTable(ds, dsTable.getName()),
									xmlWriter);
							this.closeElement("bigTable", xmlWriter);
						}

						// Restricted table.
						if (tbl.getRestrictTable(ds, dsTable.getName()) != null) {
							final RestrictedTableDefinition def = tbl
									.getRestrictTable(ds, dsTable.getName());

							this.openElement("restrictedTable", xmlWriter);
							this.writeAttribute("tableKey", dsTable.getName(),
									xmlWriter);
							this
									.writeAttribute("tableId",
											(String) this.reverseMappedObjects
													.get(tbl), xmlWriter);
							final StringBuffer cols = new StringBuffer();
							final StringBuffer names = new StringBuffer();
							for (final Iterator z = def.getAliases().entrySet()
									.iterator(); z.hasNext();) {
								final Map.Entry entry3 = (Map.Entry) z.next();
								cols.append((String) this.reverseMappedObjects
										.get((Column) entry3.getKey()));
								names.append((String) entry3.getValue());
								if (z.hasNext()) {
									cols.append(',');
									names.append(',');
								}
							}
							this.writeAttribute("aliasColumnIds", cols
									.toString(), xmlWriter);
							this.writeAttribute("aliasNames", names.toString(),
									xmlWriter);
							this.writeAttribute("expression", def
									.getExpression(), xmlWriter);
							this.closeElement("restrictedTable", xmlWriter);
						}
					}
				}
			}

			// Finish dataset.
			this.closeElement("dataset", xmlWriter);
		}

		// Write out partition tables.
		for (final Iterator dsi = mart.getPartitionTables().iterator(); dsi
				.hasNext();) {
			final PartitionTable pt = (PartitionTable) dsi.next();
			Log.debug("Writing dataset partition table: " + pt);

			this.openElement("datasetPartitionTable", xmlWriter);
			this.writeAttribute("name", pt.getName(), xmlWriter);
			this
					.writeListAttribute("selectedColumns", (String[]) pt
							.getSelectedColumnNames().toArray(new String[0]),
							xmlWriter);

			// Write out partition regex columns.
			Log.debug("Writing partition regexes");
			for (final Iterator i = pt.getSelectedColumnNames().iterator(); i
					.hasNext();) {
				final String colName = (String) i.next();
				if (colName.equals(PartitionTable.DIV_COLUMN))
					continue;
				final PartitionColumn pcol = (PartitionColumn) pt.getColumns()
						.get(colName);
				if (pcol.getRegexMatch() != null
						&& pcol.getRegexReplace() != null) {
					this.openElement("partitionRegex", xmlWriter);
					this.writeAttribute("name", pcol.getName(), xmlWriter);
					this.writeAttribute("match", pcol.getRegexMatch(),
							xmlWriter);
					this.writeAttribute("replace", pcol.getRegexReplace(),
							xmlWriter);
					this.closeElement("partitionRegex", xmlWriter);
				}
			}

			// Write out applications.
			Log.debug("Writing partition applications");
			for (final Iterator j = pt.getAllApplications().entrySet()
					.iterator(); j.hasNext();) {
				final Map.Entry entry = (Map.Entry) j.next();
				final DataSet target = (DataSet) entry.getKey();
				for (final Iterator l = ((Map) entry.getValue()).entrySet()
						.iterator(); l.hasNext();) {
					final Map.Entry entry3 = (Map.Entry) l.next();
					final PartitionTableApplication pta = (PartitionTableApplication) ((WeakReference) entry3
							.getValue()).get();
					if (pta == null)
						continue;
					this.openElement("partitionApplication", xmlWriter);
					this.writeAttribute("name", target.getName(), xmlWriter);
					this.writeAttribute("dimension", (String) entry3.getKey(),
							xmlWriter);
					final List pCols = new ArrayList();
					final List dsCols = new ArrayList();
					final List rels = new ArrayList();
					final List nameCols = new ArrayList();
					final List compounds = new ArrayList();
					for (final Iterator k = pta.getPartitionAppliedRows()
							.iterator(); k.hasNext();) {
						final PartitionAppliedRow prow = (PartitionAppliedRow) k
								.next();
						pCols.add(prow.getPartitionCol());
						dsCols.add(prow.getRootDataSetCol());
						rels.add((String) this.reverseMappedObjects.get(prow
								.getRelation()));
						nameCols.add(prow.getNamePartitionCol());
						compounds.add(new Integer(prow.getCompound()));
					}
					this.writeListAttribute("pCols", (String[]) pCols
							.toArray(new String[0]), xmlWriter);
					this.writeListAttribute("dsCols", (String[]) dsCols
							.toArray(new String[0]), xmlWriter);
					this.writeListAttribute("relationIds", (String[]) rels
							.toArray(new String[0]), xmlWriter);
					this.writeListAttribute("nameCols", (String[]) nameCols
							.toArray(new String[0]), xmlWriter);
					this.writeListAttribute("compounds", (Integer[]) compounds
							.toArray(new Integer[0]), xmlWriter);
					this.closeElement("partitionApplication", xmlWriter);
				}
			}

			// Finish dataset partition table.
			this.closeElement("datasetPartitionTable", xmlWriter);
		}

		// Finished! Close the mart tag.
		this.closeElement("mart", xmlWriter);

		// Flush.
		xmlWriter.flush();
	}

	public void endDocument() throws SAXException {
		// No action required.
	}

	public void endElement(final String namespaceURI, final String sName,
			final String qName) throws SAXException {
		// Work out what element it is we are closing.
		String eName = sName;
		if ("".equals(eName))
			eName = qName;

		// Pop the element off the stack so that the next element
		// knows that it is inside the parent of this one.
		this.objectStack.pop();
	}

	public InputSource resolveEntity(final String publicId,
			final String systemId) throws SAXException {
		Log.debug("Resolving XML entity " + publicId + " " + systemId);
		// If the public ID is our own DTD version, then we can use our
		// own copy of the DTD in our resources bundle.
		MartBuilderXML.currentReadingDTDVersion = null;
		for (int i = 0; i < MartBuilderXML.SUPPORTED_DTD_VERSIONS.length
				&& MartBuilderXML.currentReadingDTDVersion == null; i++) {
			final String currPub = MartBuilderXML.DTD_PUBLIC_ID_START
					+ MartBuilderXML.SUPPORTED_DTD_VERSIONS[i]
					+ MartBuilderXML.DTD_PUBLIC_ID_END;
			final String currUrl = MartBuilderXML.DTD_URL_START
					+ MartBuilderXML.SUPPORTED_DTD_VERSIONS[i]
					+ MartBuilderXML.DTD_URL_END;
			if (currPub.equals(publicId) || currUrl.equals(systemId))
				MartBuilderXML.currentReadingDTDVersion = MartBuilderXML.SUPPORTED_DTD_VERSIONS[i];
		}
		if (MartBuilderXML.currentReadingDTDVersion != null) {
			final String dtdDoc = "MartBuilder-"
					+ MartBuilderXML.currentReadingDTDVersion + ".dtd";
			Log.debug("Resolved to " + dtdDoc);
			return new InputSource(Resources.getResourceAsStream(dtdDoc));
		}
		// By returning null we allow the default behaviour for all other
		// DTDs.
		else {
			Log.debug("Not resolved");
			return null;
		}
	}

	public void startDocument() throws SAXException {
		// Reset all our maps of objects to IDs and clear
		// the stack of objects waiting to be processed.
		Log.debug("Started parsing XML document");
		this.mappedObjects = new HashMap();
		this.reverseMappedObjects = new HashMap();
		this.objectStack = new Stack();
	}

	public void startElement(final String namespaceURI, final String sName,
			final String qName, final Attributes attrs) throws SAXException {

		// Work out the name of the tag we are being asked to process.
		String eName = sName;
		if ("".equals(eName))
			eName = qName;

		// Construct a set of attributes from the tag.
		final Map attributes = new HashMap();
		if (attrs != null)
			for (int i = 0; i < attrs.getLength(); i++) {
				// Work out the name of the attribute.
				String aName = attrs.getLocalName(i);
				if ("".equals(aName))
					aName = attrs.getQName(i);

				// Store the attribute and value.
				final String aValue = attrs.getValue(i);
				attributes.put(aName, aValue.replaceAll("&quot;", "\"")
						.replaceAll("&lt;", "<").replaceAll("&gt;", ">")
						.replaceAll("&amp;", "&"));
			}

		// Now, attempt to recognise the tag by checking its name
		// against a set of names known to us.
		Log.debug("Reading tag " + eName + " with attributes " + attributes);

		// Start by assuming the tag produces an unnested element;
		Object element = "";

		// Mart (top-level only).
		if ("mart".equals(eName)) {
			// Start building a new mart. There can only be one mart tag
			// per file, as if more than one is found, the later tags
			// will override the earlier ones.
			final Mart mart = new Mart();
			mart.setOutputDatabase((String) attributes.get("outputDatabase"));
			mart.setOutputSchema((String) attributes.get("outputSchema"));
			mart.setOutputHost((String) attributes.get("outputHost"));
			mart.setOutputPort((String) attributes.get("outputPort"));
			mart.setOverrideHost((String) attributes.get("overrideHost"));
			mart.setOverridePort((String) attributes.get("overridePort"));
			mart.setHideMaskedSchemas(Boolean.valueOf(
					(String) attributes.get("hideMaskedSchemas"))
					.booleanValue());
			mart.setHideMaskedDataSets(Boolean.valueOf(
					(String) attributes.get("hideMaskedDataSets"))
					.booleanValue());
			// Need check to be safe against pre-0.7 versions.
			if (attributes.containsKey("nameCase"))
				mart.setCase(Integer.parseInt((String) attributes
						.get("nameCase")));
			element = this.constructedMart = mart;
		}

		// JDBC schema (anywhere, optionally inside schema group).
		else if ("jdbcSchema".equals(eName)) {
			// Start a new JDBC schema.
			final String uniqueId = (String) attributes.get("uniqueId");

			// Does it have a password? (optional)
			String password = "";
			if (attributes.containsKey("password"))
				password = (String) attributes.get("password");

			// Load the compulsory attributes.
			final String driverClassName = (String) attributes
					.get("driverClassName");
			final String url = (String) attributes.get("url");
			final String databaseName = (String) attributes.get("databaseName");
			final String schemaName = (String) attributes.get("schemaName");
			final String username = (String) attributes.get("username");
			final String name = (String) attributes.get("name");
			final boolean keyguessing = Boolean.valueOf(
					(String) attributes.get("keyguessing")).booleanValue();
			final boolean masked = Boolean.valueOf(
					(String) attributes.get("masked")).booleanValue();
			final boolean hideMasked = Boolean.valueOf(
					(String) attributes.get("hideMasked")).booleanValue();

			// Does it have partitions?
			final String partitionRegex = (String) attributes
					.get("partitionRegex");
			final String partitionExpression = (String) attributes
					.get("partitionExpression");

			// Construct the JDBC schema.
			try {
				final Schema schema = new JDBCSchema(this.constructedMart,
						driverClassName, url, databaseName, schemaName,
						username, password, name, keyguessing, partitionRegex,
						partitionExpression);
				schema.setMasked(masked);
				schema.setHideMasked(hideMasked);
				// Update the unique ID.
				if (uniqueId != null)
					schema.setUniqueId(Integer.parseInt(uniqueId));
				// Return to normal.
				schema.storeInHistory();
				// Add the schema directly to the mart if outside a group.
				this.constructedMart.getSchemas().put(schema.getOriginalName(),
						schema);
				element = schema;
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Table (inside table provider).
		else if ("table".equals(eName)) {
			// Start a new table.

			// What schema does it belong to? Throw a wobbly if not
			// currently inside a schema.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof Schema))
				throw new SAXException(Resources.get("tableOutsideSchema"));
			final Schema schema = (Schema) this.objectStack.peek();

			// Get the name and id as these are common features.
			final String id = (String) attributes.get("id");
			final String uniqueId = (String) attributes.get("uniqueId");
			final String name = (String) attributes.get("name");
			final boolean ignore = Boolean.valueOf(
					(String) attributes.get("ignore")).booleanValue();
			final String[] schemaPartitions = this.readListAttribute(
					(String) attributes.get("inSchemaPartition"), false);

			// What kind of schema?
			if (schema instanceof DataSet
					&& MartBuilderXML.currentReadingDTDVersion.equals("0.5"))
				// In this case we don't care, because we need this
				// for backward compatibility with 0.5. So, ignore it
				// with no warning. We put a dummy DataSetTable on
				// the stack.
				element = new DataSetTable(name, (DataSet) schema,
						DataSetTableType.MAIN, null, null, 1);
			else if (schema instanceof Schema)
				try {
					final Table table = new Table(schema, name);
					table.setMasked(ignore);
					table.getSchemaPartitions().clear();
					for (int i = 0; i < schemaPartitions.length; i++)
						table.getSchemaPartitions().add(
								schemaPartitions[i].intern());
					schema.getTables().put(table.getName(), table);
					element = table;
				} catch (final Exception e) {
					throw new SAXException(e);
				}
			else
				throw new SAXException(Resources.get("unknownSchemaType",
						schema.getClass().getName()));
			// Update the unique ID.
			if (uniqueId != null)
				((Table) element).setUniqueId(Integer.parseInt(uniqueId));

			// Store it in the map of IDed objects.
			this.mappedObjects.put(id, element);
		}

		// Column (inside table).
		else if ("column".equals(eName)) {
			// What table does it belong to? Throw a wobbly if not inside one.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof Table))
				throw new SAXException(Resources.get("columnOutsideTable"));
			final Table tbl = (Table) this.objectStack.peek();

			// Get the id and name as these are common features.
			final String id = (String) attributes.get("id");
			final String name = (String) attributes.get("name");
			final boolean visibleModified = Boolean.valueOf(
					(String) attributes.get("visibleModified")).booleanValue();
			final String[] schemaPartitions = this.readListAttribute(
					(String) attributes.get("inSchemaPartition"), false);

			try {
				// DataSet table column?
				if (tbl instanceof DataSetTable
						&& MartBuilderXML.currentReadingDTDVersion
								.equals("0.5")) {
					// Since 0.5 we don't bother reading this stuff.
					// But, we don't thrown an exception as it is a valid
					// tag under 0.5.
				}

				// Generic column?
				else if (tbl instanceof Table) {
					final Column column = new Column(tbl, name);
					column.setVisibleModified(visibleModified);
					column.getSchemaPartitions().clear();
					for (int i = 0; i < schemaPartitions.length; i++)
						column.getSchemaPartitions().add(
								schemaPartitions[i].intern());
					tbl.getColumns().put(column.getName(), column);
					element = column;
				}

				// Others
				else
					throw new SAXException(Resources.get("unknownTableType",
							tbl.getClass().getName()));

			} catch (final Exception e) {
				if (e instanceof SAXException)
					throw (SAXException) e;
				else
					throw new SAXException(e);
			}

			// Store it in the map of IDed objects.
			this.mappedObjects.put(id, element);
		}

		// Primary key (inside table).
		else if ("primaryKey".equals(eName)) {
			// What table does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof Table))
				throw new SAXException("pkOutsideTable");
			final Table tbl = (Table) this.objectStack.peek();

			// We don't do these for dataset tables since 0.6 as they
			// get regenerated automatically.
			if (tbl instanceof DataSetTable)
				return;

			// Get the ID.
			final String id = (String) attributes.get("id");

			try {
				// Work out what status the key is.
				final ComponentStatus status = ComponentStatus
						.get((String) attributes.get("status"));
				final boolean visibleModified = Boolean.valueOf(
						(String) attributes.get("visibleModified"))
						.booleanValue();

				// Decode the column IDs from the comma-separated list.
				final String[] pkColIds = this.readListAttribute(
						(String) attributes.get("columnIds"), false);
				final Column[] pkCols = new Column[pkColIds.length];
				for (int i = 0; i < pkColIds.length; i++)
					pkCols[i] = (Column) this.mappedObjects.get(pkColIds[i]);

				// Make the key.
				final PrimaryKey pk = new PrimaryKey(pkCols);
				pk.setStatus(status);
				pk.setVisibleModified(visibleModified);

				// Assign it to the table.
				tbl.setPrimaryKey(pk);
				element = pk;
			} catch (final Exception e) {
				throw new SAXException(e);
			}

			// Store it in the map of IDed objects.
			this.mappedObjects.put(id, element);
		}

		// Foreign key (inside table).
		else if ("foreignKey".equals(eName)) {
			// What table does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof Table))
				throw new SAXException(Resources.get("fkOutsideTable"));
			final Table tbl = (Table) this.objectStack.peek();

			// We don't do these for dataset tables since 0.6 as they
			// get regenerated automatically.
			if (tbl instanceof DataSetTable)
				return;

			// Get the ID.
			final String id = (String) attributes.get("id");

			try {
				// Work out what status it is.
				final ComponentStatus status = ComponentStatus
						.get((String) attributes.get("status"));
				final boolean visibleModified = Boolean.valueOf(
						(String) attributes.get("visibleModified"))
						.booleanValue();

				// Decode the column IDs from the comma-separated list.
				final String[] fkColIds = this.readListAttribute(
						(String) attributes.get("columnIds"), false);
				final Column[] fkCols = new Column[fkColIds.length];
				for (int i = 0; i < fkColIds.length; i++)
					fkCols[i] = (Column) this.mappedObjects.get(fkColIds[i]);

				// Make the key.
				final ForeignKey fk = new ForeignKey(fkCols);
				fk.setStatus(status);
				fk.setVisibleModified(visibleModified);

				// Add it to the table.
				tbl.getForeignKeys().add(fk);
				element = fk;
			} catch (final Exception e) {
				throw new SAXException(e);
			}

			// Store it in the map of IDed objects.
			this.mappedObjects.put(id, element);
		}

		// Relation (anywhere).
		else if ("relation".equals(eName)) {
			// Get the ID.
			final String id = (String) attributes.get("id");
			try {
				// Work out status, cardinality, and look up the keys
				// at either end.
				final ComponentStatus status = ComponentStatus
						.get((String) attributes.get("status"));
				final Cardinality card = Cardinality.get((String) attributes
						.get("cardinality"));
				final Cardinality origCard = Cardinality
						.get((String) attributes.get("originalCardinality"));
				final Key firstKey = (Key) this.mappedObjects.get(attributes
						.get("firstKeyId"));
				final Key secondKey = (Key) this.mappedObjects.get(attributes
						.get("secondKeyId"));
				final boolean visibleModified = Boolean.valueOf(
						(String) attributes.get("visibleModified"))
						.booleanValue();

				// We don't do these for dataset tables since 0.6 as they
				// get regenerated automatically. We can tell this is a
				// dataset table because at least one key ID will not
				// be found.
				if (firstKey == null || secondKey == null)
					// Element must be something.
					element = null;
				else {
					// Make it
					final Relation rel = new Relation(firstKey, secondKey, card);
					firstKey.getRelations().add(rel);
					secondKey.getRelations().add(rel);

					// Set its status.
					if (origCard != null)
						rel.setOriginalCardinality(origCard);
					rel.setStatus(status);
					rel.setVisibleModified(visibleModified);
					element = rel;
				}
			} catch (final Exception e) {
				throw new SAXException(e);
			}

			// Store it in the map of IDed objects.
			this.mappedObjects.put(id, element);
		}

		// Merged Table (inside dataset).
		else if ("mergedRelation".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("mergedRelationOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));

				// Merge it.
				if (rel != null)
					rel.setMergeRelation(w, true);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Table-hide-masked (inside dataset).
		else if ("tableHideMasked".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("tableHideMaskedOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the table.
				final String tableKey = (String) attributes.get("tableKey");

				// Hide-mask it.
				w.getMods(tableKey, "tableHideMasked").put(tableKey.intern(),
						null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Explain-hide-masked (inside dataset).
		else if ("explainHideMasked".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("explainHideMaskedOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the table.
				final String tableKey = (String) attributes.get("tableKey");

				// Hide-mask it.
				w.getMods(tableKey, "explainHideMasked").put(tableKey.intern(),
						null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Visible modified (inside dataset).
		else if ("visibleModified".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("visibleModifiedOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the table.
				final String key = (String) attributes.get("key");

				// Vis-mod it.
				w.getMods(key, "visibleModified").put(key.intern(), null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// No-left-join Table (inside dataset).
		else if ("noFinalLeftJoin".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("noFinalLeftJoinOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the table.
				final String tableKey = (String) attributes.get("tableKey");

				// Distinct it.
				w.getMods(tableKey, "noFinalLeftJoin").put(tableKey.intern(),
						null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// No-optimiser Table (inside dataset).
		else if ("skipOptimiser".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("skipOptimiserOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the table.
				final String tableKey = (String) attributes.get("tableKey");

				// Distinct it.
				w.getMods(tableKey, "skipOptimiser").put(tableKey.intern(),
						null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// No-index-optimiser Table (inside dataset).
		else if ("skipIndexOptimiser".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("skipIndexOptimiserOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the table.
				final String tableKey = (String) attributes.get("tableKey");

				// Distinct it.
				w.getMods(tableKey, "skipIndexOptimiser").put(
						tableKey.intern(), null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Distinct Table (inside dataset).
		else if ("distinctRows".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("distinctRowsOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the table.
				final String tableKey = (String) attributes.get("tableKey");

				// Distinct it.
				w.getMods(tableKey, "distinctTable").put(tableKey.intern(),
						null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Masked Table (inside dataset).
		else if ("maskedTable".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("maskedTableOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the table.
				final String tableKey = (String) attributes.get("tableKey");

				// Mask it.
				w.getMods(tableKey, "dimensionMasked").put(tableKey.intern(),
						null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Masked Relation (inside dataset).
		else if ("maskedRelation".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("maskedRelationOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));
				final String tableKey = (String) attributes.get("tableKey");

				// Mask it.
				if (rel != null)
					if (tableKey == null)
						rel.setMaskRelation(w, true);
					else
						rel.setMaskRelation(w, tableKey, true);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Transform-start Relation (inside dataset).
		else if ("transformStart".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("transformStartOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Table tbl = (Table) this.mappedObjects.get(attributes
						.get("tableId"));
				final String tableKey = (String) attributes.get("tableKey");

				// Mask it.
				if (tbl != null)
					tbl.setTransformStart(w, tableKey, true);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Alternative-join Relation (inside dataset).
		else if ("alternativeJoin".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("alternativeJoinOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));
				final String tableKey = (String) attributes.get("tableKey");

				// Mask it.
				if (rel != null)
					rel.setAlternativeJoin(w, tableKey, true);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Compound Relation (inside dataset).
		else if ("compoundRelation".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("compoundRelationOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));
				final String tableKey = (String) attributes.get("tableKey");
				final Integer n = Integer.valueOf((String) attributes.get("n"));
				final boolean parallel = Boolean.valueOf(
						(String) attributes.get("parallel")).booleanValue();

				// Compound it.
				if (rel != null && n != null) {
					final CompoundRelationDefinition def = new CompoundRelationDefinition(
							n.intValue(), parallel);
					if (tableKey == null)
						rel.setCompoundRelation(w, def);
					else
						rel.setCompoundRelation(w, tableKey, def);
				}
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Directional Relation (inside dataset).
		else if ("directionalRelation".equals(eName)) {
			// Ignore - relict from 0.6.
		}

		// Unrolled Relation (inside dataset).
		else if ("unrolledRelation".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("unrolledRelationOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));
				final Column col = (Column) this.mappedObjects.get(attributes
						.get("columnId"));
				final boolean reversed = Boolean.valueOf(
						(String) attributes.get("reversed")).booleanValue();

				// Unroll it.
				if (rel != null && col != null)
					rel.setUnrolledRelation(w, new UnrolledRelationDefinition(
							col, reversed));
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Forced Relation (inside dataset).
		else if ("forcedRelation".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("forcedRelationOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));
				final String tableKey = (String) attributes.get("tableKey");

				// Force it.
				if (rel != null)
					if (tableKey == null)
						rel.setForceRelation(w, true);
					else
						rel.setForceRelation(w, tableKey, true);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Looped-back Relation (inside dataset).
		else if ("loopbackRelation".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("loopbackRelationOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));
				final Column col = attributes.containsKey("diffColumnId") ? (Column) this.mappedObjects
						.get(attributes.get("diffColumnId"))
						: null;
				final String tableKey = (String) attributes.get("tableKey");

				// Loopback it.
				if (rel != null)
					if (tableKey == null)
						rel.setLoopbackRelation(w, col);
					else
						rel.setLoopbackRelation(w, tableKey, col);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Subclass Relation (inside dataset).
		else if ("subclassRelation".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("subclassRelationOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));

				// Subclass it.
				if (rel != null)
					rel.setSubclassRelation(w, true);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Masked Column (inside dataset).
		else if ("maskedColumn".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("maskedColumnOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final String tableKey = (String) attributes.get("tableKey");
				final String colKey = (String) attributes.get("colKey");

				// Mask it.
				w.getMods(tableKey, "columnMasked").put(colKey.intern(), null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Indexdd Column (inside dataset).
		else if ("indexedColumn".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("indexedColumnOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final String tableKey = (String) attributes.get("tableKey");
				final String colKey = (String) attributes.get("colKey");

				// Index it.
				w.getMods(tableKey, "columnIndexed").put(colKey.intern(), null);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Split Optimiser Column (inside dataset).
		else if ("splitOptimiser".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("splitOptimiserOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the relation.
				final String tableKey = (String) attributes.get("tableKey");
				final String colKey = (String) attributes.get("colKey");
				final String contentCol = (String) attributes.get("contentCol");
				final String separator = (String) attributes.get("separator");
				final boolean prefix = Boolean.valueOf(
						(String) attributes.get("prefix")).booleanValue();
				final boolean suffix = Boolean.valueOf(
						(String) attributes.get("suffix")).booleanValue();
				int size = 255;
				try {
					size = Integer.valueOf((String) attributes.get("size"))
							.intValue();
				} catch (final NumberFormatException ne) {
					size = 255;
				}
				if (size < 1)
					size = 255;

				// Index it.
				final SplitOptimiserColumnDef def = new SplitOptimiserColumnDef(
						contentCol, separator);
				def.setPrefix(prefix);
				def.setSuffix(suffix);
				def.setSize(size);
				w.getMods(tableKey, "splitOptimiserColumn").put(
						colKey.intern(), def);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Renamed table (inside dataset).
		else if ("renamedTable".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("renamedTableOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the values.
				final String tableKey = (String) attributes.get("tableKey");
				final String newName = (String) attributes.get("newName");

				w.getMods(tableKey, "tableRename").put(tableKey.intern(),
						newName);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Renamed column (inside dataset).
		else if ("renamedColumn".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("renamedColumnOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the values.
				final String tableKey = (String) attributes.get("tableKey");
				final String colKey = (String) attributes.get("colKey");
				final String newName = (String) attributes.get("newName");

				w.getMods(tableKey, "columnRename").put(colKey.intern(),
						newName);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Restricted Table (inside dataset).
		else if ("restrictedTable".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("restrictedTableOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the restriction.
				final Table tbl = (Table) this.mappedObjects.get(attributes
						.get("tableId"));
				// Default to dataset-wide restriction if 0.5 syntax used.
				final String tableKey = (String) attributes.get("tableKey");

				// Get the aliases to use for the first table.
				final Map aliases = new HashMap();
				final String[] aliasColumnIds = this.readListAttribute(
						(String) attributes.get("aliasColumnIds"), false);
				// Remove
				final String[] aliasNames = this.readListAttribute(
						(String) attributes.get("aliasNames"), false);
				for (int i = 0; i < aliasColumnIds.length; i++) {
					final Column wcol = (Column) this.mappedObjects
							.get(aliasColumnIds[i]);
					if (wcol != null)
						aliases.put(wcol, aliasNames[i]);
				}
				// Get the expression to use.
				final String expr = (String) attributes.get("expression");

				// Set up the restriction.
				if (expr != null && !aliases.isEmpty() && tableKey != null
						&& tbl != null) {
					final RestrictedTableDefinition def = new RestrictedTableDefinition(
							expr, aliases);
					tbl.setRestrictTable(w, tableKey, def);
				}
			} catch (final Exception e) {
				if (e instanceof SAXException)
					throw (SAXException) e;
				else
					throw new SAXException(e);
			}
		}

		// Big Table (inside dataset).
		else if ("bigTable".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources.get("bigTableOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the restriction.
				final Table tbl = (Table) this.mappedObjects.get(attributes
						.get("tableId"));
				// Default to dataset-wide restriction if 0.5 syntax used.
				final String tableKey = (String) attributes.get("tableKey");

				// Get the aliases to use for the first table.
				final int bigness = Integer.parseInt((String) attributes
						.get("bigness"));

				if (tableKey == null)
					tbl.setBigTable(w, bigness);
				else
					tbl.setBigTable(w, tableKey, bigness);
			} catch (final Exception e) {
				if (e instanceof SAXException)
					throw (SAXException) e;
				else
					throw new SAXException(e);
			}
		}

		// Expression Column (inside dataset).
		else if ("expressionColumn".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("expressionColumnOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				final String colKey = (String) attributes.get("colKey");
				final String tableKey = (String) attributes.get("tableKey");

				// Get the aliases to use for the first table.
				final Map aliases = new HashMap();
				final String[] aliasColumnNames = this.readListAttribute(
						(String) attributes.get("aliasColumnNames"), false);
				final String[] aliasNames = this.readListAttribute(
						(String) attributes.get("aliasNames"), false);
				for (int i = 0; i < aliasColumnNames.length; i++)
					aliases.put(aliasColumnNames[i], aliasNames[i]);
				// Get the expression to use.
				final String expr = (String) attributes.get("expression");
				final boolean groupBy = Boolean.valueOf(
						(String) attributes.get("groupBy")).booleanValue();

				// Set the expression up.
				if (expr != null && !aliases.isEmpty() && tableKey != null
						&& colKey != null) {
					final ExpressionColumnDefinition expdef = new ExpressionColumnDefinition(
							expr, aliases, groupBy, colKey);
					w.getMods(tableKey, "initialExpressions").put(
							colKey.intern(), expdef);
				}
			} catch (final Exception e) {
				if (e instanceof SAXException)
					throw (SAXException) e;
				else
					throw new SAXException(e);
			}
		}

		// Partitioned column (inside dataset).
		else if ("partitionedColumn".equals(eName)) {
			// Ignore - legacy from 0.6.
		}

		// Restricted Relation (inside dataset).
		else if ("restrictedRelation".equals(eName)) {
			// What dataset does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof DataSet))
				throw new SAXException(Resources
						.get("restrictedRelationOutsideDataSet"));
			final DataSet w = (DataSet) this.objectStack.peek();

			try {
				// Look up the restriction.
				final Relation rel = (Relation) this.mappedObjects
						.get(attributes.get("relationId"));
				final String tableKey = (String) attributes.get("tableKey");
				final int index = Integer.parseInt((String) attributes
						.get("index"));

				// Get the aliases to use for the first table.
				final Map laliases = new HashMap();
				final String[] laliasColumnIds = this.readListAttribute(
						(String) attributes.get("leftAliasColumnIds"), false);
				final String[] laliasNames = this.readListAttribute(
						(String) attributes.get("leftAliasNames"), false);
				for (int i = 0; i < laliasColumnIds.length; i++) {
					final Column wcol = (Column) this.mappedObjects
							.get(laliasColumnIds[i]);
					if (wcol != null)
						laliases.put(wcol, laliasNames[i]);
				}
				// and the second
				final Map raliases = new HashMap();
				final String[] raliasColumnIds = this.readListAttribute(
						(String) attributes.get("rightAliasColumnIds"), false);
				final String[] raliasNames = this.readListAttribute(
						(String) attributes.get("rightAliasNames"), false);
				for (int i = 0; i < raliasColumnIds.length; i++) {
					final Column wcol = (Column) this.mappedObjects
							.get(raliasColumnIds[i]);
					if (wcol != null)
						raliases.put(wcol, raliasNames[i]);
				}
				// Get the expression to use.
				final String expr = (String) attributes.get("expression");

				if (expr != null && rel != null && tableKey != null
						&& !laliases.isEmpty() && !raliases.isEmpty()) {
					final RestrictedRelationDefinition def = new RestrictedRelationDefinition(
							expr, laliases, raliases);
					rel.setRestrictRelation(w, tableKey, def, index);
				}
			} catch (final Exception e) {
				if (e instanceof SAXException)
					throw (SAXException) e;
				else
					throw new SAXException(e);
			}
		}

		// DataSet (anywhere).
		else if ("dataset".equals(eName))
			try {
				// Look up the name etc.
				// Resolve them all.
				final String name = (String) attributes.get("name");
				final boolean invisible = Boolean.valueOf(
						(String) attributes.get("invisible")).booleanValue();
				final boolean masked = Boolean.valueOf(
						(String) attributes.get("masked")).booleanValue();
				final boolean hideMasked = Boolean.valueOf(
						(String) attributes.get("hideMasked")).booleanValue();
				final Table centralTable = (Table) this.mappedObjects
						.get(attributes.get("centralTableId"));
				final String optType = (String) attributes.get("optimiser");
				final boolean index = Boolean.valueOf(
						(String) attributes.get("indexOptimiser"))
						.booleanValue();

				// Construct the dataset.
				final DataSet ds = new DataSet(this.constructedMart,
						centralTable, name);
				this.constructedMart.getDataSets()
						.put(ds.getOriginalName(), ds);

				// Work out the optimiser.
				DataSetOptimiserType opt = DataSetOptimiserType.NONE;
				try {
					opt = (DataSetOptimiserType) DataSetOptimiserType.class
							.getField(optType).get(null);
				} catch (final NoSuchFieldException nfe) {
					opt = DataSetOptimiserType.NONE;
				}

				// Assign the settings.
				ds.setDataSetOptimiserType(opt);
				ds.setInvisible(invisible);
				ds.setMasked(masked);
				ds.setHideMasked(hideMasked);
				ds.setIndexOptimiser(index);
				element = ds;
			} catch (final Exception e) {
				if (e instanceof SAXException)
					throw (SAXException) e;
				else
					throw new SAXException(e);
			}

		// DataSet partition table (anywhere).
		else if ("datasetPartitionTable".equals(eName))
			try {
				// Convert a dataset into a partition table.
				final DataSet ds = (DataSet) this.constructedMart.getDataSets()
						.get((String) attributes.get("name"));
				final String[] selectedColumns = this.readListAttribute(
						(String) attributes.get("selectedColumns"), false);
				ds.setPartitionTable(true);
				ds.asPartitionTable().setSelectedColumnNames(
						Arrays.asList(selectedColumns));
				element = ds.asPartitionTable();
			} catch (final Exception e) {
				if (e instanceof SAXException)
					throw (SAXException) e;
				else
					throw new SAXException(e);
			}

		// Partition regex (inside partition table)
		else if ("partitionRegex".equals(eName)) {
			// What pt does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof PartitionTable))
				throw new SAXException(Resources
						.get("partitionRegexOutsidePartitionTable"));
			final PartitionTable pt = (PartitionTable) this.objectStack.peek();

			final String name = (String) attributes.get("name");
			final String match = (String) attributes.get("match");
			final String replace = (String) attributes.get("replace");

			try {
				((PartitionColumn) pt.getColumns().get(name))
						.setRegexMatch(match);
				((PartitionColumn) pt.getColumns().get(name))
						.setRegexReplace(replace);
			} catch (final Exception e) {
				throw new SAXException(e);
			}
		}

		// Partition application (inside partition table)
		else if ("partitionApplication".equals(eName)) {
			// What pt does it belong to? Throw a wobbly if none.
			if (this.objectStack.empty()
					|| !(this.objectStack.peek() instanceof PartitionTable))
				throw new SAXException(Resources
						.get("partitionApplicationOutsidePartitionTable"));
			final PartitionTable pt = (PartitionTable) this.objectStack.peek();

			final DataSet ds = (DataSet) this.constructedMart.getDataSets()
					.get((String) attributes.get("name"));
			String dimension = (String) attributes.get("dimension");
			final String[] pCols = this.readListAttribute((String) attributes
					.get("pCols"), false);
			final String[] dsCols = this.readListAttribute((String) attributes
					.get("dsCols"), false);
			final String[] relIds = this.readListAttribute((String) attributes
					.get("relationIds"), true);
			final Relation[] rels = new Relation[relIds.length];
			for (int i = 0; i < relIds.length; i++)
				rels[i] = (Relation) this.mappedObjects.get(relIds[i]);
			final String[] nameCols = this.readListAttribute(
					(String) attributes.get("nameCols"), false);
			final String[] compounds = this.readListAttribute(
					(String) attributes.get("compounds"), false);
			final PartitionTableApplication pta = new PartitionTableApplication(
					pt);
			for (int i = 0; i < pCols.length; i++) {
				final PartitionAppliedRow row = new PartitionAppliedRow(
						pCols[i], dsCols[i], nameCols[i], rels[i]);
				if (compounds.length > i)
					row.setCompound(Integer.parseInt(compounds[i]));
				pta.getPartitionAppliedRows().add(row);
			}
			if (dimension == null)
				dimension = PartitionTable.NO_DIMENSION;
			if (!dimension.equals(PartitionTable.NO_DIMENSION))
				ds.getMods(dimension, "initialPTAs").put(dimension.intern(),
						pta);
			else
				pt.applyTo(ds, dimension, pta);
		} else
			throw new SAXException(Resources.get("unknownTag", eName));

		// Stick the element on the stack so that the next element
		// knows what it is inside.
		this.objectStack.push(element);
	}
}
