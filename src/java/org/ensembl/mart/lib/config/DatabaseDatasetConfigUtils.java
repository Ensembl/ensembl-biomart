/*
 Copyright (C) 2003 EBI, GRL

 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 2.1 of the License, or (at your option) any later version.

 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 Lesser General Public License for more details.

 You should have received a copy of the GNU Lesser General Public
 License along with this library; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */

package org.ensembl.mart.lib.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.swing.JOptionPane;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamSource;

import oracle.sql.BLOB;
import oracle.sql.CLOB;

import org.ensembl.mart.editor.MartEditor;
import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.mart.util.ColumnDescription;
import org.ensembl.mart.util.TableDescription;
import org.jdom.Document;
import org.jdom.output.XMLOutputter;
import org.jdom.transform.JDOMResult;
import org.jdom.transform.JDOMSource;

/**
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */

public class DatabaseDatasetConfigUtils {

	private class DescriptionValidity {
		boolean fieldValid = false;
		boolean tableValid = false;

	}

	public static void closeQuietly(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
			}
		}
	}

	public static void closeQuietly(PreparedStatement ps) {
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
			}
		}
	}

	private final String SOFTWAREVERSION = "0.6";

	private final String XSL_05_FILE = "data/mart_0_4_0_5.xsl";

	private final String XSL_06_FILE = "data/mart_0_5_0_6.xsl";

	private final String BASEMETATABLE = "meta_conf__dataset__main";

	private final String MARTUSERTABLE = "meta_conf__user__dm";

	private final String MARTINTERFACETABLE = "meta_conf__interface__dm";

	private final String MARTXMLTABLE = "meta_conf__xml__dm";

	private final String MARTRELEASETABLE = "meta_release__release__main";

	private final String MARTVERSIONTABLE = "meta_version__version__main";

	private final String MARTTEMPLATEMAINTABLE = "meta_template__template__main";

	private final String MARTTEMPLATEDMTABLE = "meta_template__xml__dm";

	private final String MAINTABLESUFFIX = "main";

	private final String DIMENSIONTABLESUFFIX = "dm";

	private final String LOOKUPTABLESUFFIX = "look";

	private final String DOESNTEXISTSUFFIX = "**DOES_NOT_EXIST**";

	private final String DEFAULTLEGALQUALIFIERS = "=";

	private final String DEFAULTQUALIFIER = "=";

	private final String DEFAULTTYPE = "text";

	private final Logger logger = Logger
			.getLogger(DatabaseDatasetConfigUtils.class.getName());

	private HashMap configInfo = new HashMap();

	private DatasetConfigXMLUtils dscutils = null;

	private DetailedDataSource dsource = null;

	private boolean readonly;

	/**
	 * Constructor for a DatabaseDatasetConfigUtils object to obtain
	 * DatasetConfig related information from a Mart Database host.
	 * 
	 * @param dscutils
	 *            - DatasetConfigXMLUtils object for parsing XML
	 * @param dsource
	 *            - DetailedDataSource object with connection to a Mart Database
	 *            host.
	 * @param readonly
	 *            - true if this is a read-only connection (meta tables will not
	 *            be altered).
	 */
	public DatabaseDatasetConfigUtils(final DatasetConfigXMLUtils dscutils,
			final DetailedDataSource dsource, final boolean readonly) {
		this.dscutils = dscutils;
		this.dsource = dsource;
		// this.connection = dsource.getConnection();
		this.readonly = readonly;
	}

	public void setReadonly(final boolean readonly) {
		this.readonly = readonly;
	}

	public String getSoftwareVersion() {
		return SOFTWAREVERSION;
	}

	/**
	 * Verify if a meta_configuration_[user] table exists. Returns false if user
	 * is null, or if the table does not exist.
	 * 
	 * @param user
	 *            - user to query
	 * @return true if meta_configuration_[user] exists, false otherwise
	 */
	public boolean datasetConfigUserTableExists(final String user) {
		boolean exists = true;
		final String table = this.BASEMETATABLE + "_" + user;

		if (user == null)
			return false;

		try {
			exists = this.tableExists(table);

			if (!exists)
				// try upper casing the name
				exists = this.tableExists(table.toUpperCase());

			if (!exists)
				// try lower casing the name
				exists = this.tableExists(table.toLowerCase());
		} catch (final SQLException e) {
			exists = false;
			if (this.logger.isLoggable(Level.INFO))
				this.logger
						.info("Recieved SQL Exception checking for user table: "
								+ e.getMessage() + "\ncontinuing!\n");
		}

		return exists;
	}

	private Set tables = null;

	private Set getTables() throws SQLException {
		if (tables == null) {
			tables = new HashSet();
			Connection conn = null;
			ResultSet vr = null;
			try {
				conn = this.dsource.getConnection();
				final String catalog = conn.getCatalog();
				vr = conn.getMetaData().getTables(catalog, this.getSchema()[0],
						null, null);
				while (vr.next()) {
					tables.add(vr.getString(3));
				}
			} finally {
				closeQuietly(vr);
				DetailedDataSource.close(conn);
			}
		}
		return tables;
	}

	private Map tableColumns;

	private Map getTableColumns(String dset, final String schema,
			final String catalog, final Connection conn) throws SQLException {
		if (tableColumns == null) {
			// tableColumns = new HashMap<String,SoftReference<Map<String,
			// Set<String>>>>();
			tableColumns = new HashMap();
		}
		Map tableColumnsForSet = null;
		SoftReference tcRef = (SoftReference) (tableColumns.get(dset));
		if (tcRef != null) {
			tableColumnsForSet = (Map) (tcRef.get());
		}
		if (tableColumnsForSet == null) {
			tableColumnsForSet = new HashMap();
			tableColumnsForSet = new HashMap();
			ResultSet rs = null;
			try {
			rs = conn.getMetaData().getColumns(catalog, schema,
					dset + "%", "%");

			while (rs.next()) {
				final String columnName = rs.getString(4).toLowerCase();
				final String tableName = rs.getString(3).toLowerCase();
				Set cols = (Set) (tableColumnsForSet.get(tableName));
				if (cols == null) {
					cols = new HashSet();
					tableColumnsForSet.put(tableName, cols);
				}
				cols.add(columnName);
			}
			} finally {
				closeQuietly(rs);
			}
			tableColumns.put(dset, new SoftReference(tableColumnsForSet));
		}
		return tableColumnsForSet;
	}

	private Set getMainTables(String dataSet, final String schema,
			final String catalog, final Connection conn) throws SQLException {
		Set subs = new HashSet();
		String pat = "^" + dataSet + ".*__main$";
		String tableName = null;
		for (Iterator i = getTableColumns(dataSet, schema, catalog, conn)
				.keySet().iterator(); i.hasNext();) {
			tableName = (String) i.next();
			if (tableName.matches(pat)) {
				subs.add(tableName);
			}
		}
		return subs;
	}

	private boolean tableExists(final String table) throws SQLException {
		String tcheck = null;
		Connection conn = null;
		boolean ret = true;
		ResultSet vr = null;
		try {
			conn = this.dsource.getConnection();
			final String catalog = conn.getCatalog();

			vr = conn.getMetaData().getTables(catalog,
					this.getSchema()[0], table, null);

			// expect at most one result, if no results, tcheck will remain null
			if (vr.next())
				tcheck = vr.getString(3);

			vr.close();

			if (tcheck == null) {
				if (this.logger.isLoggable(Level.FINE))
					this.logger.fine("Table " + table
							+ " does not exist, using " + this.BASEMETATABLE
							+ " instead\n");
				ret = false;
			}

			if (tcheck == null || !table.equals(tcheck)) {
				if (this.logger.isLoggable(Level.FINE))
					this.logger
							.fine("Returned Wrong table for verifyTable: wanted "
									+ table + " got " + tcheck + "\n");
				ret = false;
			}
			conn.close();
		} finally {
			closeQuietly(vr);
			// this throws any non-SQLException, but always closes the
			// connection
			DetailedDataSource.close(conn);
		}

		return ret;
	}

	public String getNewVersion(final String dataset) throws SQLException {
		Connection conn = null;
		String version = null;
		PreparedStatement ps = null;
		 ResultSet rs = null;
		
		try {
			conn = this.dsource.getConnection();
			ps = conn
					.prepareStatement("select release_version from "
							+ this.MARTRELEASETABLE + " where dataset = ?");
			ps.setString(1, dataset);
			rs = ps.executeQuery();
			rs.next();
			version = rs.getString(1);
		} catch (final SQLException e) {

			System.out.println("Include a " + this.MARTRELEASETABLE
					+ " table entry for dataset " + dataset
					+ " if you want auto version updating");
		} finally {
			closeQuietly(rs);
			closeQuietly(ps);
			
			// this throws any SQLException, but always closes the connection
			DetailedDataSource.close(conn);
		}

		return version;
	}

	public boolean updateLinkVersions(final DatasetConfig dsv)
			throws SQLException {
		Connection conn = null;
		boolean updated = false;
		String linkVersion = null;
		try {
			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn
					.prepareStatement("select link_version from "
							+ this.MARTRELEASETABLE + " where dataset = ?");
			ps.setString(1, dsv.getDataset());
			final ResultSet rs = ps.executeQuery();
			rs.next();
			linkVersion = rs.getString(1);
			rs.close();
			final Importable[] imps = dsv.getImportables();
			for (int i = 0; i < imps.length; i++) {
				final String currentLinkVersion = imps[i].getLinkVersion();
				if (currentLinkVersion != null && currentLinkVersion != ""
						&& !currentLinkVersion.equals(linkVersion)) {
					imps[i].setLinkVersion(linkVersion);
					updated = true;
				}

			}
			final Exportable[] exps = dsv.getExportables();
			for (int i = 0; i < exps.length; i++) {
				final String currentLinkVersion = exps[i].getLinkVersion();
				if (currentLinkVersion != null && currentLinkVersion != ""
						&& !currentLinkVersion.equals(linkVersion)) {
					exps[i].setLinkVersion(linkVersion);
					updated = true;
				}

			}
			conn.close();
		} catch (final SQLException e) {
			System.out.println("Include a " + this.MARTRELEASETABLE
					+ " table entry for dataset " + dsv.getDataset()
					+ " if you want auto link version updating");
		} finally {
			// this throws any SQLException, but always closes the connection
			DetailedDataSource.close(conn);
		}

		return updated;
	}

	/**
	 * Determine if meta_configuration exists in a Mart Database defined by the
	 * given DetailedDataSource.
	 * 
	 * @return true if meta_configuration exists, false if it does not exist
	 * @throws ConfigurationException
	 *             for all underlying Exceptions
	 */
	public boolean baseDSConfigTableExists() throws ConfigurationException {
		final String table = this.BASEMETATABLE;
		try {
			boolean exists = true;

			exists = this.tableExists(table);
			if (!exists)
				exists = this.tableExists(table.toUpperCase());
			if (!exists)
				exists = this.tableExists(table.toLowerCase());

			return exists;
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Could not find base Configuration table " + table
							+ " in DataSource: " + this.dsource
							+ "\nRecieved SQL Exception: " + e.getMessage()
							+ "\n");
		}
	}

	/**
	 * Determine if meta_template__template__main exists in a Mart Database
	 * defined by the given DetailedDataSource.
	 * 
	 * @return true if meta_template__template__main exists, false if it does
	 *         not exist
	 * @throws ConfigurationException
	 *             for all underlying Exceptions
	 */
	public boolean templateTableExists() throws ConfigurationException {
		final String table = this.MARTTEMPLATEMAINTABLE;
		try {
			boolean exists = true;

			exists = this.tableExists(table);
			if (!exists)
				exists = this.tableExists(table.toUpperCase());
			if (!exists)
				exists = this.tableExists(table.toLowerCase());

			return exists;
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Could not find base Configuration table " + table
							+ " in DataSource: " + this.dsource
							+ "\nRecieved SQL Exception: " + e.getMessage()
							+ "\n");
		}
	}

	/**
	 * Store a DatesetConfig.dtd compliant (compressed or uncompressed) XML
	 * Document in the Mart Database with a given internalName and displayName.
	 * If user is not null and meta_DatsetConfig_[user] exists, this table is
	 * the target, otherwise, meta_configuration is the target. Along with the
	 * internalName and displayName of the XML, an MD5 messageDigest of the xml
	 * is computed, and stored as well.
	 * 
	 * @param user
	 *            -- Specific User to look for meta_configuration_[user] table,
	 *            if null, or non-existent, uses meta_configuration
	 * @param internalName
	 *            -- internalName of the DatasetConfigXML being stored.
	 * @param displayName
	 *            -- displayName of the DatasetConfig XML being stored.
	 * @param dataset
	 *            -- dataset of the DatasetConfig XML being stored
	 * @param doc
	 *            - JDOM Document object representing the XML for the
	 *            DatasetConfig
	 * @param compress
	 *            -- if true, the XML is compressed using GZIP.
	 * @throws ConfigurationException
	 *             when no meta_configuration table exists, and for all
	 *             underlying Exceptions
	 */
	public void storeDatasetConfiguration(final String user,
			final String internalName, final String displayName,
			final String dataset, final String description, Document doc,
			final boolean compress, final String type, final String visible,
			final String version, final String datasetID, String martUsers,
			String interfaces, final DatasetConfig dsConfig)
			throws ConfigurationException {

		if (martUsers == null || martUsers.equals(""))
			martUsers = "default";

		if (interfaces == null || interfaces.equals(""))
			interfaces = "default";

		// Before storing check attribute and filter names are unique per
		// dataset (attribute and filter names
		// are allowed to be the same
		// Also check the importable and exportable filters and attributes are
		// defined

		// check uniqueness of internal names per page
		final AttributePage[] apages = dsConfig.getAttributePages();

		AttributePage apage;
		final String testInternalName;
		String duplicationString = "";
		final String duplicatedAttString = "";
		String spaceErrors = "";
		final String linkErrors = "";
		String brokenString = "";

		final Hashtable descriptionsMap = new Hashtable();// atts should have
		// a unique internal
		// name
		final Hashtable attributeDuplicationMap = new Hashtable();
		final Hashtable attributeListDuplicationMap = new Hashtable();

		final Hashtable descriptionsTCFieldMap = new Hashtable();// atts
		// should
		// have a
		// unique TC
		// and field
		final Hashtable attributeDuplicationTCFieldMap = new Hashtable();

		final Hashtable filterDuplicationMap = new Hashtable();

		for (int i = 0; i < apages.length; i++) {
			apage = apages[i];

			if (apage.getHidden() != null && apage.getHidden().equals("true"))
				continue;

			List testGroups = new ArrayList();
			testGroups = apage.getAttributeGroups();
			for (final Iterator groupIter = testGroups.iterator(); groupIter
					.hasNext();) {
				final AttributeGroup testGroup = (AttributeGroup) groupIter
						.next();
				final AttributeCollection[] testColls = testGroup
						.getAttributeCollections();
				for (int col = 0; col < testColls.length; col++) {
					final AttributeCollection testColl = testColls[col];

					if (testColl.getInternalName().matches("\\w+\\s+\\w+"))
						spaceErrors = spaceErrors + "AttributeCollection "
								+ testColl.getInternalName() + " in dataset "
								+ dsConfig.getDataset() + "\n";
					List testAtts = new ArrayList();

					testAtts = testColl.getAttributeDescriptions();

					for (final Iterator iter = testAtts.iterator(); iter
							.hasNext();) {
						final Object testAtt = iter.next();
						final AttributeDescription testAD = (AttributeDescription) testAtt;
						// if (testAD.getInternalName().matches("\\w+\\.\\w+")
						// ||
						// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
						if (testAD.getPointerDataset() != null
								&& !"".equals(testAD.getPointerDataset()))
							continue;// placeholder atts can be duplicated

						// don't allow any duplication of TC and field, even for
						// hidden atts
						if (descriptionsTCFieldMap
								.containsKey(testAD.getTableConstraint() + "."
										+ testAD.getField()))
							attributeDuplicationTCFieldMap.put(
									testAD.getTableConstraint() + "."
											+ testAD.getField(),
									dsConfig.getDataset());
						descriptionsTCFieldMap.put(testAD.getTableConstraint()
								+ "." + testAD.getField(), "1");

						if (testAD.getHidden() != null
								&& testAD.getHidden().equals("true"))
							continue;

						if (testAD.getInternalName().matches("\\w+\\s+\\w+"))
							spaceErrors = spaceErrors
									+ testAD.getInternalName() + " in page "
									+ apage.getInternalName() + "\n";
						if (descriptionsMap.containsKey(testAD
								.getInternalName()))
							attributeDuplicationMap.put(
									testAD.getInternalName(),
									dsConfig.getDataset());
						descriptionsMap.put(testAD.getInternalName(), "1");

						if (dsConfig.getType().equals("GenomicSequence"))
							continue;// no point in checking fields

						// test has all its fields defined - if not add a
						// message to brokenString
						if (testAD.getInternalName() == null
								|| testAD.getInternalName().equals("")
								|| testAD.getField() == null
								|| testAD.getField().equals("")
								|| testAD.getTableConstraint() == null
								|| testAD.getTableConstraint().equals("")
								|| dsConfig.getVisible() != null
								&& dsConfig.getVisible().equals("1")
								&& (testAD.getKey() == null || testAD.getKey()
										.equals(""))
						// testAD.getKey() == null || testAD.getKey().equals("")
						)
							brokenString = brokenString + "Attribute "
									+ testAD.getInternalName() + " in dataset "
									+ dsConfig.getDataset() + " and page "
									+ apage.getInternalName() + "\n";

					}

					descriptionsMap.clear();
					testAtts = testColl.getAttributeLists();

					for (final Iterator iter = testAtts.iterator(); iter
							.hasNext();) {
						final Object testAtt = iter.next();
						final AttributeList testAD = (AttributeList) testAtt;

						if (testAD.getHidden() != null
								&& testAD.getHidden().equals("true"))
							continue;

						if (testAD.getInternalName().matches("\\w+\\s+\\w+"))
							spaceErrors = spaceErrors
									+ testAD.getInternalName() + " in page "
									+ apage.getInternalName() + "\n";
						if (descriptionsMap.containsKey(testAD
								.getInternalName()))
							attributeListDuplicationMap.put(
									testAD.getInternalName(),
									dsConfig.getDataset());
						descriptionsMap.put(testAD.getInternalName(), "1");

						if (dsConfig.getType().equals("GenomicSequence"))
							continue;// no point in checking fields

						// test has all its fields defined - if not add a
						// message to brokenString
						if (testAD.getInternalName() == null
								|| testAD.getInternalName().equals(""))
							brokenString = brokenString + "AttributeList "
									+ testAD.getInternalName() + " in dataset "
									+ dsConfig.getDataset() + " and page "
									+ apage.getInternalName() + "\n";

					}

				}
			}
		}

		final Exportable[] exps = dsConfig.getExportables();
		for (int i = 0; i < exps.length; i++) {
			final String attributes = exps[i].getAttributes();
			final String[] atts = attributes.split(",");
			for (int j = 0; j < atts.length; j++)
				if (!atts[j].matches("\\w+__\\w+")
						&& !descriptionsMap.containsKey(atts[j]))
					dsConfig.removeExportable(exps[i]);// just get rid of
			// broken ones - needs
			// fixing at template
			// level
			// linkErrors = linkErrors + atts[j] + " in exportable " +
			// exps[i].getInternalName() + "\n";
			// test has all its fields defined - if not add a message to
			// brokenString
			if (exps[i].getInternalName() == null
					|| exps[i].getInternalName().equals("")
					|| exps[i].getLinkName() == null
					|| exps[i].getLinkName().equals("")
					|| exps[i].getName() == null
					|| exps[i].getName().equals("")
					|| exps[i].getAttributes() == null
					|| exps[i].getAttributes().equals(""))
				brokenString = brokenString + "Exportable "
						+ exps[i].getInternalName() + " in dataset "
						+ dsConfig.getDataset() + "\n";
		}

		// repeat for filter pages
		descriptionsMap.clear();
		final FilterPage[] fpages = dsConfig.getFilterPages();
		FilterPage fpage;
		for (int i = 0; i < fpages.length; i++) {
			fpage = fpages[i];

			if (fpage.getHidden() != null && fpage.getHidden().equals("true"))
				continue;

			List testGroups = new ArrayList();
			testGroups = fpage.getFilterGroups();
			for (final Iterator groupIter = testGroups.iterator(); groupIter
					.hasNext();) {
				final FilterGroup testGroup = (FilterGroup) groupIter.next();
				final FilterCollection[] testColls = testGroup
						.getFilterCollections();
				for (int col = 0; col < testColls.length; col++) {
					final FilterCollection testColl = testColls[col];

					if (testColl.getInternalName().matches("\\w+\\s+\\w+"))
						spaceErrors = spaceErrors + "FilterCollection "
								+ testColl.getInternalName() + " in dataset "
								+ dsConfig.getDataset() + "\n";
					List testAtts = new ArrayList();
					testAtts = testColl.getFilterDescriptions();// ? OPTIONS
					for (final Iterator iter = testAtts.iterator(); iter
							.hasNext();) {
						final Object testAtt = iter.next();
						final FilterDescription testAD = (FilterDescription) testAtt;
						if (testAD.getHidden() != null
								&& testAD.getHidden().equals("true"))
							continue;

						// if (testAD.getInternalName().matches("\\w+\\.\\w+")
						// ||
						// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
						if (testAD.getPointerDataset() != null
								&& !"".equals(testAD.getPointerDataset()))
							continue;// placeholder filts can be duplicated
						if (testAD.getInternalName().matches("\\w+\\s+\\w+"))
							spaceErrors = spaceErrors
									+ testAD.getInternalName() + " in page "
									+ fpage.getInternalName() + "\n";
						if (descriptionsMap.containsKey(testAD
								.getInternalName())) {
							filterDuplicationMap.put(testAD.getInternalName(),
									dsConfig.getDataset());
							continue;// to stop options also being assessed
						}

						descriptionsMap.put(testAD.getInternalName(), "1");
						if (dsConfig.getType().equals("GenomicSequence"))
							continue;// no point in checking fields

						// test has all its fields defined - if not add a
						// message to brokenString
						// if (testAD.getOptions().length == 0 &&
						// (testAD.getInternalName() == null ||
						// testAD.getInternalName().equals("") ||
						if ((testAD.getFilterList() == null || testAD
								.getFilterList().equals(""))
								&& (testAD.getOptions().length == 0 || testAD
										.getOptions()[0].getField() == null)
								&& (testAD.getInternalName() == null
										|| testAD.getInternalName().equals("")
										||

										testAD.getField() == null
										|| testAD.getField().equals("")
										|| testAD.getTableConstraint() == null
										|| testAD.getTableConstraint().equals(
												"")
										|| dsConfig.getVisible() != null
										&& dsConfig.getVisible().equals("1")
										&& (testAD.getKey() == null || testAD
												.getKey().equals("")) ||
										// testAD.getKey() == null ||
										// testAD.getKey().equals("") ||
										testAD.getQualifier() == null || testAD
										.getQualifier().equals("")))
							brokenString = brokenString + "Filter "
									+ testAD.getInternalName() + " in dataset "
									+ dsConfig.getDataset() + " and page "
									+ fpage.getInternalName() + "\n";

						// do options as well
						final Option[] ops = testAD.getOptions();
						if (ops.length > 0 && ops[0].getType() != null
								&& !ops[0].getType().equals(""))
							for (int j = 0; j < ops.length; j++) {
								final Option op = ops[j];
								if (op.getHidden() != null
										&& op.getHidden().equals("true"))
									continue;
								if (descriptionsMap.containsKey(op
										.getInternalName()))
									filterDuplicationMap.put(
											op.getInternalName(),
											dsConfig.getDataset());
								descriptionsMap.put(op.getInternalName(), "1");
							}
					}
				}
			}
		}
		final Importable[] imps = dsConfig.getImportables();
		for (int i = 0; i < imps.length; i++) {
			final String filters = imps[i].getFilters();
			final String[] filts = filters.split(",");
			for (int j = 0; j < filts.length; j++)
				if (!descriptionsMap.containsKey(filts[j]))
					dsConfig.removeImportable(imps[i]);// just get rid of
			// broken ones - needs
			// fixing at template
			// level
			// linkErrors = linkErrors + filts[j] + " in importable " +
			// imps[i].getInternalName() + "\n";
			// test has all its fields defined - if not add a message to
			// brokenString
			if (imps[i].getInternalName() == null
					|| imps[i].getInternalName().equals("")
					|| imps[i].getLinkName() == null
					|| imps[i].getLinkName().equals("")
					|| imps[i].getName() == null
					|| imps[i].getName().equals("")
					|| imps[i].getFilters() == null
					|| imps[i].getFilters().equals(""))
				brokenString = brokenString + "Importable "
						+ imps[i].getInternalName() + " in dataset "
						+ dsConfig.getDataset() + "\n";
		}

		if (spaceErrors != "") {
			JOptionPane.showMessageDialog(null,
					"The following internal names contain spaces:\n"
							+ spaceErrors, "ERROR", 0);
			return;// no export performed
		}

		if (brokenString != "" && dsConfig.getType().equals("TableSet")) {
			final int choice = JOptionPane.showConfirmDialog(null,
					"The following may not contain the required fields:\n"
							+ brokenString, "Export Anyway?",
					JOptionPane.YES_NO_OPTION);
			if (choice != 0)
				return;// no export performed
		}

		if (linkErrors != "") {
			JOptionPane.showMessageDialog(null,
					"The following internal names are incorrect in links:\n"
							+ linkErrors, "ERROR", 0);
			return;// no export performed
		}

		// maybe add back in later
		/*
		 * if (attributeDuplicationTCFieldMap.size() > 0){ duplicatedAttString =
		 * "The following attributes are duplicated. Use internal placeholders
		 * instead\n"; Enumeration enum = attributeDuplicationTCFieldMap.keys();
		 * while (enum.hasMoreElements()){ String intName = (String)
		 * enum.nextElement(); duplicatedAttString =
		 * duplicationString+"Attribute for "+intName+" in dataset
		 * "+dsConfig.getDataset()+"\n"; } }
		 */
		if (attributeDuplicationMap.size() > 0) {
			duplicationString = "The following attribute internal names are duplicated and will cause client problems:\n";
			final Enumeration e = attributeDuplicationMap.keys();
			while (e.hasMoreElements()) {
				final String intName = (String) e.nextElement();
				duplicationString = duplicationString + "Attribute " + intName
						+ " in dataset " + dsConfig.getDataset() + "\n";
			}
		} else if (attributeListDuplicationMap.size() > 0) {
			duplicationString = "The following attributeList internal names are duplicated and will cause client problems:\n";
			final Enumeration e = attributeListDuplicationMap.keys();
			while (e.hasMoreElements()) {
				final String intName = (String) e.nextElement();
				duplicationString = duplicationString + "AttributeList "
						+ intName + " in dataset " + dsConfig.getDataset()
						+ "\n";
			}
		} else if (filterDuplicationMap.size() > 0) {
			duplicationString = duplicationString
					+ "The following filter/option internal names are duplicated and will cause client problems:\n";
			final Enumeration e = filterDuplicationMap.keys();
			while (e.hasMoreElements()) {
				final String intName = (String) e.nextElement();
				duplicationString = duplicationString + "Filter " + intName
						+ " in dataset " + dsConfig.getDataset() + "\n";
			}
		}
		/*
		 * if (duplicatedAttString != ""){ int choice =
		 * JOptionPane.showConfirmDialog(null, duplicatedAttString, "Remove
		 * duplicates?", JOptionPane.YES_NO_OPTION); if (choice == 0){
		 * Enumeration enum = attributeDuplicationTCFieldMap.keys(); while
		 * (enum.hasMoreElements()){ String testName = (String)
		 * enum.nextElement(); int first = 0; for (int j = 0; j < apages.length;
		 * j++){ apage = apages[j]; List testAtts =
		 * apage.getAllAttributeDescriptions(); for (Iterator iter =
		 * testAtts.iterator(); iter.hasNext();) { AttributeDescription testAD =
		 * (AttributeDescription) iter.next(); if
		 * (!((testAD.getTableConstraint()
		 * +"."+testAD.getField()).equals(testName))) continue; if (first != 0){
		 * // TODO Remove attribute } first++; } } } } else{
		 * JOptionPane.showMessageDialog(null, "No Export performed", "ERROR",
		 * 0); return;//no export performed } }
		 */

		if (duplicationString != "") {
			final int choice = JOptionPane.showConfirmDialog(null,
					duplicationString, "Make Unique?",
					JOptionPane.YES_NO_OPTION);
			// make unique code
			if (choice == 0) {
				System.out.println("MAKING UNIQUE");
				String testName;
				final int i;

				Enumeration e = attributeDuplicationMap.keys();
				while (e.hasMoreElements()) {
					testName = (String) e.nextElement();

					int first = 0;
					for (int j = 0; j < apages.length; j++) {
						apage = apages[j];
						if (apage.getHidden() != null
								&& apage.getHidden().equals("true"))
							continue;

						List testAtts = new ArrayList();
						testAtts = apage.getAllAttributeDescriptions();
						for (final Iterator iter = testAtts.iterator(); iter
								.hasNext();) {
							final Object testAtt = iter.next();
							final AttributeDescription testAD = (AttributeDescription) testAtt;
							if (testAD.getHidden() != null
									&& testAD.getHidden().equals("true"))
								continue;

							// if
							// (testAD.getInternalName().matches("\\w+\\.\\w+")
							// ||
							// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
							if (testAD.getPointerDataset() != null
									&& !"".equals(testAD.getPointerDataset()))
								continue;// placeholder atts can be
							// duplicated

							if (testAD.getInternalName().equals(testName)) {
								if (first != 0) {
									testAD.setInternalName(testName + "_"
											+ first);
									doc = MartEditor.getDatasetConfigXMLUtils()
											.getDocumentForDatasetConfig(
													dsConfig);

									// continue OUTER;
								}
								first++;

							}
						}
					}
				}

				e = attributeListDuplicationMap.keys();
				while (e.hasMoreElements()) {
					testName = (String) e.nextElement();

					int first = 0;
					for (int j = 0; j < apages.length; j++) {
						apage = apages[j];
						if (apage.getHidden() != null
								&& apage.getHidden().equals("true"))
							continue;

						List testAtts = new ArrayList();
						testAtts = apage.getAllAttributeLists();
						for (final Iterator iter = testAtts.iterator(); iter
								.hasNext();) {
							final Object testAtt = iter.next();
							final AttributeList testAD = (AttributeList) testAtt;
							if (testAD.getHidden() != null
									&& testAD.getHidden().equals("true"))
								continue;

							if (testAD.getInternalName().equals(testName)) {
								if (first != 0) {
									testAD.setInternalName(testName + "_"
											+ first);
									doc = MartEditor.getDatasetConfigXMLUtils()
											.getDocumentForDatasetConfig(
													dsConfig);

									// continue OUTER;
								}
								first++;

							}
						}
					}
				}

				e = filterDuplicationMap.keys();
				while (e.hasMoreElements()) {
					testName = (String) e.nextElement();
					int first = 0;
					for (int j = 0; j < fpages.length; j++) {
						fpage = fpages[j];
						if (fpage.getHidden() != null
								&& fpage.getHidden().equals("true"))
							continue;

						List testAtts = new ArrayList();
						testAtts = fpage.getAllFilterDescriptions();
						for (final Iterator iter = testAtts.iterator(); iter
								.hasNext();) {
							final Object testAtt = iter.next();
							final FilterDescription testAD = (FilterDescription) testAtt;
							if (testAD.getHidden() != null
									&& testAD.getHidden().equals("true"))
								continue;
							// if
							// (testAD.getInternalName().matches("\\w+\\.\\w+")
							// ||
							// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
							if (testAD.getPointerDataset() != null
									&& !"".equals(testAD.getPointerDataset()))
								continue;// placeholder atts can be
							// duplicated

							if (testAD.getInternalName().equals(testName)) {
								if (first != 0) {
									testAD.setInternalName(testName + "_"
											+ first);
									doc = MartEditor.getDatasetConfigXMLUtils()
											.getDocumentForDatasetConfig(
													dsConfig);
									// continue OUTER;
								}
								first++;
							}
						}
					}
				}
			} else {
				JOptionPane.showMessageDialog(null, "No Export performed",
						"ERROR", 0);
				return;// no export performed
			}
		}

		int rowsupdated = 0;

		// if (compress)
		rowsupdated = this.storeCompressedXML(user, internalName, displayName,
				dataset, description, doc, type, visible, version, datasetID,
				martUsers, interfaces, dsConfig);
		// else
		// rowsupdated = storeUncompressedXML(user, internalName, displayName,
		// dataset, description, datasetID, doc);

		this.updateMartConfigForUser(user, this.getSchema()[0]);
		if (rowsupdated < 1)
			if (this.logger.isLoggable(Level.WARNING))
				this.logger.warning("Warning, xml for " + internalName + ", "
						+ displayName + " not stored"); // throw an exception?
	}

	private void generateTemplateXML(final DatasetConfig dsConfig)
			throws ConfigurationException {
		final DatasetConfig templateConfig = new DatasetConfig(dsConfig, true,
				false);
		final String template = dsConfig.getTemplate();

		if (!this.uniqueCheckConfig(templateConfig))
			return;

		final List filterDescriptions = templateConfig
				.getAllFilterDescriptions();
		for (int i = 0; i < filterDescriptions.size(); i++) {
			final FilterDescription fd = (FilterDescription) filterDescriptions
					.get(i);
			// make sure placeholders only have an internalName
			final String internalName = fd.getInternalName();
			// if (testAD.getInternalName().matches("\\w+\\.\\w+") ||
			// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
			if (fd.getPointerDataset() != null
					&& !"".equals(fd.getPointerDataset())) {
				fd.setTableConstraint("");
				fd.setField("");
				fd.setDisplayName("");
				fd.setDescription("");
				fd.setKey("");
				fd.setLegalQualifiers("");
			}

			// if (fd.getInternalName().matches(".+\\..+")){
			// fd.addDynamicFilterContent(new
			// DynamicFilterContent(dsConfig.getDataset(),
			// "",fd.getPointerDataset(),fd.getPointerInterface(),fd.getPointerFilter()));
			// }
			// change internal placeholders to template.filter
			// if (fd.getInternalName().matches(dsConfig.getDataset()+"\\..+")){
			// fd.setInternalName(fd.getInternalName().replaceFirst(dsConfig.getDataset(),template));
			// }

			// hack to fix broken types in existing XML as
			// updateConfigToTemplate uses list type to specify options
			if (fd.getType() != null && fd.getType().equals("list")
					&& !(fd.getOptions().length > 0))
				fd.setType("text");

			// remove dataset part from tableConstraint if present
			// if (fd.getTableConstraint() != null &&
			// !fd.getTableConstraint().equals("")
			// && !fd.getTableConstraint().equals("main"))
			// fd.setTableConstraint(fd.getTableConstraint().split("__")[1]+"__"+fd.getTableConstraint().split("__")[2]);
			// fd.setOtherFilters("");

			/*
			 * Option[] ops = fd.getOptions(); for (int j = 0; j < ops.length;
			 * j++){ Option op = ops[j]; // if a value option remove it if
			 * (op.getTableConstraint() == null){ // fd.removeOption(op);
			 * continue; } // if a filter option remove dataset part from
			 * tableConstraint //if (!op.getTableConstraint().equals("main")) //
			 * op
			 * .setTableConstraint(op.getTableConstraint().split("__")[1]+"__"+
			 * op.getTableConstraint().split("__")[2]);
			 * //op.setOtherFilters(""); }
			 */
		}

		final List attributeDescriptions = templateConfig
				.getAllAttributeDescriptions();
		for (int i = 0; i < attributeDescriptions.size(); i++) {
			final AttributeDescription ad = (AttributeDescription) attributeDescriptions
					.get(i);
			final String internalName = ad.getInternalName();
			// if (testAD.getInternalName().matches("\\w+\\.\\w+") ||
			// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
			if (ad.getPointerDataset() != null
					&& !"".equals(ad.getPointerDataset())) {

				// ad.addDynamicAttributeContent(new
				// DynamicAttributeContent(dsConfig.getDataset(),"",ad.getPointerDataset(),
				// ad.getPointerInterface(),ad.getPointerAttribute(),ad.getPointerFilter()));

				ad.setTableConstraint("");
				ad.setField("");
				ad.setDisplayName("");
				ad.setDescription("");
				ad.setKey("");
				ad.setLinkoutURL("");
			}
			// change internal placeholders to template.placeholder
			// if (ad.getInternalName().matches(dsConfig.getDataset()+"\\..+")){
			// ad.setInternalName(ad.getInternalName().replaceFirst(dsConfig.getDataset(),template));
			// }
			// if (ad.getTableConstraint() != null &&
			// !ad.getTableConstraint().equals("") &&
			// !ad.getTableConstraint().equals("main"))
			// ad.setTableConstraint(ad.getTableConstraint().split("__")[1]+"__"+ad.getTableConstraint().split("__")[2]);

			// ad.setLinkoutURL("");
		}

		// Exportable[] exps = templateConfig.getExportables();
		// for (int i = 0; i < exps.length; i++){
		// exps[i].setLinkVersion("");
		// }

		// Importable[] imps = templateConfig.getImportables();
		// for (int i = 0; i < imps.length; i++){
		// imps[i].setLinkVersion("");
		// }

		this.storeTemplateXML(templateConfig, template);

	}

	public void storeTemplateXML(final DatasetConfig templateConfig,
			final String template) throws ConfigurationException {
		if (this.readonly)
			throw new ConfigurationException(
					"Cannot store config into read-only database");
		Connection conn = null;
		try {
			conn = this.dsource.getConnection();

			this.createMetaTables("");// make sure tables exist

			String sql = "DELETE FROM " + this.getSchema()[0] + "."
					+ this.MARTTEMPLATEDMTABLE + " WHERE template='" + template
					+ "'";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.executeUpdate();

			final Document doc = MartEditor.getDatasetConfigXMLUtils()
					.getDocumentForDatasetConfig(templateConfig);

			final MessageDigest md5digest = MessageDigest
					.getInstance(DatasetConfigXMLUtils.DEFAULTDIGESTALGORITHM);
			final ByteArrayOutputStream bout = new ByteArrayOutputStream();
			final GZIPOutputStream gout = new GZIPOutputStream(bout);
			final DigestOutputStream out = new DigestOutputStream(gout,
					md5digest);
			final XMLOutputter xout = new XMLOutputter(
					org.jdom.output.Format.getRawFormat());
			xout.output(doc, out);
			gout.finish();
			final byte[] xml = bout.toByteArray();

			if (this.dsource.getJdbcDriverClassName().indexOf("oracle") >= 0)
				conn.setAutoCommit(false);
			sql = "INSERT INTO " + this.getSchema()[0] + "."
					+ this.MARTTEMPLATEDMTABLE
					+ " (template, compressed_xml) values (?, ?)";
			ps = conn.prepareStatement(sql);
			ps.setString(1, template);
			if (this.dsource.getJdbcDriverClassName().indexOf("oracle") >= 0)
				ps.setBlob(2, BLOB.empty_lob());// oracle hack
			else
				ps.setBinaryStream(2, new ByteArrayInputStream(xml), xml.length);
			ps.executeUpdate();
			ps.close();

			if (this.dsource.getJdbcDriverClassName().indexOf("oracle") >= 0) {
				// oracle hack
				final String oraclehackSQL = "SELECT compressed_xml FROM "
						+ this.getSchema()[0] + "." + this.MARTTEMPLATEDMTABLE
						+ " WHERE template = ? FOR UPDATE";
				final PreparedStatement ohack = conn
						.prepareStatement(oraclehackSQL);
				ohack.setString(1, template);
				final ResultSet rs = ohack.executeQuery();

				if (rs.next()) {
					final BLOB blob = (BLOB) rs.getBlob(1);
					final OutputStream blobout = blob.getBinaryOutputStream();
					blobout.write(xml);
					blobout.close();
				}
				ohack.close();
				rs.close();
				conn.commit();
			}

			conn.close();
		} catch (final IOException e) {
			throw new ConfigurationException(
					"Caught IOException writing out xml to OutputStream: "
							+ e.getMessage());
		} catch (final SQLException e) {
			final ConfigurationException ce = new ConfigurationException(
					"Caught SQLException updating xml for " + e.getMessage());
			ce.initCause(e);
			throw ce;
		} catch (final NoSuchAlgorithmException e) {
			throw new ConfigurationException(
					"Caught NoSuchAlgorithmException updating xml for " + ": "
							+ e.getMessage(), e);
		}

		finally {
			DetailedDataSource.close(conn);
		}
	}

	public boolean uniqueCheckConfig(final DatasetConfig config) {
		// first get rid of any duplicated atts/filters in terms of
		// TableConstraint and Fields
		final Hashtable descriptionsTCFieldMap = new Hashtable();// atts
		// should
		// have a
		// unique TC
		// and field
		final Hashtable attributeDuplicationTCFieldMap = new Hashtable();
		String duplicatedAttString = "";

		final List attributeDescriptions = config.getAllAttributeDescriptions();
		for (int i = 0; i < attributeDescriptions.size(); i++) {
			final AttributeDescription ad = (AttributeDescription) attributeDescriptions
					.get(i);
			// if (testAD.getInternalName().matches("\\w+\\.\\w+") ||
			// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
			if (ad.getPointerDataset() != null
					&& !"".equals(ad.getPointerDataset())
					|| ad.getTableConstraint() == null
					|| ad.getTableConstraint().equals(""))
				continue;// placeholder atts or non-tableSet ones eg
			// GenomicSequence can be duplicated

			// don't allow any duplication of TC and field, even for hidden atts
			String key = ad.getTableConstraint() + "." + ad.getField();
			if (descriptionsTCFieldMap.containsKey(key)) {
				final AttributePage apage = config.getPageForAttribute(ad
						.getInternalName());
				final AttributeGroup agroup = config.getGroupForAttribute(ad
						.getInternalName());
				final AttributeCollection acollection = config
						.getCollectionForAttribute(ad.getInternalName());
				System.out.println("Duplication of attribute table.column "
						+ key);
				attributeDuplicationTCFieldMap.put(
						key,
						apage.getInternalName() + "->"
								+ agroup.getInternalName() + "->"
								+ acollection.getInternalName() + "->"
								+ ad.getInternalName());
			}
			descriptionsTCFieldMap.put(key, "1");
		}

		if (attributeDuplicationTCFieldMap.size() > 0) {
			final Enumeration e = attributeDuplicationTCFieldMap.keys();
			while (e.hasMoreElements()) {
				final String intName = (String) e.nextElement();
				duplicatedAttString = duplicatedAttString + "Attribute for "
						+ intName + " at "
						+ attributeDuplicationTCFieldMap.get(intName) + "\n";
			}
		}

		if (duplicatedAttString != "") {
			JOptionPane
					.showMessageDialog(
							null,
							"Remove duplicates before generating template. Use internal placeholders if really need duplication:\n"
									+ duplicatedAttString, "ERROR", 0);
			return false;// no export of template
		}

		// first get rid of any duplicated atts/filters in terms of
		// TableConstraint and Fields
		final Hashtable filterDescriptionsTCFieldMap = new Hashtable();// atts
		// should
		// have
		// a
		// unique
		// TC
		// and
		// field
		final Hashtable filterDuplicationTCFieldMap = new Hashtable();
		String duplicatedFilterString = "";

		final List filterDescriptions = config.getAllFilterDescriptions();
		for (int i = 0; i < filterDescriptions.size(); i++) {
			final FilterDescription fd = (FilterDescription) filterDescriptions
					.get(i);
			// if (testAD.getInternalName().matches("\\w+\\.\\w+") ||
			// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
			if (fd.getPointerDataset() != null
					&& !"".equals(fd.getPointerDataset())
					|| fd.getTableConstraint() == null
					|| fd.getTableConstraint().equals(""))
				continue;// placeholder atts can be duplicated

			if (fd.getTableConstraint() == null && fd.getField() == null) {
				if (filterDescriptionsTCFieldMap.containsKey(fd
						.getInternalName()))
					continue;
				filterDescriptionsTCFieldMap.put(fd.getInternalName(), "1");// hack
				// to
				// stop
				// drop
				// down
				// lists
				// being
				// redone
				final Option[] ops = fd.getOptions();
				for (int j = 0; j < ops.length; j++) {
					// fd = new FilterDescription(ops[j]);
					// don't allow any duplication of TC and field, even for
					// hidden atts
					String optionKey = ops[j].getTableConstraint() + "."
							+ ops[j].getField() + "." + ops[j].getQualifier();
					if (filterDescriptionsTCFieldMap.containsKey(optionKey)) {
						final FilterPage fpage = config.getPageForFilter(ops[j]
								.getInternalName());
						final FilterGroup fgroup = config
								.getGroupForFilter(ops[j].getInternalName());
						final FilterCollection fcollection = config
								.getCollectionForFilter(ops[j]
										.getInternalName());
						System.out
								.println("Duplication of option " + optionKey);
						filterDuplicationTCFieldMap.put(
								optionKey,
								fpage.getInternalName() + "->"
										+ fgroup.getInternalName() + "->"
										+ fcollection.getInternalName() + "->"
										+ ops[j].getInternalName());
					}
					filterDescriptionsTCFieldMap.put(optionKey, "1");
				}
				continue;
			}
			// don't allow any duplication of TC and field, even for hidden atts
			String key = fd.getTableConstraint() + "." + fd.getField() + "."
					+ fd.getQualifier();
			if (filterDescriptionsTCFieldMap.containsKey(key)) {
				final FilterPage fpage = config.getPageForFilter(fd
						.getInternalName());
				final FilterGroup fgroup = config.getGroupForFilter(fd
						.getInternalName());
				final FilterCollection fcollection = config
						.getCollectionForFilter(fd.getInternalName());
				filterDuplicationTCFieldMap.put(
						key,
						fpage.getInternalName() + "->"
								+ fgroup.getInternalName() + "->"
								+ fcollection.getInternalName() + "->"
								+ fd.getInternalName());
				System.out.println("Duplicate filter table.field.qualifier "
						+ key);
			}
			filterDescriptionsTCFieldMap.put(key, "1");
		}

		if (filterDuplicationTCFieldMap.size() > 0) {
			final Enumeration e = filterDuplicationTCFieldMap.keys();
			while (e.hasMoreElements()) {
				final String intName = (String) e.nextElement();
				duplicatedFilterString = duplicatedFilterString + "Filter for "
						+ intName + " at "
						+ filterDuplicationTCFieldMap.get(intName) + "\n";
			}
		}

		if (duplicatedFilterString != "") {
			JOptionPane
					.showMessageDialog(
							null,
							"Remove duplicates before generating template. Use internal placeholders if really need duplication:\n"
									+ duplicatedFilterString, "ERROR", 0);
			return false;// no export of template
		}

		// do validation of template before store it and use it to update
		// dataset configs

		String duplicationString = "";
		final String filterDuplicationString = "";
		final String brokenString = "";
		String spaceErrors = "";
		String brokenFields = "";
		final Hashtable attributeDuplicationMap = new Hashtable();
		final Hashtable attributeListDuplicationMap = new Hashtable();
		final Hashtable filterDuplicationMap = new Hashtable();

		// check uniqueness of internal names per page
		final AttributePage[] apages = config.getAttributePages();
		AttributePage apage;
		final String testInternalName;

		for (int k = 0; k < apages.length; k++) {
			apage = apages[k];
			final Hashtable descriptionsMap = new Hashtable();
			if (apage.getHidden() != null && apage.getHidden().equals("true"))
				continue;

			List testGroups = new ArrayList();
			testGroups = apage.getAttributeGroups();
			for (final Iterator groupIter = testGroups.iterator(); groupIter
					.hasNext();) {
				final AttributeGroup testGroup = (AttributeGroup) groupIter
						.next();
				// List testColls = new ArrayList();
				final AttributeCollection[] testColls = testGroup
						.getAttributeCollections();
				for (int col = 0; col < testColls.length; col++) {
					final AttributeCollection testColl = testColls[col];

					if (testColl.getInternalName().matches("\\w+\\s+\\w+"))
						spaceErrors = spaceErrors + "AttributeCollection "
								+ testColl.getInternalName() + " in dataset "
								+ config.getDataset() + "\n";
					List testAtts = new ArrayList();

					testAtts = testColl.getAttributeDescriptions();

					for (final Iterator iter = testAtts.iterator(); iter
							.hasNext();) {
						final Object testAtt = iter.next();
						final AttributeDescription testAD = (AttributeDescription) testAtt;
						if (testAD.getHidden() != null
								&& testAD.getHidden().equals("true"))
							continue;
						// if (testAD.getInternalName().matches("\\w+\\.\\w+")
						// ||
						// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
						if (testAD.getPointerDataset() != null
								&& !"".equals(testAD.getPointerDataset()))
							continue;// placeholder atts can be duplicated

						if (testAD.getInternalName().matches("\\w+\\s+\\w+"))
							spaceErrors = spaceErrors + "AttributeDescription "
									+ testAD.getInternalName() + " in dataset "
									+ config.getDataset() + "\n";
						if (descriptionsMap.containsKey(testAD
								.getInternalName()))
							// duplicationString = duplicationString +
							// "Attribute " + testAD.getInternalName() + " in
							// dataset " + config.getDataset() +
							// " and page " + apage.getInternalName() + "\n";
							attributeDuplicationMap.put(
									testAD.getInternalName(),
									config.getDataset());
						// brokenDatasets.add(config.getDataset());
						descriptionsMap.put(testAD.getInternalName(), "1");

						if (config.getType().equals("GenomicSequence"))
							continue;// no point in checking fields

						// test has all its fields defined - if not add a
						// message to brokenString
						if (testAD.getInternalName() == null
								|| testAD.getInternalName().equals("")
								|| testAD.getField() == null
								|| testAD.getField().equals("")
								|| testAD.getTableConstraint() == null
								|| testAD.getTableConstraint().equals("")
								|| config.getVisible() != null
								&& config.getVisible().equals("1")
								&& (testAD.getKey() == null || testAD.getKey()
										.equals("")))
							brokenFields = brokenFields + "Attribute "
									+ testAD.getInternalName() + " in dataset "
									+ config.getDataset() + ", page "
									+ apage.getInternalName() + ", group "
									+ testGroup.getInternalName()
									+ ", collection "
									+ testColl.getInternalName() + "\n";

					}

					descriptionsMap.clear();
					testAtts = testColl.getAttributeLists();

					for (final Iterator iter = testAtts.iterator(); iter
							.hasNext();) {
						final Object testAtt = iter.next();
						final AttributeList testAD = (AttributeList) testAtt;
						if (testAD.getHidden() != null
								&& testAD.getHidden().equals("true"))
							continue;

						if (testAD.getInternalName().matches("\\w+\\s+\\w+"))
							spaceErrors = spaceErrors + "AttributeList "
									+ testAD.getInternalName() + " in dataset "
									+ config.getDataset() + "\n";
						if (descriptionsMap.containsKey(testAD
								.getInternalName()))
							// duplicationString = duplicationString +
							// "Attribute " + testAD.getInternalName() + " in
							// dataset " + config.getDataset() +
							// " and page " + apage.getInternalName() + "\n";
							attributeListDuplicationMap.put(
									testAD.getInternalName(),
									config.getDataset());
						// brokenDatasets.add(config.getDataset());
						descriptionsMap.put(testAD.getInternalName(), "1");

						if (config.getType().equals("GenomicSequence"))
							continue;// no point in checking fields

						// test has all its fields defined - if not add a
						// message to brokenString
						if (testAD.getInternalName() == null
								|| testAD.getInternalName().equals(""))
							brokenFields = brokenFields + "AttributeList "
									+ testAD.getInternalName() + " in dataset "
									+ config.getDataset() + ", page "
									+ apage.getInternalName() + ", group "
									+ testGroup.getInternalName()
									+ ", collection "
									+ testColl.getInternalName() + "\n";

					}
				}
			}
		}
		// repeat for filter pages
		final FilterPage[] fpages = config.getFilterPages();
		FilterPage fpage;
		for (int k = 0; k < fpages.length; k++) {
			fpage = fpages[k];
			final Hashtable descriptionsMap = new Hashtable();
			if (fpage.getHidden() != null && fpage.getHidden().equals("true"))
				continue;

			List testGroups = new ArrayList();
			testGroups = fpage.getFilterGroups();
			for (final Iterator groupIter = testGroups.iterator(); groupIter
					.hasNext();) {
				final FilterGroup testGroup = (FilterGroup) groupIter.next();
				// List testColls = new ArrayList();
				final FilterCollection[] testColls = testGroup
						.getFilterCollections();
				for (int col = 0; col < testColls.length; col++) {
					final FilterCollection testColl = testColls[col];

					if (testColl.getInternalName().matches("\\w+\\s+\\w+"))
						spaceErrors = spaceErrors + "FilterCollection "
								+ testColl.getInternalName() + " in dataset "
								+ config.getDataset() + "\n";
					List testAtts = new ArrayList();
					testAtts = testColl.getFilterDescriptions();// ? OPTIONS

					for (final Iterator iter = testAtts.iterator(); iter
							.hasNext();) {
						final Object testAtt = iter.next();
						final FilterDescription testAD = (FilterDescription) testAtt;
						if (testAD.getHidden() != null
								&& testAD.getHidden().equals("true"))
							continue;
						// if (testAD.getInternalName().matches("\\w+\\.\\w+")
						// ||
						// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
						if (testAD.getPointerDataset() != null
								&& !"".equals(testAD.getPointerDataset()))
							continue;

						if (testAD.getInternalName().matches("\\w+\\s+\\w+"))
							spaceErrors = spaceErrors + "FilterDescription "
									+ testAD.getInternalName() + " in dataset "
									+ config.getDataset() + "\n";
						if (descriptionsMap.containsKey(testAD
								.getInternalName())) {
							// duplicationString = duplicationString +
							// testAD.getInternalName() + " in dataset " +
							// config.getDataset() + "\n";
							// filterDuplicationString = filterDuplicationString
							// + "Filter " + testAD.getInternalName() + " in
							// dataset " + config.getDataset() +
							// " and page " + fpage.getInternalName() + "\n";
							filterDuplicationMap.put(testAD.getInternalName(),
									config.getDataset());
							// brokenDatasets.add(config.getDataset());
							continue;// to stop options also being assessed
						}

						descriptionsMap.put(testAD.getInternalName(), "1");

						if (config.getType().equals("GenomicSequence"))
							continue;// no point in checking fields

						// test has all its fields defined - if not add a
						// message to brokenString
						// only do for non-filter option filters
						if ((testAD.getFilterList() == null || testAD
								.getFilterList().equals(""))
								&& (testAD.getOptions().length == 0 || testAD
										.getOptions()[0].getField() == null)
								&& (testAD.getInternalName() == null
										|| testAD.getInternalName().equals("")
										|| testAD.getField() == null
										|| testAD.getField().equals("")
										|| testAD.getTableConstraint() == null
										|| testAD.getTableConstraint().equals(
												"")
										|| config.getVisible() != null
										&& config.getVisible().equals("1")
										&& (testAD.getKey() == null || testAD
												.getKey().equals(""))
										|| testAD.getQualifier() == null || testAD
										.getQualifier().equals("")))
							brokenFields = brokenFields + "Filter "
									+ testAD.getInternalName() + " in dataset "
									+ config.getDataset() + ", page "
									+ fpage.getInternalName() + ", group "
									+ testGroup.getInternalName()
									+ ", collection "
									+ testColl.getInternalName() + "\n";

						// do options as well
						final Option[] ops = testAD.getOptions();
						if (ops.length > 0 && ops[0].getType() != null
								&& !ops[0].getType().equals(""))
							for (int l = 0; l < ops.length; l++) {
								final Option op = ops[l];
								if (op.getHidden() != null
										&& op.getHidden().equals("true"))
									continue;
								if (descriptionsMap.containsKey(op
										.getInternalName()))
									// filterDuplicationString =
									// filterDuplicationString +
									// op.getInternalName() + " in dataset " +
									// config.getDataset() + "\n";
									filterDuplicationMap.put(
											op.getInternalName(),
											config.getDataset());
								// brokenDatasets.add(config.getDataset());
								descriptionsMap.put(op.getInternalName(), "1");
							}
					}
				}
			}
		}

		if (spaceErrors != "")
			JOptionPane.showMessageDialog(null,
					"The following internal names contain spaces:\n"
							+ spaceErrors, "ERROR", 0);

		if (brokenFields != "" && config.getType().equals("TableSet"))
			JOptionPane.showMessageDialog(null,
					"The following may not contain the required fields:\n"
							+ brokenFields, "ERROR", 0);

		if (brokenString != "")
			JOptionPane.showMessageDialog(null,
					"The following are no longer defined in the database\n"
							+ brokenString, "ERROR", 0);

		if (spaceErrors != "" || brokenFields != ""
				&& config.getType().equals("TableSet") || brokenString != "")
			return false;// no export performed

		if (attributeDuplicationMap.size() > 0) {
			duplicationString = "The following attribute internal names are duplicated and will cause client problems:\n";
			final Enumeration e = attributeDuplicationMap.keys();
			while (e.hasMoreElements()) {
				final String intName = (String) e.nextElement();
				duplicationString = duplicationString + "Attribute " + intName
						+ " in dataset " + attributeDuplicationMap.get(intName)
						+ "\n";
			}
		} else if (attributeListDuplicationMap.size() > 0) {
			duplicationString = "The following attributeList internal names are duplicated and will cause client problems:\n";
			final Enumeration e = attributeListDuplicationMap.keys();
			while (e.hasMoreElements()) {
				final String intName = (String) e.nextElement();
				duplicationString = duplicationString + "AttributeList "
						+ intName + " in dataset "
						+ attributeDuplicationMap.get(intName) + "\n";
			}
		} else if (filterDuplicationMap.size() > 0) {
			duplicationString = duplicationString
					+ "The following filter/option internal names are duplicated and will cause client problems:\n";
			final Enumeration e = filterDuplicationMap.keys();
			while (e.hasMoreElements()) {
				final String intName = (String) e.nextElement();
				duplicationString = duplicationString + "Filter " + intName
						+ " in dataset " + filterDuplicationMap.get(intName)
						+ "\n";
			}
		}

		if (duplicationString != "") {
			JOptionPane.showMessageDialog(null, duplicationString, "ERROR", 0);
			return false;
			/*
			 * int choice = JOptionPane.showConfirmDialog(null,
			 * duplicationString, "Make Unique?", JOptionPane.YES_NO_OPTION); //
			 * make unique code if (choice == 0){ System.out.println("MAKING
			 * UNIQUE"); String testName, datasetName; int i;
			 * 
			 * adaptor= new
			 * DatabaseDSConfigAdaptor(MartEditor.getDetailedDataSource(),user,
			 * martUser, true, false, true);
			 * 
			 * String[] dsList = new String[brokenDatasets.size()];
			 * brokenDatasets.toArray(dsList);
			 * 
			 * for (i = 0; i < dsList.length; i++){ config =
			 * adaptor.getDatasetConfigByDatasetInternalName
			 * (dsList[i],"default"); // convert config to latest version using
			 * xslt config =
			 * MartEditor.getDatabaseDatasetConfigUtils().getUpdatedConfig
			 * (config); dbutils.storeDatasetConfiguration( user,
			 * config.getInternalName(), config.getDisplayName(),
			 * config.getDataset(), config.getDescription(),
			 * MartEditor.getDatasetConfigXMLUtils
			 * ().getDocumentForDatasetConfig(config), true, config.getType(),
			 * config.getVisible(), config.getVersion(), config.getDatasetID(),
			 * config.getMartUsers(), config.getInterfaces(), config); } } else{
			 * JOptionPane.showMessageDialog(null, "No Export performed",
			 * "ERROR", 0); return false;//no export performed }
			 */
		}

		return true;
	}

	public void updateConfigsToTemplate(final String template,
			final DatasetConfig templateConfig) throws ConfigurationException {
		// System.err.println("CALLED: updateConfigsToTemplate
		// "+templateConfig.getDisplayName()+" "+template);

		this.storeTemplateXML(templateConfig, template);

		// extract all the dataset configs matching template and call
		// updateConfigToTemplate storing each one as returned
		final DSConfigAdaptor adaptor = new DatabaseDSConfigAdaptor(
				MartEditor.getDetailedDataSource(), MartEditor.getUser(),
				MartEditor.getMartUser(), true, false, true, true);

		final String[] dsNames = templateConfig.getDynamicDatasetNames();
		final List goodIds = new ArrayList();
		for (int i = 0; i < dsNames.length; i++) {
			final String dsName = dsNames[i];
			DatasetConfig dsConfig = null;
			final DatasetConfigIterator configs = adaptor
					.getDatasetConfigsByDataset(dsName);
			if (configs.hasNext())
				dsConfig = (DatasetConfig) configs.next();

			if (dsConfig == null) {
				templateConfig.removeDynamicDataset(templateConfig
						.getDynamicDataset(dsName));
				continue;
			}

			dsConfig = this.updateConfigToTemplate(dsConfig, templateConfig);
			goodIds.add(dsConfig.getDatasetID());

			System.out.println("STORING WITH " + dsConfig.getDisplayName()
					+ ":" + dsConfig.getVersion());
			this.storeDatasetConfiguration(MartEditor.getUser(), dsConfig
					.getInternalName(), dsConfig.getDisplayName(), dsConfig
					.getDataset(), dsConfig.getDescription(),
					MartEditor.getDatasetConfigXMLUtils()
							.getDocumentForDatasetConfig(dsConfig), true,
					dsConfig.getType(), dsConfig.getVisible(), dsConfig
							.getVersion(), dsConfig.getDatasetID(), dsConfig
							.getMartUsers(), dsConfig.getInterfaces(), dsConfig);
		}

		// Dump all datasets referring to this template that don't do so
		// anymore.
		String sql = "SELECT b.dataset, a.dataset_id_key FROM "
				+ this.getSchema()[0] + "." + this.MARTTEMPLATEMAINTABLE
				+ " a INNER JOIN " + this.getSchema()[0] + "."
				+ this.BASEMETATABLE
				+ " b ON a.dataset_id_key=b.dataset_id_key WHERE a.template='"
				+ template + "'";
		if (goodIds.size() > 0) {
			sql += " AND a.dataset_id_key NOT IN (";
			for (Iterator i = goodIds.iterator(); i.hasNext();) {
				final String id = (String) i.next();
				sql += id;
				if (i.hasNext())
					sql += ",";
			}
			sql += ")";
		}
		final List dropIds = new ArrayList();
		try {
			Connection conn = this.dsource.getConnection();
			PreparedStatement ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				final String dsName = rs.getString(1);
				final String dsId = rs.getString(2);
				dropIds.add(new String[] { dsName, dsId });
			}
		} catch (final SQLException e) {
			throw new ConfigurationException(e);
		}

		// Drop bad things.
		for (final Iterator i = dropIds.iterator(); i.hasNext();) {
			final String[] bits = (String[]) i.next();
			this.deleteDatasetConfigsForDatasetID(bits[0], bits[1],
					MartEditor.getUser(), null);
		}

		// Repeat.
		this.storeTemplateXML(templateConfig, template);

	}

	/*
	 * private void updateAttributeToTemplate(AttributeDescription
	 * configAtt,DatasetConfig dsConfig, DatasetConfig templateConfig) throws
	 * ConfigurationException{ // Don't bother with placeholders. if
	 * (configAtt.getTableConstraint().equals("") ||
	 * configAtt.getTableConstraint()==null ||
	 * (configAtt.getPointerDataset()!=null &&
	 * !"".equals(configAtt.getPointerDataset()))) return;
	 * 
	 * 
	 * String configAttName = configAtt.getInternalName(); String configAttTC;
	 * if (configAtt.getTableConstraint().equals("main")) configAttTC = "main";
	 * else configAttTC =
	 * configAtt.getTableConstraint().split("__")[1]+"__"+configAtt
	 * .getTableConstraint().split("__")[2];// template stores w/o the dataset
	 * part
	 * 
	 * String configAttField = configAtt.getField(); AttributePage configPage =
	 * dsConfig.getPageForAttribute(configAttName); String configPageName =
	 * configPage.getInternalName(); AttributeGroup configGroup =
	 * dsConfig.getGroupForAttribute(configAttName); String configGroupName =
	 * configGroup.getInternalName(); AttributeCollection configCollection =
	 * dsConfig.getCollectionForAttribute(configAttName); String
	 * configCollectionName = configCollection.getInternalName();
	 * 
	 * if
	 * (templateConfig.supportsAttributeDescription(configAttField,configAttTC
	 * )){ //System.out.println("1 - make sure dsConfig has same structure as
	 * templateConfig for this attribute:" //
	 * +configAtt.getInternalName()+":"+configAtt
	 * .getDisplayName()+":"+dsConfig.getDataset()); // remove att from old
	 * hierarchy in dsConfig
	 * configCollection.removeAttributeDescription(configAtt); if
	 * (!(configCollection.getAttributeDescriptions().size() > 0)){
	 * configGroup.removeAttributeCollection(configCollection); if
	 * (!(configGroup.getAttributeCollections().length > 0)){
	 * configPage.removeAttributeGroup(configGroup); if
	 * (!(configPage.getAttributeGroups().size() > 0)){
	 * dsConfig.removeAttributePage(configPage); } } } // need to make sure get
	 * right template attribute: if more than one exists take the one with
	 * matching // internalName as well AttributeDescription templateAttribute =
	 * templateConfig
	 * .getAttributeDescriptionByFieldNameTableConstraintInternalName
	 * (configAttField,configAttTC,configAttName);
	 * 
	 * //AttributeDescription templateAttribute =
	 * templateConfig.getAttributeDescriptionByFieldNameTableConstraint
	 * (configAttField,configAttTC); AttributePage templatePage =
	 * templateConfig.getPageForAttribute(templateAttribute.getInternalName());
	 * AttributeGroup templateGroup =
	 * templateConfig.getGroupForAttribute(templateAttribute.getInternalName());
	 * AttributeCollection templateCollection =
	 * templateConfig.getCollectionForAttribute
	 * (templateAttribute.getInternalName());
	 * 
	 * AttributeDescription configAttToAdd = new
	 * AttributeDescription(templateAttribute);
	 * configAttToAdd.setTableConstraint(configAtt.getTableConstraint());
	 * configAttToAdd.setField(configAtt.getField());
	 * 
	 * //if (configAtt.getLinkoutURL() != null &&
	 * !configAtt.getLinkoutURL().equals("")) //
	 * configAttToAdd.setLinkoutURL(configAtt.getLinkoutURL()); // dynamic
	 * content handling eg linkoutURL if (configAtt.getLinkoutURL() == null)
	 * configAtt.setLinkoutURL("");// avoids template problems if
	 * (templateAttribute.getLinkoutURL() == null)
	 * templateAttribute.setLinkoutURL("");// avoids template problems if
	 * (templateAttribute.getDynamicAttributeContents().size() > 0){ // already
	 * got multiple settings for this attribute if
	 * (!templateAttribute.containsDynamicAttributeContent
	 * (dsConfig.getDataset())) templateAttribute.addDynamicAttributeContent(new
	 * DynamicAttributeContent
	 * (dsConfig.getDataset(),configAtt.getLinkoutURL()));
	 * 
	 * configAttToAdd.setLinkoutURL(templateAttribute.
	 * getDynamicAttributeContentByInternalName
	 * (dsConfig.getDataset()).getLinkoutURL()); } else if
	 * (!configAtt.getLinkoutURL().equals(templateAttribute.getLinkoutURL())){
	 * // if this config has a different setting then start using dynamic
	 * objects // create dynamic objects - add one per existing dataset set to
	 * current template linkoutURL String[] datasetNames =
	 * getDatasetNamesForTemplate(dsConfig.getTemplate()); // if only one ds, do
	 * a straight copy. if (datasetNames.length<2) {
	 * configAttToAdd.setLinkoutURL(templateAttribute.getLinkoutURL()); } else {
	 * for (int j = 0; j < datasetNames.length; j++){ String datasetName =
	 * datasetNames[j]; if (datasetName.equals(dsConfig.getDataset())) continue;
	 * templateAttribute.addDynamicAttributeContent(new
	 * DynamicAttributeContent(datasetName,templateAttribute.getLinkoutURL()));
	 * } templateAttribute.setLinkoutURL("MULTI");
	 * 
	 * templateAttribute.addDynamicAttributeContent(new
	 * DynamicAttributeContent(dsConfig
	 * .getDataset(),configAtt.getLinkoutURL()));
	 * configAttToAdd.setLinkoutURL(templateAttribute
	 * .getDynamicAttributeContentByInternalName
	 * (dsConfig.getDataset()).getLinkoutURL()); } }
	 * 
	 * AttributePage dsConfigPage =
	 * dsConfig.getAttributePageByInternalName(templatePage.getInternalName());
	 * if (dsConfigPage == null){ dsConfigPage = new
	 * AttributePage(templatePage.getInternalName
	 * (),templatePage.getDisplayName(), templatePage.getDescription(),
	 * templatePage.getOutFormats(),templatePage.getMaxSelectString());
	 * AttributeGroup dsConfigGroup = new
	 * AttributeGroup(templateGroup.getInternalName(),
	 * templateGroup.getDisplayName(), templateGroup.getDescription(),"");
	 * AttributeCollection dsConfigCollection = new
	 * AttributeCollection(templateCollection.getInternalName(), "",
	 * templateCollection.getDisplayName(),templateCollection.getDescription());
	 * 
	 * if (templatePage.getHidden() != null)
	 * dsConfigPage.setHidden(templatePage.getHidden()); if
	 * (templatePage.getDisplay() != null)
	 * dsConfigPage.setDisplay(templatePage.getDisplay()); if
	 * (templateGroup.getHidden() != null)
	 * dsConfigGroup.setHidden(templateGroup.getHidden()); if
	 * (templateGroup.getMaxSelectString() != null)
	 * dsConfigGroup.setMaxSelect(templateGroup.getMaxSelectString()); if
	 * (templateCollection.getMaxSelectString() != null)
	 * dsConfigCollection.setMaxSelect(templateCollection.getMaxSelectString());
	 * if (templateCollection.getHidden() != null)
	 * dsConfigCollection.setHidden(templateCollection.getHidden()); if
	 * (templateAttribute.getHidden() != null)
	 * configAttToAdd.setHidden(templateAttribute.getHidden()); if
	 * (configAtt.getHidden() != null && configAtt.getHidden().equals("true"))
	 * configAttToAdd.setHidden("true");
	 * 
	 * dsConfig.addAttributePage(dsConfigPage);
	 * dsConfigPage.addAttributeGroup(dsConfigGroup);
	 * dsConfigGroup.addAttributeCollection(dsConfigCollection);
	 * dsConfigCollection.addAttributeDescription(configAttToAdd); } else{ //
	 * make sure dsConfigPage has correct displayName etc if
	 * (templatePage.getDisplayName() != null)
	 * dsConfigPage.setDisplayName(templatePage.getDisplayName()); if
	 * (templatePage.getDescription() != null)
	 * dsConfigPage.setDescription(templatePage.getDescription()); if
	 * (templatePage.getOutFormats() != null)
	 * dsConfigPage.setOutFormats(templatePage.getOutFormats()); if
	 * (templatePage.getMaxSelectString() != null)
	 * dsConfigPage.setMaxSelect(templatePage.getMaxSelectString());
	 * 
	 * AttributeGroup dsConfigGroup = (AttributeGroup)
	 * dsConfigPage.getAttributeGroupByName(templateGroup.getInternalName()); if
	 * (dsConfigGroup == null){ dsConfigGroup = new
	 * AttributeGroup(templateGroup.
	 * getInternalName(),templateGroup.getDisplayName(),
	 * templateGroup.getDescription(), ""); AttributeCollection
	 * dsConfigCollection = new
	 * AttributeCollection(templateCollection.getInternalName(), "",
	 * templateCollection.getDisplayName(),templateCollection.getDescription());
	 * 
	 * if (templateGroup.getHidden() != null)
	 * dsConfigGroup.setHidden(templateGroup.getHidden()); if
	 * (templateCollection.getHidden() != null)
	 * dsConfigCollection.setHidden(templateCollection.getHidden()); if
	 * (templateAttribute.getHidden() != null)
	 * configAttToAdd.setHidden(templateAttribute.getHidden()); if
	 * (templateGroup.getMaxSelectString() != null)
	 * dsConfigGroup.setMaxSelect(templateGroup.getMaxSelectString()); if
	 * (templateCollection.getMaxSelectString() != null)
	 * dsConfigCollection.setMaxSelect(templateCollection.getMaxSelectString());
	 * if (templateCollection.getHidden() != null)
	 * dsConfigCollection.setHidden(templateCollection.getHidden()); if
	 * (templateAttribute.getHidden() != null)
	 * configAttToAdd.setHidden(templateAttribute.getHidden()); if
	 * (configAtt.getHidden() != null && configAtt.getHidden().equals("true"))
	 * configAttToAdd.setHidden("true");
	 * dsConfigPage.addAttributeGroup(dsConfigGroup);
	 * dsConfigGroup.addAttributeCollection(dsConfigCollection);
	 * dsConfigCollection.addAttributeDescription(configAttToAdd); } else{ if
	 * (templateGroup.getDisplayName() != null)
	 * dsConfigGroup.setDisplayName(templateGroup.getDisplayName()); if
	 * (templateGroup.getDescription() != null)
	 * dsConfigGroup.setDescription(templateGroup.getDescription()); if
	 * (templateGroup.getMaxSelectString() != null)
	 * dsConfigGroup.setMaxSelect(templateGroup.getMaxSelectString());
	 * AttributeCollection dsConfigCollection = (AttributeCollection)
	 * dsConfigGroup
	 * .getAttributeCollectionByName(templateCollection.getInternalName()); if
	 * (dsConfigCollection == null){ dsConfigCollection = new
	 * AttributeCollection(templateCollection.getInternalName(),"",
	 * templateCollection.getDisplayName(),templateCollection.getDescription());
	 * 
	 * if (templateCollection.getHidden() != null)
	 * dsConfigCollection.setHidden(templateCollection.getHidden()); if
	 * (templateAttribute.getHidden() != null)
	 * configAttToAdd.setHidden(templateAttribute.getHidden());
	 * 
	 * if (templateCollection.getMaxSelectString() != null)
	 * dsConfigCollection.setMaxSelect(templateCollection.getMaxSelectString());
	 * if (configAtt.getHidden() != null &&
	 * configAtt.getHidden().equals("true")) configAttToAdd.setHidden("true");
	 * dsConfigGroup.addAttributeCollection(dsConfigCollection);
	 * dsConfigCollection.addAttributeDescription(configAttToAdd); } else{ if
	 * (templateCollection.getDisplayName() != null)
	 * dsConfigCollection.setDisplayName(templateCollection.getDisplayName());
	 * if (templateCollection.getDescription() != null)
	 * dsConfigCollection.setDescription(templateCollection.getDescription());
	 * if (templateCollection.getMaxSelectString() != null)
	 * dsConfigCollection.setMaxSelect(templateCollection.getMaxSelectString());
	 * if (templateAttribute.getHidden() != null)
	 * configAttToAdd.setHidden(templateAttribute.getHidden()); // nb the below
	 * is necessary for update logic when atts are turned off but // found had
	 * to comment out at some stage during initial template generation ? if
	 * (configAtt.getHidden() != null && configAtt.getHidden().equals("true"))
	 * configAttToAdd.setHidden("true"); // put in a check for this and FILTERS
	 * that the same configAtt not already added // this can happen when
	 * original config has duplicate filters and atts in terms of TC and Fields
	 * if(!dsConfigCollection.containsAttributeDescription(configAttToAdd.
	 * getInternalName()))
	 * dsConfigCollection.addAttributeDescription(configAttToAdd); } } } } else{
	 * //System.out.println("2 - make sure templateConfig has same structure as
	 * dsConfig for this attribute:"+configAtt.getInternalName());
	 * 
	 * AttributePage templatePage =
	 * templateConfig.getAttributePageByInternalName(configPageName); if
	 * (templatePage == null){ templatePage = new
	 * AttributePage(configPage.getInternalName(), configPage.getDisplayName(),
	 * configPage.getDescription(), configPage.getOutFormats(),
	 * configPage.getMaxSelectString()); if (configPage.getHidden() != null)
	 * templatePage.setHidden(configPage.getHidden());
	 * templateConfig.addAttributePage(templatePage); }
	 * 
	 * AttributeGroup templateGroup = (AttributeGroup)
	 * templatePage.getAttributeGroupByName(configGroupName); if (templateGroup
	 * == null){ templateGroup = new
	 * AttributeGroup(configGroup.getInternalName(),
	 * configGroup.getDisplayName(), configGroup.getDescription()); if
	 * (configGroup.getHidden() != null)
	 * templateGroup.setHidden(configGroup.getHidden()); if
	 * (configGroup.getMaxSelectString() != null)
	 * templateGroup.setMaxSelect(configGroup.getMaxSelectString());
	 * templatePage.addAttributeGroup(templateGroup); }
	 * 
	 * AttributeCollection templateCollection = (AttributeCollection)
	 * templateGroup.getAttributeCollectionByName(configCollectionName); if
	 * (templateCollection == null){ templateCollection = new
	 * AttributeCollection(configCollection.getInternalName(), "",
	 * configCollection.getDisplayName(), configCollection.getDescription()); if
	 * (configCollection.getHidden() != null)
	 * templateCollection.setHidden(configCollection.getHidden()); if
	 * (configCollection.getMaxSelectString() != null)
	 * templateCollection.setMaxSelect(configCollection.getMaxSelectString());
	 * templateGroup.addAttributeCollection(templateCollection); }
	 * 
	 * AttributeDescription templateAttToAdd = new
	 * AttributeDescription(configAtt); if
	 * (templateAttToAdd.getTableConstraint() != null &&
	 * !templateAttToAdd.getTableConstraint().equals("") &&
	 * !templateAttToAdd.getTableConstraint().equals("main"))
	 * templateAttToAdd.setTableConstraint
	 * (templateAttToAdd.getTableConstraint().
	 * split("__")[1]+"__"+templateAttToAdd
	 * .getTableConstraint().split("__")[2]);
	 * //templateAttToAdd.setLinkoutURL("");
	 * 
	 * if (configAtt.getHidden() != null)
	 * templateAttToAdd.setHidden(configAtt.getHidden());
	 * templateCollection.addAttributeDescription(templateAttToAdd); } }
	 * 
	 * 
	 * 
	 * private void updateFilterToTemplate(FilterDescription
	 * configAtt,DatasetConfig dsConfig, DatasetConfig templateConfig, String
	 * upstreamFilterName) throws ConfigurationException, SQLException{ // Skip
	 * placeholders. if (configAtt.getTableConstraint().equals("") ||
	 * configAtt.getTableConstraint()==null ||
	 * (configAtt.getPointerDataset()!=null &&
	 * !"".equals(configAtt.getPointerDataset()))) return;
	 * 
	 * String configAttName = configAtt.getInternalName(); String configAttTC;
	 * //System.out.println(configAttName+":"+configAtt.getTableConstraint());
	 * if (configAtt.getTableConstraint().equals("main")) configAttTC = "main";
	 * else configAttTC =
	 * configAtt.getTableConstraint().split("__")[1]+"__"+configAtt
	 * .getTableConstraint().split("__")[2];// template stores w/o the dataset
	 * part
	 * 
	 * String configAttField = configAtt.getField(); FilterPage configPage =
	 * dsConfig.getPageForFilter(configAttName); String configPageName =
	 * configPage.getInternalName(); FilterGroup configGroup =
	 * dsConfig.getGroupForFilter(configAttName); String configGroupName =
	 * configGroup.getInternalName(); FilterCollection configCollection =
	 * dsConfig.getCollectionForFilter(configAttName); String
	 * configCollectionName = configCollection.getInternalName(); if
	 * (templateConfig
	 * .supportsFilterDescription(configAttField,configAttTC,configAtt
	 * .getQualifier())){// will find option filters as well
	 * //System.out.println("1 - make sure dsConfig has same structure as
	 * templateConfig for this filter:" //
	 * +configAtt.getInternalName()+":"+configAtt
	 * .getDisplayName()+":"+dsConfig.getDataset()+":"+upstreamFilterName); //
	 * remove att from old hierarchy in dsConfig
	 * 
	 * FilterDescription upstreamFilter = null; if (upstreamFilterName != null){
	 * 
	 * upstreamFilter =
	 * configCollection.getFilterDescriptionByInternalName(upstreamFilterName);
	 * //System.out.println("GOT "+upstreamFilter.getInternalName()+ "FOR
	 * "+upstreamFilterName); Option opToRemove = new Option(configAtt);
	 * upstreamFilter.removeOption(opToRemove); if
	 * (!(upstreamFilter.getOptions().length > 0)){
	 * configCollection.removeFilterDescription(upstreamFilter); } } else{
	 * configCollection.removeFilterDescription(configAtt); }
	 * 
	 * if (!(configCollection.getFilterDescriptions().size() > 0)){
	 * configGroup.removeFilterCollection(configCollection); if
	 * (!(configGroup.getFilterCollections().length > 0)){
	 * configPage.removeFilterGroup(configGroup); if
	 * (!(configPage.getFilterGroups().size() > 0)){
	 * dsConfig.removeFilterPage(configPage); } } }
	 * 
	 * FilterDescription templateFilter =
	 * templateConfig.getFilterDescriptionByFieldNameTableConstraintInternalName
	 * (configAttField,configAttTC,null,configAttName);
	 * 
	 * FilterPage templatePage =
	 * templateConfig.getPageForFilter(templateFilter.getInternalName());
	 * FilterGroup templateGroup =
	 * templateConfig.getGroupForFilter(templateFilter.getInternalName());
	 * FilterCollection templateCollection =
	 * templateConfig.getCollectionForFilter(templateFilter.getInternalName());
	 * 
	 * FilterDescription configAttToAdd = null;
	 * 
	 * if (templateFilter.getTableConstraint() == null){ // have recovered a
	 * filter container for filter options // need to make sure a new filter
	 * option is added to configAttToAdd Option[] ops =
	 * templateFilter.getOptions(); for (int j = 0; j < ops.length; j++){ if
	 * (ops[j].getTableConstraint().equals(configAttTC) &&
	 * ops[j].getField().equals(configAttField)){ //Option opToAdd = ops[j];
	 * Option opToAdd = new Option(ops[j]);
	 * opToAdd.setTableConstraint(configAtt.getTableConstraint());
	 * 
	 * if (configAtt.getHidden() != null &&
	 * configAtt.getHidden().equals("true")) opToAdd.setHidden("true");
	 * 
	 * FilterDescription configFilterList =
	 * dsConfig.getFilterDescriptionByInternalName
	 * (templateFilter.getInternalName()); // get bug below when filter has been
	 * removed by the //
	 * configCollection.removeFilterDescription(upstreamFilter) call above //
	 * therefore added extra clause to test it had options still if
	 * (configFilterList != null && upstreamFilter != null &&
	 * upstreamFilter.getOptions().length > 0){ // check not already there if
	 * (!configFilterList.containsOption(opToAdd.getInternalName())){
	 * configFilterList.addOption(opToAdd); } return; } else{ configAttToAdd =
	 * new FilterDescription(templateFilter.getInternalName(),"","list","");
	 * configAttToAdd.addOption(opToAdd); break; } } } } else{ configAttToAdd =
	 * new FilterDescription(templateFilter);
	 * configAttToAdd.setTableConstraint(configAtt.getTableConstraint());
	 * configAttToAdd.setField(configAtt.getField());
	 * //configAttToAdd.setOtherFilters(configAtt.getOtherFilters()); // dynamic
	 * content handling eg linkoutURL if (configAtt.getOtherFilters() == null)
	 * configAtt.setOtherFilters("");// avoids template problems if
	 * (templateFilter.getOtherFilters() == null)
	 * templateFilter.setOtherFilters("");// avoids template problems if
	 * (templateFilter.getDynamicFilterContents().size() > 0){ // already got
	 * multiple settings for this attribute if
	 * (!templateFilter.containsDynamicFilterContent(dsConfig.getDataset())){
	 * DynamicFilterContent dynFilter = new
	 * DynamicFilterContent(dsConfig.getDataset(),configAtt.getOtherFilters());
	 * //templateFilter.addDynamicFilterContent(new
	 * DynamicFilterContent(dsConfig.getDataset(),configAtt.getOtherFilters()));
	 * dynFilter.addOptions(configAtt.getOptions());
	 * templateFilter.addDynamicFilterContent(dynFilter); }
	 * 
	 * configAttToAdd.setOtherFilters(templateFilter.
	 * getDynamicFilterContentByInternalName
	 * (dsConfig.getDataset()).getOtherFilters());
	 * configAttToAdd.removeOptions();configAttToAdd.addOptions(templateFilter.
	 * getDynamicFilterContentByInternalName
	 * (dsConfig.getDataset()).getOptions()); } else if
	 * (!configAtt.getOtherFilters().equals(templateFilter.getOtherFilters()) ||
	 * configAtt.getOptions().length > 0 || templateFilter.getOptions().length >
	 * 0){ // if this config has a different setting then start using dynamic
	 * objects // create dynamic objects - add one per existing dataset set to
	 * current template linkoutURL String[] datasetNames =
	 * getDatasetNamesForTemplate(dsConfig.getTemplate()); // if only one ds, do
	 * a straight copy. if (datasetNames.length<2) {
	 * configAttToAdd.setOtherFilters(templateFilter.getOtherFilters());
	 * configAttToAdd.removeOptions();
	 * configAttToAdd.addOptions(templateFilter.getOptions()); } else { for (int
	 * j = 0; j < datasetNames.length; j++){ String datasetName =
	 * datasetNames[j]; if (datasetName.equals(dsConfig.getDataset())) continue;
	 * 
	 * DynamicFilterContent dynFilter = new
	 * DynamicFilterContent(datasetName,templateFilter.getOtherFilters());
	 * dynFilter.addOptions(templateFilter.getOptions());
	 * templateFilter.addDynamicFilterContent(dynFilter);
	 * //templateFilter.addDynamicFilterContent(new
	 * DynamicFilterContent(datasetName,templateFilter.getOtherFilters())); }
	 * templateFilter.removeOptions(); templateFilter.setOtherFilters("MULTI");
	 * 
	 * DynamicFilterContent dynFilter = new
	 * DynamicFilterContent(dsConfig.getDataset(),configAtt.getOtherFilters());
	 * dynFilter.addOptions(configAtt.getOptions());
	 * templateFilter.addDynamicFilterContent(dynFilter);
	 * //templateFilter.addDynamicFilterContent(new
	 * DynamicFilterContent(dsConfig.getDataset(),configAtt.getOtherFilters()));
	 * 
	 * configAttToAdd.setOtherFilters(templateFilter.
	 * getDynamicFilterContentByInternalName
	 * (dsConfig.getDataset()).getOtherFilters());
	 * configAttToAdd.removeOptions();configAttToAdd.addOptions(templateFilter.
	 * getDynamicFilterContentByInternalName
	 * (dsConfig.getDataset()).getOptions()); } } /* options now handled in
	 * dynamic content above if (templateFilter.getType().equals("list")){
	 * String colForDisplay = ""; if (configAttToAdd.getColForDisplay() !=
	 * null){ colForDisplay = configAttToAdd.getColForDisplay(); }
	 * 
	 * Option[] options; if (configAtt.getOptions().length > 0){ options =
	 * configAtt.getOptions(); } else{ options =
	 * getOptions(configAttToAdd.getField(),
	 * configAttToAdd.getTableConstraint(), configAttToAdd.getKey(), dsConfig,
	 * colForDisplay); } for (int k = 0; k < options.length; k++) {
	 * configAttToAdd.insertOption(k,options[k]); } } }
	 * 
	 * 
	 * 
	 * 
	 * if (configAtt.getHidden() != null &&
	 * configAtt.getHidden().equals("true")) configAttToAdd.setHidden("true");
	 * 
	 * 
	 * FilterPage dsConfigPage =
	 * dsConfig.getFilterPageByName(templatePage.getInternalName()); if
	 * (dsConfigPage == null){ dsConfigPage = new
	 * FilterPage(templatePage.getInternalName(),templatePage.getDisplayName(),
	 * templatePage.getDescription()); FilterGroup dsConfigGroup = new
	 * FilterGroup(templateGroup.getInternalName(),
	 * templateGroup.getDisplayName(), templateGroup.getDescription());
	 * FilterCollection dsConfigCollection = new
	 * FilterCollection(templateCollection.getInternalName(),
	 * templateCollection.getDisplayName(),templateCollection.getDescription());
	 * 
	 * if (templatePage.getHidden() != null)
	 * dsConfigPage.setHidden(templatePage.getHidden()); if
	 * (templatePage.getDisplay() != null)
	 * dsConfigPage.setDisplay(templatePage.getDisplay()); if
	 * (templateGroup.getHidden() != null)
	 * dsConfigGroup.setHidden(templateGroup.getHidden()); if
	 * (templateCollection.getHidden() != null)
	 * dsConfigCollection.setHidden(templateCollection.getHidden()); if
	 * (templateFilter.getHidden() != null)
	 * configAttToAdd.setHidden(templateFilter.getHidden()); if
	 * (configAtt.getHidden() != null && configAtt.getHidden().equals("true"))
	 * configAttToAdd.setHidden("true"); dsConfig.addFilterPage(dsConfigPage);
	 * dsConfigPage.addFilterGroup(dsConfigGroup);
	 * dsConfigGroup.addFilterCollection(dsConfigCollection);
	 * dsConfigCollection.addFilterDescription(configAttToAdd); } else{ // make
	 * sure dsConfigPage has correct displayName etc if
	 * (templatePage.getDisplayName() != null)
	 * dsConfigPage.setDisplayName(templatePage.getDisplayName()); if
	 * (templatePage.getDescription() != null)
	 * dsConfigPage.setDescription(templatePage.getDescription());
	 * 
	 * FilterGroup dsConfigGroup = (FilterGroup)
	 * dsConfigPage.getFilterGroupByName(templateGroup.getInternalName()); if
	 * (dsConfigGroup == null){ dsConfigGroup = new
	 * FilterGroup(templateGroup.getInternalName
	 * (),templateGroup.getDisplayName(), templateGroup.getDescription());
	 * FilterCollection dsConfigCollection = new
	 * FilterCollection(templateCollection.getInternalName(),
	 * templateCollection.getDisplayName(),templateCollection.getDescription());
	 * 
	 * if (templateGroup.getHidden() != null)
	 * dsConfigGroup.setHidden(templateGroup.getHidden()); if
	 * (templateCollection.getHidden() != null)
	 * dsConfigCollection.setHidden(templateCollection.getHidden()); if
	 * (templateFilter.getHidden() != null)
	 * configAttToAdd.setHidden(templateFilter.getHidden()); if
	 * (configAtt.getHidden() != null && configAtt.getHidden().equals("true"))
	 * configAttToAdd.setHidden("true");
	 * dsConfigPage.addFilterGroup(dsConfigGroup);
	 * dsConfigGroup.addFilterCollection(dsConfigCollection);
	 * dsConfigCollection.addFilterDescription(configAttToAdd); } else{ if
	 * (templateGroup.getDisplayName() != null)
	 * dsConfigGroup.setDisplayName(templateGroup.getDisplayName()); if
	 * (templateGroup.getDescription() != null)
	 * dsConfigGroup.setDescription(templateGroup.getDescription());
	 * FilterCollection dsConfigCollection = (FilterCollection)
	 * dsConfigGroup.getFilterCollectionByName
	 * (templateCollection.getInternalName()); if (dsConfigCollection == null){
	 * dsConfigCollection = new
	 * FilterCollection(templateCollection.getInternalName(),
	 * templateCollection.getDisplayName(),templateCollection.getDescription());
	 * 
	 * if (templateCollection.getHidden() != null)
	 * dsConfigCollection.setHidden(templateCollection.getHidden()); if
	 * (templateFilter.getHidden() != null)
	 * configAttToAdd.setHidden(templateFilter.getHidden()); if
	 * (configAtt.getHidden() != null && configAtt.getHidden().equals("true"))
	 * configAttToAdd.setHidden("true");
	 * dsConfigGroup.addFilterCollection(dsConfigCollection);
	 * dsConfigCollection.addFilterDescription(configAttToAdd); } else{ if
	 * (templateCollection.getDisplayName() != null)
	 * dsConfigCollection.setDisplayName(templateCollection.getDisplayName());
	 * if (templateCollection.getDescription() != null)
	 * dsConfigCollection.setDescription(templateCollection.getDescription());
	 * if (templateFilter.getHidden() != null)
	 * configAttToAdd.setHidden(templateFilter.getHidden()); if
	 * (configAtt.getHidden() != null && configAtt.getHidden().equals("true"))
	 * configAttToAdd.setHidden("true"); // put in a check for this and FILTERS
	 * that the same configAtt not already added // this can happen when
	 * original config has duplicate filters and atts in terms of TC and Fields
	 * 
	 * if
	 * (!dsConfigCollection.containsFilterDescription(configAttToAdd.getInternalName
	 * ())){ dsConfigCollection.addFilterDescription(configAttToAdd); } } } } }
	 * else{ //System.out.println("2 - make sure templateConfig has same
	 * structure as dsConfig for this filter:"+configAtt.getInternalName());
	 * 
	 * FilterPage templatePage =
	 * templateConfig.getFilterPageByName(configPageName); if (templatePage ==
	 * null){ templatePage = new FilterPage(configPage.getInternalName(),
	 * configPage.getDisplayName(), configPage.getDescription()); if
	 * (configPage.getHidden() != null)
	 * templatePage.setHidden(configPage.getHidden());
	 * templateConfig.addFilterPage(templatePage); }
	 * 
	 * FilterGroup templateGroup = (FilterGroup)
	 * templatePage.getFilterGroupByName(configGroupName); if (templateGroup ==
	 * null){ templateGroup = new FilterGroup(configGroup.getInternalName(),
	 * configGroup.getDisplayName(), configGroup.getDescription()); if
	 * (configGroup.getHidden() != null)
	 * templateGroup.setHidden(configGroup.getHidden());
	 * templatePage.addFilterGroup(templateGroup); }
	 * 
	 * FilterCollection templateCollection = (FilterCollection)
	 * templateGroup.getFilterCollectionByName(configCollectionName); if
	 * (templateCollection == null){ templateCollection = new
	 * FilterCollection(configCollection.getInternalName(),
	 * configCollection.getDisplayName(), configCollection.getDescription()); if
	 * (configCollection.getHidden() != null)
	 * templateCollection.setHidden(configCollection.getHidden());
	 * templateGroup.addFilterCollection(templateCollection); }
	 * 
	 * FilterDescription templateAttToAdd = new FilterDescription(configAtt); //
	 * hack to fix broken types in existing XML as updateConfigToTemplate uses
	 * list type to specify options if (templateAttToAdd.getType() != null &&
	 * templateAttToAdd.getType().equals("list") &&
	 * !(templateAttToAdd.getOptions().length > 0)){
	 * templateAttToAdd.setType("text"); }
	 * 
	 * //templateAttToAdd.setTableConstraint("");
	 * //templateAttToAdd.setField(""); // remove dataset part from
	 * tableConstraint if present if (templateAttToAdd.getTableConstraint() !=
	 * null && !templateAttToAdd.getTableConstraint().equals("") &&
	 * !templateAttToAdd.getTableConstraint().equals("main"))
	 * templateAttToAdd.setTableConstraint
	 * (templateAttToAdd.getTableConstraint().
	 * split("__")[1]+"__"+templateAttToAdd
	 * .getTableConstraint().split("__")[2]);
	 * //templateAttToAdd.setOtherFilters("");
	 * 
	 * Option[] ops = templateAttToAdd.getOptions(); for (int j = 0; j <
	 * ops.length; j++){ Option op = ops[j]; // if a value option remove it if
	 * (op.getTableConstraint() == null || op.getTableConstraint().equals("")){
	 * //templateAttToAdd.removeOption(op); continue; } // if a filter option
	 * remove dataset part from tableConstraint if
	 * (!op.getTableConstraint().equals("main")){
	 * //System.out.println(op.getDisplayName()+":"+op.getTableConstraint());
	 * op.setTableConstraint(op.getTableConstraint().split("__")[1]+"__"+op.
	 * getTableConstraint().split("__")[2]); } //op.setOtherFilters(""); }
	 * 
	 * if (configAtt.getHidden() != null)
	 * templateAttToAdd.setHidden(configAtt.getHidden()); // need to test if
	 * needs to be an option instead if (upstreamFilterName != null){
	 * FilterDescription upstreamFilter =
	 * configCollection.getFilterDescriptionByInternalName(upstreamFilterName);
	 * FilterDescription templateFilter =
	 * templateCollection.getFilterDescriptionByInternalName
	 * (upstreamFilterName); if (templateFilter == null){ templateFilter = new
	 * FilterDescription(upstreamFilter); templateFilter.removeOptions();
	 * templateCollection.addFilterDescription(templateFilter); }
	 * templateFilter.addOption(new Option(templateAttToAdd)); } else{
	 * templateCollection.addFilterDescription(templateAttToAdd); } } }
	 */

	public DatasetConfig updateConfigToTemplate(final DatasetConfig dsConfig,
			final DatasetConfig templateConfig) throws ConfigurationException {

		// add any missing stuff from template to the dataset - useful for naive
		// don't bother with MULTI settings as XSLT transformation will remove
		// anyhow

		// now ZAPS dataset and completely overwrites it.

		// Clear ds exps and imps

		if (logger.isLoggable(Level.FINE))
			logger.fine("Removing importables\n");

		Importable[] dsImps = dsConfig.getImportables();
		for (int j = 0; j < dsImps.length; j++)
			dsConfig.removeImportable(dsImps[j]);
		if (logger.isLoggable(Level.FINE))
			logger.fine("Removing exportables\n");

		Exportable[] dsExps = dsConfig.getExportables();
		for (int j = 0; j < dsExps.length; j++)
			dsConfig.removeExportable(dsExps[j]);

		if (logger.isLoggable(Level.FINE))
			logger.fine("Removing filter pages\n");

		// Clear ds filters and atts.
		FilterPage[] dsFiltps = dsConfig.getFilterPages();
		for (int j = 0; j < dsFiltps.length; j++)
			dsConfig.removeFilterPage(dsFiltps[j]);
		if (logger.isLoggable(Level.FINE))
			logger.fine("Removing attribute pages\n");

		AttributePage[] dsAttps = dsConfig.getAttributePages();
		for (int j = 0; j < dsAttps.length; j++)
			dsConfig.removeAttributePage(dsAttps[j]);

		if (logger.isLoggable(Level.FINE))
			logger.fine("Populating from scratch\n");

		// Populate ds from scratch from template.
		FilterPage[] templatePages = templateConfig.getFilterPages();
		for (int i = 0; i < templatePages.length; i++) {
			if (logger.isLoggable(Level.FINE))
				logger.fine("Populating template page " + i + "\n");

			final FilterPage templatePage = templatePages[i];
			// add the missing placeholder to the dsConfig
			FilterPage configPage = new FilterPage(templatePage);
			dsConfig.addFilterPage(configPage);
			templateConfig.getDynamicDataset(dsConfig.getDataset())
					.resolveText(configPage, templatePage);

			final List configGroups = configPage.getFilterGroups();
			for (int j = 0; j < configGroups.size(); j++) {
				final FilterGroup configGroup = (FilterGroup) configGroups
						.get(j);
				templateConfig.getDynamicDataset(dsConfig.getDataset())
						.resolveText(configGroup, configGroup);
				final FilterCollection[] configCollections = configGroup
						.getFilterCollections();
				for (int k = 0; k < configCollections.length; k++) {
					final FilterCollection configCollection = configCollections[k];
					templateConfig.getDynamicDataset(dsConfig.getDataset())
							.resolveText(configCollection, configCollection);
					final List configFilters = configCollection
							.getFilterDescriptions();
					for (int l = 0; l < configFilters.size(); l++) {

						final FilterDescription configAtt = (FilterDescription) configFilters
								.get(l);

						templateConfig.getDynamicDataset(dsConfig.getDataset())
								.resolveText(configAtt, configAtt);

						if (!configAtt.getSpecificFilterContents().isEmpty()) {
							final SpecificFilterContent sf = configAtt
									.getSpecificFilterContent(dsConfig
											.getDataset());
							// dstaines change
							if (!configAtt.isUseDefault() && sf == null) {
								configCollection
										.removeFilterDescription(configAtt);
								continue;
							}

							if (sf != null) {
								final Option[] options = sf.getOptions();
								for (int r = 0; r < options.length; r++)
									configAtt.addOption(new Option(options[r]));

								templateConfig.getDynamicDataset(
										dsConfig.getDataset()).resolveText(
										configAtt, sf);

							}
							// Remove specifics.
							configAtt.getSpecificFilterContents().clear();
						}

						if (configAtt.getTableConstraint() != null
								&& !configAtt.getTableConstraint().equals(
										"main"))
							configAtt.setTableConstraint(dsConfig.getDataset()
									+ "__" + configAtt.getTableConstraint());

						// Resolve options.
						final List options = new ArrayList(
								Arrays.asList(configAtt.getOptions()));
						final class placeholder {
							private QueryFilterSettings parent;

							public placeholder(QueryFilterSettings parent) {
								this.parent = parent;
							}
						}
						QueryFilterSettings parent = configAtt;
						for (int r = 0; r < options.size(); r++) {
							final Object obj = options.get(r);
							if (obj instanceof placeholder) {
								parent = ((placeholder) obj).parent;
								continue;
							}
							final Option option = (Option) obj;

							templateConfig.getDynamicDataset(
									dsConfig.getDataset()).resolveText(option,
									option);

							if (!option.getSpecificOptionContents().isEmpty()) {
								final SpecificOptionContent sf = option
										.getSpecificOptionContent(dsConfig
												.getDataset());

								if (sf != null) {
									templateConfig.getDynamicDataset(
											dsConfig.getDataset()).resolveText(
											option, sf);
								}
								// Remove specifics.
								option.getSpecificOptionContents().clear();
							}

							options.add(new placeholder(option));
							options.addAll(Arrays.asList(option.getOptions()));
						}
					}
				}
			}
		}

		AttributePage[] templateAttPages = templateConfig.getAttributePages();
		for (int i = 0; i < templateAttPages.length; i++) {
			final AttributePage templatePage = templateAttPages[i];
			if (logger.isLoggable(Level.FINE))
				logger.fine("Populating template attribute page " + i + "\n");
			// add the missing placeholder to the dsConfig
			AttributePage configPage = new AttributePage(templatePage);
			dsConfig.addAttributePage(configPage);
			templateConfig.getDynamicDataset(dsConfig.getDataset())
					.resolveText(configPage, templatePage);
			final List configGroups = configPage.getAttributeGroups();
			for (int j = 0; j < configGroups.size(); j++) {
				final AttributeGroup configGroup = (AttributeGroup) configGroups
						.get(j);
				templateConfig.getDynamicDataset(dsConfig.getDataset())
						.resolveText(configGroup, configGroup);
				final AttributeCollection[] configCollections = configGroup
						.getAttributeCollections();
				for (int k = 0; k < configCollections.length; k++) {
					final AttributeCollection configCollection = configCollections[k];
					templateConfig.getDynamicDataset(dsConfig.getDataset())
							.resolveText(configCollection, configCollection);
					List configAttributes = configCollection
							.getAttributeDescriptions();
					for (int l = 0; l < configAttributes.size(); l++) {

						final AttributeDescription configAtt = (AttributeDescription) configAttributes
								.get(l);

						templateConfig.getDynamicDataset(dsConfig.getDataset())
								.resolveText(configAtt, configAtt);

						if (!configAtt.getSpecificAttributeContents().isEmpty()) {
							final SpecificAttributeContent sf = configAtt
									.getSpecificAttributeContent(dsConfig
											.getDataset());
							// dstaines change for EG - I don't want to have to
							// put in 50 different specific contents
							if (!configAtt.isUseDefault() && sf == null) {
								configCollection
										.removeAttributeDescription(configAtt);
								continue;
							}

							if (sf != null) {
								templateConfig.getDynamicDataset(
										dsConfig.getDataset()).resolveText(
										configAtt, sf);

							}
							// Remove specifics.
							configAtt.getSpecificAttributeContents().clear();
						}

						if (configAtt.getTableConstraint() != null
								&& !configAtt.getTableConstraint().equals(
										"main"))
							configAtt.setTableConstraint(dsConfig.getDataset()
									+ "__" + configAtt.getTableConstraint());
					}

					// Insert all attribute lists from the template into the
					// dataset.
					configAttributes = configCollection.getAttributeLists();
					for (int l = 0; l < configAttributes.size(); l++) {

						final AttributeList configAtt = (AttributeList) configAttributes
								.get(l);

						templateConfig.getDynamicDataset(dsConfig.getDataset())
								.resolveText(configAtt, configAtt);
					}

				}
			}
		}

		if (logger.isLoggable(Level.FINE))
			logger.fine("Doing some more stuff\n");
		Connection conn = null;
		try {
			conn = this.dsource.getConnection();

			final String[] mains = dsConfig.getStarBases();
			final String[] keys = dsConfig.getPrimaryKeys();
			// Iterate through dsConfig and remove empty
			// collections/groups/pages.
			final AttributePage apages[] = dsConfig.getAttributePages();
			for (int pi = 0; pi < apages.length; pi++) {
				for (final Iterator li = apages[pi].getAttributeGroups()
						.iterator(); li.hasNext();) {
					final AttributeGroup agroup = (AttributeGroup) li.next();
					final AttributeCollection[] acolls = agroup
							.getAttributeCollections();
					for (int ci = 0; ci < acolls.length; ci++) {
						final AttributeCollection validAttrs = this
								.getValidatedAttributeCollection(acolls[ci],
										dsConfig.getDataset(), conn, mains,
										keys);
						for (final Iterator ai = acolls[ci]
								.getAttributeDescriptions().iterator(); ai
								.hasNext();) {
							final AttributeDescription attr = (AttributeDescription) ai
									.next();
							if (validAttrs
									.getAttributeDescriptionByInternalName(
											attr.getInternalName()).isBroken())
								acolls[ci].removeAttributeDescription(attr);
						}
						if (!(acolls[ci].getAttributeDescriptions().size()
								+ acolls[ci].getAttributeLists().size() > 0))
							agroup.removeAttributeCollection(acolls[ci]);
					}
					if (!(agroup.getAttributeCollections().length > 0))
						apages[pi].removeAttributeGroup(agroup);
				}
				if (!(apages[pi].getAttributeGroups().size() > 0))
					dsConfig.removeAttributePage(apages[pi]);
			}
			final FilterPage fpages[] = dsConfig.getFilterPages();
			for (int pi = 0; pi < fpages.length; pi++) {
				for (final Iterator li = fpages[pi].getFilterGroups()
						.iterator(); li.hasNext();) {
					final FilterGroup agroup = (FilterGroup) li.next();
					final FilterCollection[] acolls = agroup
							.getFilterCollections();
					for (int ci = 0; ci < acolls.length; ci++) {
						final FilterCollection validAttrs = this
								.getValidatedFilterCollection(acolls[ci],
										dsConfig.getDataset(), dsConfig, conn,
										mains, keys);
						for (final Iterator ai = acolls[ci]
								.getFilterDescriptions().iterator(); ai
								.hasNext();) {
							final FilterDescription attr = (FilterDescription) ai
									.next();
							if (validAttrs.getFilterDescriptionByInternalName(
									attr.getInternalName())
									.isBrokenExceptOpts()
									|| this.pruneBrokenOptions(
											attr,
											validAttrs
													.getFilterDescriptionByInternalName(attr
															.getInternalName())))
								acolls[ci].removeFilterDescription(attr);
						}
						if (!(acolls[ci].getFilterDescriptions().size() > 0))
							agroup.removeFilterCollection(acolls[ci]);
					}
					if (!(agroup.getFilterCollections().length > 0))
						fpages[pi].removeFilterGroup(agroup);
				}
				if (!(fpages[pi].getFilterGroups().size() > 0))
					dsConfig.removeFilterPage(fpages[pi]);
			}

			// then add extra template config importables to config if correct
			// conditions
			final Importable[] tempImps = templateConfig.getImportables();
			OUTER: for (int i = 0; i < tempImps.length; i++) {
				// skip if the importable has dynamic content or a linkVersion
				// as won't know what to use
				final String[] filterNames = tempImps[i].getFilters()
						.split(",");
				for (int j = 0; j < filterNames.length; j++)
					// skip if filters are not defined in the dsConfig
					if (!dsConfig.containsFilterDescription(filterNames[j])
							|| dsConfig.getFilterDescriptionByInternalName(
									filterNames[j]).getHidden() != null
							&& dsConfig
									.getFilterDescriptionByInternalName(
											filterNames[j]).getHidden()
									.equals("true"))
						continue OUTER;

				// if passsed all above tests then add template Importable to
				// datasetConfig
				final Importable newImp = new Importable(tempImps[i]);

				templateConfig.getDynamicDataset(dsConfig.getDataset())
						.resolveText(newImp, tempImps[i]);

				dsConfig.addImportable(newImp);
			}

			final Exportable[] tempExps = templateConfig.getExportables();
			OUTER: for (int i = 0; i < tempExps.length; i++) {
				// skip if the importable has dynamic content or a linkVersion
				// as won't know what to use

				final String[] attNames = tempExps[i].getAttributes()
						.split(",");
				for (int j = 0; j < attNames.length; j++) {
					// don't test if using placeholder atts as will always fail
					if ("true".equals(tempExps[i].getPointer()))
						continue;

					// skip if filters are not defined in the dsConfig
					if (!attNames[j].matches(".+__.+")
							&& (!dsConfig
									.containsAttributeDescription(attNames[j]) || dsConfig
									.getAttributeDescriptionByInternalName(
											attNames[j]).getHidden() != null
									&& dsConfig
											.getAttributeDescriptionByInternalName(
													attNames[j]).getHidden()
											.equals("true")))
						continue OUTER;
				}

				// if passsed all above tests then add template Importable to
				// datasetConfig
				final Exportable newExp = new Exportable(tempExps[i]);

				templateConfig.getDynamicDataset(dsConfig.getDataset())
						.resolveText(newExp, tempExps[i]);

				dsConfig.addExportable(newExp);
			}

			conn.close();
		} catch (final SQLException e) {

		} finally {
			DetailedDataSource.close(conn);
		}
		if (logger.isLoggable(Level.FINE))
			logger.fine("Done it and gluing it together\n");

		final String dsName = dsConfig.getDataset();
		final String intName = dsConfig.getInternalName();
		final String dsID = dsConfig.getDatasetID();
		templateConfig.getDynamicDataset(dsConfig.getDataset()).resolveText(
				dsConfig, templateConfig);
		dsConfig.setDatasetID(dsID);
		dsConfig.setInternalName(intName);
		dsConfig.setDataset(dsName);
		dsConfig.setTemplate(templateConfig.getTemplate());
		return dsConfig;
	}

	/*
	 * public int templateTest(String template) throws ConfigurationException{
	 * Connection conn = null; try { conn = dsource.getConnection(); String sql
	 * = "SELECT count(*) FROM "+getSchema()[0]+"."+MARTTEMPLATEMAINTABLE+"
	 * WHERE template='"+template+"'"; PreparedStatement ps =
	 * conn.prepareStatement(sql); ResultSet rs = ps.executeQuery(); rs.next();
	 * int result = rs.getInt(1); return result; } catch (SQLException e) {
	 * throw new ConfigurationException( "Caught SQLException performing
	 * template count: " + e.getMessage()); } finally {
	 * DetailedDataSource.close(conn); } }
	 */

	public int templateCount(final String template)
			throws ConfigurationException {

		Connection conn = null;
		try {
			conn = this.dsource.getConnection();
			final String sql = "SELECT count(*) FROM " + this.getSchema()[0]
					+ "." + this.MARTTEMPLATEMAINTABLE + " WHERE template='"
					+ template + "'";
			final PreparedStatement ps = conn.prepareStatement(sql);
			final ResultSet rs = ps.executeQuery();
			rs.next();
			final int result = rs.getInt(1);
			conn.close();
			return result;
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQLException updating xml for: " + e.getMessage());
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	public boolean naiveExportWouldOverrideExistingConfig(final String user,
			final String datasetID, final String displayName,
			final String dataset, final String type, final String version)
			throws ConfigurationException {
		boolean exists = false;
		Connection conn = null;
		try {
			conn = this.dsource.getConnection();
			final String metatable = this.createMetaTables(user);
			// Name/version/type already exists? Reuse it.
			if (datasetID == null || datasetID.equals("")) {
				String sql = "SELECT dataset_id_key FROM "
						+ this.getSchema()[0] + "." + metatable
						+ " where dataset='" + dataset + "' and type='" + type
						+ "'";// ? and version=?";
				if (displayName != null && !displayName.equals(""))
					sql += " and display_name='" + displayName + "'";
				if (version != null && !version.equals(""))
					sql += " and version='" + version + "'";
				System.out.println(sql);
				// String sql = "SELECT dataset_id_key FROM " +
				// getSchema()[0]+"." + metatable.toUpperCase() + " where
				// display_name=? and dataset=? and type=? and version=?";

				final PreparedStatement ps = conn.prepareStatement(sql);
				// ps.setString(1, displayName);
				// ps.setString(2, dataset);
				// ps.setString(3, type);
				// ps.setString(4, version);

				final ResultSet rs = ps.executeQuery();
				if (rs.next())
					exists = true;
				rs.close();
				ps.close();
			}
			conn.close();
			return exists;
		} catch (final ConfigurationException e) {
			throw e;
		} catch (final Exception e) {
			throw new ConfigurationException(e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	private int storeCompressedXML(final String user,
			final String internalName, String displayName,
			final String dataset, final String description, final Document doc,
			final String type, final String visible, String version,
			String datasetID, final String martUsers, final String interfaces,
			final DatasetConfig dsConfig) throws ConfigurationException {
		if (this.dsource.getJdbcDriverClassName().indexOf("oracle") >= 0)
			return this.storeCompressedXMLOracle(user, internalName,
					displayName, dataset, description, doc, type, visible,
					version, datasetID, martUsers, interfaces, dsConfig);
		if (this.readonly)
			throw new ConfigurationException(
					"Cannot store config into read-only database");

		System.err.println("STORING XML: " + dsConfig.getDisplayName() + " "
				+ dsConfig.getTemplate());

		Connection conn = null;
		try {
			conn = this.dsource.getConnection();
			final String metatable = this.createMetaTables(user);
			// Name/version/type already exists? Reuse it.
			if (datasetID == null || datasetID.equals("")) {
				final String sql = "SELECT dataset_id_key FROM "
						+ this.getSchema()[0]
						+ "."
						+ metatable
						+ " where display_name=? and dataset=? and type=? and version=?";
				final PreparedStatement ps = conn.prepareStatement(sql);
				ps.setString(1, displayName);
				ps.setString(2, dataset);
				ps.setString(3, type);
				ps.setString(4, version);
				final ResultSet rs = ps.executeQuery();
				if (rs.next())
					datasetID = "" + rs.getInt(1);
				rs.close();
				ps.close();
			}

			// Doesn't already exist? Create a new one.
			if (datasetID == null || datasetID.equals("")) {
				final String sql = "SELECT MAX(dataset_id_key) FROM "
						+ this.getSchema()[0] + "." + this.BASEMETATABLE;
				final PreparedStatement ps = conn.prepareStatement(sql);
				final ResultSet rs = ps.executeQuery();
				rs.next();
				int result = rs.getInt(1);
				result++;
				final Integer datasetNo = new Integer(result);
				datasetID = datasetNo.toString();
				rs.close();
				ps.close();
			}

			// sort out meta_users and meta_interfaces tables first
			String sql = "DELETE FROM " + this.getSchema()[0] + "."
					+ this.MARTUSERTABLE + " WHERE dataset_id_key=" + datasetID;
			// System.out.println(sql);
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			if (martUsers != "") {
				final String[] martUserEntries = martUsers.split(",");
				for (int i = 0; i < martUserEntries.length; i++) {
					sql = "INSERT INTO " + this.getSchema()[0] + "."
							+ this.MARTUSERTABLE + " VALUES (" + datasetID
							+ ",'" + martUserEntries[i] + "')";
					// System.out.println(sql);
					ps = conn.prepareStatement(sql);
					ps.executeUpdate();
				}
			}
			sql = "DELETE FROM " + this.getSchema()[0] + "."
					+ this.MARTINTERFACETABLE + " WHERE dataset_id_key="
					+ datasetID;
			// System.out.println(sql);
			ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			if (interfaces != "") {
				final String[] interfaceEntries = interfaces.split(",");
				for (int i = 0; i < interfaceEntries.length; i++) {
					// System.out.println(sql);
					ps = conn.prepareStatement("INSERT INTO "
							+ this.getSchema()[0] + "."
							+ this.MARTINTERFACETABLE + " VALUES (" + datasetID
							+ ",'" + interfaceEntries[i] + "')");
					ps.executeUpdate();
				}
			}

			// add new template setting
			final String template = dsConfig.getTemplate();
			/*
			 * if (template.equals("")){ template = dataset;
			 * dsConfig.setTemplate(template); }
			 */
			// moved below sql after templateCount check as breaking new
			// creation
			// sql = "DELETE FROM "+getSchema()[0]+"."+MARTTEMPLATEMAINTABLE+"
			// WHERE dataset_id_key="+datasetID;
			// ps = conn.prepareStatement(sql);
			// ps.executeUpdate();
			// ps = conn.prepareStatement("INSERT INTO
			// "+getSchema()[0]+"."+MARTTEMPLATEMAINTABLE+" VALUES
			// ("+datasetID+",'"
			// +template+"')");
			// ps.executeUpdate();
			// sql = "SELECT count(*) FROM
			// "+getSchema()[0]+"."+MARTTEMPLATEMAINTABLE+" WHERE
			// template='"+template+"'";
			// ps = conn.prepareStatement(sql);
			// ResultSet rs = ps.executeQuery();
			// rs.next();
			// int result = rs.getInt(1);
			/*
			 * int result = templateCount(template); if (result > 0){//usual 1:1
			 * dataset:template do not get template merging //
			 * System.out.println("SHOULD MERGE CONFIG AND TEMPLATE TOGETHER
			 * NOW");
			 * 
			 * dsConfig = updateConfigToTemplate(dsConfig,1); // convert config
			 * to latest version using xslt dsConfig =
			 * getXSLTransformedConfig(dsConfig); doc =
			 * MartEditor.getDatasetConfigXMLUtils
			 * ().getDocumentForDatasetConfig(dsConfig); } else{
			 * generateTemplateXML(dsConfig); }
			 */

			// trial move from above
			sql = "DELETE FROM " + this.getSchema()[0] + "."
					+ this.MARTTEMPLATEMAINTABLE + " WHERE dataset_id_key="
					+ datasetID;
			ps = conn.prepareStatement(sql);
			ps.executeUpdate();

			ps = conn.prepareStatement("INSERT INTO " + this.getSchema()[0]
					+ "." + this.MARTTEMPLATEMAINTABLE + " VALUES ("
					+ datasetID + ",'" + template + "')");
			ps.executeUpdate();

			final Timestamp tstamp = new Timestamp(System.currentTimeMillis());
			String[] mytimeStamp = new String[2];
			mytimeStamp = tstamp.toString().split("\\."); // avoid the milli
			// seconds in the
			// end of the string
			doc.getRootElement().setAttribute("modified", mytimeStamp[0]);
			// System.out.println(doc.getRootElement().getAttributeValue("modified").toString());

			final String insertSQL1 = "INSERT INTO "
					+ this.getSchema()[0]
					+ "."
					+ metatable
					+ " (display_name, dataset, description, "
					+ "type, visible, version,dataset_id_key,modified) values (?, ?, ?, ?, ?, ?,?,NOW())";
			final String insertSQL2 = "INSERT INTO " + this.getSchema()[0]
					+ "." + this.MARTXMLTABLE
					+ " (dataset_id_key, xml, compressed_xml, "
					+ "message_digest) values (?, ?, ?, ?)";

			if (this.logger.isLoggable(Level.FINE))
				this.logger.fine("\ninserting with SQL " + insertSQL1 + "\n");

			final MessageDigest md5digest = MessageDigest
					.getInstance(DatasetConfigXMLUtils.DEFAULTDIGESTALGORITHM);
			final ByteArrayOutputStream bout = new ByteArrayOutputStream();
			final GZIPOutputStream gout = new GZIPOutputStream(bout);
			final DigestOutputStream out = new DigestOutputStream(gout,
					md5digest);
			// should calculate digest on unzipped data, eg, bytes before they
			// are sent to gout.write

			final XMLOutputter xout = new XMLOutputter(
					org.jdom.output.Format.getRawFormat());

			xout.output(doc, out);
			gout.finish();

			final byte[] xml = bout.toByteArray();

			final byte[] md5 = md5digest.digest();
			// recover uncompressed XML as well
			final ByteArrayOutputStream bout2 = new ByteArrayOutputStream();
			final DigestOutputStream dout = new DigestOutputStream(bout2,
					md5digest);
			final XMLOutputter xout2 = new XMLOutputter(
					org.jdom.output.Format.getRawFormat());
			xout2.output(doc, dout);

			final byte[] uncompressedXML = bout2.toByteArray();

			bout.close();
			gout.close();
			out.close();
			// System.out.println("ABOUT TO DEL");
			final int rowstodelete = this.getDSConfigEntryCountFor(metatable,
					datasetID, internalName);
			// System.out.println("ROWS "+rowstodelete);
			if (rowstodelete > 0)
				this.deleteOldDSConfigEntriesFor(metatable, datasetID,
						internalName);

			final PreparedStatement ps1 = conn.prepareStatement(insertSQL1);
			final PreparedStatement ps2 = conn.prepareStatement(insertSQL2);

			// redo displayName and version as updateConfigToTemplate may have
			// changed
			// ? why don't get all settings direct from dsConfig and get rid of
			// method params
			displayName = dsConfig.getDisplayName();
			version = dsConfig.getVersion();

			// ps.setString(1, internalName);
			ps1.setString(1, displayName);
			ps1.setString(2, dataset);
			ps1.setString(3, description);
			ps2.setInt(1, Integer.parseInt(datasetID));
			// ps2.setString(1, datasetID);
			// ps2.setClob(2, (Clob) new ByteArrayInputStream(uncompressedXML));
			ps2.setBinaryStream(2, new ByteArrayInputStream(uncompressedXML),
					uncompressedXML.length);// uncompressed
			ps2.setBinaryStream(3, new ByteArrayInputStream(xml), xml.length);// compressed
			ps2.setBytes(4, md5);
			ps1.setString(4, type);
			// ps1.setString(5, visible);
			ps1.setInt(5, Integer.parseInt(visible));
			ps1.setString(6, version);
			ps1.setInt(7, Integer.parseInt(datasetID));
			// ps1.setString(7, datasetID);

			// Timestamp tstamp = new Timestamp(System.currentTimeMillis());
			//ps1.setTimestamp(8, tstamp); fails with mysql strict mode, which we we are using as of July 2015
			//ps1.setString(8, mytimeStamp[0] );

			int ret = ps1.executeUpdate();
			ret = ps2.executeUpdate();
			// ret = ps3.executeUpdate();

			ps.close();
			ps1.close();
			ps2.close();
			// ps3.close();
			conn.close();
			return ret;
		} catch (final IOException e) {
			throw new ConfigurationException(
					"Caught IOException writing out xml to OutputStream: "
							+ e.getMessage());
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQLException updating xml for " + internalName
							+ ", " + displayName + ": " + e.getMessage());
		} catch (final NoSuchAlgorithmException e) {
			throw new ConfigurationException(
					"Caught NoSuchAlgorithmException updating xml for "
							+ internalName + ", " + displayName + ": "
							+ e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	private int storeCompressedXMLOracle(final String user,
			final String internalName, final String displayName,
			final String dataset, final String description, final Document doc,
			final String type, final String visible, final String version,
			String datasetID, final String martUsers, final String interfaces,
			final DatasetConfig dsConfig) throws ConfigurationException {
		if (this.readonly)
			throw new ConfigurationException(
					"Cannot store config into read-only database");

		Connection conn = null;
		try {
			// String metatable = getDSConfigTableFor(user);
			final String metatable = this.createMetaTables(user);

			conn = this.dsource.getConnection();
			conn.setAutoCommit(false);
			// Name/version/type already exists? Reuse it.
			if (datasetID == null || datasetID.equals("")) {
				String sql = "SELECT dataset_id_key FROM "
						+ this.getSchema()[0] + "." + metatable
						+ " where dataset='" + dataset + "' and type='" + type
						+ "'";// ? and version=?";
				if (displayName != null && !displayName.equals(""))
					sql += " and display_name='" + displayName + "'";
				if (version != null && !version.equals(""))
					sql += " and version='" + version + "'";
				final PreparedStatement ps = conn.prepareStatement(sql);
				// ps.setString(1, displayName);
				// ps.setString(2, dataset);
				// ps.setString(3, type);
				// ps.setString(4, version);
				final ResultSet rs = ps.executeQuery();
				if (rs.next())
					datasetID = "" + rs.getInt(1);
				rs.close();
				ps.close();
			}

			// Doesn't already exist? Create a new one.
			if (datasetID == null || datasetID.equals("")) {
				final String sql = "SELECT MAX(dataset_id_key) FROM "
						+ this.getSchema()[0] + "." + this.BASEMETATABLE;
				final PreparedStatement ps = conn.prepareStatement(sql);
				final ResultSet rs = ps.executeQuery();
				rs.next();
				int result = rs.getInt(1);
				result++;
				final Integer datasetNo = new Integer(result);
				datasetID = datasetNo.toString();
				rs.close();
				ps.close();
			}

			// sort out meta_users and meta_interfaces tables first
			String sql = "DELETE FROM " + this.getSchema()[0] + "."
					+ this.MARTUSERTABLE + " WHERE dataset_id_key=" + datasetID;
			System.out.println(sql);
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			final String[] martUserEntries = martUsers.split(",");
			for (int i = 0; i < martUserEntries.length; i++) {
				sql = "INSERT INTO " + this.getSchema()[0] + "."
						+ this.MARTUSERTABLE + " VALUES (" + datasetID + ",'"
						+ martUserEntries[i] + "')";
				System.out.println(sql);
				ps = conn.prepareStatement(sql);
				ps.executeUpdate();
			}

			sql = "DELETE FROM " + this.getSchema()[0] + "."
					+ this.MARTINTERFACETABLE + " WHERE dataset_id_key="
					+ datasetID;
			System.out.println(sql);
			ps = conn.prepareStatement(sql);
			ps.executeUpdate();
			final String[] interfaceEntries = interfaces.split(",");
			for (int i = 0; i < interfaceEntries.length; i++) {
				System.out.println(sql);
				ps = conn.prepareStatement("INSERT INTO " + this.getSchema()[0]
						+ "." + this.MARTINTERFACETABLE + " VALUES ("
						+ datasetID + ",'" + interfaceEntries[i] + "')");
				ps.executeUpdate();
			}

			// add new template setting
			final String template = dsConfig.getTemplate();
			/*
			 * if (template.equals("")){ template = dataset;
			 * dsConfig.setTemplate(template); }
			 */
			sql = "DELETE FROM " + this.getSchema()[0] + "."
					+ this.MARTTEMPLATEMAINTABLE + " WHERE dataset_id_key="
					+ datasetID;
			ps = conn.prepareStatement(sql);
			ps.executeUpdate();

			ps = conn.prepareStatement("INSERT INTO " + this.getSchema()[0]
					+ "." + this.MARTTEMPLATEMAINTABLE + " VALUES ("
					+ datasetID + ",'" + template + "')");
			ps.executeUpdate();

			/*
			 * sql = "SELECT count(*) FROM
			 * "+getSchema()[0]+"."+MARTTEMPLATEMAINTABLE+" WHERE
			 * template='"+template+"'"; ps = conn.prepareStatement(sql);
			 * ResultSet rs = ps.executeQuery(); rs.next(); int result =
			 * rs.getInt(1); if (result > 1){//usual 1:1 dataset:template do not
			 * get template merging System.out.println("SHOULD MERGE CONFIG AND
			 * TEMPLATE TOGETHER NOW"); /* Merge dsConfig and existing template:
			 * - completely new filters and atts get added to template -
			 * existing ones get fitted to template layout in dsConfig - method
			 * should store template XML once done and return edited dsConfig
			 * 
			 * dsConfig = updateConfigToTemplate(dsConfig); doc =
			 * MartEditor.getDatasetConfigXMLUtils
			 * ().getDocumentForDatasetConfig(dsConfig); } else{
			 * System.out.println("OVERWRITE TEMPLATE WITH THIS LAYOUT");
			 * generateTemplateXML(dsConfig); }
			 */

			final Timestamp tstamp = new Timestamp(System.currentTimeMillis());
			String[] mytimeStamp = new String[2];
			mytimeStamp = tstamp.toString().split("\\."); // avoid the milli
			// seconds in the
			// end of the string
			doc.getRootElement().setAttribute("modified", mytimeStamp[0]);

			// String insertSQL = INSERTCOMPRESSEDXMLA + metatable +
			// INSERTCOMPRESSEDXMLB;
			final String insertSQL1 = "INSERT INTO "
					+ this.getSchema()[0]
					+ "."
					+ metatable
					+ " (display_name, dataset, description, "
					+ "type, visible, version,dataset_id_key,modified) values (?, ?, ?, ?, ?, ?,?,?)";
			final String insertSQL2 = "INSERT INTO " + this.getSchema()[0]
					+ "." + this.MARTXMLTABLE
					+ " (dataset_id_key, compressed_xml, "
					+ "message_digest) values (?, ?, ?)";

			// String oraclehackSQL = SELECTCOMPRESSEDXMLFORUPDATE + metatable +
			// GETANYNAMESWHERINAME + " FOR UPDATE";
			final String oraclehackSQL = "SELECT compressed_xml FROM "
					+ this.getSchema()[0] + "." + this.MARTXMLTABLE
					+ " WHERE dataset_id_key = ? FOR UPDATE";

			if (this.logger.isLoggable(Level.FINE))
				this.logger.fine("\ninserting with SQL " + insertSQL1
						+ "\nOracle: " + oraclehackSQL + "\n");

			final MessageDigest md5digest = MessageDigest
					.getInstance(DatasetConfigXMLUtils.DEFAULTDIGESTALGORITHM);
			final ByteArrayOutputStream bout = new ByteArrayOutputStream();
			final GZIPOutputStream gout = new GZIPOutputStream(bout);
			final DigestOutputStream out = new DigestOutputStream(gout,
					md5digest);
			// should calculate digest on unzipped data, eg, bytes before they
			// are sent to gout.write

			final XMLOutputter xout = new XMLOutputter(
					org.jdom.output.Format.getRawFormat());

			xout.output(doc, out);
			gout.finish();

			final byte[] xml = bout.toByteArray();
			final byte[] md5 = md5digest.digest();
			bout.close();
			gout.close();
			out.close();

			final int rowstodelete = this.getDSConfigEntryCountFor(metatable,
					datasetID, internalName);

			if (rowstodelete > 0)
				this.deleteOldDSConfigEntriesFor(metatable, datasetID,
						internalName);

			final PreparedStatement ps1 = conn.prepareStatement(insertSQL1);
			final PreparedStatement ps2 = conn.prepareStatement(insertSQL2);
			final PreparedStatement ohack = conn
					.prepareStatement(oraclehackSQL);

			// ps.setString(1, internalName);
			// ohack.setString(1, internalName);
			ps1.setString(1, displayName);
			ps1.setString(2, dataset);
			ohack.setString(1, datasetID);
			ps1.setString(3, description);
			ps2.setString(1, datasetID);
			ps2.setBlob(2, BLOB.empty_lob());
			ps2.setBytes(3, md5);
			ps1.setString(4, type);
			ps1.setString(5, visible);
			ps1.setString(6, version);
			ps1.setString(7, datasetID);

			// Timestamp tstamp = new Timestamp(System.currentTimeMillis());
			ps1.setTimestamp(8, tstamp);

			int ret = ps1.executeUpdate();
			ret = ps2.executeUpdate();

			final ResultSet rs = ohack.executeQuery();

			if (rs.next()) {
				final BLOB blob = (BLOB) rs.getBlob(1);

				final OutputStream blobout = blob.getBinaryOutputStream();
				blobout.write(xml);
				blobout.close();
			}

			conn.commit();
			rs.close();
			ohack.close();
			ps1.close();
			ps2.close();
			ps.close();
			conn.close();
			return ret;
		} catch (final IOException e) {
			throw new ConfigurationException(
					"Caught IOException writing out xml to OutputStream: "
							+ e.getMessage(), e);
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQLException updating xml for " + internalName
							+ ", " + displayName + ": " + e.getMessage(), e);
		} catch (final NoSuchAlgorithmException e) {
			throw new ConfigurationException(
					"Caught NoSuchAlgorithmException updating xml for "
							+ internalName + ", " + displayName + ": "
							+ e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	private void updateMartConfigForUser(final String user, final String schema)
			throws ConfigurationException {
		this.configInfo = new HashMap();
		this.initMartConfigForUser(user, schema, "");
	}

	private void initMartConfigForUser(final String user, final String schema,
			final String martUser) throws ConfigurationException {
		if (!this.configInfo.containsKey(user)) {
			final HashMap userMap = new HashMap();
			this.configInfo.put(user, userMap);
		}

		Connection conn = null;
		try {
			// BELOW NEEDS CHANGING SO NOT CALLED FROM EXPLORER
			final String metatable = this.createMetaTables(user);

			String sql;
			if (martUser != null && !martUser.equals("")) {
				if (!this.dscutils.includeHiddenMembers)
					sql = "SELECT interface, display_name, dataset, description, message_digest, type, visible, version,"
							+ "md.dataset_id_key, modified "
							+ "FROM "
							+ schema
							+ "."
							+ this.MARTINTERFACETABLE
							+ " mi, "
							+ schema
							+ "."
							+ this.BASEMETATABLE
							+ " md, "
							+ schema
							+ "."
							+ this.MARTXMLTABLE
							+ " mx, "
							+ schema
							+ "."
							+ this.MARTUSERTABLE
							+ " mu "
							+ "WHERE mu.dataset_id_key=md.dataset_id_key  AND md.dataset_id_key=mx.dataset_id_key AND md.dataset_id_key=mi.dataset_id_key AND mart_user = '"
							+ martUser + "' " + " AND visible = 1";
				else
					sql = "SELECT interface, display_name, dataset, description, message_digest, type, visible, version,"
							+ "md.dataset_id_key, modified "
							+ "FROM "
							+ schema
							+ "."
							+ this.MARTINTERFACETABLE
							+ " mi, "
							+ schema
							+ "."
							+ this.BASEMETATABLE
							+ " md, "
							+ schema
							+ "."
							+ this.MARTXMLTABLE
							+ " mx, "
							+ schema
							+ "."
							+ this.MARTUSERTABLE
							+ " mu "
							+ "WHERE mu.dataset_id_key=md.dataset_id_key  AND md.dataset_id_key=mx.dataset_id_key AND md.dataset_id_key=mi.dataset_id_key AND mart_user = '"
							+ martUser + "'";
			} else if (!this.dscutils.includeHiddenMembers)
				sql = "SELECT interface, display_name, dataset, description, message_digest, type, visible, version,"
						+ "md.dataset_id_key, modified "
						+ "FROM "
						+ schema
						+ "."
						+ this.MARTINTERFACETABLE
						+ " mi, "
						+ schema
						+ "."
						+ this.BASEMETATABLE
						+ " md "
						+ schema
						+ "."
						+ this.MARTXMLTABLE
						+ " mx "
						+ "WHERE md.dataset_id_key=mi.dataset_id_key  AND md.dataset_id_key=mx.dataset_id_key AND visible = 1";
			else
				sql = "SELECT interface, display_name, dataset, description, message_digest, type, visible, version,"
						+ "md.dataset_id_key, modified "
						+ "FROM "
						+ schema
						+ "."
						+ this.MARTINTERFACETABLE
						+ " mi, "
						+ schema
						+ "."
						+ this.BASEMETATABLE
						+ " md, "
						+ schema
						+ "."
						+ this.MARTXMLTABLE
						+ " mx "
						+ "WHERE md.dataset_id_key=mi.dataset_id_key AND md.dataset_id_key=mx.dataset_id_key";

			if (this.logger.isLoggable(Level.FINE))
				this.logger.fine("Using " + sql
						+ " to get unloaded DatasetConfigs for user " + user
						+ "\n");

			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn.prepareStatement(sql);

			final ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				final String iname = rs.getString(1);
				final String dname = rs.getString(2);
				final String dset = rs.getString(3);
				final String description = rs.getString(4);
				final String type = rs.getString(6);
				final String visible = rs.getString(7);
				final String version = rs.getString(8);
				final String datasetID = rs.getString(9);
				final byte[] digest = rs.getBytes(5);
				final String modified = rs.getString(10);
				String martUsers = "";
				String comma = "";
				try {
					final PreparedStatement martUserStatement = conn
							.prepareStatement("SELECT mart_user FROM " + schema
									+ "." + this.MARTUSERTABLE
									+ " WHERE dataset_id_key=" + datasetID);
					final ResultSet martUserResultSet = martUserStatement
							.executeQuery();
					while (martUserResultSet.next()) {
						martUsers += comma + martUserResultSet.getString(1);
						comma = ",";
					}
				} catch (final SQLException e) {
					System.out.println("PROBLEM QUERYING " + this.MARTUSERTABLE
							+ " TABLE " + e.toString());
				}
				String interfaces = "";
				comma = "";
				try {
					final PreparedStatement interfacesStatement = conn
							.prepareStatement("SELECT interface FROM " + schema
									+ "." + this.MARTINTERFACETABLE + ""
									+ " WHERE dataset_id_key=" + datasetID);
					final ResultSet interfaceResultSet = interfacesStatement
							.executeQuery();
					while (interfaceResultSet.next()) {
						interfaces += comma + interfaceResultSet.getString(1);
						comma = ",";
					}
				} catch (final SQLException e) {
					System.out.println("PROBLEM QUERYING "
							+ this.MARTINTERFACETABLE + " TABLE "
							+ e.toString());
				}
				// always set internalName of dataset to default - not really
				// used anywhere now
				// internalName can probably be safely removed from
				// DatasetConfig or at least from constructor
				final DatasetConfig dsv = new DatasetConfig("default", dname,
						dset, description, type, visible, "", version, "",
						datasetID, modified, martUsers, interfaces, "", "", "",
						"", "", "");
				dsv.setMessageDigest(digest);

				final HashMap userMap = (HashMap) this.configInfo.get(user);

				if (!userMap.containsKey(dset)) {
					final HashMap dsetMap = new HashMap();
					userMap.put(dset, dsetMap);
				}

				final HashMap dsetMap = (HashMap) userMap.get(dset);
				// dsetMap.put(iname, dsv);
				dsetMap.put(datasetID, dsv);
			}
			rs.close();
			conn.close();
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQL Exception during fetch of requested digest: "
							+ e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	/**
	 * Returns all template names from the meta_template__xml__dm table for the
	 * given user.
	 * 
	 * @param user
	 *            -- user for meta_configuration table, if
	 *            meta_configuration_user does not exist, meta_configuration is
	 *            attempted.
	 * @return String[] dataset names
	 * @throws ConfigurationException
	 *             when valid meta_configuration table does not exist, and for
	 *             all underlying SQL Exceptions
	 */
	public String[] getAllTemplateNames() throws ConfigurationException {

		Connection conn = null;

		try {
			final String sql = "SELECT template FROM " + this.getSchema()[0]
					+ "." + this.MARTTEMPLATEDMTABLE;
			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn.prepareStatement(sql);
			final ResultSet rs = ps.executeQuery();
			final List results = new ArrayList();
			while (rs.next())
				results.add(rs.getString(1));
			rs.close();
			ps.close();
			final String[] templateNames = new String[results.size()];
			results.toArray(templateNames);
			conn.close();
			return templateNames;
		} catch (final SQLException e) {
			System.out.println("PROBLEM QUERYING " + this.MARTTEMPLATEDMTABLE
					+ " TABLE " + e.toString());
			return null;
		} finally {
			DetailedDataSource.close(conn);
		}

	}

	/**
	 * Returns all datasets names for the given template
	 * 
	 * @param template
	 *            .
	 * @return String[] dataset names
	 * @throws ConfigurationException
	 *             when valid meta_configuration table does not exist, and for
	 *             all underlying SQL Exceptions
	 */
	public String[] getDatasetNamesForTemplate(final String template)
			throws ConfigurationException {

		Connection conn = null;

		try {
			final String sql = "SELECT c.dataset FROM "
					+ this.getSchema()[0]
					+ "."
					+ this.BASEMETATABLE
					+ " c, "
					+ this.getSchema()[0]
					+ "."
					+ this.MARTTEMPLATEMAINTABLE
					+ " t where c.dataset_id_key=t.dataset_id_key and t.template = ?";
			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, template);
			final ResultSet rs = ps.executeQuery();
			final List results = new ArrayList();
			while (rs.next())
				results.add(rs.getString(1));
			rs.close();
			ps.close();

			final String[] templateNames = new String[results.size()];
			results.toArray(templateNames);
			return templateNames;
		} catch (final SQLException e) {
			System.out.println("PROBLEM QUERYING " + this.MARTTEMPLATEMAINTABLE
					+ " TABLE " + e.toString());
			return null;
		} finally {
			DetailedDataSource.close(conn);
		}

	}

	/**
	 * Returns either the template from the meta_template__template__main table
	 * or if there is noy yet a template the dataset name from
	 * meta_conf__dataset__main table.
	 * 
	 * @return HashMap keyed on config name with value = 1 if a template, 0 if
	 *         just a datasetConfig
	 * @throws ConfigurationException
	 *             for all underlying SQL Exceptions
	 */
	public Collection getImportOptions() throws ConfigurationException {

		Connection conn = null;

		try {
			this.setReadonly(false);// needed to make sure template tables are
			// created if missing
			this.createMetaTables("");
			this.setReadonly(true);
			final HashSet importOptions = new HashSet();
			String sql = null;
			if ("oracle".equals(this.dsource.getDatabaseType()))
				sql = "SELECT DISTINCT template " + "FROM "
						+ this.getSchema()[0] + "."
						+ this.MARTTEMPLATEMAINTABLE;
			else if ("postgres".equals(this.dsource.getDatabaseType()))
				sql = "SELECT DISTINCT template " + "FROM "
						+ this.getSchema()[0] + "."
						+ this.MARTTEMPLATEMAINTABLE;
			else
				sql = "SELECT DISTINCT template " + "FROM "
						+ this.getSchema()[0] + "."
						+ this.MARTTEMPLATEMAINTABLE;

			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn.prepareStatement(sql);
			final ResultSet rs = ps.executeQuery();
			while (rs.next())
				importOptions.add(rs.getString(1));
			conn.close();
			return importOptions;
		} catch (final SQLException e) {
			System.out.println("PROBLEM QUERYING " + this.MARTTEMPLATEDMTABLE
					+ " TABLE " + e.toString());
			return null;
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	/**
	 * Returns all dataset names from the meta_configuration table for the given
	 * user.
	 * 
	 * @param user
	 *            -- user for meta_configuration table, if
	 *            meta_configuration_user does not exist, meta_configuration is
	 *            attempted.
	 * @return String[] dataset names
	 * @throws ConfigurationException
	 *             when valid meta_configuration table does not exist, and for
	 *             all underlying SQL Exceptions
	 */
	public String[] getAllDatasetNames(final String user, final String martUser)
			throws ConfigurationException {
		if (!this.configInfo.containsKey(user))
			this.initMartConfigForUser(user, this.getSchema()[0], martUser);

		final HashMap userMap = (HashMap) this.configInfo.get(user);

		// sort the names alphabetically
		final SortedSet names = new TreeSet(userMap.keySet());

		final String[] ret = new String[names.size()];
		names.toArray(ret);
		return ret;
	}

	/**
	 * Returns all of the internalNames for the given dataset, as stored in the
	 * meta_configuration table for the Mart Database for the given user.
	 * 
	 * @param user
	 *            -- user for meta_configuration table, if
	 *            meta_configuration_user does not exist, meta_configuration is
	 *            attempted.
	 * @param dataset
	 *            -- dataset for which internalNames are requested
	 * @return String[] containing all of the internalNames for the requested
	 *         dataset.
	 * @throws ConfigurationException
	 *             when valid meta_configuration tables do not exist, and for
	 *             all underlying Exceptons.
	 */
	/*
	 * public String[] getAllInternalNamesForDataset(String user, String
	 * dataset) throws ConfigurationException { if
	 * (!configInfo.containsKey(user))
	 * initMartConfigForUser(user,getSchema()[0],"");
	 * 
	 * HashMap userMap = (HashMap) configInfo.get(user);
	 * 
	 * if (!userMap.containsKey(dataset))
	 * initMartConfigForUser(user,getSchema()[0],"");
	 * 
	 * if (!userMap.containsKey(dataset)) return new String[0];
	 * 
	 * HashMap dsetMap = (HashMap) userMap.get(dataset);
	 * 
	 * //sorted alphabetically SortedSet names = new TreeSet(dsetMap.keySet());
	 * 
	 * String[] ret = new String[names.size()]; names.toArray(ret); return ret;
	 * }
	 */

	/**
	 * Returns all of the datsetIDs for the given dataset, as stored in the
	 * meta_configuration table for the Mart Database.
	 * 
	 * @param dataset
	 *            -- dataset for which datasetIDs are requested
	 * @return String[] containing all of the datasetIDs for the requested
	 *         dataset.
	 * @throws ConfigurationException
	 *             when valid meta_configuration tables do not exist, and for
	 *             all underlying Exceptions.
	 */
	public String[] getAllDatasetIDsForDataset(final String user,
			final String dataset) throws ConfigurationException {
		if (!this.configInfo.containsKey(user))
			this.initMartConfigForUser(user, this.getSchema()[0], "");

		final HashMap userMap = (HashMap) this.configInfo.get(user);

		if (!userMap.containsKey(dataset))
			this.initMartConfigForUser(user, this.getSchema()[0], "");

		if (!userMap.containsKey(dataset))
			return new String[0];

		final HashMap dsetMap = (HashMap) userMap.get(dataset);

		// sorted alphabetically
		final SortedSet names = new TreeSet(dsetMap.keySet());

		final String[] ret = new String[names.size()];
		names.toArray(ret);
		return ret;
	}

	/**
	 * Returns a DatasetConfig object from the Mart Database using a supplied
	 * DetailedDataSource for a given user, defined with the given internalName
	 * and dataset.
	 * 
	 * @param user
	 *            -- Specific User to look for meta_configuration_[user] table,
	 *            if null, or non-existent, uses meta_configuration
	 * @param dataset
	 *            -- dataset for which DatasetConfig is requested
	 * @param internalName
	 *            -- internalName of desired DatasetConfig object
	 * @return DatasetConfig defined by given internalName
	 * @throws ConfigurationException
	 *             when valid meta_configuration tables are absent, and for all
	 *             underlying Exceptions
	 */
	public DatasetConfig getDatasetConfigByDatasetID(final String user,
			final String dataset, final String datasetID, final String schema)
			throws ConfigurationException {

		if (!this.configInfo.containsKey(user))
			this.initMartConfigForUser(user, schema, "");

		final HashMap userMap = (HashMap) this.configInfo.get(user);

		if (!userMap.containsKey(dataset))
			this.initMartConfigForUser(user, schema, "");

		if (!userMap.containsKey(dataset))
			return null;

		final HashMap dsetMap = (HashMap) userMap.get(dataset);

		if (!dsetMap.containsKey(datasetID))
			this.initMartConfigForUser(user, schema, "");

		// hack for push action handling - only the dataset is known in the
		// current model - just return the first dataset
		if (datasetID.equals(""))
			return (DatasetConfig) (dsetMap.values().toArray())[0];

		if (!dsetMap.containsKey(datasetID))
			return null;

		final DatasetConfig dsv = (DatasetConfig) dsetMap.get(datasetID);

		return dsv;
	}

	/**
	 * Returns a DatasetConfig JDOM Document from the Mart Database using a
	 * supplied DetailedDataSource for a given user, defined with the given
	 * internalName and dataset.
	 * 
	 * @param user
	 *            -- Specific User to look for meta_configuration_[user] table,
	 *            if null, or non-existent, uses meta_configuration
	 * @param dataset
	 *            -- dataset for which DatasetConfig document is requested
	 * @param internalName
	 *            -- internalName of desired DatasetConfig document
	 * @return DatasetConfig JDOM Document defined by given displayName and
	 *         dataset
	 * @throws ConfigurationException
	 *             when valid meta_configuration tables are absent, and for all
	 *             underlying Exceptions
	 */
	public Document getDatasetConfigDocumentByDatasetID(final String user,
			final String dataset, final String datasetID, final String schema)
			throws ConfigurationException {
		if (this.dsource.getJdbcDriverClassName().indexOf("oracle") >= 0) {
			System.out.println("Driver is Oracle");
			return this.getDatasetConfigDocumentByDatasetIDOracle(user,
					dataset, datasetID);
		}

		Connection conn = null;
		try {
			final String metatable = this.createMetaTables(user);
			final String sql = "select xml, compressed_xml from "
					+ schema
					+ "."
					+ this.MARTXMLTABLE
					+ " mx, "
					+ schema
					+ "."
					+ metatable
					+ " md where md.dataset_id_key=mx.dataset_id_key and md.dataset_id_key = ? and md.dataset = ?";

			this.logger.info("Using " + sql
					+ " to get DatasetConfig for datasetID " + datasetID
					+ "and dataset " + dataset + "\n");

			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn.prepareStatement(sql);
			// ps.setString(1, datasetID);
			ps.setInt(1, Integer.parseInt(datasetID));
			ps.setString(2, dataset);

			final ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				// will only get one result
				rs.close();
				conn.close();
				return null;
			}

			final byte[] stream = rs.getBytes(1);
			final byte[] cstream = rs.getBytes(2);

			rs.close();

			InputStream rstream = null;
			if (cstream != null)
				rstream = new GZIPInputStream(new ByteArrayInputStream(cstream));
			else
				rstream = new ByteArrayInputStream(stream);
			conn.close();
			Document doc = this.dscutils.getDocumentForXMLStream(rstream);
			if (doc == null) {
				System.err.println("Doc is null for datasetID " + datasetID
						+ "and dataset " + dataset);
			}
			return doc;
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQL Exception during fetch of requested DatasetConfig: "
							+ e.getMessage(), e);
		} catch (final IOException e) {
			throw new ConfigurationException(
					"Caught IOException during fetch of requested DatasetConfig: "
							+ e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	/**
	 * ...
	 * 
	 * @param config
	 * @return DatasetConfig JDOM Document
	 * @throws ConfigurationException
	 *             when ...
	 */
	public DatasetConfig getXSLTransformedConfig(final DatasetConfig config)
			throws ConfigurationException {

		try {

			final Document sourceDoc = MartEditor.getDatasetConfigXMLUtils()
					.getDocumentForDatasetConfig(config);
			// Element thisElement = sourceDoc.getRootElement();
			// String template = thisElement.getAttributeValue("template", "");
			System.out.println("XSLT TRANSFORMING");

			// 0.4 to 0.5 transform
			final InputStream xsl05 = this.getClass().getClassLoader()
					.getResourceAsStream(this.XSL_05_FILE);
			final Transformer transformer05 = TransformerFactory.newInstance()
					.newTransformer(new StreamSource(xsl05));
			final JDOMResult out05 = new JDOMResult();
			transformer05.transform(new JDOMSource(sourceDoc), out05);

			// 0.5 to 0.6 transform

			InputStream xsl06 = this.getClass().getClassLoader()
					.getResourceAsStream(XSL_06_FILE);
			Transformer transformer06 = TransformerFactory.newInstance()
					.newTransformer(new StreamSource(xsl06));
			JDOMResult out06 = new JDOMResult();
			transformer06.transform(new JDOMSource(out05.getDocument()), out06);

			// Final result - currently 0.6 output
			final Document resultDoc = out06.getDocument();

			final DatasetConfig newConfig = new DatasetConfig(
					config.getInternalName(), config.getDisplayName(),
					config.getDataset(), config.getDescription(),
					config.getType(), config.getVisible(),
					config.getVisibleFilterPage(), config.getVersion(),
					config.getOptionalParameter(), config.getDatasetID(),
					config.getModified(), config.getMartUsers(),
					config.getInterfaces(), config.getprimaryKeyRestriction(),
					config.getTemplate(), this.SOFTWAREVERSION,
					config.getNoCount(), config.getEntryLabel(),
					config.getSplitNameUsing());
			this.dscutils.loadDatasetConfigWithDocument(newConfig, resultDoc);
			newConfig.setTemplate(config.getTemplate());// hack as for some
			// reason sourceDoc has
			// template set to
			// dataset and hence
			// lose true template
			newConfig.setTemplateFlag(config.getTemplateFlag());
			newConfig.setSoftwareVersion(this.SOFTWAREVERSION);
			newConfig.setNoCount(config.getNoCount());
			return newConfig;

		} catch (final Exception e) {
			throw new ConfigurationException(
					"Caught Exception during transformation of requested DatasetConfig: "
							+ e.getMessage(), e);
		}

	}

	/**
	 * Returns a DatasetConfig JDOM Document from the Mart Database using a
	 * supplied DetailedDataSource for a given user, defined with the given
	 * internalName and dataset.
	 * 
	 * @param user
	 *            -- Specific User to look for meta_configuration_[user] table,
	 *            if null, or non-existent, uses meta_configuration
	 * @param dataset
	 *            -- dataset for which DatasetConfig document is requested
	 * @param internalName
	 *            -- internalName of desired DatasetConfig document
	 * @return DatasetConfig JDOM Document defined by given displayName and
	 *         dataset
	 * @throws ConfigurationException
	 *             when valid meta_configuration tables are absent, and for all
	 *             underlying Exceptions
	 */
	public Document getTemplateDocument(final String template)
			throws ConfigurationException {
		// if (dsource.getJdbcDriverClassName().indexOf("oracle") >= 0)
		// return getTemplateDocumentOracle(template);

		Connection conn = null;
		try {
			final String sql = "select compressed_xml from "
					+ this.getSchema()[0] + "." + this.MARTTEMPLATEDMTABLE
					+ " where template = ?";

			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, template);

			final ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				// will only get one result
				rs.close();
				conn.close();
				return null;
			}

			InputStream rstream;
			if (this.dsource.getJdbcDriverClassName().indexOf("oracle") >= 0) {
				final BLOB cstream = (BLOB) rs.getBlob(1);
				rstream = new GZIPInputStream(cstream.getBinaryStream());
			} else {
				final byte[] cstream = rs.getBytes(1);
				rstream = new GZIPInputStream(new ByteArrayInputStream(cstream));
			}
			conn.close();
			return this.dscutils.getDocumentForXMLStream(rstream);

			// } catch (SQLException e) {
			// throw new ConfigurationException(
			// "Caught SQL Exception during fetch of requested DatasetConfig: "
			// + e.getMessage(),
			// e);
			// } catch (IOException e) {
			// throw new ConfigurationException(
			// "Caught IOException during fetch of requested DatasetConfig: " +
			// e.getMessage(),
			// e);
		} catch (final Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	private Document getDatasetConfigDocumentByDatasetIDOracle(
			final String user, final String dataset, final String datasetID)
			throws ConfigurationException {
		Connection conn = null;
		try {
			final String metatable = this.createMetaTables(user);
			final String sql = "select xml, compressed_xml from "
					+ this.getSchema()[0] + "." + this.MARTXMLTABLE
					+ " where dataset_id_key = ?";

			if (this.logger.isLoggable(Level.FINE))
				this.logger.fine("Using " + sql
						+ " to get DatasetConfig for datasetID " + datasetID
						+ "and dataset " + dataset + "\n");

			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, datasetID);
			// ps.setString(2, dataset);

			final ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				// will only get one result
				rs.close();
				conn.close();
				return null;
			}

			final CLOB stream = (CLOB) rs.getClob(1);
			final BLOB cstream = (BLOB) rs.getBlob(2);

			InputStream rstream = null;
			if (cstream != null)
				rstream = new GZIPInputStream(cstream.getBinaryStream());
			else
				rstream = stream.getAsciiStream();

			final Document ret = this.dscutils.getDocumentForXMLStream(rstream);
			rstream.close();
			rs.close();
			return ret;
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQL Exception during fetch of requested DatasetConfig: "
							+ e.getMessage(), e);
		} catch (final IOException e) {
			throw new ConfigurationException(
					"Caught IOException during fetch of requested DatasetConfig: "
							+ e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	/**
	 * Returns a DatasetConfig object from the Mart Database using a supplied
	 * DetailedDataSource for a given user, defined with the given dataset and
	 * displayName
	 * 
	 * @param dataset
	 *            -- dataset for which DatsetConfig is requested
	 * @param user
	 *            -- Specific User to look for meta_configuration_[user] table,
	 *            if null, or non-existent, uses meta_configuration
	 * @param displayName
	 *            -- String displayName for requested DatasetConfig
	 * @return DatasetConfig with given displayName and dataset
	 * @throws ConfigurationException
	 *             when valid meta_configuration tables are absent, and for all
	 *             underlying Exceptions
	 */
	// public DatasetConfig getDatasetConfigByDatasetDisplayName(String user,
	// String dataset, String displayName)
	// throws ConfigurationException {
	// Connection conn = null;
	// try {
	// String metatable = getDSConfigTableFor(user);
	// String sql = GETALLNAMESQL + metatable + GETANYNAMESWHEREDNAME;
	//
	// if (logger.isLoggable(Level.FINE))
	// logger.fine(
	// "Using " + sql + " to get DatasetConfig for displayName " + displayName +
	// "and dataset " + dataset + "\n");
	//
	// conn = dsource.getConnection();
	// PreparedStatement ps = conn.prepareStatement(sql);
	// ps.setString(1, displayName);
	// ps.setString(2, dataset);
	//
	// ResultSet rs = ps.executeQuery();
	// if (!rs.next()) {
	// // will only get one result
	// rs.close();
	// conn.close();
	// return null;
	// }
	//
	// String iname = rs.getString(1);
	// String dname = rs.getString(2);
	// String dprefix = rs.getString(3);
	// String description = rs.getString(4);
	// byte[] digest = rs.getBytes(5);
	// rs.close();
	//
	// DatasetConfig dsv = new DatasetConfig(iname, dname, dprefix,
	// description);
	// dsv.setMessageDigest(digest);
	// return dsv;
	// } catch (SQLException e) {
	// throw new ConfigurationException("Caught SQL Exception during fetch of
	// requested digest: " + e.getMessage(), e);
	// } finally {
	// DetailedDataSource.close(conn);
	// }
	// }
	/**
	 * Returns a DatasetConfig JDOM Document for a given user, dataset, and
	 * displayName
	 * 
	 * @param user
	 *            -- mart user. Determines which meta table to extract
	 *            DatasetConfig information from.
	 * @param dataset
	 *            -- dataset of required DatasetConfig
	 * @param displayName
	 *            -- displayName of required DatasetConfig
	 * @return JDOM Document for required DatasetConfig XML
	 * @throws ConfigurationException
	 *             for all underlying Exceptions
	 */
	// public Document getDatasetConfigDocumentByDatasetDisplayName(String user,
	// String dataset, String displayName)
	// throws ConfigurationException {
	// if (dsource.getJdbcDriverClassName().indexOf("oracle") >= 0)
	// return getDatasetConfigDocumentByDatasetDisplayNameOracle(user, dataset,
	// displayName);
	//
	// Connection conn = null;
	// try {
	// String metatable = getDSConfigTableFor(user);
	// String sql = GETDOCBYDNAMESELECT + metatable + GETDOCBYDNAMEWHERE;
	//
	// if (logger.isLoggable(Level.FINE))
	// logger.fine(
	// "Using "
	// + sql
	// + " to get DatasetConfig Document for displayName "
	// + displayName
	// + " and dataset "
	// + dataset
	// + "\n");
	//
	// conn = dsource.getConnection();
	// PreparedStatement ps = conn.prepareStatement(sql);
	// ps.setString(1, displayName);
	// ps.setString(2, dataset);
	//
	// ResultSet rs = ps.executeQuery();
	// if (!rs.next()) {
	// // will only get one result
	// rs.close();
	// conn.close();
	// return null;
	// }
	//
	// byte[] stream = rs.getBytes(1);
	// byte[] cstream = rs.getBytes(2);
	//
	// rs.close();
	//
	// InputStream rstream = null;
	// if (cstream != null)
	// rstream = new GZIPInputStream(new ByteArrayInputStream(cstream));
	// else
	// rstream = new ByteArrayInputStream(stream);
	//
	// return dscutils.getDocumentForXMLStream(rstream);
	// } catch (SQLException e) {
	// throw new ConfigurationException(
	// "Caught SQL Exception during fetch of requested DatasetConfig: " +
	// e.getMessage(),
	// e);
	// } catch (IOException e) {
	// throw new ConfigurationException(
	// "Caught IOException during fetch of requested DatasetConfig: " +
	// e.getMessage(),
	// e);
	// } catch (ConfigurationException e) {
	// throw e;
	// } finally {
	// DetailedDataSource.close(conn);
	// }
	// }
	// private Document
	// getDatasetConfigDocumentByDatasetDisplayNameOracle(String user, String
	// dataset, String displayName)
	// throws ConfigurationException {
	//
	// Connection conn = null;
	// try {
	// String metatable = getDSConfigTableFor(user);
	// String sql = GETDOCBYDNAMESELECT + metatable + GETDOCBYDNAMEWHERE;
	//
	// if (logger.isLoggable(Level.FINE))
	// logger.fine(
	// "Using " + sql + " to get DatasetConfig for displayName " + displayName +
	// "and dataset " + dataset + "\n");
	//
	// conn = dsource.getConnection();
	// PreparedStatement ps = conn.prepareStatement(sql);
	// ps.setString(1, displayName);
	// ps.setString(2, dataset);
	//
	// ResultSet rs = ps.executeQuery();
	// if (!rs.next()) {
	// // will only get one result
	// rs.close();
	// conn.close();
	// return null;
	// }
	//
	// CLOB stream = (CLOB) rs.getClob(1);
	// BLOB cstream = (BLOB) rs.getBlob(2);
	//
	// InputStream rstream = null;
	// if (cstream != null) {
	// rstream = new GZIPInputStream(cstream.getBinaryStream());
	// } else
	// rstream = stream.getAsciiStream();
	//
	// Document ret = dscutils.getDocumentForXMLStream(rstream);
	// rstream.close();
	// rs.close();
	// return ret;
	// } catch (SQLException e) {
	// throw new ConfigurationException(
	// "Caught SQL Exception during fetch of requested DatasetConfig: " +
	// e.getMessage(),
	// e);
	// } catch (IOException e) {
	// throw new ConfigurationException(
	// "Caught IOException during fetch of requested DatasetConfig: " +
	// e.getMessage(),
	// e);
	// } finally {
	// DetailedDataSource.close(conn);
	// }
	// }
	/**
	 * Get a message digest for a given DatasetConfig, given by dataset and
	 * internalName
	 * 
	 * @param user
	 *            -- user for meta_configuration_[user] table, if null,
	 *            meta_configuration is attempted
	 * @param dataset
	 *            -- dataset for which digest is requested
	 * @param internalName
	 *            -- internalName for DatasetConfig digest desired.
	 * @return byte[] digest for given dataset and displayName
	 * @throws ConfigurationException
	 *             for all underlying Exceptions
	 */
	public byte[] getDSConfigMessageDigestByDatasetID(final String user,
			final String dataset, final String datasetID)
			throws ConfigurationException {

		final DatasetConfig dsv = this.getDatasetConfigByDatasetID(user,
				dataset, datasetID, this.getSchema()[0]);
		if (dsv == null)
			return null;

		return dsv.getMessageDigest();
	}

	/**
	 * Determine if a DatasetConfig in memory is different from the one in the
	 * database by comparing the MD5SUMs
	 * 
	 * @param user
	 *            - user in database
	 * @param dsc
	 *            - DatasetConfig
	 * @return true if equal, false otherwise
	 * @throws ConfigurationException
	 *             for all underlying exceptions
	 */
	public boolean isDatasetConfigChanged(final String user,
			final DatasetConfig dsc) throws ConfigurationException {
		final byte[] thisDigest = this.dscutils
				.getMessageDigestForDatasetConfig(dsc);
		// byte[] thisDigest = dsc.getMessageDigest();
		final byte[] dbDigest = this.getDSConfigMessageDigestByDatasetID(user,
				dsc.getDataset(), dsc.getDatasetID());

		System.out.println("this digest " + thisDigest);
		System.out.println("dbDigest digest " + dbDigest);

		return MessageDigest.isEqual(thisDigest, dbDigest);
	}

	private int getDSConfigEntryCountFor(final String metatable,
			final String datasetID, final String internalName)
			throws ConfigurationException {

		// fully qualify for 'non-public' postgres schemas
		// System.out.println("DISPLAY NAME" + displayName);
		String existSQL;
		// if (displayName != null)
		existSQL = "select count(*) from " + this.getSchema()[0] + "."
				+ metatable + " where dataset_id_key = ?";
		// else
		// existSQL = "select count(*) from " + getSchema()[0]+"."+metatable +
		// ALT" where dataset_id_key = ?";

		if (this.logger.isLoggable(Level.FINE))
			this.logger.fine("Getting DSConfigEntryCount with SQL " + existSQL
					+ "\n");

		int ret = 0;
		System.out.println(existSQL);
		Connection conn = null;
		try {
			conn = this.dsource.getConnection();
			final PreparedStatement ps = conn.prepareStatement(existSQL);
			// ps.setString(1, internalName);
			// if (displayName != null)
			// ps.setString(3, displayName);
			// ps.setString(1, datasetID);
			ps.setInt(1, Integer.parseInt(datasetID));

			final ResultSet rs = ps.executeQuery();
			rs.next();
			ret = rs.getInt(1);
			rs.close();
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQL exception attempting to determing count of rows for "
							+ internalName + " from " + metatable + "\n" + e);
		} finally {
			DetailedDataSource.close(conn);
		}

		return ret;
	}

	/**
	 * Removes all records in a given metatable for the given dataset,
	 * internalName and displayName. Throws an error if the rows deleted do not
	 * equal the number of rows obtained using
	 * DatabaseDatasetConfigAdaptor.getDSConfigEntryCountFor().
	 * 
	 * @param metatable
	 *            - meta_configuration table to use to delete entries
	 * @param dataset
	 *            - dataset for DatasetConfig entries to delete from metatable
	 * @param internalName
	 *            - internalName of DatasetConfig entries to delete from
	 *            metatable
	 * @param displayName
	 *            - displayName of DatasetConfig entries to delete from
	 *            metatable
	 * @throws ConfigurationException
	 *             if number of rows to delete doesnt match number returned by
	 *             getDSConfigEntryCountFor()
	 */
	public void deleteOldDSConfigEntriesFor(final String metatable,
			final String datasetID, final String internalName)
			throws ConfigurationException {
		if (this.readonly)
			throw new ConfigurationException(
					"Cannot delete config from a read-only database");

		final String deleteSQL1 = "delete from " + this.getSchema()[0] + "."
				+ metatable + " where dataset_id_key = ?";
		final String deleteSQL2 = "delete from " + this.getSchema()[0] + "."
				+ this.MARTXMLTABLE + " where dataset_id_key = ?";

		final int rowstodelete = this.getDSConfigEntryCountFor(metatable,
				datasetID, internalName);
		if (this.logger.isLoggable(Level.FINE))
			this.logger.fine("Deleting old DSConfigEntries with SQL "
					+ deleteSQL1 + "\n");

		int rowsdeleted;

		Connection conn = null;
		try {
			conn = this.dsource.getConnection();
			PreparedStatement ds = conn.prepareStatement(deleteSQL1);
			// ds.setString(1, datasetID);
			ds.setInt(1, Integer.parseInt(datasetID));
			rowsdeleted = ds.executeUpdate();
			ds = conn.prepareStatement(deleteSQL2);
			// ds.setString(1, datasetID);
			ds.setInt(1, Integer.parseInt(datasetID));
			ds.executeUpdate();
			ds.close();
		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQLException during delete of old Entries for "
							+ internalName + " in table " + metatable + "\n");
		} finally {
			DetailedDataSource.close(conn);
		}

		if (!(rowsdeleted == rowstodelete))
			throw new ConfigurationException(
					"Did not delete old XML data rows for " + internalName
							+ ", " + datasetID + "\n");
	}

	/**
	 * Removes all records in a given metatable for the given dataset
	 * 
	 * @param dataset
	 *            - dataset for DatasetConfig entries to delete from metatable
	 * @throws ConfigurationException
	 *             if number of rows to delete doesnt match number returned by
	 *             getDSConfigEntryCountFor()
	 */
	/*
	 * public void deleteDatasetConfigsForDataset(String dataset) throws
	 * ConfigurationException { String deleteSQL1 = "delete from " +
	 * BASEMETATABLE + DELETEDATASETCONFIG; String deleteSQL2 = "delete from " +
	 * getSchema()[0] + "."+MARTXMLTABLE+" " + DELETEDATASETCONFIG;
	 * 
	 * Connection conn = null; try { conn = dsource.getConnection();
	 * PreparedStatement ds = conn.prepareStatement(deleteSQL1); ds.setString(1,
	 * dataset); ds.executeUpdate(); ds.close(); ds =
	 * conn.prepareStatement(deleteSQL2); ds.setString(1, dataset);
	 * ds.executeUpdate(); ds.close(); } catch (SQLException e) { throw new
	 * ConfigurationException("Caught SQLException during delete\n"); } finally
	 * { DetailedDataSource.close(conn); } }
	 */

	/*
	 * public int checkDatasetID(String datasetID, String datasetName) throws
	 * ConfigurationException{ // needs to create meta tables in do not exist
	 * (eg. xml from file) if (!baseDSConfigTableExists())
	 * createMetaTables(dsource.getUser());
	 * 
	 * 
	 * String sql = "select count(*) from " + getSchema()[0] +
	 * "."+BASEMETATABLE+" where dataset_id_key = '" +
	 * datasetID+"' and dataset != '" + datasetName + "'"; Connection conn =
	 * null; try { conn = dsource.getConnection(); PreparedStatement ps =
	 * conn.prepareStatement(sql); ResultSet rs = ps.executeQuery(); rs.next();
	 * int result = rs.getInt(1); return result; } catch (SQLException e) {
	 * throw new ConfigurationException("Caught SQLException during ps.execule:
	 * \n"+e.getMessage()); } finally { DetailedDataSource.close(conn); } }
	 */

	/**
	 * Removes all records in a given metatable for the given dataset and
	 * internal name
	 * 
	 * @param dataset
	 *            - dataset for DatasetConfig entries to delete from metatable
	 * @param internalName
	 *            - internal name for DatasetConfig entry to delete from
	 *            metatable
	 * @throws ConfigurationException
	 *             if number of rows to delete doesnt match number returned by
	 *             getDSConfigEntryCountFor()
	 */

	public void deleteDatasetConfigsForDatasetID(final String dataset,
			final String datasetID, final String user, final String template)
			throws ConfigurationException {
		final String deleteSQL1 = "delete from " + this.getSchema()[0] + "."
				+ this.BASEMETATABLE + " where dataset = ?"
				+ " and dataset_id_key = ?";
		final String deleteSQL2 = "delete from " + this.getSchema()[0] + "."
				+ this.MARTXMLTABLE + " where dataset_id_key = ?";
		final String deleteUserSQL = "delete from " + this.getSchema()[0] + "."
				+ this.MARTUSERTABLE + " where dataset_id_key = ?";
		final String deleteInterfaceSQL = "delete from " + this.getSchema()[0]
				+ "." + this.MARTINTERFACETABLE + " where dataset_id_key = ?";
		final String deleteTemplateSQL = "delete from " + this.getSchema()[0]
				+ "." + this.MARTTEMPLATEMAINTABLE
				+ " where dataset_id_key = ?";
		final String deleteRealTemplateSQL = "delete from "
				+ this.getSchema()[0] + "." + this.MARTTEMPLATEDMTABLE
				+ " where template = ?";
		if (this.readonly)
			throw new ConfigurationException(
					"Cannot delete config from a read-only database");

		Connection conn = null;
		try {
			conn = this.dsource.getConnection();
			PreparedStatement ds = conn.prepareStatement(deleteSQL1);
			ds.setString(1, dataset);
			ds.setString(2, datasetID);
			ds.executeUpdate();

			ds = conn.prepareStatement(deleteSQL2);
			ds.setString(1, datasetID);
			ds.executeUpdate();

			ds = conn.prepareStatement(deleteUserSQL);
			ds.setString(1, datasetID);
			ds.executeUpdate();

			ds = conn.prepareStatement(deleteInterfaceSQL);
			ds.setString(1, datasetID);
			ds.executeUpdate();

			ds = conn.prepareStatement(deleteTemplateSQL);
			ds.setString(1, datasetID);
			ds.executeUpdate();

			if (template != null) {
				System.err.println("delete");
				ds = conn.prepareStatement(deleteRealTemplateSQL);
				ds.setString(1, template);
				ds.executeUpdate();
			}

			ds.close();

			this.updateMartConfigForUser(user, this.getSchema()[0]);

		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQLException during delete\n");
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	/**
	 * Removes all records in a given metatable for the given template
	 * 
	 * @param template
	 *            - template for TemplateConfig entries to delete from metatable
	 * @throws ConfigurationException
	 *             if something goes wrong
	 */

	public void deleteTemplateConfigs(final String template)
			throws ConfigurationException {
		// Get all ds and delete those first.
		final String[] dsNs = this.getDatasetNamesForTemplate(template);
		for (int i = 0; i < dsNs.length; i++) {
			final String dsN = dsNs[i];
			final String[] ids = this.getAllDatasetIDsForDataset(
					MartEditor.getUser(), dsN);
			for (int j = 0; j < ids.length; j++)
				this.deleteDatasetConfigsForDatasetID(dsN, ids[j],
						MartEditor.getUser(), template);
		}

		final String deleteTemplateSQL = "delete from " + this.getSchema()[0]
				+ "." + this.MARTTEMPLATEMAINTABLE + " where template = ?";
		final String deleteRealTemplateSQL = "delete from "
				+ this.getSchema()[0] + "." + this.MARTTEMPLATEDMTABLE
				+ " where template = ?";
		if (this.readonly)
			throw new ConfigurationException(
					"Cannot delete config from a read-only database");

		Connection conn = null;
		try {
			conn = this.dsource.getConnection();

			PreparedStatement ds = conn.prepareStatement(deleteRealTemplateSQL);
			ds.setString(1, template);
			ds.executeUpdate();

			ds = conn.prepareStatement(deleteTemplateSQL);
			ds.setString(1, template);
			ds.executeUpdate();

			ds.close();

		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQLException during delete\n");
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	/**
	 * Get the correct DatasetConfig table for a given user in the Mart Database
	 * stored in the given DetailedDataSource.
	 * 
	 * @param user
	 *            -- user to retrieve a DatasetConfig table. If user is null, or
	 *            if meta_configuration_[user] does not exist returns
	 *            DatabaseDatasetConfigUtils.BASEMETATABLE.
	 * @return String meta table name
	 * @throws ConfigurationException
	 *             if both meta_configuration_[user] and
	 *             DatabaseDatasetConfigUtils.BASEMETATABLE are absent, and for
	 *             all underlying exceptions.
	 */
	public String createMetaTables(final String user)
			throws ConfigurationException {

		String metatable = this.BASEMETATABLE;

		if (this.readonly)
			return metatable;

		final String CREATETABLE = "create table " + this.getSchema()[0];

		final String MYSQL_META1 = CREATETABLE
				+ "."
				+ this.BASEMETATABLE
				+ "(dataset_id_key int not null,dataset varchar(100),display_name varchar(200),description varchar(200),type varchar(20), "
				+ "visible int(1) unsigned, version varchar(25), modified TIMESTAMP NOT NULL,UNIQUE (dataset_id_key))";
		final String MYSQL_META2 = CREATETABLE
				+ "."
				+ this.MARTXMLTABLE
				+ " ( dataset_id_key int not null, xml longblob, "
				+ "compressed_xml longblob, message_digest blob, UNIQUE (dataset_id_key))";
		final String MYSQL_USER = CREATETABLE
				+ "."
				+ this.MARTUSERTABLE
				+ " ( dataset_id_key int, mart_user varchar(100),UNIQUE(dataset_id_key,mart_user))";
		final String MYSQL_INTERFACE = CREATETABLE
				+ "."
				+ this.MARTINTERFACETABLE
				+ " ( dataset_id_key int, interface varchar(100),UNIQUE(dataset_id_key,interface))";
		final String MYSQL_VERSION = CREATETABLE + "." + this.MARTVERSIONTABLE
				+ " ( version varchar(10))";
		final String MYSQL_TEMPLATE_MAIN = CREATETABLE
				+ "."
				+ this.MARTTEMPLATEMAINTABLE
				+ " ( dataset_id_key int not null, template varchar(100) not null)";
		final String MYSQL_TEMPLATE_DM = CREATETABLE
				+ "."
				+ this.MARTTEMPLATEDMTABLE
				+ " ( template varchar(100), compressed_xml longblob, UNIQUE(template))";

		final String ORACLE_META1 = CREATETABLE
				+ "."
				+ this.BASEMETATABLE
				+ " (dataset_id_key number(1),dataset varchar2(100),display_name varchar2(200),description varchar2(200),type varchar2(20), "
				+ "visible number(1), version varchar2(25),  modified timestamp,UNIQUE (dataset_id_key))";
		final String ORACLE_META2 = CREATETABLE
				+ "."
				+ this.MARTXMLTABLE
				+ " (dataset_id_key number(1), xml clob, compressed_xml blob, message_digest blob,UNIQUE (dataset_id_key))";
		final String ORACLE_USER = CREATETABLE
				+ "."
				+ this.MARTUSERTABLE
				+ " (dataset_id_key number(1), mart_user varchar2(100), UNIQUE(dataset_id_key,mart_user))";
		final String ORACLE_INTERFACE = CREATETABLE
				+ "."
				+ this.MARTINTERFACETABLE
				+ " (dataset_id_key number(1), interface varchar2(100), UNIQUE(dataset_id_key,interface))";
		final String ORACLE_VERSION = CREATETABLE + "." + this.MARTVERSIONTABLE
				+ " ( version varchar2(10))";
		final String ORACLE_TEMPLATE_MAIN = CREATETABLE + "."
				+ this.MARTTEMPLATEMAINTABLE
				+ " ( dataset_id_key number(1), template varchar2(100))";
		final String ORACLE_TEMPLATE_DM = CREATETABLE
				+ "."
				+ this.MARTTEMPLATEDMTABLE
				+ " ( template varchar2(100), compressed_xml blob, UNIQUE(template))";

		final String POSTGRES_META1 = CREATETABLE
				+ "."
				+ this.BASEMETATABLE
				+ " (dataset_id_key integer,"
				+ "dataset varchar(100), display_name varchar(200),  description varchar(200),  type varchar(20), "
				+ "visible integer, version varchar(25),  modified timestamp, UNIQUE (dataset_id_key))";
		final String POSTGRES_META2 = CREATETABLE
				+ "."
				+ this.MARTXMLTABLE
				+ "(dataset_id_key integer,"
				// +
				// "xml text, compressed_xml bytea, message_digest bytea,UNIQUE (dataset_id_key))";
				+ "xml bytea, compressed_xml bytea, message_digest bytea,UNIQUE (dataset_id_key))";
		final String POSTGRES_USER = CREATETABLE
				+ "."
				+ this.MARTUSERTABLE
				+ " (dataset_id_key integer, mart_user varchar(100), UNIQUE(dataset_id_key,mart_user))";
		final String POSTGRES_INTERFACE = CREATETABLE
				+ "."
				+ this.MARTINTERFACETABLE
				+ " (dataset_id_key integer, interface varchar(100), UNIQUE(dataset_id_key,interface))";
		final String POSTGRES_VERSION = CREATETABLE + "."
				+ this.MARTVERSIONTABLE + " ( version varchar(10))";
		final String POSTGRES_TEMPLATE_MAIN = CREATETABLE + "."
				+ this.MARTTEMPLATEMAINTABLE
				+ " ( dataset_id_key integer, template varchar(100))";
		final String POSTGRES_TEMPLATE_DM = CREATETABLE
				+ "."
				+ this.MARTTEMPLATEDMTABLE
				+ " ( template varchar(100), compressed_xml bytea, UNIQUE(template))";

		// override if user not null
		if (this.datasetConfigUserTableExists(user))
			metatable += "_" + user;
		else {

			// if version not up to date and template tables exist then delete
			// them
			if (this.templateTableExists()) {
				Connection conn = null;
				try {
					conn = this.dsource.getConnection();
					final ResultSet vr = conn.getMetaData().getTables(
							conn.getCatalog(), this.dsource.getSchema(),
							"meta_version__version__main", null);
					// expect at most one result, if no results, tcheck will
					// remain null
					String tcheck = null;
					if (vr.next())
						tcheck = vr.getString(3);
					vr.close();
					if (tcheck != null) {// don't check databases with no
						// version table yet
						final PreparedStatement ps = conn
								.prepareStatement("select version from "
										+ this.getSchema()[0]
										+ ".meta_version__version__main");
						final ResultSet rs = ps.executeQuery();
						rs.next();
						final String version = rs.getString(1);
						rs.close();
						if (!version.equals(this.SOFTWAREVERSION)) {
							/*
							 * Decided no need to delete existing templates from
							 * now on as 0.5 and 0.6 are compatible now
							 * 
							 * final String TEMPLATEMAIN = "delete from
							 * "+this.getSchema()[0]+"
							 * ."+this.MARTTEMPLATEMAINTABLE; final String
							 * TEMPLATEDM = "delete from
							 * "+this.getSchema()[0]+".
							 * "+this.MARTTEMPLATEDMTABLE; final String
							 * VERSIONSQL = "update
							 * "+this.getSchema()[0]+"."+this.MARTVERSIONTABLE+"
							 * set version='"+this.SOFTWAREVERSION+"'";
							 * 
							 * if (JOptionPane.showConfirmDialog(null,"WARNING -
							 * EXISTING TEMPLATES ARE NOT
							 * "+this.SOFTWAREVERSION+" COMPATIBLE AND NEED
							 * DELETING.\n" + "Are you sure you want to do
							 * this?","DELETETEMPLATES?
							 * ",JOptionPane.YES_NO_OPTION)!=JOptionPane.YES_OPTION)
							 * return metatable;
							 * 
							 * //System.out.println("THE VERSIONS DO NOT MATCH
							 * FOR DB - LINES TO RUN (NOT PUT IN
							 * YET)\n"+TEMPLATEMAIN+"
							 * \n"+TEMPLATEDM+"\n"+VERSIONSQL); final
							 * PreparedStatement
							 * ps1=conn.prepareStatement(TEMPLATEMAIN);
							 * ps1.executeUpdate(); final PreparedStatement
							 * ps2=conn.prepareStatement(TEMPLATEDM);
							 * ps2.executeUpdate(); final PreparedStatement
							 * ps3=conn.prepareStatement(VERSIONSQL);
							 * ps3.executeUpdate();
							 */
						}
					}
					conn.close();
				} catch (final SQLException e) {
					throw new ConfigurationException(
							"Caught SQLException during create meta tables\n"
									+ e);
				} finally {
					DetailedDataSource.close(conn);
				}
			}

			else {
				Connection conn = null;
				try {
					conn = this.dsource.getConnection();
					String CREATE_SQL1 = new String();
					String CREATE_SQL2 = new String();
					if (this.dsource.getDatabaseType().equals("oracle")) {
						CREATE_SQL1 = ORACLE_TEMPLATE_MAIN;
						CREATE_SQL2 = ORACLE_TEMPLATE_DM;
					}
					if (this.dsource.getDatabaseType().equals("postgres")) {
						CREATE_SQL1 = POSTGRES_TEMPLATE_MAIN;
						CREATE_SQL2 = POSTGRES_TEMPLATE_DM;
					}
					if (this.dsource.getDatabaseType().equals("mysql")) {
						CREATE_SQL1 = MYSQL_TEMPLATE_MAIN;
						CREATE_SQL2 = MYSQL_TEMPLATE_DM;
					}

					PreparedStatement ps = conn.prepareStatement(CREATE_SQL1);
					ps.executeUpdate();
					ps = conn.prepareStatement(CREATE_SQL2);
					ps.executeUpdate();

					System.out.println("created template tables");

					conn.close();
				} catch (final SQLException e) {
					throw new ConfigurationException(
							"Caught SQLException during create meta tables\n"
									+ e);
				} finally {
					DetailedDataSource.close(conn);
				}
			}

			if (!this.baseDSConfigTableExists()) {
				Connection conn = null;
				try {
					conn = this.dsource.getConnection();
					String CREATE_SQL1 = new String();
					String CREATE_SQL2 = new String();
					String CREATE_USER = new String();
					String CREATE_INTERFACE = new String();
					String CREATE_VERSION = new String();
					final String INSERT_VERSION = "INSERT INTO "
							+ this.getSchema()[0] + "." + this.MARTVERSIONTABLE
							+ " VALUES ('" + this.SOFTWAREVERSION + "')";
					if (this.dsource.getDatabaseType().equals("oracle")) {
						CREATE_SQL1 = ORACLE_META1;
						CREATE_SQL2 = ORACLE_META2;
						CREATE_USER = ORACLE_USER;
						CREATE_INTERFACE = ORACLE_INTERFACE;
						CREATE_VERSION = ORACLE_VERSION;
					}
					if (this.dsource.getDatabaseType().equals("postgres")) {
						CREATE_SQL1 = POSTGRES_META1;
						CREATE_SQL2 = POSTGRES_META2;
						CREATE_USER = POSTGRES_USER;
						CREATE_INTERFACE = POSTGRES_INTERFACE;
						CREATE_VERSION = POSTGRES_VERSION;
					}
					if (this.dsource.getDatabaseType().equals("mysql")) {
						CREATE_SQL1 = MYSQL_META1;
						CREATE_SQL2 = MYSQL_META2;
						CREATE_USER = MYSQL_USER;
						CREATE_INTERFACE = MYSQL_INTERFACE;
						CREATE_VERSION = MYSQL_VERSION;
					}

					// System.out.println("CREATE_SQL: "+CREATE_SQL+"
					// CREATE_USER: "+CREATE_USER);

					PreparedStatement ps = conn.prepareStatement(CREATE_SQL1);
					ps.executeUpdate();
					ps = conn.prepareStatement(CREATE_SQL2);
					ps.executeUpdate();

					final PreparedStatement ps1 = conn
							.prepareStatement(CREATE_USER);
					ps1.executeUpdate();

					final PreparedStatement ps2 = conn
							.prepareStatement(CREATE_INTERFACE);
					ps2.executeUpdate();

					PreparedStatement ps3 = conn
							.prepareStatement(CREATE_VERSION);
					ps3.executeUpdate();
					ps3 = conn.prepareStatement(INSERT_VERSION);
					ps3.executeUpdate();

					System.out.println("created meta tables");

					conn.close();
				} catch (final SQLException e) {
					throw new ConfigurationException(
							"Caught SQLException during create meta tables\n"
									+ e);
				} finally {
					DetailedDataSource.close(conn);
				}

			}
		}

		return metatable;
	}

	public void dropMetaTables() throws ConfigurationException {
		if (this.readonly)
			throw new ConfigurationException(
					"Cannot drop tables in a read-only database");

		final String DROPTABLE = "drop table " + this.getSchema()[0];

		final String MYSQL_META1 = DROPTABLE + "." + this.BASEMETATABLE;
		final String MYSQL_META2 = DROPTABLE + "." + this.MARTXMLTABLE;
		final String MYSQL_USER = DROPTABLE + "." + this.MARTUSERTABLE;
		final String MYSQL_INTERFACE = DROPTABLE + "."
				+ this.MARTINTERFACETABLE;
		final String MYSQL_VERSION = DROPTABLE + "." + this.MARTVERSIONTABLE;
		final String MYSQL_TEMPLATEMAIN = "delete from " + this.getSchema()[0]
				+ "." + this.MARTTEMPLATEMAINTABLE;
		final String MYSQL_TEMPLATEDM = "delete from " + this.getSchema()[0]
				+ "." + this.MARTTEMPLATEDMTABLE;

		if (this.baseDSConfigTableExists()) {
			Connection conn = null;
			try {
				conn = this.dsource.getConnection();

				PreparedStatement ps = conn.prepareStatement(MYSQL_META1);
				ps.executeUpdate();
				ps = conn.prepareStatement(MYSQL_META2);
				ps.executeUpdate();

				final PreparedStatement ps1 = conn.prepareStatement(MYSQL_USER);
				ps1.executeUpdate();

				final PreparedStatement ps2 = conn
						.prepareStatement(MYSQL_INTERFACE);
				ps2.executeUpdate();

				final PreparedStatement ps3 = conn
						.prepareStatement(MYSQL_VERSION);
				ps3.executeUpdate();

				final PreparedStatement ps4 = conn
						.prepareStatement(MYSQL_TEMPLATEMAIN);
				ps4.executeUpdate();

				final PreparedStatement ps5 = conn
						.prepareStatement(MYSQL_TEMPLATEDM);
				ps5.executeUpdate();

				conn.close();
			} catch (final SQLException e) {
				throw new ConfigurationException(
						"Caught SQLException during drop meta tables\n" + e);
			} finally {
				DetailedDataSource.close(conn);
			}
		}

	}

	public DatasetConfig getValidatedDatasetConfig(final DatasetConfig dsv)
			throws SQLException, ConfigurationException {

		final String catalog = "";
		final String schema = this.getSchema()[0];

		final DatasetConfig validatedDatasetConfig = new DatasetConfig(dsv,
				true, false);
		final Connection conn = this.dsource.getConnection();
		// create the connection object
		// connection = dsource.getConnection();

		// want to copy existing Elements to the new Object as is
		final String dset = validatedDatasetConfig.getDataset();
		boolean hasBrokenStars = false;
		final String[] starbases = validatedDatasetConfig.getStarBases();
		final String[] validatedStars = new String[starbases.length];

		for (int i = 0, n = starbases.length; i < n; i++) {
			final String starbase = starbases[i];
			final String validatedStar = this.getValidatedStarBase(schema,
					catalog, starbase, conn);

			if (!validatedStar.equals(starbase)) {
				hasBrokenStars = true;
				System.out
						.println("MAIN TABLE IS BROKEN AND NEEDS REMOVING AND RE-ADDING "
								+ starbase);
				validatedDatasetConfig.removeMainTable(starbase);
			}
			validatedStars[i] = validatedStar;
		}

		if (hasBrokenStars) {
			validatedDatasetConfig.setStarsBroken();
			validatedDatasetConfig.addMainTables(validatedStars);
		}

		boolean hasBrokenPKeys = false;
		final String[] pkeys = validatedDatasetConfig.getPrimaryKeys();
		final String[] validatedKeys = new String[pkeys.length];

		for (int i = 0, n = pkeys.length; i < n; i++) {
			final String pkey = pkeys[i];
			final String validatedKey = this.getValidatedPrimaryKey(schema,
					catalog, pkey, conn);

			if (!validatedKey.equals(pkey)) {
				hasBrokenPKeys = true;
				System.out
						.println("KEY IS BROKEN AND NEEDS REMOVING AND RE-ADDING "
								+ pkey);
				validatedDatasetConfig.removePrimaryKey(pkey);
			}

			validatedKeys[i] = validatedKey;
		}

		if (hasBrokenPKeys) {
			validatedDatasetConfig.setPrimaryKeysBroken();
			validatedDatasetConfig.addPrimaryKeys(validatedKeys);
		}

		final boolean hasBrokenDefaultFilters = false;

		final List brokenFilters = new ArrayList();

		/*
		 * DATASET OPTIONS DO NOT EXIST boolean hasBrokenOptions = false;
		 * Option[] options = validatedDatasetConfig.getOptions(); HashMap
		 * brokenOptions = new HashMap();
		 * 
		 * for (int i = 0, n = options.length; i < n; i++) { Option
		 * validatedOption = getValidatedOption(schema, catalog, options[i],
		 * dset, conn,mains,keys);
		 * 
		 * if (validatedOption.isBroken()) { hasBrokenOptions = true;
		 * brokenOptions.put(new Integer(i), validatedOption); } }
		 * 
		 * if (hasBrokenOptions) { validatedDatasetConfig.setOptionsBroken();
		 * 
		 * for (Iterator iter = brokenOptions.keySet().iterator();
		 * iter.hasNext();) { Integer position = (Integer) iter.next(); Option
		 * brokenOption = (Option) brokenOptions.get(position);
		 * 
		 * validatedDatasetConfig.removeOption(options[position.intValue()]);
		 * validatedDatasetConfig.insertOption(position.intValue(),
		 * brokenOption); } }
		 */
		boolean hasBrokenAttributePages = false;
		final AttributePage[] apages = validatedDatasetConfig
				.getAttributePages();
		final HashMap brokenAPages = new HashMap();

		for (int i = 0, n = apages.length; i < n; i++) {
			final AttributePage validatedPage = this.getValidatedAttributePage(
					apages[i], dset, conn, starbases, pkeys);

			if (validatedPage.isBroken()) {
				hasBrokenAttributePages = true;
				brokenAPages.put(new Integer(i), validatedPage);
			}
		}

		if (hasBrokenAttributePages) {
			validatedDatasetConfig.setAttributePagesBroken();

			for (final Iterator iter = brokenAPages.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();
				final AttributePage brokenAPage = (AttributePage) brokenAPages
						.get(position);

				validatedDatasetConfig.removeAttributePage(apages[position
						.intValue()]);
				validatedDatasetConfig.insertAttributePage(position.intValue(),
						brokenAPage);
			}
		}

		boolean hasBrokenFilterPages = false;
		final HashMap brokenFPages = new HashMap();
		final FilterPage[] allPages = validatedDatasetConfig.getFilterPages();

		for (int i = 0, n = allPages.length; i < n; i++) {
			final FilterPage validatedPage = this.getValidatedFilterPage(
					allPages[i], dset, validatedDatasetConfig, conn, starbases,
					pkeys);
			if (validatedPage.isBroken()) {
				hasBrokenFilterPages = true;
				brokenFPages.put(new Integer(i), validatedPage);
			}
		}

		if (hasBrokenFilterPages) {
			validatedDatasetConfig.setFilterPagesBroken();

			for (final Iterator iter = brokenFPages.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();
				final FilterPage brokenPage = (FilterPage) brokenFPages
						.get(position);
				validatedDatasetConfig.removeFilterPage(allPages[position
						.intValue()]);
				validatedDatasetConfig.insertFilterPage(position.intValue(),
						brokenPage);
			}
		}
		DetailedDataSource.close(conn);
		return validatedDatasetConfig;
	}

	public String getBrokenElements(final DatasetConfig dsv)
			throws SQLException, ConfigurationException {

		String brokenElements = "";
		final String catalog = "";
		final String schema = this.getSchema()[0];
		final Connection conn = this.dsource.getConnection();
		// DatasetConfig validatedDatasetConfig = new DatasetConfig(dsv, true,
		// false);
		// want to copy existing Elements to the new Object as is

		// List ads = dsv.getAllAttributeDescriptions();

		final String[] mains = dsv.getStarBases();
		final String[] keys = dsv.getPrimaryKeys();

		final AttributePage[] apages = dsv.getAttributePages();
		for (int j = 0; j < apages.length; j++) {
			final AttributePage apage = apages[j];
			final List agroups = apage.getAttributeGroups();
			for (int k = 0; k < agroups.size(); k++) {
				final AttributeGroup agroup = (AttributeGroup) agroups.get(k);
				final AttributeCollection[] acolls = agroup
						.getAttributeCollections();
				for (int l = 0; l < acolls.length; l++) {
					final AttributeCollection acollection = acolls[l];
					final List ads = acollection.getAttributeDescriptions();
					for (int i = 0, n = ads.size(); i < n; i++) {
						final AttributeDescription testAD = (AttributeDescription) ads
								.get(i);
						if (testAD.getHidden() != null
								&& testAD.getHidden().equals("true"))
							continue;
						final AttributeDescription validatedAD = this
								.getValidatedAttributeDescription(schema,
										catalog, testAD, dsv.getDataset(),
										conn, mains, keys);
						// if (validatedAD.isBroken()) {
						if (validatedAD.hasBrokenField()
								|| validatedAD.hasBrokenTableConstraint())
							brokenElements = brokenElements + "Attribute "
									+ validatedAD.getInternalName()
									+ " in dataset " + dsv.getDataset()
									+ ", page " + apage.getInternalName()
									+ ", group " + agroup.getInternalName()
									+ ", collection "
									+ acollection.getInternalName() + "\n";
					}
				}
			}
		}

		final FilterPage[] fpages = dsv.getFilterPages();
		for (int j = 0; j < fpages.length; j++) {
			final FilterPage apage = fpages[j];
			final List agroups = apage.getFilterGroups();
			for (int k = 0; k < agroups.size(); k++) {
				final FilterGroup agroup = (FilterGroup) agroups.get(k);
				final FilterCollection[] acolls = agroup.getFilterCollections();
				for (int l = 0; l < acolls.length; l++) {
					final FilterCollection acollection = acolls[l];
					final List ads = acollection.getFilterDescriptions();
					for (int i = 0, n = ads.size(); i < n; i++) {
						final FilterDescription testAD = (FilterDescription) ads
								.get(i);
						if (testAD.getHidden() != null
								&& testAD.getHidden().equals("true"))
							continue;
						final FilterDescription validatedAD = this
								.getValidatedFilterDescription(schema, catalog,
										testAD, dsv.getDataset(), dsv, conn,
										mains, keys);
						// if (validatedAD.isBroken()) {
						if (validatedAD.hasBrokenField()
								|| validatedAD.hasBrokenTableConstraint())
							brokenElements = brokenElements + "Filter "
									+ validatedAD.getInternalName()
									+ " in dataset " + dsv.getDataset()
									+ ", page " + apage.getInternalName()
									+ ", group " + agroup.getInternalName()
									+ ", collection "
									+ acollection.getInternalName() + "\n";
					}
				}
			}
		}

		// List ads = dsv.getAllFilterDescriptions();
		// for (int i = 0, n = ads.size(); i < n; i++) {
		// FilterDescription testAD = (FilterDescription) ads.get(i);
		// if (testAD.getHidden() != null && testAD.getHidden().equals("true"))
		// continue;

		// FilterDescription validatedAD = getValidatedFilterDescription(schema,
		// catalog, testAD, dsv.getDataset(), dsv, conn);
		// if (validatedAD.hasBrokenField() ||
		// validatedAD.hasBrokenTableConstraint()) { // don't use isBroken() as
		// options always set to broken
		// brokenElements = brokenElements + "Filter " +
		// validatedAD.getInternalName() + " in dataset " + dsv.getDataset() +
		// "\n";
		// }
		// }

		DetailedDataSource.close(conn);
		return brokenElements;
	}

	private String getValidatedStarBase(final String schema,
			final String catalog, final String starbase, final Connection conn)
			throws SQLException {
		String validatedStarBase = new String(starbase);

		// String table = starbase + "%" + MAINTABLESUFFIX;
		final String table = starbase;
		boolean isBroken = true;

		// Connection conn = null;

		try {
			// conn = dsource.getConnection();
			final ResultSet rs = conn.getMetaData().getTables(catalog, schema,
					table, null);
			while (rs.next()) {
				final String thisTable = rs.getString(3);
				if (thisTable.toLowerCase().startsWith(starbase.toLowerCase())) {
					isBroken = false;
					break;
				} else if (this.logger.isLoggable(Level.FINE))
					this.logger.fine("Recieved table " + thisTable
							+ " when querying for " + table + "\n");
			}
		} finally {
			// DetailedDataSource.close(conn);
		}

		if (isBroken)
			validatedStarBase += this.DOESNTEXISTSUFFIX;
		return validatedStarBase;
	}

	private String getValidatedPrimaryKey(final String schema,
			final String catalog, final String primaryKey, final Connection conn)
			throws SQLException {
		String validatedPrimaryKey = new String(primaryKey);

		String tablePattern = "%" + this.MAINTABLESUFFIX;

		if (this.dsource.getDatabaseType().equals("oracle"))
			tablePattern = tablePattern.toUpperCase();
		// System.out.println("databaseType() "+dsource.getDatabaseType());

		boolean isBroken = true;

		// Connection conn = null;

		try {
			// conn = dsource.getConnection();
			final ResultSet columns = conn.getMetaData().getColumns(catalog,
					schema, tablePattern, primaryKey);
			while (columns.next()) {
				final String thisColumn = columns.getString(4);
				if (thisColumn.equalsIgnoreCase(primaryKey)) {
					isBroken = false;
					break;
				} else if (this.logger.isLoggable(Level.FINE))
					this.logger.fine("Recieved column " + thisColumn
							+ " during query for primary key " + primaryKey
							+ "\n");
			}
		} finally {
			// DetailedDataSource.close(conn);
		}
		if (isBroken)
			validatedPrimaryKey += this.DOESNTEXISTSUFFIX;

		return validatedPrimaryKey;
	}

	private FilterPage getValidatedFilterPage(final FilterPage page,
			final String dset, final DatasetConfig dsv, final Connection conn,
			final String[] mains, final String[] keys) throws SQLException,
			ConfigurationException {
		final FilterPage validatedPage = new FilterPage(page);

		boolean hasBrokenGroups = false;
		final HashMap brokenGroups = new HashMap();

		final List allGroups = validatedPage.getFilterGroups();
		for (int i = 0, n = allGroups.size(); i < n; i++) {
			final Object group = allGroups.get(i);

			if (group instanceof FilterGroup) {
				// FilterGroup gr = (FilterGroup) group;
				// if ((gr.getInternalName().equals("expression")))
				// continue;// hack for expression - breaks current code - needs
				// fixing
				final FilterGroup validatedGroup = this
						.getValidatedFilterGroup((FilterGroup) group, dset,
								dsv, conn, mains, keys);

				if (validatedGroup.isBroken()) {
					hasBrokenGroups = true;
					brokenGroups.put(new Integer(i), validatedGroup);
				}
			} // else not needed yet

			if (hasBrokenGroups) {
				validatedPage.setGroupsBroken();

				for (final Iterator iter = brokenGroups.keySet().iterator(); iter
						.hasNext();) {
					final Integer position = (Integer) iter.next();
					final Object brokenGroup = brokenGroups.get(position);

					if (brokenGroup instanceof FilterGroup) {
						validatedPage.removeFilterGroup((FilterGroup) allGroups
								.get(position.intValue()));

						validatedPage.insertFilterGroup(position.intValue(),
								(FilterGroup) brokenGroup);
						allGroups.remove(position.intValue());
						allGroups.add(position.intValue(), brokenGroup);
					} // else not needed yet
				}
			}
		}

		return validatedPage;
	}

	private FilterGroup getValidatedFilterGroup(final FilterGroup group,
			final String dset, final DatasetConfig dsv, final Connection conn,
			final String[] mains, final String[] keys) throws SQLException,
			ConfigurationException {
		final FilterGroup validatedGroup = new FilterGroup(group);

		final FilterCollection[] collections = validatedGroup
				.getFilterCollections();

		boolean hasBrokenCollections = false;
		final HashMap brokenCollections = new HashMap();

		for (int i = 0, n = collections.length; i < n; i++) {
			final FilterCollection validatedCollection = this
					.getValidatedFilterCollection(collections[i], dset, dsv,
							conn, mains, keys);

			if (validatedCollection.isBroken()) {
				hasBrokenCollections = true;
				brokenCollections.put(new Integer(i), validatedCollection);
			}
		}

		if (hasBrokenCollections) {
			validatedGroup.setCollectionsBroken();

			for (final Iterator iter = brokenCollections.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();
				final FilterCollection brokenCollection = (FilterCollection) brokenCollections
						.get(position);

				validatedGroup.removeFilterCollection(collections[position
						.intValue()]);

				validatedGroup.insertFilterCollection(position.intValue(),
						brokenCollection);
			}
		}

		return validatedGroup;
	}

	private FilterCollection getValidatedFilterCollection(
			final FilterCollection collection, final String dset,
			final DatasetConfig dsv, final Connection conn,
			final String[] mains, final String[] keys) throws SQLException,
			ConfigurationException {

		final String catalog = "";
		final String schema = this.getSchema()[0];

		final FilterCollection validatedFilterCollection = new FilterCollection(
				collection);
		final List allFilts = validatedFilterCollection.getFilterDescriptions();

		boolean filtersValid = true;
		final HashMap brokenFilts = new HashMap();

		for (int i = 0, n = allFilts.size(); i < n; i++) {
			final Object element = allFilts.get(i);

			if (element instanceof FilterDescription) {
				final FilterDescription validatedFilter = this
						.getValidatedFilterDescription(schema, catalog,
								(FilterDescription) element, dset, dsv, conn,
								mains, keys);
				if (validatedFilter.isBroken()) {

					filtersValid = false;
					brokenFilts.put(new Integer(i), validatedFilter);
				}
			} // else not needed yet
		}

		if (!filtersValid) {
			validatedFilterCollection.setFiltersBroken();

			for (final Iterator iter = brokenFilts.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();
				final Object brokenFilter = brokenFilts.get(position);

				if (brokenFilter instanceof FilterDescription) {
					validatedFilterCollection
							.removeFilterDescription((FilterDescription) allFilts
									.get(position.intValue()));

					validatedFilterCollection.insertFilterDescription(
							position.intValue(),
							(FilterDescription) brokenFilter);
					allFilts.remove(position.intValue());
					allFilts.add(position.intValue(), brokenFilter);
				} // else not needed yet
			}
		}

		return validatedFilterCollection;
	}

	private FilterDescription getValidatedFilterDescription(String schema,
			final String catalog, final FilterDescription filter,
			final String dset, final DatasetConfig dsv, final Connection conn,
			final String[] mains, final String[] keys) throws SQLException,
			ConfigurationException {
		final FilterDescription validatedFilter = new FilterDescription(filter);

		DatasetConfig otherDataset = null;
		// if a placeholder get the real filter
		// if (testAD.getInternalName().matches("\\w+\\.\\w+") ||
		// testAD.getInternalName().matches("\\w+\\.\\w+\\.\\w+")){
		if (validatedFilter.getPointerDataset() != null
				&& !"".equals(validatedFilter.getPointerDataset()))
			return validatedFilter;

		if (validatedFilter.getField() != null) {

			String field = validatedFilter.getField();
			if (this.dsource.getDatabaseType().equals("oracle"))
				field = field.toUpperCase();

			String tableConstraint = validatedFilter.getTableConstraint();
			if (tableConstraint == null)
				return validatedFilter;

			DescriptionValidity desVal = validateDescription(schema, catalog,
					dset, conn, field, tableConstraint);

			boolean fieldValid = desVal.fieldValid;
			boolean tableValid = desVal.tableValid;
			//
			// // if the tableConstraint is null, this field must be available
			// in
			// // one of the main tables
			// String table = !tableConstraint.equals("main") ? tableConstraint
			// : dset + "%" + this.MAINTABLESUFFIX;
			// // System.out.println("!!!" + field + "\t" + tableConstraint +
			// "\t"
			// // + table);
			//
			// if (this.dsource.getDatabaseType().equals("oracle"))
			// table = table.toUpperCase();
			// // System.out.println("WAITING FOR CONNECTION");
			// // Connection conn = dsource.getConnection();
			// // System.out.println("GOT CONNECTION");
			//
			// final ResultSet rs = conn.getMetaData().getColumns(catalog,
			// schema,
			// table, field);
			// while (rs.next()) {
			// final String columnName = rs.getString(4);
			// final String tableName = rs.getString(3);
			// final boolean[] valid = this.isValidDescription(columnName,
			// field, tableName, tableConstraint);
			// fieldValid = valid[0];
			// tableValid = valid[1];
			//
			// if (valid[0] && valid[1])
			// // System.out.println(columnName + "\t" + tableName + "\t" +
			// // field);
			// break;
			// }
			// rs.close();
			// conn.close();
			// DetailedDataSource.close(conn);

			// test for all nulls as well if flagged and set fieldValid and
			// tableValid = false if all nulls
			if (tableConstraint.equals("main")) {
				// tableConstraint = [] mains;
				for (int i = 0; i < keys.length; i++) {
					tableConstraint = mains[i];
					if (keys[i].equals(validatedFilter.getKey()))
						break;
				}
			} else if (desVal.fieldValid && desVal.tableValid
					&& validatedFilter.getKey() != null) {
				// DMS says check the key too!
				DescriptionValidity keyVal = validateDescription(schema,
						catalog, dset, conn, validatedFilter.getKey(),
						tableConstraint);

				if (!keyVal.fieldValid || !keyVal.tableValid) {
					String suggestedKey = suggestKeyFix(schema, catalog, dset,
							conn, tableConstraint);
					System.err.println(dset + "_" + tableConstraint + "."
							+ validatedFilter.getKey() + " does not exist for "
							+ " filter " + validatedFilter.getInternalName());
					if (suggestedKey != null) {
						System.err.println("<SpecificFilterContent key=\""
								+ suggestedKey + "\" field=\""
								+ validatedFilter.getField()
								+ "\" internalName=\"" + dset + "\" />");
						validatedFilter.setKey(suggestedKey);
					}
				}

			}
			if ("true".equals(validatedFilter.getCheckForNulls()) && fieldValid
					&& tableValid && this.isAllNull(field, tableConstraint)) {
				fieldValid = false;
				tableValid = false;
			}

			if (!fieldValid || !tableValid) {
				validatedFilter.setHidden("true");
				validatedFilter.setFieldBroken(); // so gets changed in the
				// update
				return validatedFilter;// do not want to do anymore validation
			} else if (validatedFilter.getHidden() != null
					&& validatedFilter.getHidden().equals("true")) {
				validatedFilter.setHidden("false");
				validatedFilter.setFieldBroken(); // so gets changed in the
				// update
			}

			// System.out.println(tableConstraint+"."+field+"="+tableValid+"/"+fieldValid);

			if (!(fieldValid && tableValid)) {
				validatedFilter.setFieldBroken();
				validatedFilter.setTableConstraintBroken();
			}

		}

		boolean optionsValid = true;
		final HashMap brokenOptions = new HashMap();
		final Option[] options = validatedFilter.getOptions();
		if (options.length > 0 && options[0].getValue() != null
				&& !options[0].getValue().equals("only")) {// UPDATE VALUE
			// OPTIONS
			// regenerate options and push actions
			// System.out.println("UPDATING OPTIONS");
			// store the option/push action structure so can recreate
			final PushAction[] pas = options[0].getPushActions();
			final String[] pushActions = new String[pas.length];
			final String[] orderBy = new String[pas.length];
			// System.out.println("PA" + pas[0].getInternalName());
			// System.out.println("PA" + pas[0].getOptions()[0]);
			PushAction[] secondaryPAs = null;
			String[] secondaryPushActions = null;
			String[] secondaryOrderBy = null;
			if (pas.length > 0) {
				secondaryPAs = pas[0].getOptions()[0].getPushActions();
				secondaryPushActions = new String[secondaryPAs.length];
				secondaryOrderBy = new String[secondaryPAs.length];
			}
			for (int n = 0; n < pas.length; n++) {
				pushActions[n] = pas[n].getRef();
				orderBy[n] = pas[n].getOrderBy();
				for (int p = 0; p < secondaryPAs.length; p++) {
					secondaryPushActions[p] = secondaryPAs[p].getRef();
					secondaryOrderBy[p] = secondaryPAs[p].getOrderBy();

				}
			}

			final String field = validatedFilter.getField();
			final String tableName = validatedFilter.getTableConstraint();

			if (field == null || tableName == null)
				return validatedFilter;
			// remove all options
			final String[] oldOptionOrder = new String[options.length];
			for (int j = 0; j < options.length; j++) {
				oldOptionOrder[j] = options[j].getInternalName();
				// System.out.println("REMOVING OPTIONS");
				validatedFilter.removeOption(options[j]);
			}

			final String joinKey = validatedFilter.getKey();
			validatedFilter.setType("list");
			validatedFilter.setDisplayType("list");
			validatedFilter.setStyle("menu");
			validatedFilter.setGraph(filter.getGraph());
			validatedFilter.setQualifier("=");
			validatedFilter.setLegalQualifiers("=");
			final String colForDisplay = validatedFilter.getColForDisplay();
			Option[] ops;
			if (otherDataset != null)
				ops = this.getOptions(field, tableName, joinKey, otherDataset,
						otherDataset.getDataset(), colForDisplay);
			else
				ops = this.getOptions(field, tableName, joinKey, dsv,
						dsv.getDataset(), colForDisplay);
			// add back any options
			final HashMap valMap = new HashMap();// use to keep options in
			// existing order if
			// possible
			for (int k = 0; k < ops.length; k++)
				valMap.put(ops[k].getInternalName(), ops[k]);
			int j;
			int k = 0;
			for (j = 0; j < oldOptionOrder.length; j++)
				if (valMap.containsKey(oldOptionOrder[j])) {
					// System.out.println("ADDING OPTIONS BACK 1 ");
					validatedFilter.insertOption(k,
							(Option) valMap.get(oldOptionOrder[j]));
					k++;
					valMap.remove(oldOptionOrder[j]);
				}
			for (final Iterator iter = valMap.keySet().iterator(); iter
					.hasNext();) {
				final String position = (String) iter.next();
				// System.out.println("ADDING OPTIONS BACK 2");
				validatedFilter.insertOption(k, (Option) valMap.get(position));
				k++;
			}
			// add back any PushActions
			for (int n = 0; n < pushActions.length; n++) {

				final String filter2 = pushActions[n];
				final String orderSQL = orderBy[n];

				if (validatedFilter.getOtherFilters() != null) {// push action
					// refers to a
					// placeholder
					// filter
					String otherDatasetFilter1 = null;
					otherDataset = null;
					FilterDescription fd2 = null;
					final String[] otherFilters = validatedFilter
							.getOtherFilters().split(";");
					OUTER: for (int p = 0; p < otherFilters.length; p++) {
						final String[] schemas = this.getSchema();
						for (int q = 0; q < schemas.length; q++) {
							final DatabaseDatasetConfigUtils newUtils = MartEditor
									.getDatabaseDatasetConfigUtilsBySchema(schemas[q]);
							// System.out.println("NEW ONE HAS DSOURCE " +
							// newUtils.getAllDatasetNames(null)[0]);
							// System.out.println(schemas[q] + " TEST " +
							// otherFilters[p]);
							otherDataset = MartEditor
									.getDatabaseDatasetConfigUtilsBySchema(
											schemas[q])
									.getDatasetConfigByDatasetID(null,
											otherFilters[p].split("\\.")[0],
											"", schemas[q]);// TODO - FIX THIS
							// METHOD

							if (otherDataset == null)
								continue;
							this.dscutils
									.loadDatasetConfigWithDocument(
											otherDataset,
											MartEditor
													.getDatabaseDatasetConfigUtilsBySchema(
															schemas[q])
													.getDatasetConfigDocumentByDatasetID(
															null,
															otherFilters[p]
																	.split("\\.")[0],
															otherDataset
																	.getDatasetID(),
															schemas[q]));
							if (otherDataset.containsFilterDescription(filter2))
								fd2 = otherDataset
										.getFilterDescriptionByInternalName(filter2);
							if (fd2 != null) {
								schema = schemas[q];
								otherDatasetFilter1 = otherFilters[p]
										.split("\\.")[1];
								break OUTER;
							}
						}
					}
					// System.out.println("FILTER " +
					// validatedFilter.getInternalName());
					// needs if fd2 = null fix
					if (fd2 == null) {
						JOptionPane
								.showMessageDialog(
										null,
										"Problem finding a placeholder dataset for "
												+ validatedFilter
														.getInternalName()
												+ " push actions. Have you set the correct databases in the schema database connection box?");
						return validatedFilter;
					}

					fd2.setType("drop_down_basic_filter");
					final String pushField = fd2.getField();
					final String pushColForDisplay = fd2.getColForDisplay();
					String pushInternalName = fd2.getInternalName();
					if (pushInternalName.matches("\\w+\\.\\w+"))
						pushInternalName = pushInternalName.split("\\.")[0]
								+ "__" + pushInternalName.split("\\.")[1];
					final String pushTableName = fd2.getTableConstraint();

					Option[] options2;

					final String pafield = otherDataset
							.getFilterDescriptionByInternalName(
									otherDatasetFilter1).getField();

					options2 = validatedFilter.getOptions();

					for (int i = 0; i < options2.length; i++) {
						final Option op = options2[i];
						final String opName = op.getDisplayName();
						final PushAction pa = new PushAction(pushInternalName
								+ "_push_" + opName.replaceAll(" ", "_"), null,
								null, pushInternalName, orderSQL);
						// System.out.println("1A"+pushField+"\t"+pushTableName+"\t"+field+"\t"+opName+"\t"+orderSQL);
						pa.addOptions(this.getLookupOptions(pushField,
								pushTableName, otherDataset, fd2.getKey(),
								otherDataset.getDataset(), pafield, opName,
								orderSQL, schema, pushColForDisplay));

						// ADD ANY SECONDARY PUSH ACTIONS
						for (int p = 0; p < secondaryPushActions.length; p++) {
							final String secFilter2 = secondaryPushActions[p];
							final String secOrderSQL = secondaryOrderBy[p];
							final FilterDescription referredFilter = dsv
									.getFilterDescriptionByInternalName(pushActions[n]);
							if (referredFilter.getOtherFilters() != null) {
								String secOtherDatasetFilter1 = null;
								DatasetConfig secOtherDataset = null;
								FilterDescription fd3 = null;
								final String[] secOtherFilters = referredFilter
										.getOtherFilters().split(";");
								for (int q = 0; q < secOtherFilters.length; q++) {
									secOtherDataset = this
											.getDatasetConfigByDatasetID(null,
													secOtherFilters[q]
															.split("\\.")[0],
													"", this.getSchema()[0]);
									this.dscutils
											.loadDatasetConfigWithDocument(
													secOtherDataset,
													this.getDatasetConfigDocumentByDatasetID(
															null,
															secOtherFilters[p]
																	.split("\\.")[0],
															secOtherDataset
																	.getDatasetID(),
															this.getSchema()[0]));
									if (secOtherDataset
											.containsFilterDescription(secFilter2))
										fd3 = secOtherDataset
												.getFilterDescriptionByInternalName(secFilter2);
									if (fd3 != null) {
										secOtherDatasetFilter1 = secOtherFilters[p]
												.split("\\.")[1];
										break;
									}
								}
								fd3.setType("drop_down_basic_filter");
								final String secPushField = fd3.getField();
								final String secColForDisplay = fd3
										.getColForDisplay();
								final String secPushInternalName = fd3
										.getInternalName();
								final String secPushTableName = fd3
										.getTableConstraint();

								final String secPafield = secOtherDataset
										.getFilterDescriptionByInternalName(
												secOtherDatasetFilter1)
										.getField();
								final Option[] options3 = pa.getOptions();
								for (int r = 0; r < options3.length; r++) {
									final Option op3 = options3[r];
									final String secOpName = op3
											.getDisplayName();
									final PushAction secondaryPA = new PushAction(
											secPushInternalName
													+ "_push_"
													+ secOpName.replaceAll(" ",
															"_"), null, null,
											secPushInternalName, secOrderSQL);
									// System.out.println("1B"+pushField+"\t"+pushTableName+"\t"+field+"\t"+opName+"\t"+orderSQL);
									secondaryPA.addOptions(this
											.getLookupOptions(secPushField,
													secPushTableName,
													secOtherDataset, fd3
															.getKey(),
													secOtherDataset
															.getDataset(),
													secPafield, secOpName,
													secOrderSQL, schema,
													secColForDisplay));
									options3[r].addPushAction(secondaryPA);
								}
							}
						}

						if (pa.getOptions().length > 0)
							options2[i].addPushAction(pa);
					}
				} else {// push action refers to a filter in this dataset
					final FilterDescription fd2 = dsv
							.getFilterDescriptionByInternalName(filter2);// doesn't
					// work
					// for
					// placeholder
					fd2.setType("drop_down_basic_filter");
					final String pushField = fd2.getField();
					final String pushColForDisplay = fd2.getColForDisplay();
					String pushInternalName = fd2.getInternalName();
					if (pushInternalName.matches("\\w+\\.\\w+"))
						pushInternalName = pushInternalName.split("\\.")[0]
								+ "__" + pushInternalName.split("\\.")[1];
					final String pushTableName = fd2.getTableConstraint();

					String pafield;
					Option[] options2;
					pafield = validatedFilter.getField();
					options2 = validatedFilter.getOptions();

					for (int i = 0; i < options2.length; i++) {
						final Option op = options2[i];
						final String opName = op.getDisplayName();
						final PushAction pa = new PushAction(pushInternalName
								+ "_push_" + opName.replaceAll(" ", "_"), null,
								null, pushInternalName, orderSQL);

						// System.out.println("2A"+pushField+"\t"+pushTableName+"\t"+field+"\t"+opName+"\t"+orderSQL);
						pa.addOptions(this.getLookupOptions(pushField,
								pushTableName, dsv, fd2.getKey(),
								dsv.getDataset(), field, opName, orderSQL,
								schema, pushColForDisplay));
						// ADD ANY SECONDARY PUSH ACTION
						for (int p = 0; p < secondaryPushActions.length; p++) {
							final String secFilter2 = secondaryPushActions[p];
							final String secOrderSQL = secondaryOrderBy[p];
							final FilterDescription referredFilter = dsv
									.getFilterDescriptionByInternalName(pushActions[n]);
							if (referredFilter.getOtherFilters() != null) {
								String secOtherDatasetFilter1 = null;
								DatasetConfig secOtherDataset = null;
								FilterDescription fd3 = null;
								final String[] secOtherFilters = referredFilter
										.getOtherFilters().split(";");
								for (int q = 0; q < secOtherFilters.length; q++) {
									secOtherDataset = this
											.getDatasetConfigByDatasetID(null,
													secOtherFilters[q]
															.split("\\.")[0],
													"", this.getSchema()[0]);
									this.dscutils
											.loadDatasetConfigWithDocument(
													secOtherDataset,
													this.getDatasetConfigDocumentByDatasetID(
															null,
															secOtherFilters[p]
																	.split("\\.")[0],
															secOtherDataset
																	.getDatasetID(),
															this.getSchema()[0]));
									if (secOtherDataset
											.containsFilterDescription(secFilter2))
										fd3 = secOtherDataset
												.getFilterDescriptionByInternalName(secFilter2);
									if (fd3 != null) {
										secOtherDatasetFilter1 = secOtherFilters[p]
												.split("\\.")[1];
										break;
									}
								}
								fd3.setType("drop_down_basic_filter");
								final String secPushField = fd3.getField();
								final String secColForDisplay = fd3
										.getColForDisplay();
								final String secPushInternalName = fd3
										.getInternalName();
								final String secPushTableName = fd3
										.getTableConstraint();

								final String secPafield = secOtherDataset
										.getFilterDescriptionByInternalName(
												secOtherDatasetFilter1)
										.getField();
								final Option[] options3 = pa.getOptions();
								for (int r = 0; r < options3.length; r++) {
									final Option op3 = options3[r];
									final String secOpName = op3
											.getDisplayName();
									final PushAction secondaryPA = new PushAction(
											secPushInternalName
													+ "_push_"
													+ secOpName.replaceAll(" ",
															"_"), null, null,
											secPushInternalName, secOrderSQL);
									// System.out.println("2B"+secPushField+"\t"+secPushTableName+"\t"+secPafield+"\t"+secOpName+"\t"+secOrderSQL);
									secondaryPA.addOptions(this
											.getLookupOptions(secPushField,
													secPushTableName,
													secOtherDataset, fd3
															.getKey(),
													secOtherDataset
															.getDataset(),
													secPafield, secOpName,
													secOrderSQL, schema,
													secColForDisplay));
									options3[r].addPushAction(secondaryPA);
								}
							}
						}

						if (pa.getOptions().length > 0)
							options2[i].addPushAction(pa);

					}
				}
			}// end of add push actions code
			validatedFilter.setOptionsBroken();// need to set broken so
			// getValidatedCollection knows
			// to change it
			return validatedFilter;
		}// END OF ADD VALUE OPTIONS

		// VALIDATE "FILTER-TYPE" OPTIONS
		// Connection conn = dsource.getConnection();
		for (int j = 0; j < options.length; j++) {
			final Option validatedOption = this.getValidatedOption(schema,
					catalog, options[j], dset, conn, mains, keys);
			if (validatedOption.isBroken()) {
				optionsValid = false;
				brokenOptions.put(new Integer(j), validatedOption);
			}
		}
		// conn.close();
		// DetailedDataSource.close(conn);
		if (!optionsValid) {
			validatedFilter.setOptionsBroken();

			for (final Iterator iter = brokenOptions.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();
				final Option brokenOption = (Option) brokenOptions
						.get(position);
				validatedFilter.removeOption(options[position.intValue()]);
				validatedFilter.insertOption(position.intValue(), brokenOption);
			}
		}
		return validatedFilter;
	}

	private Option getValidatedOption(final String schema,
			final String catalog, final Option option, final String dset,
			final Connection conn, final String[] mains, final String[] keys)
			throws ConfigurationException, SQLException {
		final Option validatedOption = new Option(option);
		// hack to ignore the expression drop down menu

		if (validatedOption.getType() != null
				&& validatedOption.getType().equals("tree"))
			return validatedOption;

		if (validatedOption.getField() != null) {

			final String field = option.getField();
			String tableConstraint = option.getTableConstraint();

			// if the tableConstraint is null, this field must be available in
			// one of the main tables

			if (!tableConstraint.equals("main"))
				if (!tableConstraint.startsWith(dset + "__"))
					if (tableConstraint.matches(".*__.*__.*"))
						tableConstraint = dset + "__"
								+ tableConstraint.split("__")[1] + "__"
								+ tableConstraint.split("__")[2];
					else
						tableConstraint = dset + "__"
								+ tableConstraint.split("__")[0] + "__"
								+ tableConstraint.split("__")[1];

			// String table = !tableConstraint.equals("main") ? tableConstraint
			// : dset + "%" + this.MAINTABLESUFFIX;
			// // String table = (tableConstraint != null) ? "%" +
			// tableConstraint
			// // + "%" : "%" + MAINTABLESUFFIX;
			//
			// if (this.dsource.getDatabaseType().equals("oracle"))
			// table = table.toUpperCase();
			// // System.out.println("databaseType()
			// "+dsource.getDatabaseType());
			//
			// // Connection conn = dsource.getConnection();
			// final ResultSet rs = conn.getMetaData().getColumns(catalog,
			// schema,
			// table, field);
			// while (rs.next()) {
			// final String columnName = rs.getString(4);
			// final String tableName = rs.getString(3);
			//
			// final boolean[] valid = this.isValidDescription(columnName,
			// field, tableName, tableConstraint);
			// fieldValid = valid[0];
			// tableValid = valid[1];
			// if (valid[0] && valid[1])
			// break;
			// }
			// rs.close();

			DescriptionValidity desVal = validateDescription(schema, catalog,
					dset, conn, field, tableConstraint);

			boolean fieldValid = desVal.fieldValid;
			boolean tableValid = desVal.tableValid;

			// conn.close();
			// DetailedDataSource.close(conn);

			// test for all nulls as well if flagged and set fieldValid and
			// tableValid = false if all nulls
			if (tableConstraint.equals("main")) {
				// tableConstraint = [] mains;
				boolean found = false;
				for (int i = 0; i < keys.length; i++) {
					tableConstraint = mains[i];
					if (keys[i].equals(validatedOption.getKey())) {
						found = true;
						break;
					}
				}
				if (found) {
					DescriptionValidity keyVal = validateDescription(schema,
							catalog, dset, conn, validatedOption.getKey(),
							tableConstraint);
					if (!keyVal.fieldValid) {
						String suggestedKey = suggestKeyFix(schema, catalog,
								dset, conn, tableConstraint);
						System.err.println(dset + "_" + tableConstraint + "."
								+ validatedOption.getKey()
								+ " does not exist for " + " option "
								+ validatedOption.getInternalName());
						if (suggestedKey != null) {
							System.err.println("<SpecificOptionContent key=\""
									+ suggestedKey + "\" field=\""
									+ validatedOption.getField()
									+ "\" internalName=\"" + dset + "\" />");
							validatedOption.setKey(suggestedKey);
						}
					}
					if (!keyVal.tableValid) {
						System.err.println("Incorrect table for key "
								+ validatedOption.getKey());
					}
				}
			} else if (desVal.fieldValid && desVal.tableValid
					&& validatedOption.getKey() != null) {
				// DMS says check the key too!
				DescriptionValidity keyVal = validateDescription(schema,
						catalog, dset, conn, validatedOption.getKey(),
						tableConstraint);
				if (!keyVal.fieldValid) {
					String suggestedKey = suggestKeyFix(schema, catalog, dset,
							conn, tableConstraint);
					System.err.println(dset + "_" + tableConstraint + "."
							+ validatedOption.getKey() + " does not exist for "
							+ " option " + validatedOption.getInternalName());
					if (suggestedKey != null) {
						System.err.println("<SpecificOptionContent key=\""
								+ suggestedKey + "\" field=\""
								+ validatedOption.getField()
								+ "\" internalName=\"" + dset + "\" />");
						validatedOption.setKey(suggestedKey);
					}
				}
				if (!keyVal.tableValid) {
					System.err.println("Incorrect table for key "
							+ validatedOption.getKey());
				}
			}
			if ("true".equals(validatedOption.getCheckForNulls()) && fieldValid
					&& tableValid && this.isAllNull(field, tableConstraint)) {
				fieldValid = false;
				tableValid = false;
			}

			if (!fieldValid || !tableValid)
				// System.out.println("CHNAGING OPTION\t" + validatedOption);
				validatedOption.setHidden("true");
			else if (validatedOption.getHidden() != null
					&& validatedOption.getHidden().equals("true")) {
				validatedOption.setHidden("false");
				validatedOption.setFieldBroken(); // so gets changed in the
				// update
			}

			if (!(fieldValid && tableValid)) {
				validatedOption.setFieldBroken(); // eg. if field is valid,
				// Option.hasBrokenField
				// will return false
				validatedOption.setTableConstraintBroken();
				// eg. if table is valid, Option.hasBrokenTableConstraint will
				// return false
			}
		} else {

			// check Options/PushAction Options
			boolean optionsValid = true;
			final HashMap brokenOptions = new HashMap();

			final Option[] options = validatedOption.getOptions();
			for (int j = 0, m = options.length; j < m; j++) {
				final Option validatedSubOption = this.getValidatedOption(
						schema, catalog, options[j], dset, conn, mains, keys);
				if (validatedSubOption.isBroken()) {
					optionsValid = false;
					brokenOptions.put(new Integer(j), validatedSubOption);
				}
			}

			if (!optionsValid) {
				validatedOption.setOptionsBroken();
				// if optionsValid is false, option.hasBrokenOptions would be
				// true

				for (final Iterator iter = brokenOptions.keySet().iterator(); iter
						.hasNext();) {
					final Integer position = (Integer) iter.next();
					final Option brokenOption = (Option) brokenOptions
							.get(position);

					// remove the old version of the broken option
					validatedOption.removeOption(options[position.intValue()]);
					// insert the validated version of the broken option in its
					// place
					validatedOption.insertOption(position.intValue(),
							brokenOption);
				}
			}

			boolean pushActionsValid = true;
			final HashMap brokenPushActions = new HashMap();
			final PushAction[] pas = validatedOption.getPushActions();
			for (int j = 0, m = pas.length; j < m; j++) {
				final PushAction validatedAction = this.getValidatedPushAction(
						schema, catalog, pas[j], dset, conn, mains, keys);
				if (validatedAction.isBroken()) {
					pushActionsValid = false;
					brokenPushActions.put(new Integer(j), validatedAction);
				}
			}

			if (!pushActionsValid) {
				validatedOption.setPushActionsBroken();
				// if pushActionsValid is false, option.hasBrokenPushActions
				// will be true

				for (final Iterator iter = brokenPushActions.keySet()
						.iterator(); iter.hasNext();) {
					final Integer position = (Integer) iter.next();
					final PushAction brokenPushAction = (PushAction) brokenPushActions
							.get(position);

					validatedOption.removePushAction(pas[position.intValue()]);
					validatedOption.addPushAction(brokenPushAction); // PushActions
					// are
					// not
					// sensitive
					// to
					// position
				}
			}
		}

		return validatedOption;
	}

	private String suggestKeyFix(final String schema, final String catalog,
			final String dset, final Connection conn, String tableConstraint)
			throws SQLException {
		Set columns = (Set) getTableColumns(dset, schema, catalog, conn).get(
				tableConstraint.toLowerCase());
		String key = null;
		if(columns!=null) {
		for (Object col : columns) {
			String c = (String) col;
			if (c.matches(".*_key$")) {
				key = (String) col;
				break;
			}
		}
		}
		return key;
	}

	private boolean pruneBrokenOptions(final FilterDescription original,
			final FilterDescription validated) throws SQLException {

		if (original.getOptions().length == 0)
			return validated.isBroken();

		// to keep ontology filters working with nested options
		if ("1".equals(original.getGraph()))
			return false;

		final Option[] originalOpts = original.getOptions();

		for (int i = 0; i < originalOpts.length; i++)
			if (!validated.containsOption(originalOpts[i].getInternalName()))
				original.removeOption(originalOpts[i]);
			else if (this
					.pruneBrokenOption(originalOpts[i], validated
							.getOptionByInternalName(originalOpts[i]
									.getInternalName())))
				original.removeOption(originalOpts[i]);

		return original.getOptions().length == 0;
	}

	private boolean pruneBrokenOption(final Option original,
			final Option validated) {

		if (original.getOptions().length + original.getPushActions().length == 0
				|| validated.hasBrokenField()
				|| validated.hasBrokenTableConstraint())
			return validated.isBroken();

		final Option[] originalOpts = original.getOptions();

		for (int i = 0; i < originalOpts.length; i++)
			if (!validated.containsOption(originalOpts[i].getInternalName()))
				original.removeOption(originalOpts[i]);
			else if (this
					.pruneBrokenOption(originalOpts[i], validated
							.getOptionByInternalName(originalOpts[i]
									.getInternalName())))
				original.removeOption(originalOpts[i]);
		final PushAction[] originalPAs = original.getPushActions();

		for (int i = 0; i < originalPAs.length; i++)
			original.removePushAction(originalPAs[i]);

		final PushAction[] validPAs = validated.getPushActions();

		for (int i = 0; i < validPAs.length; i++) {
			final PushAction newPA = new PushAction(validPAs[i]);
			final Option[] oldOpts = validPAs[i].getOptions();

			for (int j = 0; j < oldOpts.length; j++)
				if (this.pruneBrokenOption(oldOpts[j], oldOpts[j]))
					newPA.removeOption(oldOpts[j]);

			if (newPA.getOptions().length > 0)
				original.addPushAction(newPA);
		}

		return original.getOptions().length + original.getPushActions().length == 0;
	}

	private PushAction getValidatedPushAction(final String schema,
			final String catalog, final PushAction action, final String dset,
			final Connection conn, final String[] mains, final String[] keys)
			throws ConfigurationException, SQLException {
		final PushAction validatedPushAction = new PushAction(action);

		boolean optionsValid = true;
		final HashMap brokenOptions = new HashMap();

		final Option[] options = validatedPushAction.getOptions();

		for (int j = 0, m = options.length; j < m; j++) {
			final Option validatedSubOption = this.getValidatedOption(schema,
					catalog, options[j], dset, conn, mains, keys);
			if (validatedSubOption.isBroken()) {
				optionsValid = false;
				brokenOptions.put(new Integer(j), validatedSubOption);
			}
		}

		if (!optionsValid) {
			validatedPushAction.setOptionsBroken(); // if optionsValid is false,
			// option.hasBrokenOptions
			// would be true

			for (final Iterator iter = brokenOptions.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();
				final Option brokenOption = (Option) brokenOptions
						.get(position);

				validatedPushAction.removeOption(options[position.intValue()]);
				validatedPushAction.insertOption(position.intValue(),
						brokenOption);
			}
		}

		return validatedPushAction;
	}

	private AttributePage getValidatedAttributePage(final AttributePage page,
			final String dset, final Connection conn, final String[] mains,
			final String[] keys) throws ConfigurationException, SQLException {
		final AttributePage validatedPage = new AttributePage(page);

		boolean hasBrokenGroups = false;
		final HashMap brokenGroups = new HashMap();

		final List allGroups = page.getAttributeGroups();
		for (int i = 0, n = allGroups.size(); i < n; i++) {
			final Object group = allGroups.get(i);

			if (group instanceof AttributeGroup) {
				final AttributeGroup validatedGroup = this
						.getValidatedAttributeGroup((AttributeGroup) group,
								dset, conn, mains, keys);

				if (validatedGroup.isBroken()) {
					hasBrokenGroups = true;
					// brokenGroups.put(new Integer(i), group);
					brokenGroups.put(new Integer(i), validatedGroup);
				}
			} // else not needed yet
		}

		if (hasBrokenGroups) {
			validatedPage.setGroupsBroken();

			for (final Iterator iter = brokenGroups.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();

				final Object brokenGroup = brokenGroups.get(position);
				if (brokenGroup instanceof AttributeGroup) {

					validatedPage
							.removeAttributeGroup((AttributeGroup) allGroups
									.get(position.intValue()));
					validatedPage.insertAttributeGroup(position.intValue(),
							(AttributeGroup) brokenGroup);
					allGroups.remove(position.intValue());
					allGroups.add(position.intValue(), brokenGroup);

				} // else not needed
			}
		}

		return validatedPage;
	}

	private AttributeGroup getValidatedAttributeGroup(
			final AttributeGroup group, final String dset,
			final Connection conn, final String[] mains, final String[] keys)
			throws ConfigurationException, SQLException {
		final AttributeGroup validatedGroup = new AttributeGroup(group);

		boolean hasBrokenCollections = false;
		final HashMap brokenCollections = new HashMap();

		final AttributeCollection[] collections = group
				.getAttributeCollections();
		for (int i = 0, n = collections.length; i < n; i++) {
			final AttributeCollection validatedCollection = this
					.getValidatedAttributeCollection(collections[i], dset,
							conn, mains, keys);

			if (validatedCollection.isBroken()) {
				hasBrokenCollections = true;
				brokenCollections.put(new Integer(i), validatedCollection);
			}
		}

		if (hasBrokenCollections) {
			validatedGroup.setCollectionsBroken();

			for (final Iterator iter = brokenCollections.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();
				final AttributeCollection brokenCollection = (AttributeCollection) brokenCollections
						.get(position);

				validatedGroup.removeAttributeCollection(collections[position
						.intValue()]);
				validatedGroup.insertAttributeCollection(position.intValue(),
						brokenCollection);
			}
		}

		return validatedGroup;
	}

	private AttributeCollection getValidatedAttributeCollection(
			final AttributeCollection collection, final String dset,
			final Connection conn, final String[] mains, final String[] keys)
			throws ConfigurationException, SQLException {

		final String catalog = "";
		final String schema = this.getSchema()[0];

		final AttributeCollection validatedAttributeCollection = new AttributeCollection(
				collection);
		boolean hasBrokenAttributes = false;
		final HashMap brokenAtts = new HashMap();

		final List allAtts = collection.getAttributeDescriptions();
		for (int i = 0, n = allAtts.size(); i < n; i++) {
			final Object attribute = allAtts.get(i);

			if (attribute instanceof AttributeDescription) {
				final AttributeDescription validatedAttributeDescription = this
						.getValidatedAttributeDescription(schema, catalog,
								(AttributeDescription) attribute, dset, conn,
								mains, keys);

				if (validatedAttributeDescription.isBroken()) {
					hasBrokenAttributes = true;
					brokenAtts.put(new Integer(i),
							validatedAttributeDescription);
				}
			} // else not needed yet
		}

		if (hasBrokenAttributes) {
			validatedAttributeCollection.setAttributesBroken();

			for (final Iterator iter = brokenAtts.keySet().iterator(); iter
					.hasNext();) {
				final Integer position = (Integer) iter.next();
				final Object brokenAtt = brokenAtts.get(position);

				if (brokenAtt instanceof AttributeDescription) {

					validatedAttributeCollection
							.removeAttributeDescription((AttributeDescription) allAtts
									.get(position.intValue()));
					validatedAttributeCollection.insertAttributeDescription(
							position.intValue(),
							(AttributeDescription) brokenAtt);
					allAtts.remove(position.intValue());
					allAtts.add(position.intValue(), brokenAtt);
				} // else not needed yet
			}
		}

		return validatedAttributeCollection;
	}

	private AttributeDescription getValidatedAttributeDescription(
			final String schema, final String catalog,
			final AttributeDescription description, final String dset,
			final Connection conn, final String[] mains, final String[] keys)
			throws ConfigurationException, SQLException {
		final AttributeDescription validatedAttribute = new AttributeDescription(
				description);

		if (validatedAttribute.getPointerDataset() != null
				&& !"".equals(validatedAttribute.getPointerDataset()))
			return validatedAttribute;

		String field = description.getField();
		if (field == null)
			return validatedAttribute;

		String tableConstraint = description.getTableConstraint();
		if (tableConstraint == null)
			return validatedAttribute;

		DescriptionValidity desVal = validateDescription(schema, catalog, dset,
				conn, field, tableConstraint);

		boolean fieldValid = desVal.fieldValid;
		boolean tableValid = desVal.tableValid;

		// String table = !tableConstraint.equals("main") ? tableConstraint :
		// dset
		// + "%" + this.MAINTABLESUFFIX;
		// if (this.dsource.getDatabaseType().equals("oracle"))
		// table = table.toUpperCase();
		//
		// Set<String> cols = getTableColumns(schema, catalog,
		// conn).get(tableName);
		// final ResultSet rs = conn.getMetaData().getColumns(catalog, schema,
		// table, field);
		// while (rs.next()) {
		// final String columnName = rs.getString(4);
		// final String tableName = rs.getString(3);
		// final boolean[] valid = this.isValidDescription(columnName, field,
		// tableName, tableConstraint);
		// fieldValid = valid[0];
		// tableValid = valid[1];
		// if (valid[0] && valid[1])
		// break;
		// }
		// rs.close();
		// conn.close();
		// DetailedDataSource.close(conn);
		// test for all nulls as well if flagged and set fieldValid and
		// tableValid = false if all nulls
		if (tableConstraint.equals("main")) {
			// tableConstraint = [] mains;
			for (int i = 0; i < keys.length; i++) {
				tableConstraint = mains[i];
				if (keys[i].equals(validatedAttribute.getKey()))
					break;
			}
		} else if (desVal.fieldValid && desVal.tableValid
				&& validatedAttribute.getKey() != null) {
			// DMS says check the key too!
			DescriptionValidity keyVal = validateDescription(schema, catalog,
					dset, conn, validatedAttribute.getKey(), tableConstraint);
			if (!keyVal.fieldValid || !keyVal.tableValid) {
				String suggestedKey = suggestKeyFix(schema, catalog, dset,
						conn, tableConstraint);
				System.err.println(dset + "_" + tableConstraint + "."
						+ validatedAttribute.getKey() + " does not exist for "
						+ " attribute " + validatedAttribute.getInternalName());
				if (suggestedKey != null) {
					System.err.println("<SpecificAttributeContent key=\""
							+ suggestedKey + "\" field=\""
							+ validatedAttribute.getField()
							+ "\" internalName=\"" + dset + "\" />");
					validatedAttribute.setKey(suggestedKey);
				}
			}
		}
		if ("true".equals(validatedAttribute.getCheckForNulls()) && fieldValid
				&& tableValid && this.isAllNull(field, tableConstraint)) {
			fieldValid = false;
			tableValid = false;
		}

		// System.out.println(dset+"\t"+tableConstraint+"\t"+tableValid+"\t"+field+"\t"+fieldValid);

		if (!fieldValid || !tableValid)
			validatedAttribute.setHidden("true");
		else if (validatedAttribute.getHidden() != null
				&& validatedAttribute.getHidden().equals("true")) {
			validatedAttribute.setHidden("false");
			validatedAttribute.setFieldBroken(); // so gets changed in the
			// update
		}

		if (!(fieldValid && tableValid)) {
			validatedAttribute.setFieldBroken();
			validatedAttribute.setTableConstraintBroken();
		}

		return validatedAttribute;
	}

	private DescriptionValidity validateDescription(final String schema,
			final String catalog, final String dset, final Connection conn,
			String field, String tableConstraint) throws SQLException {
		DescriptionValidity desVal = new DescriptionValidity();
		if (tableConstraint.equals("main")) {
			Set mainNames = getMainTables(dset, schema, catalog, conn);
			String tableName = null;
			for (Iterator i = mainNames.iterator(); i.hasNext();) {
				tableName = (String) i.next();
				Set columns = (Set) getTableColumns(dset, schema, catalog, conn)
						.get(tableName.toLowerCase());
				if (columns != null) {
					String columnName = null;
					for (Iterator i2 = columns.iterator(); i2.hasNext();) {
						columnName = (String) i2.next();
						final boolean[] valid = this.isValidDescription(
								columnName, field, tableName, tableConstraint);
						desVal.fieldValid = valid[0];
						desVal.tableValid = valid[1];
						if (valid[0] && valid[1])
							break;
					}
				}
				if (desVal.fieldValid && desVal.tableValid)
					break;
			}
		} else {
			String tableName = tableConstraint;
			Set columns = (Set) getTableColumns(dset, schema, catalog, conn)
					.get(tableConstraint.toLowerCase());
			if (columns != null) {
				String columnName = null;
				for (Iterator i = columns.iterator(); i.hasNext();) {
					columnName = (String) i.next();
					final boolean[] valid = this.isValidDescription(columnName,
							field, tableName, tableConstraint);
					desVal.fieldValid = valid[0];
					desVal.tableValid = valid[1];
					if (valid[0] && valid[1])
						break;
				}
			}
		}
		// System.out.println(tableConstraint+"."+field+"="+desVal.tableValid+"/"+desVal.fieldValid);
		return desVal;
	}

	private boolean[] isValidDescription(final String columnName,
			final String descriptionField, final String tableName,
			final String descriptionTableConstraint) {
		final boolean[] validFlags = new boolean[] { false, false };

		// oracle case mismatch
		if (columnName.toLowerCase().equals(descriptionField.toLowerCase())) {
			validFlags[0] = true;

			if (descriptionTableConstraint != null) {
				if (tableName.toLowerCase().indexOf(
						descriptionTableConstraint.toLowerCase()) > -1)
					validFlags[1] = true;
				else if (this.logger.isLoggable(Level.FINE))
					this.logger.fine("Recieved correct field, but tableName "
							+ tableName + " does not contain "
							+ descriptionTableConstraint + "\n");
			} else
				validFlags[1] = true;
		} else if (this.logger.isLoggable(Level.FINE))
			this.logger.fine("RECIEVED " + columnName + " WHEN EXPECTING "
					+ descriptionField + " from table " + tableName + " ( = "
					+ descriptionTableConstraint + " ?)\n");

		return validFlags;
	}

	/**
	 * Returns a list of potential (Naive) dataset names from a given Mart
	 * compliant database hosted on a given DetailedDataSource. These names can
	 * be used as an argument to getNaiveMainTablesFor,
	 * getNaiveDimensionTablesFor and getNaiveDatasetConfigFor.
	 * 
	 * @param schema
	 *            -- name of the RDBMS instance to search for potential datasets
	 * @return String[] of potential dataset names
	 * @throws SQLException
	 */
	public String[] getNaiveDatasetNamesFor(final String schema)
			throws SQLException {
		final String[] potentials = this.getNaiveMainTablesFor(schema, null);

		// System.out.println("HERe size "+potentials.length);

		// now weed them to a subset, attempting to unionize conformed dimension
		// names
		// List retList = new ArrayList();
		final Set retSet = new HashSet();

		for (int i = 0, n = potentials.length; i < n; i++) {
			final String curval = potentials[i];
			if (curval.startsWith("meta") || curval.startsWith("META"))
				continue;

			retSet.add(curval.replaceFirst("__.+__[Mm][Aa][Ii][Nn]", ""));
		}

		final String[] dsList = new String[retSet.size()];
		retSet.toArray(dsList);
		Arrays.sort(dsList);
		return dsList;
	}

	/**
	 * Retruns a String[] of possible main tables for a given Mart Compliant
	 * database, hosted on a given RDBMS, with an (optional) datasetName to key
	 * upon. With no datasetName, all possible main tables from the database are
	 * returned.
	 * 
	 * @param schema
	 *            -- name of the RDBMS instance to search for potential tables
	 * @param datasetName
	 *            -- name of the dataset to constrain the search (can be a
	 *            result of getNaiveDatasetNamesFor, or null)
	 * @return String[] of potential main table names
	 * @throws SQLException
	 */
	public String[] getNaiveMainTablesFor(final String schema,
			final String datasetName) throws SQLException {
		// want sorted entries, dont need to worry about duplicates
		final Set potentials = new TreeSet();

		final Connection conn = this.dsource.getConnection();

		final DatabaseMetaData dmd = conn.getMetaData();

		// This is crap DMD seems to be ignoring "__" in the search pattern
		// and it is imposible to have two datasets with overlapping names
		String tablePattern = datasetName != null ? datasetName + "%" : "%";
		// String tablePattern = (datasetName != null) ? datasetName + "__%":
		// "%";

		tablePattern += this.MAINTABLESUFFIX;
		final String capTablePattern = tablePattern.toUpperCase();

		// get all main tables

		if (this.dsource.getDatabaseType().equals("oracle")) {
			// String databaseName2 = getSchema();

			// first search for tablePattern
			ResultSet rsTab = dmd.getTables(null, schema, tablePattern, null);

			while (rsTab.next()) {
				final String tableName = rsTab.getString(3);
				// potentials.add(tableName);
				final String tableDataset = tableName.split("__")[0];
				if (datasetName == null || tableDataset.equals(datasetName))
					potentials.add(tableName);

			}
			rsTab.close();

			// now try capitals, should NOT get mixed results
			rsTab = dmd.getTables(null, schema, capTablePattern, null);
			while (rsTab.next()) {
				final String tableName = rsTab.getString(3);
				// NN
				// System.out.println(tableName);

				// if (!potentials.contains(tableName))
				// potentials.add(tableName);
				final String tableDataset = tableName.split("__")[0];
				if (datasetName == null
						|| tableDataset.equals(datasetName.toUpperCase()))
					potentials.add(tableName);
			}
			rsTab.close();

		}
		if (this.dsource.getDatabaseType().equals("mysql")) {

			// System.out.println("schema "+schema+" tablePattern
			// "+tablePattern);

			// ====
			// first search for tablePattern
			ResultSet rsTab = dmd.getTables(null, schema, tablePattern, null);

			while (rsTab.next()) {

				final String tableName = rsTab.getString(3);
				final String tableDataset = tableName.split("__")[0];
				if (datasetName == null || tableDataset.equals(datasetName))
					potentials.add(tableName);
			}
			rsTab.close();

			// now try capitals, should NOT get mixed results
			rsTab = dmd.getTables(null, schema, capTablePattern, null);
			while (rsTab.next()) {
				final String tableName = rsTab.getString(3);

				if (!potentials.contains(tableName)) {
					final String tableDataset = tableName.split("__")[0];
					if (datasetName == null || tableDataset.equals(datasetName))
						potentials.add(tableName);
				}
			}
			rsTab.close();

		}
		if (this.dsource.getDatabaseType().equals("postgres")) {
			// databaseName=POSTGRESDBNAME;

			// System.out.println("PG schema "+schema+" tablePattern
			// "+tablePattern);

			// System.out.println("Schema "+databaseName2);

			// first search for tablePattern
			ResultSet rsTab = dmd.getTables(null, schema, tablePattern, null);

			while (rsTab.next()) {
				final String tableName = rsTab.getString(3);

				// System.out.println("tableName "+tableName);
				// modified by yong liang for #25
				final String tableDataset = tableName.split("__")[0];
				if (datasetName == null || tableDataset.equals(datasetName))
					potentials.add(tableName);
			}
			rsTab.close();

			// now try capitals, should NOT get mixed results
			rsTab = dmd.getTables(null, schema, capTablePattern, null);
			while (rsTab.next()) {
				final String tableName = rsTab.getString(3);
				// NN
				// System.out.println(tableName);

				// if (!potentials.contains(tableName))
				// potentials.add(tableName);
				final String tableDataset = tableName.split("__")[0];
				if (datasetName == null
						|| tableDataset.equals(datasetName.toUpperCase()))
					potentials.add(tableName);
			}
			rsTab.close();

		}

		conn.close();

		final String[] retList = new String[potentials.size()];
		potentials.toArray(retList);

		return retList;
	}

	/**
	 * Retruns a String[] of main tables sorted on the number of join keys they
	 * contain
	 * 
	 * @param databaseName
	 *            -- name of the RDBMS instance to search for potential tables
	 * @param datasetName
	 *            -- name of the dataset to constrain the search (can be a
	 *            result of getNaiveDatasetNamesFor, or null)
	 * @return String[] of potential main table names
	 * @throws SQLException
	 */
	public String[] sortNaiveMainTables(final String[] mainTables,
			final String databaseName) throws SQLException {

		if (mainTables.length == 1)
			return mainTables;
		final List sortedMainTables = new ArrayList();
		int resolution = 1;
		int numberKeys;
		while (sortedMainTables.size() < mainTables.length) {
			for (int i = 0, n = mainTables.length; i < n; i++) {
				numberKeys = 0;
				final String tableName = mainTables[i];

				final TableDescription table = this.getTableDescriptionFor(
						databaseName, tableName);
				// System.out.println("DATABASE NAME "+databaseName);

				for (int j = 0, m = table.columnDescriptions.length; j < m; j++) {
					final ColumnDescription column = table.columnDescriptions[j];
					final String cname = column.name;

					// System.out.println("COLUMN "+cname);

					// NN
					if (cname.endsWith("_KEY") || cname.endsWith("_key"))
						// if (cname.endsWith("_key"))
						numberKeys++;
				}
				if (numberKeys == resolution) {
					sortedMainTables.add(tableName);
					resolution++;
					break;
				}

				// System.out.println("before tttable descripiton table
				// "+tableName+ " sorted length "+
				// sortedMainTables.size() +"< "+mainTables.length+ " resolution
				// "+resolution +
				// "number of keys"+ numberKeys);

			}
			// incase first main table has 2 keys
			if (sortedMainTables.size() < 1)
				resolution++;

		}
		final String[] retList = new String[sortedMainTables.size()];
		sortedMainTables.toArray(retList);
		return retList;
	}

	/**
	 * Returns a String[] of potential dimension tables from a given Mart
	 * Compliant database, hosted on a given RDBMS, constrained to an (optional)
	 * dataset.
	 * 
	 * @param databaseName
	 *            -- name of the RDBMS instance to search for potential tables
	 * @param datasetName
	 *            -- name of the dataset to constrain the search (can be a
	 *            result of getNaiveDatasetNamesFor, or null)
	 * @return String[] of potential dimension table names
	 * @throws SQLException
	 */
	public String[] getNaiveDimensionTablesFor(final String databaseName,
			final String datasetName) throws SQLException {
		// want sorted entries, dont need to worry about duplicates
		final Set potentials = new TreeSet();

		final Connection conn = this.dsource.getConnection();

		final DatabaseMetaData dmd = conn.getMetaData();

		// Note: currently this isnt cross platform,
		// as some RDBMS capitalize all names of tables
		// Either need to find a capitalization scheme general
		// to all RDBMS, or search for both MAIN and main
		// and force intermart consistency of capitalization for
		// those RDBMS which allow users freedom to capitalize as
		// they see fit. Currently does the latter.

		String tablePattern = datasetName != null ? datasetName + "%" : "%";
		// String tablePattern = (datasetName != null) ? datasetName : "%";

		tablePattern += this.DIMENSIONTABLESUFFIX;
		final String capTablePattern = tablePattern.toUpperCase();

		// get all dimension tables
		// first search for tablePattern
		ResultSet rsTab = dmd.getTables(null, databaseName, tablePattern, null);

		while (rsTab.next()) {
			final String tableName = rsTab.getString(3);
			potentials.add(tableName);
		}
		rsTab.close();

		// now try capitals, should NOT get mixed results
		rsTab = dmd.getTables(null, databaseName, capTablePattern, null);
		while (rsTab.next()) {
			final String tableName = rsTab.getString(3);

			if (!potentials.contains(tableName))
				potentials.add(tableName);
		}
		rsTab.close();
		conn.close();

		final String[] retList = new String[potentials.size()];
		potentials.toArray(retList);
		return retList;
	}

	/**
	 * Returns a TableDescription object describing a particular table in a
	 * given database, hosted on a given RDBMS.
	 * 
	 * @param schema
	 *            -- name of the RDBMS instance housing the requested table
	 * @param tableName
	 *            -- name of the desired table, as might be returned by a call
	 *            to getXXXTablesFor
	 * @return TableDescription object describing the table
	 * @throws SQLException
	 */
	public TableDescription getTableDescriptionFor(final String schema,
			final String tableName) throws SQLException {
		final Connection conn = this.dsource.getConnection();
		final DatabaseMetaData dmd = conn.getMetaData();

		final List columns = new ArrayList();
		final ResultSet rset = dmd.getColumns(null, schema, tableName, null);
		// System.out.println("columns schema"+schema+" table name "+tableName);

		while (rset.next())
			if (rset.getString(3).toLowerCase().equals(tableName.toLowerCase())) {
				final String cname = rset.getString(4);
				final int javaType = rset.getInt(5);
				final String dbType = rset.getString(6);
				final int maxLength = rset.getInt(7);

				final ColumnDescription column = new ColumnDescription(cname,
						dbType, javaType, maxLength);
				columns.add(column);
			}
		rset.close();
		DetailedDataSource.close(conn);

		final ColumnDescription[] cols = new ColumnDescription[columns.size()];
		columns.toArray(cols);

		final TableDescription table = new TableDescription(tableName, cols);
		return table;
	}

	// TODO: change this when Mart Compliant Schema is fully optimized
	/**
	 * Returns a Naive DatasetConfig for a given dataset within a given Mart
	 * Compliant database, hosted on a given RDBMS.
	 * 
	 * @param schema
	 *            -- name of the RDBMS instance to search for potential tables
	 * @param datasetName
	 *            -- name of the dataset to constrain the search (can be a
	 *            result of getNaiveDatasetNamesFor, or null)
	 * @return DatasetConfig nievely generated
	 * @throws ConfigurationException
	 * @throws SQLException
	 */
	public DatasetConfig getNaiveDatasetConfigFor(final String schema,
			final String datasetName) throws ConfigurationException,
			SQLException {

		final Timestamp tstamp = new Timestamp(System.currentTimeMillis());
		final Connection conn = this.dsource.getConnection();

		final DatasetConfig dsv = new DatasetConfig("default", datasetName,
				datasetName, "", "TableSet", "1", "", "", "", "",
				tstamp.toString(), "default", "default", "", datasetName,
				this.SOFTWAREVERSION, "", "", "");

		final AttributePage ap = new AttributePage();
		ap.setInternalName("naive_attributes");
		ap.setDisplayName("ATTRIBUTES");
		ap.setOutFormats("html,txt,csv,tsv,xls");

		final FilterPage fp = new FilterPage();
		fp.setInternalName("naive_filters");
		fp.setDisplayName("FILTERS");

		final AttributeGroup ag = new AttributeGroup();
		ag.setInternalName("features");
		ag.setDisplayName("FEATURES");

		final FilterGroup fg = new FilterGroup();
		fg.setInternalName("filters");
		fg.setDisplayName("FILTERS");

		// need to sort starbases in order of the number of keys they contain
		// primaryKeys should be in this same order

		final Hashtable attMap = new Hashtable();
		final Hashtable filtMap = new Hashtable();
		int duplicated = 0;

		final List mainTables = new ArrayList();
		final List finalMains = new ArrayList();

		mainTables.addAll(Arrays.asList(this.sortNaiveMainTables(
				this.getNaiveMainTablesFor(schema, datasetName), schema)));

		final List primaryKeys = new ArrayList();
		for (int i = 0, n = mainTables.size(); i < n; i++) {
			final String tableName = (String) mainTables.get(i);
			// can have single main tables with no keys so add here now
			finalMains.add(mainTables.get(i));

			final TableDescription table = this.getTableDescriptionFor(schema,
					tableName);
			for (int j = 0, m = table.columnDescriptions.length; j < m; j++) {
				final ColumnDescription column = table.columnDescriptions[j];
				final String cname = column.name;
				if ((cname.endsWith("_key") || cname.endsWith("_KEY"))
						&& !primaryKeys.contains(cname))
					primaryKeys.add(cname);
			}
		}

		final String[] sbases = new String[finalMains.size()];
		finalMains.toArray(sbases);
		dsv.addMainTables(sbases);

		final String[] pkeys = new String[finalMains.size()];
		if (primaryKeys.size() > 0) {
			// make sure no of keys matches no of mains
			for (int i = 0; i < sbases.length; i++)
				pkeys[i] = (String) primaryKeys.get(i);
			dsv.addPrimaryKeys(pkeys);
		}

		final List allTables = new ArrayList();
		allTables.addAll(mainTables);
		allTables.addAll(Arrays.asList(this.getNaiveDimensionTablesFor(schema,
				datasetName)));

		final List allCols = new ArrayList();

		// ID LIST FC and FDs
		final FilterCollection fcList = new FilterCollection();
		fcList.setInternalName("id_list");
		fcList.setDisplayName("ID LIST");

		final FilterDescription fdBools = new FilterDescription();
		fdBools.setInternalName("naive_id_list_filters");
		fdBools.setType("boolean_list");
		fdBools.setDisplayType("container");
		final FilterDescription fdLists = new FilterDescription();
		fdLists.setInternalName("naive_id_list_limit_filters");
		fdLists.setType("id_list");
		fdLists.setDisplayType("container");

		for (int i = 0, n = allTables.size(); i < n; i++) {
			final String tableName = (String) allTables.get(i);
			// System.out.println ("Second time tablename "+ tableName);
			String content = null;
			final String fullTableName = tableName;
			final String[] tableTokenizer = tableName.split("__");
			content = tableTokenizer[1];

			AttributeCollection ac = null;
			if (!this.isLookupTable(tableName)) {
				ac = new AttributeCollection();
				ac.setInternalName(content);
				ac.setDisplayName(content.replaceAll("_", " "));
			}

			FilterCollection fc = null;
			if (this.isMainTable(tableName)) {

				fc = new FilterCollection();
				fc.setInternalName(content);
				fc.setDisplayName(content.replaceAll("_", " "));
			}

			final TableDescription table = this.getTableDescriptionFor(schema,
					tableName);

			// need to find the lowest joinKey for table first;
			String joinKey = null;
			outer: for (int k = pkeys.length - 1; k > -1; k--)
				for (int j = 0, m = table.columnDescriptions.length; j < m; j++) {
					final ColumnDescription column = table.columnDescriptions[j];
					final String cname = column.name;
					if (cname.equals(pkeys[k])) {
						joinKey = cname;
						break outer;
					}
				}

			for (int j = 0, m = table.columnDescriptions.length; j < m; j++) {
				final ColumnDescription column = table.columnDescriptions[j];
				duplicated = 0;
				// NN
				final String cname = column.name.toLowerCase();
				// String cname = column.name;

				// ignore the key columns as atts and filters
				if (cname.endsWith("_key"))
					continue;

				// if the column already seen in a higher resolution
				// main table ignore

				if (this.isMainTable(tableName) && allCols.contains(cname))
					continue;

				final int ctype = column.javaType; // java generalized type
				// across all JDBC instances
				// String ctype = column.dbType; //as in RDBMS table definition
				final int csize = column.maxLength;

				if (this.logger.isLoggable(Level.FINE))
					this.logger.fine(tableName + ": " + cname + "-- type : "
							+ ctype + "\n");

				final String shortTableName = this.isMainTable(tableName) ? "main"
						: tableName.indexOf("__") > 0 ? tableName
								.substring(tableName.indexOf("__") + 2)
								: tableName;

				if (this.isMainTable(tableName)
						|| this.isDimensionTable(tableName)) {
					// System.out.println ("tableName before AllNULL
					// "+tableName);
					if (this.isAllNull(cname, fullTableName))
						continue;

					// System.out.println ("tableName after AllNULL
					// "+tableName);

					if (this.isMainTable(tableName)) {
						// System.out.println("Resetting table name to: "+
						// tableName);

						allCols.add(cname);
						if (!cname.endsWith("_bool")) {
							if (filtMap.containsKey(cname))
								duplicated = 1;
							filtMap.put(cname, "1");
							fc.addFilterDescription(this.getFilterDescription(
									cname, shortTableName, ctype, joinKey, dsv,
									duplicated));
						} else {
							if (filtMap.containsKey(cname))
								duplicated = 1;
							filtMap.put(cname, "1");
							final FilterDescription fdBool = this
									.getFilterDescription(cname,
											shortTableName, ctype, joinKey,
											dsv, duplicated);

							final Option opBool = new Option(fdBool);
							fdBools.addOption(opBool);
						}
					}
					if (!cname.endsWith("_bool")) {
						if (attMap.containsKey(cname))
							duplicated = 1;
						attMap.put(cname, "1");
						final AttributeDescription ad = this
								.getAttributeDescription(cname, shortTableName,
										csize, joinKey, duplicated);
						// ad.setHidden("false");
						ac.addAttributeDescription(ad);
						if (cname.endsWith("_list")
								|| cname.equals("dbprimary_id")) {
							duplicated = 0;

							final String descriptiveName = shortTableName
									.split("__")[0].replaceFirst("xref_", "");
							if (filtMap.containsKey(descriptiveName))
								duplicated = 1;
							filtMap.put(descriptiveName, "1");

							// if (!cname.equals("display_id_list")){
							// if (filtMap.containsKey(cname)){
							// //System.out.println(cname + " is duplicated");
							// duplicated = 1;
							// }
							// filtMap.put(cname,"1");
							// }
							final FilterDescription fdList = this
									.getFilterDescription(cname,
											shortTableName, ctype, joinKey,
											dsv, duplicated);
							final Option op = new Option(fdList);
							fdLists.addOption(op);

						}
					}

				} else if (this.isLookupTable(tableName)) {
					if (cname.startsWith("glook_")
							|| cname.startsWith("silent_")) {
						if (fc == null) {
							fc = new FilterCollection();
							fc.setInternalName(content);
							fc.setDisplayName(content.replaceAll("_", " "));
						}
						if (filtMap.containsKey(cname))
							duplicated = 1;
						filtMap.put(cname, "1");
						fc.addFilterDescription(this.getFilterDescription(
								cname, shortTableName, ctype, joinKey, dsv,
								duplicated));

					}
				} else if (this.logger.isLoggable(Level.FINE))
					this.logger.fine("Skipping " + tableName + "\n");
			}
			if (ac != null && ac.getAttributeDescriptions().size() > 0)
				ag.addAttributeCollection(ac);

			if (fc != null && fc.getFilterDescriptions().size() > 0)
				fg.addFilterCollection(fc);
		}
		if (fdBools != null && fdBools.getOptions().length > 0)
			fcList.addFilterDescription(fdBools);
		if (fdLists != null && fdLists.getOptions().length > 0)
			fcList.addFilterDescription(fdLists);
		if (fcList != null && fcList.getFilterDescriptions().size() > 0)
			fg.addFilterCollection(fcList);
		if (ag != null && ag.getAttributeCollections().length > 0)
			ap.addAttributeGroup(ag);
		if (fg != null && fg.getFilterCollections().length > 0)
			fp.addFilterGroup(fg);
		if (ap != null && ap.getAttributeGroups().size() > 0)
			dsv.addAttributePage(ap);
		if (fp != null && fp.getFilterGroups().size() > 0)
			dsv.addFilterPage(fp);

		if (!this.configInfo.containsKey(MartEditor.getUser()))
			this.configInfo.put(MartEditor.getUser(), new HashMap());
		final HashMap userMap = (HashMap) this.configInfo.get(MartEditor
				.getUser());

		if (!userMap.containsKey(datasetName)) {
			final HashMap dsetMap = new HashMap();
			userMap.put(datasetName, dsetMap);
		}

		final HashMap dsetMap = (HashMap) userMap.get(datasetName);
		// dsetMap.put(iname, dsv);
		dsetMap.put("", dsv);

		return dsv;
	}

	public void stripTableConstraints(final DatasetConfig config) {
		final List thingsToChange = new ArrayList();
		// Attributes first.
		thingsToChange.addAll(config.getAllAttributeDescriptions());
		// Now filters.
		thingsToChange.addAll(config.getAllFilterDescriptions());
		// Iterate.
		for (int i = 0; i < thingsToChange.size(); i++) {
			final BaseNamedConfigurationObject obj = (BaseNamedConfigurationObject) thingsToChange
					.get(i);
			String tblCon = obj.getAttribute("tableConstraint");
			if (tblCon != null && !"".equals(tblCon) && !"main".equals(tblCon)
					&& tblCon.split("__").length > 2) {
				tblCon = tblCon.split("__")[1] + "__" + tblCon.split("__")[2];
				obj.setAttribute("tableConstraint", tblCon);
			}
			// Add options and pushActions.
			if (obj instanceof FilterDescription)
				thingsToChange.addAll(Arrays.asList(((FilterDescription) obj)
						.getOptions()));
			else if (obj instanceof Option) {
				thingsToChange
						.addAll(Arrays.asList(((Option) obj).getOptions()));
				thingsToChange.addAll(Arrays.asList(((Option) obj)
						.getPushActions()));
			} else if (obj instanceof PushAction)
				thingsToChange.addAll(Arrays.asList(((PushAction) obj)
						.getOptions()));
		}
	}

	public DatasetConfig getNewFiltsAtts(String schema,
			final DatasetConfig dsv, final boolean store)
			throws ConfigurationException, SQLException {
		if (this.dsource.getDatabaseType().equals("oracle"))
			schema = schema.toUpperCase();

		// System.out.println ("************* SCHEMA FROM GET NEW ATTT
		// "+schema);
		final String template = dsv.getTemplate();
		final DatasetConfig templateConfig = new DatasetConfig("template", "",
				template + "_template", "", "", "", "", "", "", "", "", "", "",
				"", template, "", "", "", "");
		MartEditor.getDatasetConfigXMLUtils().loadDatasetConfigWithDocument(
				templateConfig, this.getTemplateDocument(template));

		// if (dsource.getDatabaseType().equals("oracle"))
		// databaseName=getSchema();
		// System.out.println("databaseType() "+dsource.getDatabaseType());

		// if (dsource.getDatabaseType().equals("postgres"))
		// databaseName=POSTGRESDBNAME;

		final String datasetName = dsv.getDataset();

		final AttributePage ap = new AttributePage();
		ap.setInternalName("new_attributes");
		ap.setDisplayName("NEW_ATTRIBUTES");

		final FilterPage fp = new FilterPage();
		fp.setInternalName("new_filters");
		fp.setDisplayName("NEW_FILTERS");

		final AttributeGroup ag = new AttributeGroup();
		ag.setInternalName("new_attributes");
		ag.setDisplayName("NEW_ATTRIBUTES");

		final FilterGroup fg = new FilterGroup();
		fg.setInternalName("new_filters");
		fg.setDisplayName("NEW_FILTERS");

		final Hashtable attMap = new Hashtable();
		final Hashtable filtMap = new Hashtable();
		int duplicated = 0;

		// need to sort starbases in order of the number of keys they contain
		// primaryKeys should be in this same order

		final List starbases = new ArrayList();
		starbases.addAll(Arrays.asList(this.sortNaiveMainTables(
				this.getNaiveMainTablesFor(schema, datasetName), schema)));

		final List primaryKeys = new ArrayList();

		for (int i = 0, n = starbases.size(); i < n; i++) {
			final String tableName = (String) starbases.get(i);

			final TableDescription table = this.getTableDescriptionFor(schema,
					tableName);

			for (int j = 0, m = table.columnDescriptions.length; j < m; j++) {
				final ColumnDescription column = table.columnDescriptions[j];
				final String cname = column.name;
				// if (cname.endsWith("_key") && (!primaryKeys.contains(cname)))
				if ((cname.endsWith("_key") || cname.endsWith("_KEY"))
						&& !primaryKeys.contains(cname))
					primaryKeys.add(cname);
			}

		}

		final String[] sbases = new String[starbases.size()];
		starbases.toArray(sbases);
		// dsv.addMainTables(sbases);

		final String[] pkeys = new String[primaryKeys.size()];
		primaryKeys.toArray(pkeys);
		// dsv.addPrimaryKeys(pkeys);

		final List allTables = new ArrayList();
		allTables.addAll(starbases);

		allTables.addAll(Arrays.asList(this.getNaiveDimensionTablesFor(schema,
				datasetName)));

		// no longer required
		// allTables.addAll(Arrays.asList(getNaiveLookupTablesFor(schema,
		// datasetName)));
		final List allCols = new ArrayList();

		// ID LIST FC and FDs
		final FilterCollection fcList = new FilterCollection();
		fcList.setInternalName("id_list");
		fcList.setDisplayName("ID LIST");

		final FilterDescription fdBools = new FilterDescription();
		fdBools.setInternalName("new_id_list_filters");
		fdBools.setType("boolean_list");
		fdBools.setDisplayType("container");
		final FilterDescription fdLists = new FilterDescription();
		fdLists.setInternalName("new_id_list_limit_filters");
		fdLists.setType("id_list");
		fdLists.setDisplayType("container");

		for (int i = 0, n = allTables.size(); i < n; i++) {
			String tableName = (String) allTables.get(i);
			String content = null;
			final String fullTableName = tableName;
			final String[] tableTokenizer = tableName.split("__");
			content = tableTokenizer[1];

			AttributeCollection ac = null;
			if (!this.isLookupTable(tableName)) {
				ac = new AttributeCollection();
				ac.setInternalName(content);
				ac.setDisplayName(content.replaceAll("_", " "));
			}

			FilterCollection fc = null;
			if (this.isMainTable(tableName)) {

				fc = new FilterCollection();
				fc.setInternalName(content);
				fc.setDisplayName(content.replaceAll("_", " "));
			}

			final TableDescription table = this.getTableDescriptionFor(schema,
					tableName);

			// need to find the lowest joinKey for table first;
			String joinKey = null;
			outer: for (int k = pkeys.length - 1; k > -1; k--)
				for (int j = 0, m = table.columnDescriptions.length; j < m; j++) {
					final ColumnDescription column = table.columnDescriptions[j];
					final String cname = column.name;
					if (cname.equals(pkeys[k])) {
						joinKey = cname;
						break outer;
					}
				}

			for (int j = 0, m = table.columnDescriptions.length; j < m; j++) {
				final ColumnDescription column = table.columnDescriptions[j];

				// String cname = column.name;
				final String cname = column.name.toLowerCase();

				// System.out.println("TESTING COL " + cname);

				// ignore the key columns as atts and filters
				if (cname.endsWith("_key"))
					continue;

				final String shortTableName = this.isMainTable(tableName) ? "main"
						: tableName.indexOf("__") > 0 ? tableName
								.substring(tableName.indexOf("__") + 2)
								: tableName;

				// if the column already seen in a higher resolution
				// main table ignore

				if (this.isMainTable(tableName) && allCols.contains(cname))
					continue;

				final int ctype = column.javaType; // java generalized type
				// across all JDBC instances
				// String ctype = column.dbType; //as in RDBMS table definition
				final int csize = column.maxLength;
				if (this.logger.isLoggable(Level.FINE))
					this.logger.fine(tableName + ": " + cname + "-- type : "
							+ ctype + "\n");

				if (this.isMainTable(tableName)
						|| this.isDimensionTable(tableName)) {
					if (this.isAllNull(cname, fullTableName))
						continue;

					if (this.isMainTable(tableName)) {
						tableName = "main";
						allCols.add(cname);
						if (!cname.endsWith("_bool")) {
							FilterDescription currFilt = null;
							if (templateConfig
									.getFilterDescriptionByFieldNameTableConstraint(
											cname, shortTableName, null) != null)

								// System.out.println("cname "+ cname+ "
								// tableName " + tableName);
								currFilt = templateConfig
										.getFilterDescriptionByFieldNameTableConstraint(
												cname, shortTableName, null);
							// System.out.println(currFilt);
							if (currFilt == null) {
								System.err.println("Found new filter " + cname);
								if (filtMap.containsKey(cname))
									duplicated = 1;
								filtMap.put(cname, "1");
								fc.addFilterDescription(this
										.getFilterDescription(cname,
												shortTableName, ctype, joinKey,
												templateConfig, duplicated));
								// System.out.println("Going to null ");
							} else { // update options if has any
								// System.out.println(currFilt.getInternalName());
								// if (currFilt.hasOptions())
								// updateDropDown(dsv, currFilt);// these are
								// created from scratch during validation now
							}

						} else { // is a main table bool filter
							if (filtMap.containsKey(cname))
								duplicated = 1;
							filtMap.put(cname, "1");
							final FilterDescription fdBool = this
									.getFilterDescription(cname,
											shortTableName, ctype, joinKey,
											templateConfig, duplicated);

							final Option opBool = new Option(fdBool);
							boolean newOption = true;

							// cycle through all options looking for a match
							final FilterPage[] fps = templateConfig
									.getFilterPages();
							outer: for (int k = 0; k < fps.length; k++) {
								List fds = new ArrayList();
								fds = fps[k].getAllFilterDescriptions();
								for (int l = 0; l < fds.size(); l++) {
									final FilterDescription fdCurrent = (FilterDescription) fds
											.get(l);
									final Option[] ops = fdCurrent.getOptions();
									for (int p = 0, q = ops.length; p < q; p++)
										if (ops[p].getField() != null
												&& ops[p].getField().equals(
														cname)
												&& ops[p].getTableConstraint() != null
												&& ops[p].getTableConstraint()
														.equals(shortTableName)) {
											newOption = false;
											break outer;
										}
								}
							}

							// could be present as a FD as well
							FilterDescription currFilt = null;
							if (templateConfig
									.getFilterDescriptionByFieldNameTableConstraint(
											cname, shortTableName, null) != null)
								currFilt = templateConfig
										.getFilterDescriptionByFieldNameTableConstraint(
												cname, shortTableName, null);
							if (currFilt != null)
								newOption = false;

							if (newOption) {
								// option with this field and table
								// name doesn't already exist
								fdBools.addOption(opBool);
								System.err
										.println("Found new option " + opBool);

							}

						}
					}
					if (!cname.endsWith("_bool")) {
						if (attMap.containsKey(cname))
							duplicated = 1;
						attMap.put(cname, "1");
						final AttributeDescription ad = this
								.getAttributeDescription(cname, shortTableName,
										csize, joinKey, duplicated);

						if (templateConfig
								.getAttributeDescriptionByFieldNameTableConstraint(
										cname, shortTableName) == null)
							ac.addAttributeDescription(ad);
						duplicated = 0;
						if (cname.endsWith("_list")
								|| cname.equals("dbprimary_id")) {
							if (filtMap.containsKey(cname)) {
								// duplicated = 1;
							}
							filtMap.put(cname, "1");
							final FilterDescription fdList = this
									.getFilterDescription(cname,
											shortTableName, ctype, joinKey,
											templateConfig, duplicated);
							final Option op = new Option(fdList);

							boolean newOption = true;

							// cycle through all options looking for a match
							final FilterPage[] fps = templateConfig
									.getFilterPages();
							outer: for (int k = 0; k < fps.length; k++) {
								List fds = new ArrayList();
								fds = fps[k].getAllFilterDescriptions();
								for (int l = 0; l < fds.size(); l++) {
									final FilterDescription fdCurrent = (FilterDescription) fds
											.get(l);
									final Option[] ops = fdCurrent.getOptions();
									for (int p = 0, q = ops.length; p < q; p++)
										if (ops[p].getField() != null
												&& ops[p].getField().equals(
														cname)
												&& ops[p].getTableConstraint() != null
												&& ops[p].getTableConstraint()
														.equals(shortTableName)) {
											newOption = false;
											break outer;
										}
								}
							}

							// could be present as a FD as well
							FilterDescription currFilt = null;
							if (templateConfig
									.getFilterDescriptionByFieldNameTableConstraint(
											cname, shortTableName, null) != null)
								currFilt = templateConfig
										.getFilterDescriptionByFieldNameTableConstraint(
												cname, shortTableName, null);
							if (currFilt != null)
								newOption = false;

							if (newOption) {
								System.err.println("Found new option " + op);
								fdLists.addOption(op);
							}

						}
					}

				} else if (this.isLookupTable(tableName)) {
					if (cname.startsWith("glook_")
							|| cname.startsWith("silent_")) {
						if (fc == null) {
							fc = new FilterCollection();
							fc.setInternalName(content);
							fc.setDisplayName(content.replaceAll("_", " "));
						}

						final FilterDescription currFilt = templateConfig
								.getFilterDescriptionByFieldNameTableConstraint(
										cname, shortTableName, null);
						if (currFilt == null) {
							System.err.println("Found new filter " + cname);

							if (filtMap.containsKey(cname))
								duplicated = 1;
							filtMap.put(cname, "1");
							fc.addFilterDescription(this.getFilterDescription(
									cname, shortTableName, ctype, joinKey,
									templateConfig, duplicated));
						} else { // update options if has any
							// if (currFilt.hasOptions())
							// updateDropDown(dsv, currFilt);// these are
							// created from scratch during validation now
						}

					}
				} else if (this.logger.isLoggable(Level.FINE))
					this.logger.fine("Skipping " + tableName + "\n");
			}

			if (ac != null && ac.getAttributeDescriptions().size() > 0)
				ag.addAttributeCollection(ac);

			if (fc != null && fc.getFilterDescriptions().size() > 0)
				fg.addFilterCollection(fc);
		}

		if (fdBools != null && fdBools.getOptions().length > 0)
			fcList.addFilterDescription(fdBools);
		if (fdLists != null && fdLists.getOptions().length > 0)
			fcList.addFilterDescription(fdLists);
		if (fcList != null && fcList.getFilterDescriptions().size() > 0)
			fg.addFilterCollection(fcList);

		if (ag != null && ag.getAttributeCollections().length > 0)
			ap.addAttributeGroup(ag);
		if (fg != null && fg.getFilterCollections().length > 0)
			fp.addFilterGroup(fg);
		if (ap != null && ap.getAttributeGroups().size() > 0)
			templateConfig.addAttributePage(ap);
		if (fp != null && fp.getFilterGroups().size() > 0)
			templateConfig.addFilterPage(fp);

		// make sure template has same version as config incase it has got
		// increased
		templateConfig.setSoftwareVersion(dsv.getSoftWareVersion());
		if (store)
			this.storeTemplateXML(templateConfig, template);

		return templateConfig;
	}

	/*
	 * private void updateDropDown(DatasetConfig dsConfig, FilterDescription
	 * fd1) throws ConfigurationException, SQLException {
	 * 
	 * System.out.println("UPDATING OPTION " + fd1.getInternalName());
	 * 
	 * Option[] ops = fd1.getOptions(); System.out.println("LENGTH
	 * "+ops.length);
	 * 
	 * 
	 * if (ops[0].getTableConstraint() != null) return; // drop down lists of
	 * options shouldn't be updated
	 * 
	 * PushAction[] pas = ops[0].getPushActions();
	 * 
	 * PushAction[] pas2 = null; if (pas.length > 0){ Option[] paOps =
	 * pas[0].getOptions(); if (paOps.length > 0) pas2 =
	 * paOps[0].getPushActions(); }
	 * 
	 * System.out.println("REMOVING OPS STARTED"); fd1.removeOptions(); //for
	 * (int i = 0; i < ops.length; i++) { // fd1.removeOption(ops[i]); //}
	 * System.out.println("REMOVING FINISHED"); String field = fd1.getField();
	 * String tableName = fd1.getTableConstraint(); String joinKey =
	 * fd1.getKey(); fd1.setType("list"); fd1.setDisplayType("list");
	 * fd1.setStyle("menu"); fd1.setQualifier("="); fd1.setLegalQualifiers("=");
	 * String colForDisplay = fd1.getColForDisplay();
	 * 
	 * Option[] options = getOptions(field, tableName, joinKey, dsConfig,
	 * colForDisplay);
	 * 
	 * fd1.addOptions(options); // update push actions if any
	 * 
	 * for (int k = 0; k < pas.length; k++) {
	 * 
	 * String ref = pas[k].getRef();
	 * 
	 * 
	 * 
	 * FilterDescription fd2 = dsConfig.getFilterDescriptionByInternalName(ref);
	 * 
	 * if (fd2 != null){// because of martp placeholders
	 * updatePushAction(dsConfig, fd1, fd2,""); } } Option[] newOps =
	 * fd1.getOptions(); for (int k = 0; k < newOps.length; k++) { for (int l =
	 * 0; l < pas.length; l++) { PushAction[] newPas =
	 * newOps[k].getPushActions(); PushAction newPa = null; for (int z = 0; z <
	 * newPas.length; z++) { if (newPas[z].getRef().equals(pas[l].getRef())) {
	 * newPa = newPas[z]; break; } }
	 * 
	 * for (int m = 0; m < pas2.length; m++) { String ref2 = pas2[m].getRef();
	 * FilterDescription fd3 =
	 * dsConfig.getFilterDescriptionByInternalName(ref2);
	 * //System.out.println("LOOKING FOR " + ref2 + " IN " +
	 * dsConfig.getDataset()); if (fd3 == null){ //System.out.println("NOT
	 * FOUND"); continue; } updatePushAction(dsConfig, newPa, fd3,""); } } } }
	 * 
	 * private void updatePushAction(DatasetConfig dsConfig,
	 * BaseConfigurationObject bo, FilterDescription fd2, String orderBy) throws
	 * ConfigurationException, SQLException { //System.out.println(fd2);
	 * fd2.setType("drop_down_basic_filter");
	 * 
	 * String pushField = fd2.getField(); String pushColForDisplay =
	 * fd2.getColForDisplay(); String pushInternalName = fd2.getInternalName();
	 * if (pushInternalName.matches("\\w+\\.\\w+")) pushInternalName =
	 * pushInternalName.split("\\.")[0]+"__"+pushInternalName.split("\\.")[1];
	 * String pushTableName = fd2.getTableConstraint();
	 * 
	 * if (pushTableName.equals("main")){ String[] mains =
	 * dsConfig.getStarBases(); pushTableName = mains[0]; }
	 * 
	 * //String orderSQL = JOptionPane.showInputDialog("Optional column name to
	 * order " + pushInternalName + " :"); // can add push actions to existing
	 * push actions so need to know the class of the node String className =
	 * bo.getClass().getName(); String field; Option[] options;
	 * 
	 * if (className.equals("org.ensembl.mart.lib.config.FilterDescription")) {
	 * FilterDescription fd1 = (FilterDescription) bo; field = fd1.getField();
	 * //if (!fd1.getTableConstraint().equals(pushTableName)) // field =
	 * "olook_" + field; options = fd1.getOptions(); } else { PushAction pa1 =
	 * (PushAction) bo; String intName = pa1.getInternalName(); field =
	 * intName.split("_push")[0]; //if (field.startsWith("glook_")) { // field =
	 * field.replaceFirst("glook_", ""); //} options = pa1.getOptions(); }
	 * 
	 * 
	 * for (int i = 0; i < options.length; i++) { Option op = options[i]; String
	 * opName = op.getInternalName(); PushAction pa = new
	 * PushAction(pushInternalName + "_push_" + opName, null, null,
	 * pushInternalName, orderBy);
	 * 
	 * pa.addOptions(getLookupOptions(pushField, pushTableName, field, opName,
	 * orderBy,getSchema()[0],pushColForDisplay));
	 * 
	 * if (pa.getOptions().length > 0) { //System.out.println("ADDING PA\t" +
	 * op.getInternalName()); op.addPushAction(pa); } } }
	 */
	private boolean isDimensionTable(final String tableName) {
		if (tableName.toLowerCase().endsWith(this.DIMENSIONTABLESUFFIX))
			return true;
		return false;
	}

	private boolean isMainTable(final String tableName) {
		if (tableName.toLowerCase()
				.endsWith(this.MAINTABLESUFFIX.toLowerCase()))
			return true;
		return false;
	}

	private boolean isLookupTable(final String tableName) {
		if (tableName.toLowerCase().endsWith(
				this.LOOKUPTABLESUFFIX.toLowerCase()))
			return true;
		return false;
	}

	private AttributeDescription getAttributeDescription(
			final String columnName, final String tableName, int maxSize,
			final String joinKey, final int duplicated)
			throws ConfigurationException {
		final AttributeDescription att = new AttributeDescription();
		att.setField(columnName);

		if (columnName.endsWith("_list") || columnName.equals("dbprimary_id")) {
			final String descriptiveName = tableName.split("__")[0]
					.replaceFirst("xref_", "");
			att.setInternalName(descriptiveName.toLowerCase());
			final String displayName = descriptiveName.replaceAll("_", " ");
			att.setDisplayName(displayName.substring(0, 1).toUpperCase()
					+ displayName.substring(1));
		} else {

			if (duplicated == 1)
				att.setInternalName(tableName + "_" + columnName.toLowerCase());
			else
				att.setInternalName(columnName.toLowerCase());
			final String displayName = columnName.replaceAll("_", " ");
			att.setDisplayName(displayName.substring(0, 1).toUpperCase()
					+ displayName.substring(1));
		}
		// att.setDisplayName(columnName.replaceAll("_", " "));
		att.setKey(joinKey);
		att.setTableConstraint(tableName);

		if (maxSize > 255)
			maxSize = 255;

		att.setMaxLength(String.valueOf(maxSize));

		return att;
	}

	private FilterDescription getFilterDescription(final String columnName,
			final String tableName, final int columnType, final String joinKey,
			final DatasetConfig dsv, final int duplicated) throws SQLException,
			ConfigurationException {

		final FilterDescription filt = new FilterDescription();
		filt.setField(columnName);
		String descriptiveName = columnName;
		if (columnName.endsWith("_bool")) {
			if (duplicated == 1)
				filt.setInternalName(tableName + "_" + columnName.toLowerCase());
			else {
				final String internalName = columnName
						.replaceFirst("_bool", "").toLowerCase();
				filt.setInternalName("with_" + internalName);
			}
			descriptiveName = descriptiveName.substring(0, 1).toUpperCase()
					+ descriptiveName.substring(1);
			descriptiveName = "with "
					+ descriptiveName.replaceFirst("_bool", "") + " ID(s)";
			filt.setType("boolean");
			filt.setQualifier("only");
			filt.setLegalQualifiers("only,excluded");
			filt.setDisplayType("list");
			filt.setStyle("radio");
			final Option op1 = new Option("only", "true", "Only", "", "", "",
					"", "only", "", "", "", "", "", "", "", "", "", "", "", "",
					"", "", "", "", "", "", "", "", "", "", "", "");
			final Option op2 = new Option("excluded", "true", "Excluded", "",
					"", "", "", "excluded", "", "", "", "", "", "", "", "", "",
					"", "", "", "", "", "", "", "", "", "", "", "", "", "", "");
			filt.addOption(op1);
			filt.addOption(op2);
		} else if (columnName.endsWith("_list")
				|| columnName.equals("dbprimary_id")) {
			descriptiveName = columnName.replaceFirst("_list", "");
			filt.setType("list");
			filt.setQualifier("=");
			filt.setLegalQualifiers("=,in");
			// hack for multiple display_id in ensembl xref tables
			if (descriptiveName.equals("display_id")
					|| descriptiveName.equals("dbprimary_id"))
				descriptiveName = tableName.split("__")[0].replaceFirst(
						"xref_", "");

			if (duplicated == 1)
				filt.setInternalName(tableName + "_"
						+ descriptiveName.toLowerCase());
			else
				filt.setInternalName(descriptiveName.toLowerCase());
			filt.setDisplayType("text");
			filt.setMultipleValues("1");
			descriptiveName = descriptiveName + " ID(s)";
			descriptiveName = descriptiveName.substring(0, 1).toUpperCase()
					+ descriptiveName.substring(1);
		} else {
			if (duplicated == 1)
				filt.setInternalName(tableName + "_" + columnName.toLowerCase());
			else
				filt.setInternalName(columnName.toLowerCase());
			filt.setType(this.DEFAULTTYPE);
			filt.setQualifier(this.DEFAULTQUALIFIER);
			filt.setLegalQualifiers(this.DEFAULTLEGALQUALIFIERS);
			filt.setDisplayType("text");
			descriptiveName = descriptiveName.substring(0, 1).toUpperCase()
					+ descriptiveName.substring(1);
		}

		final String displayName = descriptiveName.replaceAll("_", " ");
		filt.setDisplayName(displayName);// .substring(0,1).toUpperCase() +
		// displayName.substring(1));
		filt.setTableConstraint(tableName);
		filt.setKey(joinKey);

		return filt;
	}

	public Option[] getOptions(final String columnName, String tableName,
			final String joinKey, final DatasetConfig dsConfig,
			final String dataset, final String colForDisplay)
			throws SQLException, ConfigurationException {

		final List options = new ArrayList();

		if (tableName.equalsIgnoreCase("main")) {
			final String[] starNames = dsConfig.getStarBases();
			final String[] primaryKeys = dsConfig.getPrimaryKeys();
			tableName = starNames[0];// in case no keys for a lookup type
			// dataset
			for (int k = 0; k < primaryKeys.length; k++)
				// System.out.println(joinKey+":"+primaryKeys[k]);
				if (primaryKeys[k].equalsIgnoreCase(joinKey))
					tableName = starNames[k];
		}

		if (!tableName.startsWith(dataset + "__"))
			if (tableName.matches(".*__.*__.*"))
				tableName = dataset + "__" + tableName.split("__")[1] + "__"
						+ tableName.split("__")[2];
			else
				tableName = dataset + "__" + tableName.split("__")[0] + "__"
						+ tableName.split("__")[1];

		// does this table actually exist?
		if (!tableExists(tableName)) {

			logger.warning("Cannot get options as table " + tableName
					+ " does not exist");

		} else {

			logger.info("Retrieving options from table " + tableName);
			Connection conn = null;
			ResultSet rs = null;
			PreparedStatement ps = null;
			try {
				conn = this.dsource.getConnection();
				String sql;
				if (colForDisplay != null && !colForDisplay.equals(""))
					sql = "SELECT DISTINCT " + columnName + "," + colForDisplay
							+ " FROM " + this.getSchema()[0] + "." + tableName
							+ " WHERE " + columnName + " IS NOT NULL ORDER BY "
							+ colForDisplay;
				else
					sql = "SELECT DISTINCT " + columnName + " FROM "
							+ this.getSchema()[0] + "." + tableName + " WHERE "
							+ columnName + " IS NOT NULL ORDER BY "
							+ columnName;
				// System.out.println(sql);
				ps = conn.prepareStatement(sql);
				rs = ps.executeQuery();
				String value;
				Option op;
				while (rs.next()) {
					value = rs.getString(1);

					// fix for an empty string
					if (value.length() == 0) {
						System.out
								.println("MAKE DROP DOWN WARNING: Detected empty string(s) in "
										+ tableName + "." + columnName);
						continue;
					}

					op = new Option();

					// if (!colForDisplay.equals("")){
					if (colForDisplay != null && !colForDisplay.equals(""))
						op.setDisplayName(rs.getString(2));
					else
						op.setDisplayName(value);
					final String intName = value.replaceAll(" ", "_");
					// op.setInternalName(intName.toLowerCase());
					op.setInternalName(intName);// breaks push action if make
												// lower
												// case
					// NN
					// if (!(columnName.startsWith("silent_") ||
					// columnName.startsWith("SILENT_"))) //prob. not needed, to
					// check
					// if (!columnName.startsWith("silent_"))
					op.setValue(value);
					// op.setValue(intName);
					op.setSelectable("true");
					options.add(op);
				}
			} finally {
				closeQuietly(rs);
				closeQuietly(ps);
				if (conn != null) {
					DetailedDataSource.close(conn);
				}
			}

		}

		final Option[] retOptions = new Option[options.size()];
		options.toArray(retOptions);
		return retOptions;
	}

	/*
	 * public Option[] getOntologyOptions(String ontologyTable, String ontology,
	 * String vocabTable, String nodeTable) throws SQLException,
	 * ConfigurationException {
	 * 
	 * Connection conn = dsource.getConnection(); String sql = "SELECT id FROM "
	 * + ontologyTable + " WHERE name = '" + ontology+"'"; PreparedStatement ps
	 * = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery();
	 * rs.next(); String ontologyId = rs.getString(1);
	 * 
	 * //String[] tableParts = ontologyTable.split("evoc"); //String vocabTable
	 * = tableParts[0]+"evoc_vocabulary__evoc_vocabulary__main"; //String
	 * nodeTable = tableParts[0]+"evoc_node__evoc_node__main";
	 * 
	 * Option[] retOptions =
	 * recurseOntology(ontologyId,"0",vocabTable,nodeTable,conn);
	 * DetailedDataSource.close(conn); return retOptions; }
	 */

	public Option[] getOntologyOptions(final String childTermCol,
			final String childIdCol, final String childTable,
			final String parentIdCol) throws SQLException,
			ConfigurationException {

		Connection conn = null;
		ResultSet rs = null;
		PreparedStatement ps = null;
		try {
			conn = this.dsource.getConnection();

			final String sql = "SELECT min(" + parentIdCol + ") FROM "
					+ childTable;
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			rs.next();
			final String rootId = rs.getString(1);

			final Option[] retOptions = this.recurseOntology(childTermCol,
					childIdCol, childTable, parentIdCol, rootId, conn);
			return retOptions;
		} finally {
			closeQuietly(rs);
			closeQuietly(ps);
			if (conn != null) {
				DetailedDataSource.close(conn);
			}
		}

	}

	private Option[] recurseOntology(final String childTermCol,
			final String childIdCol, final String childTable,
			final String parentIdCol, final String parentId,
			final Connection conn) throws SQLException {
		// Connection conn = dsource.getConnection();
		final List options = new ArrayList();

		final String sql = "SELECT " + childTermCol + "," + childIdCol
				+ " FROM " + childTable + " WHERE " + parentIdCol + " = "
				+ parentId;
		final PreparedStatement ps = conn.prepareStatement(sql);
		final ResultSet rs = ps.executeQuery();
		String value;
		Option op;
		while (rs.next()) {
			value = rs.getString(1);
			op = new Option();
			op.setDisplayName(value);
			final String intName = value.replaceAll(" ", "_");
			op.setInternalName(intName.toLowerCase());
			op.setValue(value);
			op.setSelectable("true");
			// recurse here to add in suboptions
			final Option[] subOps = this.recurseOntology(childTermCol,
					childIdCol, childTable, parentIdCol, rs.getString(2), conn);
			op.addOptions(subOps);
			options.add(op);
		}
		final Option[] retOptions = new Option[options.size()];
		options.toArray(retOptions);
		return retOptions;
	}

	/*
	 * private Option[] recurseOntology(String ontologyId, String parentId,
	 * String vocabTable, String nodeTable, Connection conn) throws
	 * SQLException{
	 * 
	 * List options = new ArrayList();
	 * 
	 * String sql = "SELECT ct.term, c.id, c.parent_id, c.ontology_id FROM
	 * "+vocabTable+" ct, "+nodeTable+ " c WHERE c.parent_id = "+parentId+" AND
	 * c.ontology_id = "+ontologyId+" AND c.id = ct.node_id"; PreparedStatement
	 * ps = conn.prepareStatement(sql); ResultSet rs = ps.executeQuery(); String
	 * value; Option op; while (rs.next()) { value = rs.getString(1); op = new
	 * Option(); op.setDisplayName(value); String intName =
	 * value.replaceAll(" ", "_"); op.setInternalName(intName.toLowerCase());
	 * op.setValue(value); op.setSelectable("true"); // recurse here to add in
	 * suboptions Option[] subOps =
	 * recurseOntology(ontologyId,rs.getString(2),vocabTable,nodeTable,conn);
	 * op.addOptions(subOps); options.add(op); } Option[] retOptions = new
	 * Option[options.size()]; options.toArray(retOptions); return retOptions; }
	 */

	public Option[] getLookupOptions(final String columnName, String tableName,
			final DatasetConfig dsConfig, final String joinKey,
			final String dataset, final String whereName,
			final String whereValue, String orderSQL, final String schema,
			final String colForDisplay) throws SQLException,
			ConfigurationException {

		if (tableName.equalsIgnoreCase("main")) {
			final String[] starNames = dsConfig.getStarBases();
			final String[] primaryKeys = dsConfig.getPrimaryKeys();
			tableName = starNames[0];// in case no keys for a lookup type
			// dataset
			for (int k = 0; k < primaryKeys.length; k++)
				// System.out.println(joinKey+":"+primaryKeys[k]);
				if (primaryKeys[k].equalsIgnoreCase(joinKey))
					tableName = starNames[k];
		}

		if (!tableName.startsWith(dataset + "__"))
			if (tableName.matches(".*__.*__.*"))
				tableName = dataset + "__" + tableName.split("__")[1] + "__"
						+ tableName.split("__")[2];
			else
				tableName = dataset + "__" + tableName.split("__")[0] + "__"
						+ tableName.split("__")[1];

		final List options = new ArrayList();
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = this.dsource.getConnection();
			if (orderSQL == null || orderSQL.equals("")) {
				if (!"".equals(colForDisplay))
					orderSQL = "ORDER BY " + colForDisplay;
				else
					orderSQL = "ORDER BY " + columnName;
			} else
				orderSQL = " ORDER BY " + orderSQL;

			String sql;
			if (colForDisplay != null && !colForDisplay.equals(""))
				sql = "SELECT DISTINCT " + columnName + "," + colForDisplay
						+ " FROM " + schema + "." + tableName + " WHERE "
						+ whereName + "=\'" + whereValue + "\' AND "
						+ columnName + " IS NOT NULL " + orderSQL;
			else
				sql = "SELECT DISTINCT " + columnName + " FROM " + schema + "."
						+ tableName + " WHERE " + whereName + "=\'"
						+ whereValue + "\' AND " + columnName + " IS NOT NULL "
						+ orderSQL;
			// System.out.println(sql);
			ps = conn.prepareStatement(sql);
			rs = null;
			try {
				rs = ps.executeQuery();
			} catch (final Exception e) {
				JOptionPane.showMessageDialog(null, "Problem with SQL: " + sql);
			}
			String value;
			Option op;

			while (rs != null && rs.next()) {
				value = rs.getString(1);
				op = new Option();
				// System.out.println(value);
				if (colForDisplay != null && !colForDisplay.equals(""))
					op.setDisplayName(rs.getString(2));
				else
					op.setDisplayName(value);
				op.setInternalName(value);
				op.setValue(value);
				op.setSelectable("true");
				options.add(op);
			}
			// conn.close();
			DetailedDataSource.close(conn);
			final Option[] retOptions = new Option[options.size()];
			options.toArray(retOptions);
			return retOptions;
		} finally {
			closeQuietly(rs);
			closeQuietly(ps);
			if (conn != null) {
				DetailedDataSource.close(conn);
			}
		}
	}

	private boolean isAllNull(final String cname, final String tableName)
			throws SQLException, ConfigurationException {

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = this.dsource.getConnection();

			// added getSchema() to fully qualify this to work with 'non-public'
			// postgres schemas
			final StringBuffer sql = new StringBuffer("SELECT " + cname
					+ " FROM " + this.getSchema()[0] + "." + tableName
					+ " WHERE " + cname + " IS NOT NULL");

			if (this.dsource.getDatabaseType().equals("oracle"))
				sql.append(" and rownum <=1");
			else if (this.dsource.getDatabaseType().equals("mysql"))
				sql.append(" limit 1");
			else if (this.dsource.getDatabaseType().equals("postgres"))
				sql.append(" limit 1");
			else
				throw new ConfigurationException("unsupported RDBMS type:"
						+ this.dsource.getDatabaseType());

			ps = conn.prepareStatement(sql.toString());
			rs = ps.executeQuery();
			rs.next();

			if (rs.isFirst()) {
				ps.close();
				rs.close();
				conn.close();
				return false;
			} else {
				// System.out.println("ALL NULLS\t" + cname + "\t" + tableName);
				ps.close();
				rs.close();
				conn.close();
				return true;
			}

		} catch (final SQLException e) {
			throw new ConfigurationException(
					"Caught SQLException during attempt to count non-null values\n",
					e);
		} finally {
			closeQuietly(rs);
			closeQuietly(ps);
			if (conn != null) {
				DetailedDataSource.close(conn);
			}
		}
	}

	public String[] getSchema() {

		/*
		 * String schema = null; if(dsource.getDatabaseType().equals("oracle"))
		 * schema = dsource.getSchema().toUpperCase(); else schema =
		 * dsource.getSchema();
		 */
		String[] schema = null;
		if (this.dsource.getDatabaseType().equals("oracle"))
			schema = this.dsource.getSchema().toUpperCase().split(";");
		else
			schema = this.dsource.getSchema().split(";");
		return schema;
	}

}
