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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import oracle.sql.BLOB;
import oracle.sql.CLOB;

import org.ensembl.mart.lib.DetailedDataSource;
import org.jdom.Attribute;
import org.jdom.DocType;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;
import org.xml.sax.InputSource;

/**
 * Collection of static methods for translating MartRegistry.dtd compliant documents
 * to and from MartRegistry objects.  Contains all of the necessary XML parsing logic
 * to accomplish these tasks.
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class MartRegistryXMLUtils {

	private static Logger logger = Logger.getLogger(MartRegistryXMLUtils.class.getName());

	//element names
	private static final String VSCHEMA = "virtualSchema";
	private static final String MARTREGISTRY = "MartRegistry";
	private static final String URLLOCATION = "MartURLLocation";
	private static final String DATABASELOCATION = "MartDBLocation";
	private static final String REGISTRYLOCATION = "RegistryURLPointer";
	private static final String REGISTRYDBLOCATION = "RegistryDBPointer";

	/*
	 * meta_registry
	   * ------------------------
	 * xml            longblob
	 * compressed_xml longblob
	 */

	//TODO maybe make a user registry?
	private static final String SELECTXMLFORUPDATE1 = "select xml from ";
	private static final String SELECTXMLFORUPDATE2 = ".meta_registry FOR UPDATE";
	private static final String SELECTCOMPRESSEDXMLFORUPDATE1 = "select compressed_xml from ";
	private static final String SELECTCOMPRESSEDXMLFORUPDATE2 = ".meta_registry FOR UPDATE";
	private static final String UPDATECOMPRESSEDREGISTRYXML1 = "insert into ";
	private static final String UPDATECOMPRESSEDREGISTRYXML2= ".meta_registry(compressed_xml) values(?)";
	private static final String UPDATEREGISTRYXML1 = "insert into ";
	private static final String UPDATEREGISTRYXML2 = ".meta_registry(xml) values(?)";
	private static final String CLEANREGISTRYTABLE1 = "delete from ";
	private static final String CLEANREGISTRYTABLE2 = ".meta_registry";
	private static final String GETREGISTRYSQL1 = "select xml, compressed_xml from ";
	private static final String GETREGISTRYSQL2 = ".meta_registry limit 1";

	/**
	 * @param dsource
	 * @return
	 */
	public static MartRegistry DataSourceToMartRegistry(DetailedDataSource dsource) throws ConfigurationException {
		return (DocumentToMartRegistry(DataSourceToRegistryDocument(dsource)));
	}

	public static Document DataSourceToRegistryDocument(DetailedDataSource dsource) throws ConfigurationException {
		if (dsource.getJdbcDriverClassName().indexOf("oracle") >= 0)
			return DataSourceToRegistryDocumentOracle(dsource);

		Connection conn = null;
		try {
			String getRegistrySQL = GETREGISTRYSQL1 + dsource.getSchema() + GETREGISTRYSQL2;
			
			if (logger.isLoggable(Level.FINE))
				logger.fine("Using " + getRegistrySQL + " to get Registry\n");

			conn = dsource.getConnectionNoVersionCheck();
			PreparedStatement ps = conn.prepareStatement(getRegistrySQL);
			//System.out.println(getRegistrySQL);
			ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				// will only get one result
				rs.close();
				DetailedDataSource.close(conn);
				return null;
			}

			byte[] stream = rs.getBytes(1);
			byte[] cstream = rs.getBytes(2);

			rs.close();

			InputStream rstream = null;
			if (cstream != null)
				rstream = new GZIPInputStream(new ByteArrayInputStream(cstream));
			else
				rstream = new ByteArrayInputStream(stream);

			return XMLStreamToDocument(rstream, false);
		} catch (SQLException e) {
			throw new ConfigurationException("Caught SQL Exception during fetch of registry: " + e.getMessage(), e);
		} catch (IOException e) {
			throw new ConfigurationException("Caught IOException during fetch of registry: " + e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	private static Document DataSourceToRegistryDocumentOracle(DetailedDataSource dsource)
		throws ConfigurationException {
		Connection conn = null;
		try {
			String getRegistrySQL = GETREGISTRYSQL1 + dsource.getSchema() + GETREGISTRYSQL2;
			
			if (logger.isLoggable(Level.FINE))
				logger.fine("Using " + getRegistrySQL + " to get Registry\n");

			conn = dsource.getConnectionNoVersionCheck();
			PreparedStatement ps = conn.prepareStatement(getRegistrySQL);

			ResultSet rs = ps.executeQuery();
			if (!rs.next()) {
				// will only get one result
				rs.close();
				conn.close();
				return null;
			}

			CLOB stream = (CLOB) rs.getClob(1);
			BLOB cstream = (BLOB) rs.getBlob(2);

			InputStream rstream = null;
			if (cstream != null) {
				rstream = new GZIPInputStream(cstream.getBinaryStream());
			} else
				rstream = stream.getAsciiStream();

			Document ret = XMLStreamToDocument(rstream, false);
			rstream.close();
			rs.close();
			return ret;
		} catch (SQLException e) {
			throw new ConfigurationException(
				"Caught SQL Exception during fetch of requested DatasetConfig: " + e.getMessage(),
				e);
		} catch (IOException e) {
			throw new ConfigurationException(
				"Caught IOException during fetch of requested DatasetConfig: " + e.getMessage(),
				e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	public static void cleanRegistryTable(DetailedDataSource dsource) throws ConfigurationException {
		Connection conn = null;

		try {
			String CLEANREGISTRYTABLE = CLEANREGISTRYTABLE1 + dsource.getSchema() + CLEANREGISTRYTABLE2;
			conn = dsource.getConnectionNoVersionCheck();
			PreparedStatement ps = conn.prepareStatement(CLEANREGISTRYTABLE);

			ps.executeUpdate();
			ps.close();
		} catch (SQLException e) {
			throw new ConfigurationException("Couldnt clean old Registry: " + e.getMessage() + "\n", e);
		} finally {
			DetailedDataSource.close(conn);
		}

	}

	public static void storeMartRegistryDocumentToDataSource(DetailedDataSource dsource, Document doc, boolean compress)
		throws ConfigurationException {
		int rowsupdated = 0;

		cleanRegistryTable(dsource);
		if (compress)
			rowsupdated = storeCompressedRegistryXML(dsource, doc);
		else
			rowsupdated = storeUncompressedRegistryXML(dsource, doc);

		if (rowsupdated < 1)
			if (logger.isLoggable(Level.WARNING))
				logger.warning("Warning, registry xml not stored"); //throw an exception?  
	}

	private static int storeCompressedRegistryXML(DetailedDataSource dsource, Document doc)
		throws ConfigurationException {
		if (dsource.getJdbcDriverClassName().indexOf("oracle") >= 0)
			return storeCompressedRegistryXMLOracle(dsource, doc);
		Connection conn = null;
		try {
			String UPDATECOMPRESSEDREGISTRYXML = UPDATECOMPRESSEDREGISTRYXML1 + dsource.getSchema() + UPDATECOMPRESSEDREGISTRYXML2;
			conn = dsource.getConnectionNoVersionCheck();
			ByteArrayOutputStream bout = new ByteArrayOutputStream();

			XMLOutputter xout = new XMLOutputter(org.jdom.output.Format.getRawFormat());
			GZIPOutputStream gout = new GZIPOutputStream(bout);

			xout.output(doc, gout);
			gout.finish();

			byte[] xml = bout.toByteArray();
			bout.close();
			gout.close();

			PreparedStatement ps = conn.prepareStatement(UPDATECOMPRESSEDREGISTRYXML);
			ps.setBinaryStream(1, new ByteArrayInputStream(xml), xml.length);

			int ret = ps.executeUpdate();
			ps.close();

			return ret;
		} catch (IOException e) {
			throw new ConfigurationException("Caught IOException writing out xml to OutputStream: " + e.getMessage(), e);
		} catch (SQLException e) {
			throw new ConfigurationException("Caught SQLException updating Registry xml: " + e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	private static int storeCompressedRegistryXMLOracle(DetailedDataSource dsource, Document doc)
		throws ConfigurationException {
		Connection conn = null;
		try {
			String UPDATECOMPRESSEDREGISTRYXML = UPDATECOMPRESSEDREGISTRYXML1 + dsource.getSchema() + UPDATECOMPRESSEDREGISTRYXML2;
			String SELECTCOMPRESSEDXMLFORUPDATE = SELECTCOMPRESSEDXMLFORUPDATE1 + dsource.getSchema() + SELECTCOMPRESSEDXMLFORUPDATE2;
			
			if (logger.isLoggable(Level.FINE))
				logger.fine("\ninserting with SQL " + UPDATECOMPRESSEDREGISTRYXML + "\n");

			conn = dsource.getConnectionNoVersionCheck();
			conn.setAutoCommit(false);

			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			XMLOutputter xout = new XMLOutputter(org.jdom.output.Format.getRawFormat());
			GZIPOutputStream gout = new GZIPOutputStream(bout);

			xout.output(doc, gout);
			gout.finish();

			byte[] xml = bout.toByteArray();

			bout.close();
			gout.close();

			PreparedStatement ps = conn.prepareStatement(UPDATECOMPRESSEDREGISTRYXML);
			PreparedStatement ohack = conn.prepareStatement(SELECTCOMPRESSEDXMLFORUPDATE);

			int ret = ps.executeUpdate();

			ResultSet rs = ohack.executeQuery();

			if (rs.next()) {
				CLOB clob = (CLOB) rs.getClob(1);

				OutputStream clobout = clob.getAsciiOutputStream();
				clobout.write(xml);
				clobout.close();
			}

			conn.commit();
			rs.close();
			ohack.close();
			ps.close();

			return ret;
		} catch (IOException e) {
			throw new ConfigurationException("Caught IOException writing out xml to OutputStream: " + e.getMessage(), e);
		} catch (SQLException e) {
			throw new ConfigurationException("Caught SQLException updating registry xml: " + e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	private static int storeUncompressedRegistryXML(DetailedDataSource dsource, Document doc)
		throws ConfigurationException {
		if (dsource.getJdbcDriverClassName().indexOf("oracle") >= 0)
			return storeUncompressedRegistryXMLORacle(dsource, doc);

		Connection conn = null;
		try {
			String UPDATEREGISTRYXML = UPDATEREGISTRYXML1 + dsource.getSchema() + UPDATEREGISTRYXML2;
			if (logger.isLoggable(Level.FINE))
				logger.fine("\ninserting with SQL " + UPDATEREGISTRYXML + "\n");

			conn = dsource.getConnectionNoVersionCheck();
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			XMLOutputter xout = new XMLOutputter(org.jdom.output.Format.getRawFormat());

			xout.output(doc, bout);

			byte[] xml = bout.toByteArray();
			bout.close();

			PreparedStatement ps = conn.prepareStatement(UPDATEREGISTRYXML);
			ps.setBinaryStream(1, new ByteArrayInputStream(xml), xml.length);

			int ret = ps.executeUpdate();
			ps.close();

			return ret;
		} catch (IOException e) {
			throw new ConfigurationException("Caught IOException writing out xml to OutputStream: " + e.getMessage(), e);
		} catch (SQLException e) {
			throw new ConfigurationException("Caught SQLException updating Registry xml: " + e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}

	private static int storeUncompressedRegistryXMLORacle(DetailedDataSource dsource, Document doc)
		throws ConfigurationException {
		Connection conn = null;
		try {
			String UPDATEREGISTRYXML = UPDATEREGISTRYXML1 + dsource.getSchema() + UPDATEREGISTRYXML2;
			String SELECTXMLFORUPDATE = SELECTXMLFORUPDATE1 + dsource.getSchema() + SELECTXMLFORUPDATE2;
			if (logger.isLoggable(Level.FINE))
				logger.fine("\ninserting with SQL " + UPDATEREGISTRYXML + "\n");

			conn = dsource.getConnectionNoVersionCheck();
			conn.setAutoCommit(false);

			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			XMLOutputter xout = new XMLOutputter(org.jdom.output.Format.getRawFormat());

			xout.output(doc, bout);

			byte[] xml = bout.toByteArray();

			bout.close();

			PreparedStatement ps = conn.prepareStatement(UPDATEREGISTRYXML);
			PreparedStatement ohack = conn.prepareStatement(SELECTXMLFORUPDATE);

			int ret = ps.executeUpdate();

			ResultSet rs = ohack.executeQuery();

			if (rs.next()) {
				CLOB clob = (CLOB) rs.getClob(1);

				OutputStream clobout = clob.getAsciiOutputStream();
				clobout.write(xml);
				clobout.close();
			}

			conn.commit();
			rs.close();
			ohack.close();
			ps.close();

			return ret;
		} catch (IOException e) {
			throw new ConfigurationException("Caught IOException writing out xml to OutputStream: " + e.getMessage(), e);
		} catch (SQLException e) {
			throw new ConfigurationException("Caught SQLException updating registry xml: " + e.getMessage(), e);
		} finally {
			DetailedDataSource.close(conn);
		}
	}
	public static MartRegistry XMLStreamToMartRegistry(InputStream in) throws ConfigurationException {
		return XMLStreamToMartRegistry(in, false);
	}

	public static MartRegistry XMLStreamToMartRegistry(InputStream in, boolean validate) throws ConfigurationException {
		return DocumentToMartRegistry(XMLStreamToDocument(in, validate));
	}

	public static Document XMLStreamToDocument(InputStream in, boolean validate) throws ConfigurationException {
		try {
			SAXBuilder builder = new SAXBuilder();
			// set the EntityResolver to a allow it to get the DTD from the Classpath.
			builder.setEntityResolver(new ClasspathDTDEntityResolver());
			builder.setValidation(validate);

			InputSource is = new InputSource(in);

			Document doc = builder.build(is);

			return doc;
		} catch (Exception e) {
			throw new ConfigurationException(e);
		}
	}

	public static MartRegistry DocumentToMartRegistry(Document doc) throws ConfigurationException {
		Element thisElement = doc.getRootElement();

		MartRegistry martreg = new MartRegistry();
		for (Iterator iter = thisElement.getChildren().iterator(); iter.hasNext();) {
            Element element = (Element) iter.next();
            if (element.getName().equals(VSCHEMA)) {
              String name = element.getAttributeValue("name");
              
              virtualSchema vschema = new virtualSchema(name);
              MartLocation[] martLocs = getLocations(element);
              for (int i = 0, n = martLocs.length; i < n; i++) {
                vschema.addMartLocation(martLocs[i]);
              }
              martreg.addVirtualSchema(vschema);
            } else {
                if (element.getName().equals(URLLOCATION))
      				martreg.addMartLocation(getURLLocation(element));
                else if (element.getName().equals(DATABASELOCATION))
                  martreg.addMartLocation(getDBLocation(element));
                else if (element.getName().equals(REGISTRYLOCATION))
                  martreg.addMartLocation(getRegLocation(element));
                else if (element.getName().equals(REGISTRYDBLOCATION))
                  martreg.addMartLocation(getRegDBLocation(element));
                //else not needed
            }
        }

		return martreg;
	}

	private static MartLocation[] getLocations(Element thisElement) throws ConfigurationException {
	    List locs = new ArrayList();
		for (Iterator iter = thisElement.getChildren(URLLOCATION).iterator(); iter.hasNext();) {
			Element urlloc = (Element) iter.next();
			locs.add(getURLLocation(urlloc));
		}

		for (Iterator iter = thisElement.getChildren(DATABASELOCATION).iterator(); iter.hasNext();) {
			Element dbloc = (Element) iter.next();
			locs.add(getDBLocation(dbloc));
		}

		for (Iterator iter = thisElement.getChildren(REGISTRYLOCATION).iterator(); iter.hasNext();) {
			Element regloc = (Element) iter.next();
			locs.add(getRegLocation(regloc));
		}

		for (Iterator iter = thisElement.getChildren(REGISTRYDBLOCATION).iterator(); iter.hasNext();) {
			Element regloc = (Element) iter.next();
			locs.add(getRegDBLocation(regloc));
		}
		
		return (MartLocation[]) locs.toArray(new MartLocation[locs.size()]);
	}
	
	public static MartRegistry ByteArrayToMartRegistry(byte[] b) throws ConfigurationException {
		ByteArrayInputStream bin = new ByteArrayInputStream(b);
		return XMLStreamToMartRegistry(bin);
	}

	// private static ElementToObject methods 
	private static MartLocation getURLLocation(Element urlloc) throws ConfigurationException {
		URLLocation loc = new URLLocation();
		loadAttributesFromElement(urlloc, loc);

		//fail now if the url string is not valid
		loc.getUrl();

		return loc;
	}

	private static MartLocation getDBLocation(Element dbloc) throws ConfigurationException {
		DatabaseLocation loc = new DatabaseLocation();
		loadAttributesFromElement(dbloc, loc);
		return loc;
	}

	private static MartLocation getRegLocation(Element regloc) throws ConfigurationException {
		RegistryFileLocation loc = new RegistryFileLocation();
		loadAttributesFromElement(regloc, loc);

		//fail now if the url string is not valid
		loc.getUrl();

		return loc;
	}

	private static MartLocation getRegDBLocation(Element regloc) throws ConfigurationException {
		RegistryDBLocation loc = new RegistryDBLocation();
		loadAttributesFromElement(regloc, loc);

		//fail now if the DataSource connection information is not valid
		loc.getDetailedDataSource();

		return loc;
	}

	private static void loadAttributesFromElement(Element thisElement, BaseConfigurationObject obj) {
		List attributes = thisElement.getAttributes();

		for (int i = 0, n = attributes.size(); i < n; i++) {
			Attribute att = (Attribute) attributes.get(i);
			String name = att.getName();

			obj.setAttribute(name, thisElement.getAttributeValue(name));
		}
	}

	/**
	 * Writes a MartRegistry object as XML to the given File.  Handles opening and closing of the OutputStream.
	 * @param dsv -- MartRegistry object
	 * @param file -- File to write XML
	 * @throws ConfigurationException for underlying Exceptions
	 */
	public static void MartRegistryToFile(MartRegistry mr, File file) throws ConfigurationException {
		DocumentToFile(MartRegistryToDocument(mr), file);
	}

	/**
	 * Writes a MartRegistry object as XML to the given OutputStream.  Does not close the OutputStream after writing.
	 * If you wish to write a Document to a File, use MartRegistryToFile instead, as it handles opening and closing the OutputStream.
	 * @param dsv -- MartRegistry object to write as XML
	 * @param out -- OutputStream to write, not closed after writing
	 * @throws ConfigurationException for underlying Exceptions
	 */
	public static void MartRegistryToOutputStream(MartRegistry dsv, OutputStream out) throws ConfigurationException {
		DocumentToOutputStream(MartRegistryToDocument(dsv), out);
	}

	/**
	 * Writes a JDOM Document as XML to a given File.  Handles opening and closing of the OutputStream.
	 * @param doc -- Document representing a MartRegistry.dtd compliant XML document
	 * @param file -- File to write.
	 * @throws ConfigurationException for underlying Exceptions.
	 */
	public static void DocumentToFile(Document doc, File file) throws ConfigurationException {
		try {
			FileOutputStream out = new FileOutputStream(file);
			DocumentToOutputStream(doc, out);
			out.close();
		} catch (FileNotFoundException e) {
			throw new ConfigurationException(
				"Caught FileNotFoundException writing Document to File provided " + e.getMessage(),
				e);
		} catch (ConfigurationException e) {
			throw e;
		} catch (IOException e) {
			throw new ConfigurationException("Caught IOException creating FileOutputStream " + e.getMessage(), e);
		}
	}

	/**
	 * Takes a JDOM Document and writes it as MartRegistry.dtd compliant XML to a given OutputStream.
	 * Does NOT close the OutputStream after writing.  If you wish to write a Document to a File,
	 * use DocumentToFile instead, as it handles opening and closing the OutputStream. 
	 * @param doc -- Document representing a MartRegistry.dtd compliant XML document
	 * @param out -- OutputStream to write to, not closed after writing
	 * @throws ConfigurationException for underlying IOException
	 */
	public static void DocumentToOutputStream(Document doc, OutputStream out) throws ConfigurationException {
		XMLOutputter xout = new XMLOutputter(org.jdom.output.Format.getRawFormat());

		try {
			xout.output(doc, out);
		} catch (IOException e) {
			throw new ConfigurationException("Caught IOException writing XML to OutputStream " + e.getMessage(), e);
		}
	}

	public static byte[] DocumentToByteArray(Document doc) throws ConfigurationException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DocumentToOutputStream(doc, bout);
		return bout.toByteArray();
	}

	public static byte[] MartRegistryToByteArray(MartRegistry martreg) throws ConfigurationException {
		return DocumentToByteArray(MartRegistryToDocument(martreg));
	}

	public static Document MartRegistryToDocument(MartRegistry martreg) throws ConfigurationException {
		Element root = new Element(MARTREGISTRY);

		Object[] obs = martreg.getElementsInOrder();
		for (int i = 0, n = obs.length; i < n; i++) {
            if (obs[i] instanceof virtualSchema) {
                virtualSchema vschema = (virtualSchema) obs[i];
                root.addContent( getVirtualSchemaElement( vschema ) );
            } else {
                //MartLocation
                MartLocation location = (MartLocation) obs[i];
                putLocation(location, root);
            }
        }

		Document thisDoc = new Document(root);
		thisDoc.setDocType(new DocType(MARTREGISTRY));
		return thisDoc;
	}

	private static void putLocation(MartLocation location, Element containerElement) throws ConfigurationException {
			if (location.getType().equals(MartLocationBase.URL))
				containerElement.addContent(getURLLocationElement((URLLocation) location));
			else if (location.getType().equals(MartLocationBase.DATABASE))
				containerElement.addContent(getDatabaseLocationElement((DatabaseLocation) location));
			else if (location.getType().equals(MartLocationBase.REGISTRYFILE))
				containerElement.addContent(getRegistryLocationElement((RegistryFileLocation) location));
			//else not needed, but may need to add other else ifs in future
	}
	
	private static Element getVirtualSchemaElement( virtualSchema vSchema ) throws ConfigurationException {
	    Element vSchemaElement = new Element(VSCHEMA);
	    
	    vSchemaElement.setAttribute("name", vSchema.getName());
	    MartLocation[] martlocs = vSchema.getMartLocations();
	    for (int i = 0, n = martlocs.length; i < n; i++) {
            putLocation(martlocs[i], vSchemaElement);
        }
	    return vSchemaElement;
	}
	
	//private static ObjectToElement methods
	private static Element getURLLocationElement(URLLocation loc) throws ConfigurationException {
		Element location = new Element(URLLOCATION);
		loadElementAttributesFromObject(loc, location);
		return location;
	}

	private static Element getDatabaseLocationElement(DatabaseLocation loc) throws ConfigurationException {
		Element location = new Element(DATABASELOCATION);
		loadElementAttributesFromObject(loc, location);
		return location;
	}

	private static Element getRegistryLocationElement(RegistryFileLocation loc) throws ConfigurationException {
		Element location = new Element(URLLOCATION);
		loadElementAttributesFromObject(loc, location);
		return location;
	}

	private static void loadElementAttributesFromObject(BaseConfigurationObject obj, Element thisElement) {
		String[] titles = obj.getXmlAttributeTitles();

		//sort the attribute titles before writing them out, so that MD5SUM is supported
		Arrays.sort(titles);

		for (int i = 0, n = titles.length; i < n; i++) {
			String key = titles[i];

			if (validString(obj.getAttribute(key)))
				thisElement.setAttribute(key, obj.getAttribute(key));
		}
	}

	private static boolean validString(String test) {
		return (test != null && test.length() > 0);
	}
}
