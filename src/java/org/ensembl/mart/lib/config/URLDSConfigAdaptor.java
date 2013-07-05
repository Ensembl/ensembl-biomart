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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.mart.lib.InputSourceUtil;
import org.ensembl.util.StringUtil;

/**
 * DSConfigAdaptor implimenting object designed to provide a DatasetConfig object from
 * from an URL.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class URLDSConfigAdaptor extends LeafDSConfigAdaptor implements DSConfigAdaptor, Comparable {
  private final URL dsvurl;
  private final int hashcode;

  private DatasetConfig dsv;
  private String[] inames;
  private String[] dnames;
  private Logger logger = Logger.getLogger(URLDSConfigAdaptor.class.getName());
  private String adaptorName;
  private boolean ignoreCache = false;
  private DatasetConfigXMLUtils dscutils = null;
  
  /**
   * Construct a DSConfigAdaptor from a url containing a DatasetConfig.dtd compliant XML Document,
   * with optional JDOM validation.
   * @param url -- url containing a DatasetConfig.dtd compliant XML document
   * @param ignoreCache -- if true, never caches and always parses the source URL for its DatasetConfig objects
   * @param includeHiddenMembers -- if true, hidden members will be included in DatasetConfig objects, if false they will be skipped
   * @throws ConfigurationException for all underlying Exceptions.
   */
  public URLDSConfigAdaptor(URL url, boolean ignoreCache, boolean includeHiddenMembers) throws ConfigurationException {
    if (url == null)
      throw new ConfigurationException("DSConfigURLAdaptors must be instantiated with a URL\n");
    dsvurl = url;
    this.ignoreCache = ignoreCache;
      
    setName(dsvurl.toString());
    
    dscutils = new DatasetConfigXMLUtils(includeHiddenMembers);

    hashcode = dsvurl.hashCode();
    update();
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigs()
   */
  public DatasetConfigIterator getDatasetConfigs() throws ConfigurationException {
    List l = new ArrayList();
    l.add(dsv);
    return new DatasetConfigIterator(l.iterator());
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#update()
   */
  public void update() throws ConfigurationException {
    try {
	dscutils.setFullyLoadMode(true);
      dsv = dscutils.getDatasetConfigForXMLStream(InputSourceUtil.getStreamForURL(dsvurl));
    } catch (Exception e) {
      throw new ConfigurationException(
        "Could not load DatasetConfig from URL: " + dsvurl.toString() + " " + e.getMessage(),
        e);
    }

    inames = new String[] { dsv.getInternalName()};
    dnames = new String[] { dsv.getDisplayName()};

    dsv.setDSConfigAdaptor(this);
  }

  /**
   * Useful debug output
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();

    buf.append("[");
    buf.append(" url=").append(dsvurl.toString());
    buf.append(", dataset DisplayName=").append(dsv.getDisplayName());
    buf.append("]");

    return buf.toString();
  }

  /**
   * Allows Equality Comparisons manipulation of DSConfigAdaptor objects.  Although
   * any DSConfigAdaptor object can be compared with any other DSConfigAdaptor object, to provide
   * consistency with the compareTo method, in practice, it is almost impossible for different DSVIewAdaptor
   * implimentations to equal.
   */
  public boolean equals(Object o) {
    return o instanceof DSConfigAdaptor && hashCode() == o.hashCode();
  }

  /**
   * Based solely on the underlying URL.
   * Two URLDSConfigAdaptors should return the same hashCode if they
   * are based on the same underlying URL. If the underlying DatasetConfig
   * has changed between instantiation of the two URLDSConfigAdaptor objects,
   * a call to update() should resolve this.
   */
  public int hashCode() {
    return hashcode;
  }

  /**
   * allows any DSConfigAdaptor implimenting object to be compared to any other
   * DSConfigAdaptor implimenting object, based on their hashCode.
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(Object o) {
    return hashCode() - ((DSConfigAdaptor) o).hashCode();
  }

  /*
   * (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#lazyLoad(org.ensembl.mart.lib.config.DatasetConfig)
   */
  public void lazyLoad(DatasetConfig dsv) throws ConfigurationException {
    try {
      dscutils.loadDatasetConfigWithDocument( dsv, dscutils.getDocumentForXMLStream( InputSourceUtil.getStreamForURL( dsvurl ) ) );
    } catch (IOException e) {
      throw new ConfigurationException("Recieved IOException lazyLoading DatasetConfig: " + e.getMessage(), e);
    }
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getMartLocations()
   */
  public MartLocation[] getMartLocations() throws ConfigurationException {
    return new MartLocation[] { new URLLocation(dsvurl, adaptorName, "true")};
  }

  /**
   * Writes a DatasetConfig object as DatasetConfig.dtd compliant XML to a File.
   * @param dsv -- DatasetConfig object to store to the file system
   * @param file -- File to write XML
   * @throws ConfigurationException for underlying Exceptions
   */
  public static void StoreDatasetConfig(DatasetConfig dsv, File file) throws ConfigurationException {
    DatasetConfigXMLUtils dscutils = new DatasetConfigXMLUtils(false); //hidden members are only applicable to incoming XML streams
    dscutils.writeDatasetConfigToFile(dsv, file);
  }

  /**
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#supportsDataset(java.lang.String)
   */
  public boolean supportsDataset(String dataset) throws ConfigurationException {
    return dsv.getDataset().equals(dataset);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigsByDataset(java.lang.String)
   */
  public DatasetConfigIterator getDatasetConfigsByDataset(String dataset) throws ConfigurationException {

    if (supportsDataset(dataset))
      return getDatasetConfigs();
    else
      return new DatasetConfigIterator(new ArrayList().iterator()); //empty iterator 
  }
  
  /**
   * @return "URL"
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDisplayName()
   */
  public String getDisplayName() {
    return "URL";
  }

  /**
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigByDatasetInternalName(java.lang.String, java.lang.String)
   */
  public DatasetConfig getDatasetConfigByDatasetInternalName(String dataset, String internalName)
    throws ConfigurationException {

    boolean same = StringUtil.compare(dataset, dsv.getDataset()) == 0;
    same = same && StringUtil.compare(internalName, dsv.getInternalName()) == 0;

    if (same)
      return new DatasetConfig(dsv, false, true); // lazyLoaded copy
    else
      return null;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigByDatasetDisplayName(java.lang.String, java.lang.String)
   */
  public DatasetConfig getDatasetConfigByDatasetDisplayName(String dataset, String displayName)
    throws ConfigurationException {
      boolean same = StringUtil.compare(dataset, dsv.getDataset()) == 0;
      same = same && StringUtil.compare(displayName, dsv.getDisplayName()) == 0;

      if (same)
        return new DatasetConfig(dsv, false, true); // lazyLoaded copy
      else
        return null;
  }
  
  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetNames()
   */
  public String[] getDatasetNames(boolean includeHidden) throws ConfigurationException {
      if (includeHidden || ( (dsv.getVisible() != null) &&  (Integer.valueOf(dsv.getVisible()).intValue() > 0) ))
        return new String[0];
    return new String[] { dsv.getDataset()};
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetNames(java.lang.String)
   */
  public String[] getDatasetNames(String adaptorName, boolean includeHidden) throws ConfigurationException {
    if (this.adaptorName.equals(adaptorName))
      return getDatasetNames(includeHidden);
    else
      return new String[0];
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigDisplayNamesByDataset(java.lang.String)
   */
  public String[] getDatasetConfigDisplayNamesByDataset(String dataset) throws ConfigurationException {
    if (dsv.getDataset().equals(dataset))
      return new String[] { dsv.getDisplayName()};
    else
      return new String[0];
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigInternalNamesByDataset(java.lang.String)
   */
  public String[] getDatasetConfigInternalNamesByDataset(String dataset) throws ConfigurationException {
    if (dsv.getDataset().equals(dataset))
      return new String[] { dsv.getInternalName()};
    else
      return new String[0];
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getName()
   */
  public String getName() {
    return adaptorName;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#setName(java.lang.String)
   */
  public void setName(String adaptorName) {
    this.adaptorName = adaptorName;
  }
  
  /**
   * URLDSConfigAdaptor Objects do not contain child DSConfigAdaptor Objects.
   * @return false
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#supportsAdaptor(java.lang.String)
   */
  public boolean supportsAdaptor(String adaptorName) throws ConfigurationException {
    return false;
  }
  
  /**
   * This adapytor is not associated with a data source so it returns null.
   * @return null.
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDataSource()
   */
  public DetailedDataSource getDataSource() {
    return null;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getNumDatasetConfigs()
   */
  public int getNumDatasetConfigs(boolean visibleOnly) {
    if (visibleOnly)
      if ( (dsv.getVisible() != null) &&  (Integer.valueOf(dsv.getVisible()).intValue() > 0) )
        return 1;
      else
        return 0;
    else
      return 1;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getNumDatasetConfigsByDataset(java.lang.String)
   */
  public int getNumDatasetConfigsByDataset(String dataset) {
    if (dsv.getDataset().equals(dataset))
      return 1;
    else
      return 0;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#containsDatasetConfig(org.ensembl.mart.lib.config.DatasetConfig)
   */
  public boolean containsDatasetConfig(DatasetConfig dsv) throws ConfigurationException {
    return this.dsv != null && this.dsv.equals(dsv);
  }
  
  /**
   * Do nothing.
   */
  public void clearCache() {
    
  }
}
