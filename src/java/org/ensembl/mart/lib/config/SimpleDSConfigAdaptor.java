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

import java.util.ArrayList;
import java.util.List;

import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.util.StringUtil;

/**
 * DSConfigAdaptor implimenting object designed to store a single
 * DatasetConfig object.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class SimpleDSConfigAdaptor implements DSConfigAdaptor, Comparable {


  private final DatasetConfig dsv;
  private final String[] inames;
  private final String[] dnames;
  private final int hashcode;
  private String adaptorName = null;

  /**
   * Constructor for an immutable SimpleDSConfigAdaptor object.
   * Really only for development purposes.  If you do use this, make sure you pass in either
   * a fully instantiated DatasetConfig object (all FilterPage and AttributePage objects loaded) or
   * a DatasetConfig object with a different underlying DatasetConfigAdaptor object (this DSConfigAdaptor implementation
   * doesnt insert itself as the adaptor for a given DatasetConfig object, and its lazyLoad() is never called.
   * @param dset -- DatasetConfig object
   * @throws ConfigurationException when the DatasetConfig is null
   */
  public SimpleDSConfigAdaptor(DatasetConfig dset) throws ConfigurationException {
    if (dset == null)
      throw new ConfigurationException("SimpleDatasetConfig objects must be instantiated with a DatasetConfig object");
    inames = new String[] { dset.getInternalName()};
    dnames = new String[] { dset.getDisplayName()};
    dsv = dset;

    hashcode = dsv.hashCode();
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
    //immutable object, cannot be updated.
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();

    buf.append("[");
    buf.append(" dataset DisplayName=").append(dsv.getDisplayName());
    buf.append("]");

    return buf.toString();
  }

  /**
   * Allows Equality Comparisons manipulation of SimpleDSConfigAdaptor objects
   */
  public boolean equals(Object o) {
    return o instanceof SimpleDSConfigAdaptor && hashCode() == o.hashCode();
  }

  /**
   * Calculated from the underlying DataSetView hashCode.
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

  /**
   * Currently doesnt do anything, as Simple DatasetConfig objects are fully loaded
   * at instantiation.  Could change in the future.
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#lazyLoad(DatasetConfig)
   */
  public void lazyLoad(DatasetConfig dsv) throws ConfigurationException {
    // Doesnt do anything, should be fully instantiated
  }

  /**
   * Throws a ConfigurationException, as this doesnt have a compatible MartLocation element.
   * Client code should create one of the supported Adaptors from the DatasetConfig for this adaptor,
   * and use that one to create the MartRegistry object instead.
   */
  public MartLocation[] getMartLocations() throws ConfigurationException {
    throw new ConfigurationException("Cannot create a MartLocation from a SimpleDatasetConfigAdaptor\n");
  }

  /**
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#supportsDataset(java.lang.String)
   */
  public boolean supportsDataset(String dataset)
    throws ConfigurationException {
    return dsv.getDataset().equals(dataset);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigsByDataset(java.lang.String)
   */
  public DatasetConfigIterator getDatasetConfigsByDataset(String dataset)
    throws ConfigurationException {

      if (supportsDataset(dataset))
        return getDatasetConfigs();
      else
        return new DatasetConfigIterator(new ArrayList().iterator()); //empty iterator 
  }

  /**
   * @return "Simple" 
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDisplayName()
   */
  public String getDisplayName() {
    return "Simple";
  }

  /**
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigByDatasetInternalName(java.lang.String, java.lang.String)
   */
  public DatasetConfig getDatasetConfigByDatasetInternalName(
    String dataset,
    String internalName)
    throws ConfigurationException {
    
    boolean same = StringUtil.compare(dataset, dsv.getDataset()) == 0;
    same = same && StringUtil.compare(internalName, dsv.getInternalName()) == 0;

    if (same)
      return new DatasetConfig(dsv, false, true);//lazyLoaded copy
    else
      return null;
    }

  /**
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigByDatasetDisplayName(java.lang.String, java.lang.String)
   */
  public DatasetConfig getDatasetConfigByDatasetDisplayName(
    String dataset,
    String displayName)
    throws ConfigurationException {
    
    boolean same = StringUtil.compare(dataset, dsv.getDataset()) == 0;
    same = same && StringUtil.compare(displayName, dsv.getDisplayName()) == 0;

    if (same)
      return new DatasetConfig(dsv,false, true);//lazyLoaded copy
    else
      return null;
    }
    
  /**
   * SimpleDSConfigAdaptor Objects do not contain child DSConfigAdaptor Objects
   * @return null
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getAdaptorByName(java.lang.String)
   */
  public DSConfigAdaptor getAdaptorByName(String adaptorName) throws ConfigurationException {
      return null;
  }

  /**
   * SimpleDSConfigAdaptor objects do not contain child DSConfigAdaptor Objects
   * @return Empty String[]
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getAdaptorNames()
   */
  public String[] getAdaptorNames() throws ConfigurationException {
    return new String[0];
  }

  /**
   * SimpleDSConfigAdaptor objects do not contain child DSConfigAdaptor Objects
   * @return Empty DSConfigAdaptor[]
   * @see org.ensembl.mart.lib.config.LeafDSConfigAdaptor#getLeafAdaptors()
   */
  public DSConfigAdaptor[] getLeafAdaptors() throws ConfigurationException {
    return new DSConfigAdaptor[0];
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetNames()
   */
  public String[] getDatasetNames(boolean includeHidden) throws ConfigurationException {
      if (includeHidden || ( (dsv.getVisible() != null) &&  (Integer.valueOf(dsv.getVisible()).intValue() > 0) ))
      return new String[] { dsv.getDataset() };
    else
      return new String[0];    
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
      return new String[] { dsv.getDisplayName() };
    else
      return new String[0];
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigInternalNamesByDataset(java.lang.String)
   */
  public String[] getDatasetConfigInternalNamesByDataset(String dataset) throws ConfigurationException {
    if (dsv.getDataset().equals(dataset))
      return new String[] { dsv.getInternalName() };
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
   * SimpleDSConfigAdaptor objects do not contain child DSConfigAdaptor Objects
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
  public boolean containsDatasetConfig(DatasetConfig dsvc) throws ConfigurationException {
    return dsv != null && dsv.equals(dsvc);
  }
  
  /**
   * Do nothing.
   */
  public void clearCache() {
    
  }
}
