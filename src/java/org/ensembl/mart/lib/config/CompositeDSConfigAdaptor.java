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
	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.ensembl.mart.lib.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.ensembl.mart.lib.DetailedDataSource;

/**
 * A composite DSConfigAdaptor that combines the datasets from all contained 
 * DSConfigAdaptors.
 */
public class CompositeDSConfigAdaptor implements MultiDSConfigAdaptor, Comparable {

  //instanceCount for default adaptorName
  private static int count = 0;

  private final String DEFAULT_ADAPTOR_NAME = "Composite";

  protected Set adaptors = new HashSet();
  protected Set adaptorNameMap = new HashSet();
  protected String adaptorName = null;

  /**
   * Creates instance of CompositeDSConfigAdaptor.
   */
  public CompositeDSConfigAdaptor() {
    adaptorName = DEFAULT_ADAPTOR_NAME + count++;
  }

  /**
   * Adds adaptor.  
   * @param adaptor adaptor to be added. Do not add an ancestor CompositeDSConfigAdaptor
   * to this instance or you will cause circular references when the getXXX() methods are called.
   */
  public void add(DSConfigAdaptor adaptor) {
    if (adaptor.getName() != null)
      adaptorNameMap.add(adaptor.getName());
    adaptors.add(adaptor);
  }

  /**
   * Remove adaptor if present.
   * @param adaptor adaptor to be removed
   * @return true if adaptor was removed, otherwise false.
   */
  public boolean remove(DSConfigAdaptor adaptor) {
    if (adaptorNameMap.contains(adaptor.getName()))
      adaptorNameMap.remove(adaptor.getName());
    return adaptors.remove(adaptor);
  }

  /**
   * Removes all adaptors.
   */
  public void clear() {
    adaptorNameMap.clear();
    adaptors.clear();
  }

  /**
   * Gets currently available adaptors.
   * @return all adaptors currently managed by this instance. Empty array 
   * if non available.
   */
  public DSConfigAdaptor[] getLeafAdaptors() throws ConfigurationException {
    List leafAdaptors = new ArrayList();
    
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      Object adaptor = iter.next();
      if (adaptor instanceof LeafDSConfigAdaptor)
        leafAdaptors.add(adaptor);
      else {
        leafAdaptors.addAll( Arrays.asList( ( (DSConfigAdaptor) adaptor).getLeafAdaptors() ) );
      }        
    }
    
    DSConfigAdaptor[] ret = new DSConfigAdaptor[leafAdaptors.size()];
    leafAdaptors.toArray(ret);
    return ret;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigs()
   */
  public DatasetConfigIterator getDatasetConfigs() throws ConfigurationException {
    DatasetConfigIterator dsviter = null;

    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();

      if (dsviter == null)
        dsviter = new DatasetConfigIterator(adaptor.getDatasetConfigs());
      else
        dsviter.addDatasetConfigIterator(adaptor.getDatasetConfigs());
    }

    if (dsviter == null)
      dsviter = new DatasetConfigIterator(new ArrayList().iterator()); //empty iterator
    return dsviter;
  }

  /**
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#update()
   */
  public void update() throws ConfigurationException {
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();
      adaptor.update();
    }
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#lazyLoad(org.ensembl.mart.lib.config.DatasetConfig)
   */
  public void lazyLoad(DatasetConfig dsv) throws ConfigurationException {
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();
      if (adaptor.supportsDataset(dsv.getDataset())) {
        adaptor.lazyLoad(dsv);
        break;
      }
    }
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.MultiDSConfigAdaptor#removeDatasetConfig(org.ensembl.mart.lib.config.DatasetConfig)
   */
  public boolean removeDatasetConfig(DatasetConfig dsv) throws ConfigurationException {
    boolean removed = false;

    for (Iterator iter = adaptors.iterator(); !removed && iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();

      if (adaptor.supportsDataset(dsv.getDataset())) {
        if (adaptor instanceof MultiDSConfigAdaptor) {
          removed =
            ((MultiDSConfigAdaptor) adaptor).removeDatasetConfig(
              adaptor.getDatasetConfigByDatasetInternalName(dsv.getDataset(), dsv.getInternalName()));
        } else {
          DatasetConfig thisDSV = adaptor.getDatasetConfigByDatasetInternalName(dsv.getDataset(), dsv.getInternalName());
          if (thisDSV.equals(dsv))
            removed = adaptors.remove(adaptor);
        }
      }
    }

    return removed;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getMartLocations()
   */
  public MartLocation[] getMartLocations() throws ConfigurationException {
    List locations = new ArrayList();

    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();

      locations.addAll(Arrays.asList(adaptor.getMartLocations()));
    }

    MartLocation[] retlocs = new MartLocation[locations.size()];
    locations.toArray(retlocs);
    return retlocs;
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
   * Calculated from all included adaptor hashCodes.
   */
  public int hashCode() {
    int hsh = (adaptorName != null) ? adaptorName.hashCode() : 0;

    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();
      int h = adaptor.hashCode();
      hsh += h;
    }

    return hsh;
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
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#supportsDataset(java.lang.String)
   */
  public boolean supportsDataset(String dataset) throws ConfigurationException {
    return getNumDatasetConfigsByDataset(dataset) > 0;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigsByDataset(java.lang.String)
   */
  public DatasetConfigIterator getDatasetConfigsByDataset(String dataset) throws ConfigurationException {

    DatasetConfigIterator dsviter = null;

    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();

      if (adaptor.supportsDataset(dataset)) {
        if (dsviter == null)
          dsviter = new DatasetConfigIterator(adaptor.getDatasetConfigsByDataset(dataset));
        else
          dsviter.addDatasetConfigIterator(adaptor.getDatasetConfigsByDataset(dataset));
      }
    }

    if (dsviter == null)
      dsviter = new DatasetConfigIterator(new ArrayList().iterator()); //empty iterator
    return dsviter;
  }

  /**
   * @return "Composite"
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDisplayName()
   */
  public String getDisplayName() {
    return "Composite";
  }

  /**
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigByDatasetInternalName(java.lang.String, java.lang.String)
   */
  public DatasetConfig getDatasetConfigByDatasetInternalName(String dataset, String internalName)
    throws ConfigurationException {

    DatasetConfig view = null;
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();

      if (adaptor.supportsDataset(dataset)) {
        view = adaptor.getDatasetConfigByDatasetInternalName(dataset, internalName);
        
        if (view != null)
          break;
      }
    }

    return view;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigByDatasetDisplayName(java.lang.String, java.lang.String)
   */
  public DatasetConfig getDatasetConfigByDatasetDisplayName(String dataset, String displayName)
    throws ConfigurationException {
      DatasetConfig view = null;
      for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
        DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();

        if (adaptor.supportsDataset(dataset)) {
          view = adaptor.getDatasetConfigByDatasetDisplayName(dataset, displayName);
        
          if (view != null)
            break;
        }
      }

      return view;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetNames()
   */
  public String[] getDatasetNames(boolean includeHidden) throws ConfigurationException {
    List l = new ArrayList();
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();
      l.addAll(Arrays.asList( adaptor.getDatasetNames(includeHidden) ));
    }

    return (String[]) l.toArray(new String[l.size()]);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getAdaptorByName(java.lang.String)
   */
  public DSConfigAdaptor getAdaptorByName(String adaptorName) throws ConfigurationException {
    DSConfigAdaptor dsva = null;

    if (adaptorNameMap.contains(adaptorName)) {
      for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
        DSConfigAdaptor element = (DSConfigAdaptor) iter.next();
        if (element.getName().equals(adaptorName)) {
          dsva = element;
          break;
        } else if (element.supportsAdaptor(adaptorName))
          dsva = element.getAdaptorByName(adaptorName);
      }

    }

    return dsva;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getAdaptorNames()
   */
  public String[] getAdaptorNames() throws ConfigurationException {
    return (String[]) adaptorNameMap.toArray(new String[adaptorNameMap.size()]);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetNames(java.lang.String)
   */
  public String[] getDatasetNames(String adaptorName, boolean includeHidden) throws ConfigurationException {
    List l = new ArrayList();

    if (adaptorNameMap.contains(adaptorName)) {
      for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
        DSConfigAdaptor element = (DSConfigAdaptor) iter.next();

        if (element.getName().equals(adaptorName)) {
          l.addAll(Arrays.asList(element.getDatasetNames(includeHidden)));
          break;
        } else if (element.supportsAdaptor(adaptorName)) {
          l.addAll(Arrays.asList(element.getDatasetNames(adaptorName, includeHidden)));
          break;
        }
      }
    }

    return (String[]) l.toArray(new String[l.size()]);
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

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigDisplayNamesByDataset(java.lang.String)
   */
  public String[] getDatasetConfigDisplayNamesByDataset(String dataset) throws ConfigurationException {
    List l = new ArrayList();
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();

      if (adaptor.supportsDataset(dataset)) {
        l.addAll(Arrays.asList(adaptor.getDatasetConfigDisplayNamesByDataset(dataset)));
      }
    }

    return (String[]) l.toArray(new String[l.size()]);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigInternalNamesByDataset(java.lang.String)
   */
  public String[] getDatasetConfigInternalNamesByDataset(String dataset) throws ConfigurationException {
    List l = new ArrayList();

    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();

      if (adaptor.supportsDataset(dataset)) {
        l.addAll(Arrays.asList(adaptor.getDatasetConfigInternalNamesByDataset(dataset)));
      }
    }

    return (String[]) l.toArray(new String[l.size()]);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#supportsAdaptor(java.lang.String)
   */
  public boolean supportsAdaptor(String adaptorName) throws ConfigurationException {
    boolean supports = adaptorNameMap.contains(adaptorName);

    if (!supports) {
      for (Iterator iter = adaptors.iterator(); !supports && iter.hasNext();) {
        DSConfigAdaptor element = (DSConfigAdaptor) iter.next();

        supports = element.supportsAdaptor(adaptorName);
      }
    }

    return supports;
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
    int ret = 0;
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();
      ret += adaptor.getNumDatasetConfigs(visibleOnly);
    }
    
    return ret;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getNumDatasetConfigsByDataset(java.lang.String)
   */
  public int getNumDatasetConfigsByDataset(String dataset) {
    int ret = 0;
    
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();
      ret += adaptor.getNumDatasetConfigsByDataset(dataset);
    }
    
    return ret;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.MultiDSConfigAdaptor#containsDatasetConfig(org.ensembl.mart.lib.config.DatasetConfig)
   */
  public boolean containsDatasetConfig(DatasetConfig dsv) throws ConfigurationException {
    boolean ret = false;
    
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();
      ret = adaptor.containsDatasetConfig(dsv);
      if (ret)
        break;
    }
    
    return ret;
  }
  
  
  public void clearCache() {
    for (Iterator iter = adaptors.iterator(); iter.hasNext();) {
      DSConfigAdaptor adaptor = (DSConfigAdaptor) iter.next();
      adaptor.clearCache();
    }
  }
}
