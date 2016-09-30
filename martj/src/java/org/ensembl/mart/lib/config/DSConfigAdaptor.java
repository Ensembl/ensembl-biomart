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

import org.ensembl.mart.lib.DetailedDataSource;

/**
 * Interface for Objects providing access to one or more DatasetConfig Objects via
 * implimentation specific methods for accessing and parsing DatasetConfig.dtd compliant
 * documents from a target source.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public interface DSConfigAdaptor {

  /**
   * sets the name of this DSConfigAdptor
   * @param adaptorName - name of the adaptor
   */
  public void setName(String adaptorName);
  
  /**
   * Returns the name of this DSConfigAdaptor
   * @return String name of DSConfigAdaptor
   */
  public String getName();
  
  /**
   * Determine if the adaptor contains a given DatasetConfig.
   * @param dsv - DatasetConfig to search
   * @return true if adaptor contains DatasetConfig, false otherwise
   * @throws ConfigurationException for underlying Exceptions
   */
  public boolean containsDatasetConfig(DatasetConfig dsv) throws ConfigurationException;
  
  /**
   * Returns a count of the number of (optionally visible) 
   * DatasetConfigs held by this adaptor
   * @return int number of datasetviews held by the adaptor
   */
  public int getNumDatasetConfigs(boolean visibleOnly);
  
  /**
   * Returns a DatasetConfig[] consisting of all DatasetConfig objects provided by a particular
   * DSConfigAdaptor object.
   * @return DatasetConfigIterator dsetconfigs
   * @throws ConfigurationException for all underlying exceptions
   * @see org.ensembl.mart.lib.config.DatasetConfigIterator
   */
  public DatasetConfigIterator getDatasetConfigs() throws ConfigurationException;

  /**
   * Returns a specific DatasetConfig object, named by the given dataset
   * and internalName.
   * @param dataset
   * @param internalName
   * @return DatasetConfig named by the given dataset
   * and internalName.
   * @throws ConfigurationException for all underlying Exceptions
   */
  public DatasetConfig getDatasetConfigByDatasetInternalName(
    String dataset,
    String internalName)
    throws ConfigurationException;

  /**
   * Returns a specific DatasetConfig object, named by the given dataset
   * and displayName.
   * @param dataset
   * @param internalName
   * @return DatasetConfig named by the given dataset
   * and internalName.
   * @throws ConfigurationException for all underlying Exceptions
   */
  public DatasetConfig getDatasetConfigByDatasetDisplayName(
    String dataset,
    String displayName)
    throws ConfigurationException;

  /**
   * Determine if the DSConfigAdaptor contains a DatasetConfig with the given
   * dataset name.
   * @param dataset -- dataset name
   * @return true if supported, false otherwise
   * @throws ConfigurationException for all underlying Exceptions
   */
  public boolean supportsDataset(String dataset) throws ConfigurationException;

  /**
   * Returns the number of DatasetConfigs for a given Dataset supported by the adaptor
   * @param dataset -- name of dataset for which count of DatasetConfigs is desired
   * @return number of DatasetConfigs supporting this dataset.
   */
  public int getNumDatasetConfigsByDataset(String dataset);
  
  /**
   * Returns specific DatasetConfigs with the given dataset name 
   * @param dataset -- dataset name
   * @return DatasetConfigIterator with DatasetConfigs for the given dataset name, empty Iterator if none found
   * @throws ConfigurationException for all underlying Exceptions
   * @see org.ensembl.mart.lib.config.DatasetConfigIterator
   */
  public DatasetConfigIterator getDatasetConfigsByDataset(String dataset)
    throws ConfigurationException;

  /**
   * Returns a list of the DatasetConfig internalNames for a particular dataset.
   * @param dataset - name of the dataset for which internalNames are required.
   * @return String[] list of datasetconfig internalNames associated with the dataset.
   * @throws ConfigurationException
   */
  public String[] getDatasetConfigInternalNamesByDataset(String dataset) throws ConfigurationException;
  
  /**
   * Returns a list of the DatasetConfig displayNames for a particular dataset.
   * @param dataset - name of the dataset for which displayNames are required.
   * @return String[] list of datasetconfig displayNames associated with the dataset.
   * @throws ConfigurationException
   */
  public String[] getDatasetConfigDisplayNamesByDataset(String dataset) throws ConfigurationException;
  
  /**
   * Returns a list of the names of the Datasets for which DatasetConfigs are held
   * for an Adaptor.
   * @return String[] list of dataset names
   * @throws ConfigurationException
   */
  public String[] getDatasetNames(boolean includeHidden) throws ConfigurationException;
  
  /**
   * Returns a list of the names of Datasets for which DatasetConfigs are held, given a particular
   * adaptor name.  May return an empty list for adaptors with zero child adaptors.
   * @param adaptorName - name of DSConfigAdaptor object for which DatasetNames are required
   * @return String[] list of datatset names
   * @throws ConfigurationException
   */
  public String[] getDatasetNames(String adaptorName, boolean includeHidden) throws ConfigurationException;
  
  /**
   * Returns all primitive DSConfigAdaptor objects contained with this Object (which may be a zero length list
   * for some implimentations). This recurses through composite adaptors and only returns those adaptors 
   * that do not themselves contain adaptors, i.e. the "leaf" adaptors in the object graph.
   * @return Array of DSConfigAdaptor objects
   * @throws ConfigurationException
   */
  public DSConfigAdaptor[] getLeafAdaptors() throws ConfigurationException;
  
  /**
   * Determine if a DSConfigAdaptor supports (contains somewhere in its adaptors, or its adaptors child adaptors)
   * a given DSConfigAdaptor named by adaptorName
   * @param adaptorName - name of requested DSConfigAdaptor
   * @return boolean true if the requested DSConfigAdaptor is supported by this DSConfigAdaptor, false otherwise
   * @throws ConfigurationException
   */
  public boolean supportsAdaptor(String adaptorName) throws ConfigurationException;
  
  /**
   * Returns a DSConfigAdaptor object named by the given adaptor name
   * @param adaptorName - name of DSConfigAdaptor required 
   * @return DSConfigAdaptor named by adaptorName
   * @throws ConfigurationException
   */
  public DSConfigAdaptor getAdaptorByName(String adaptorName) throws ConfigurationException;
  
  /**
   * Returns the names of all DSConfigAdaptor objects contained within this Object (which may be a zero
   * length list for some implimentations). Note, this only returns the names of Adaptors held by this Adaptor,
   * and does not return names of child adaptors of children to this Adaptor.
   * @return String[] names of all adaptors in this Object.
   * @throws ConfigurationException
   */
  public String[] getAdaptorNames() throws ConfigurationException;
  
  /**
    * If a DSConfigAdaptor implimenting object caches names and DatasetConfig objects, this method updates the cache contents
    * based on a comparison with the information stored in the object's target source.  May not actually do anything for some implimentations.
    * 
    * @throws ConfigurationException for all underlying Exceptions
    */
  public void update() throws ConfigurationException;

  /**
   * Method to allow DatasetConfig objects to be instantiated with a minimum of information, but then be lazy loaded with the rest of their
   * XML data when needed.  This method is intended primarily to be used by the DatasetConfig object itself, which automatically lazy loads itself
   * using the adaptor that it was instantiated with.
   * @param dsv -- DatasetConfig Object to be lazy loaded.  Input reference is modified by the method.
   * @throws ConfigurationException for all underlying Exceptions
   */
  public void lazyLoad(DatasetConfig dsv) throws ConfigurationException;

  /**
   * All implimentations should be able to create MartLocation objects which can be added to 
   * a MartRegistry object when RegistryDSConfigAdaptor.getMartRegistry method is called.
   * @return MartLocation[] array
   * @throws ConfigurationException for any underlying Exceptions.
   */
  public MartLocation[] getMartLocations() throws ConfigurationException;

  /**
   * All implementations should provide a display name.
   * @return display name for this adaptor.
   */
  public String getDisplayName();

  /**
   * All implementations should either return a datasource if available,
   * otherwise null.
   * @return datasource if available, otherwise null.
   */
  public DetailedDataSource getDataSource();


  /**
   * Clears cache if one is used, otherwise does nothing.
   */
  public void clearCache();

}
