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

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.util.StringUtil;

/**
 * DSConfigAdaptor implimentation that retrieves DatasetConfig objects from
 * a Mart Database.
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class DatabaseDSConfigAdaptor extends LeafDSConfigAdaptor implements MultiDSConfigAdaptor, Comparable, Runnable {

  //each dataset will have 2 name maps, and a Set of DatasetConfig objects associated with it in an ArrayList
  private final int INAME_INDEX = 0;

  private DatasetConfigCache cache = null;

  private String dbpassword;
  private Logger logger = Logger.getLogger(DatabaseDSConfigAdaptor.class.getName());
  private List dsviews = new ArrayList();
  private HashMap datasetNameMap = new HashMap();

  private final DetailedDataSource dataSource;
  private final DatasetConfigXMLUtils dscutils;
  private final DatabaseDatasetConfigUtils dbutils;

  private final String user;
  private final String martUser;
  private final int hashcode;
  private String adaptorName = null;

  private boolean clearCache = false; //developer hack to clear the cache
  //will be replaced soon with user supported clearing
  private boolean ignoreCache = false;
  
  private boolean loadFully = false;
  
  private boolean readonly = false;

  //To propogate update exceptions from thread
  private Thread updateThread = null;
  private ConfigurationException updateException = null;

  /**
   * Constructor for a DatabaseDSConfigAdaptor
   * @param ds -- DataSource for Mart RDBMS
   * @param user -- user for RDBMS connection, AND meta_DatasetConfig_user table
   * @param ignoreCache -- if true, cached XML is completely ignored, and all XML is pulled from the Database
   * @param loadFully -- if true, all DatasetConfiguration Objects are fully loaded into memory,
   *                      no lazy loading occurs (this should only be used by big servers with reasonable memory).
   *                      Note, setting this true also be default sets ignoreCache to true.
   * @param includeHiddenMembers -- if true, hidden members are included in DatasetConfig objects, otherwise they are not included
   * @param readonly - if true, meta tables are not altered.
   * @throws ConfigurationException if DataSource or user is null
   */
  public DatabaseDSConfigAdaptor(
    DetailedDataSource ds,
    String user,
    String martUser,
    boolean ignoreCache,
    boolean loadFully,
    boolean includeHiddenMembers,
    boolean readonly)
    throws ConfigurationException {
    if (ds == null || user == null)
      throw new ConfigurationException("DatabaseDSConfigAdaptor Objects must be instantiated with a DataSource and User\n");

    this.user = user;
    this.martUser = martUser;
    dataSource = ds;
    this.ignoreCache = ignoreCache;
    this.loadFully = loadFully;
    this.readonly = readonly;
    
    dscutils = new DatasetConfigXMLUtils(includeHiddenMembers);
    
    if (loadFully) {
        dscutils.setFullyLoadMode(loadFully);
        this.ignoreCache = true;
    }
    
    dbutils = new DatabaseDatasetConfigUtils(dscutils, dataSource, readonly);

    String host = ds.getHost();
    String port = ds.getPort();
    String databaseName = ds.getDatabaseName();

    adaptorName = ds.getName();

    if (!ignoreCache) {
      String cacheName = ds.getHost() + "__" + ds.getDatabaseName();
      cache = new DatasetConfigCache(this, new String[] { cacheName, user }, dscutils);

      //set up the preferences node with the datasource information as the root node
      if (clearCache) {
        cache.clearCache();
      }
    }

    int tmp = user.hashCode();
    tmp = (31 * tmp) + host.hashCode();
    tmp = (port != null) ? (31 * tmp) + port.hashCode() : tmp;
    tmp = (ds.getDatabaseType() != null) ? (31 * tmp) + ds.getDatabaseType().hashCode() : tmp;
    tmp = (databaseName != null) ? (31 * tmp) + databaseName.hashCode() : tmp;
    tmp = (31 * tmp) + ds.getJdbcDriverClassName().hashCode();
    tmp = (31 * tmp) + adaptorName.hashCode();
    hashcode = tmp;

    update();
  }

  /**
   * This method should ONLY be used if the user is not concerned with network password snooping, as it does
   * not do anything to encrypt the password provided.  It is really a convenience method for users wishing to
   * create MartRegistry files with their database password attribute filled in.
   * @param password -- String password for underlying DataSource
   * @see org.ensembl.mart.lib.config.DatabaseDSConfigAdaptor#getMartLocations
   */
  public void setDatabasePassword(String password) {
    dbpassword = password;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigs()
   */
  public DatasetConfigIterator getDatasetConfigs() throws ConfigurationException {
    checkUpdateException();
    return new DatasetConfigIterator(dsviews.iterator());
  }

  public void addDatasetConfig(DatasetConfig dsv) throws ConfigurationException {
    checkUpdateException();
    if (!(datasetNameMap.containsKey(dsv.getDataset()))) {
      dsv.setDSConfigAdaptor(this);
      dsviews.add(dsv); //add to the global dsviews list

      HashMap inameMap = new HashMap();

      inameMap.put(dsv.getDatasetID(), dsv);

      Vector maps = new Vector();
      maps.add(INAME_INDEX, inameMap);

      datasetNameMap.put(dsv.getDataset(), maps);
    } else {
      Vector maps = (Vector) datasetNameMap.get(dsv.getDataset());
      HashMap inameMap = (HashMap) maps.get(INAME_INDEX);

      if (!inameMap.containsKey(dsv.getDatasetID())) {
        dsv.setDSConfigAdaptor(this);
        dsviews.add(dsv); //add to the global dsviews list

        inameMap.put(dsv.getDatasetID(), dsv);

        maps.remove(INAME_INDEX);
        maps.add(INAME_INDEX, inameMap);

        datasetNameMap.put(dsv.getDataset(), maps);
      }
    }
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.MultiDSConfigAdaptor#removeDatasetConfig(org.ensembl.mart.lib.config.DatasetConfig)
   */
  public boolean removeDatasetConfig(DatasetConfig dsv) throws ConfigurationException {
    checkUpdateException();
    if (datasetNameMap.containsKey(dsv.getDataset())) {
      Vector maps = (Vector) datasetNameMap.get(dsv.getDataset());
      HashMap inameMap = (HashMap) maps.get(INAME_INDEX);

      if (inameMap.containsKey(dsv.getDatasetID())) {
        datasetNameMap.remove(dsv.getDataset());
        inameMap.remove(dsv.getDatasetID());

        dsviews.remove(dsv);

        dsv.setDSConfigAdaptor(null);

        //if this dataset is completely removed from the adaptor, make sure its keys reflect its removal
        if (getNumDatasetConfigsByDataset(dsv.getDataset()) > 0) {
          maps.remove(INAME_INDEX);
          maps.add(INAME_INDEX, inameMap);
          datasetNameMap.put(dsv.getDataset(), maps);
        } else
          datasetNameMap.remove(dsv.getDataset());

        return true;
      } else
        return false;
    } else
      return false;
  }

  private void checkMemoryForUpdate(String dataset, HashMap inameMap, String datasetID) throws ConfigurationException {
    if (logger.isLoggable(Level.FINE))
      logger.fine(" Already loaded, check for update\n");

    byte[] nDigest = dbutils.getDSConfigMessageDigestByDatasetID(user, dataset, datasetID);
    byte[] oDigest = ((DatasetConfig) inameMap.get(datasetID)).getMessageDigest();

    if (!MessageDigest.isEqual(oDigest, nDigest)) {

      if (logger.isLoggable(Level.FINE))
        logger.fine("Needs update\n");

      removeDatasetConfig((DatasetConfig) inameMap.get(datasetID));
      loadFromDatabase(dataset, datasetID);
    }
  }

  private boolean cacheUpToDate(String dataset, String datasetID) throws ConfigurationException {
    byte[] sourceDigest = dbutils.getDSConfigMessageDigestByDatasetID(user, dataset, datasetID);

    return cache.cacheUpToDate(sourceDigest, dataset, datasetID);
  }

  private void loadCacheOrUpdate(String dataset, String datasetID) throws ConfigurationException {
    if (cacheUpToDate(dataset, datasetID)) {
      if (logger.isLoggable(Level.FINE))
        logger.fine("Attempting to load from cache\n");

      DatasetConfig newDSV = null;
      try {
        newDSV = cache.getDatasetConfig(dataset, datasetID, this);
      } catch (ConfigurationException e) {
        if (logger.isLoggable(Level.FINE))
          logger.fine(
            "Could not load " + dataset + " " + datasetID + " from cache: " + e.getMessage() + "\nloading from database!\n");
        loadFromDatabase(dataset, datasetID);
      }
      addDatasetConfig(newDSV);
    } else if (logger.isLoggable(Level.FINE))
      logger.fine("Cache is not up to date for " + dataset + " " + datasetID + "\n loading from Database!\n");
    loadFromDatabase(dataset, datasetID);
  }

  private void loadFromDatabase(String dataset, String datasetID) throws ConfigurationException {
    if (logger.isLoggable(Level.FINE))
      logger.fine("Dataset " + dataset + " datasetID " + datasetID + " Not in cache, loading from database\n");
	
    DatasetConfig newDSV = dbutils.getDatasetConfigByDatasetID(user, dataset, datasetID,dbutils.getSchema()[0]);
    
    if (loadFully)
        dscutils.loadDatasetConfigWithDocument(newDSV, dbutils.getDatasetConfigDocumentByDatasetID(user, dataset, datasetID,dbutils.getSchema()[0]));
    
    addDatasetConfig(newDSV);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#update()
   */
  public synchronized void update() throws ConfigurationException {
    checkUpdateException();
    updateThread = new Thread(this, "DatabaseDSConfigAdaptorUpdateThread");
    updateThread.start();
  }

  private void lazyLoadWithDatabase(DatasetConfig dsv) throws ConfigurationException {
    if (logger.isLoggable(Level.FINE))
      logger.fine("lazy loading from database\n");

    dscutils.loadDatasetConfigWithDocument(
      dsv,
      dbutils.getDatasetConfigDocumentByDatasetID(user, dsv.getDataset(), dsv.getDatasetID(),dbutils.getSchema()[0]));

    if (!ignoreCache) {
      //cache this DatasetConfig, as, for some reason, it is needing to be cached
      cache.removeDatasetConfig(dsv.getDataset(), dsv.getInternalName());
      cache.addDatasetConfig(dsv);
    }
  }

  private void lazyLoadWithCache(DatasetConfig dsv) throws ConfigurationException {
    try {
      cache.lazyLoadWithCache(dsv);
    } catch (ConfigurationException e) {
      if (logger.isLoggable(Level.FINE))
        logger.fine(
          "Recieved Exception attempting to lazyLoad from cache: " + e.getMessage() + "\nlazyLoading from Database!\n");
      lazyLoadWithDatabase(dsv);
    }
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#lazyLoad(org.ensembl.mart.lib.config.DatasetConfig)
   */
  public void lazyLoad(DatasetConfig dsv) throws ConfigurationException {
    String dataset = dsv.getDataset();
    //String iname = dsv.getInternalName();
	String datasetID = dsv.getDatasetID();
	
    if (!ignoreCache && cacheUpToDate(dataset, datasetID))
      lazyLoadWithCache(dsv);
    else
      lazyLoadWithDatabase(dsv);
  }

  /**
   * Note, this method will only include the DataSource password in the resulting MartLocation object
   * if the user set the password using the setDatabasePassword method of this adaptor. Otherwise, 
   * regardless of whether the underlying DataSource was created
   * with a password, the resulting DatabaseLocation element will not have
   * a password attribute.  Users may need to hand modify any MartRegistry documents
   * that they create in these cases.  Users are encouraged to use
   * passwordless, readonly access users.
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getMartLocations()
   */
  public MartLocation[] getMartLocations() throws ConfigurationException {
    checkUpdateException();
    
    MartLocation dbloc =
      new DatabaseLocation(
        dataSource.getHost(),
        dataSource.getPort(),
        dataSource.getDatabaseType(),
        dataSource.getDatabaseName(),
		dataSource.getSchema(),
        user,
        martUser,
        dbpassword,
        adaptorName, "true");
    return new MartLocation[] { dbloc };
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
   * Calculation is purely based on the DataSource and user hashCode.  Any
   * DatabaseDSConfigAdaptor based on these two inputs should represent the same
   * collection of DatasetConfig objects.
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
    return hashcode - ((DSConfigAdaptor) o).hashCode();
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#supportsDataset(java.lang.String)
   */
  public boolean supportsDataset(String dataset) throws ConfigurationException {
    checkUpdateException();
    return getNumDatasetConfigsByDataset(dataset) > 0;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigsByDataset(java.lang.String)
   */
  public DatasetConfigIterator getDatasetConfigsByDataset(String dataset) throws ConfigurationException {
    checkUpdateException();

    ArrayList l = new ArrayList();

    for (int i = 0, n = dsviews.size(); i < n; i++) {
      DatasetConfig view = (DatasetConfig) dsviews.get(i);
      if (view.getDataset().equals(dataset)) {
        l.add(new DatasetConfig(view, false, false));
        //return copy of datasetview, so that lazyLoad doesnt expand reference to original
      }
    }

    return new DatasetConfigIterator(l.iterator());
  }

  /**
   * @return datasource.toString() if datasource is not null, otherwise 
   * "No Database".
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDisplayName()
   */
  public String getDisplayName() {

    return (dataSource == null) ? "No Database" : dataSource.toString();
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigByDatasetInternalName(java.lang.String, java.lang.String)
   */
  public DatasetConfig getDatasetConfigByDatasetInternalName(String dataset, String internalName)
    throws ConfigurationException {

    checkUpdateException();
    DatasetConfig view = null;
    for (int i = 0; view == null && i < dsviews.size(); ++i) {

      DatasetConfig dsv = (DatasetConfig) dsviews.get(i);
      if (dsv.getDataset().equals(dataset) && dsv.getInternalName().equals(internalName)) {
        //lazyLoaded copy unless loadFully is true
        if (loadFully)
            view = new DatasetConfig(dsv, true, false);
        else
            view = new DatasetConfig(dsv, false, true);
      }
    }

    return view;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigByDatasetDisplayName(java.lang.String, java.lang.String)
   */
  public DatasetConfig getDatasetConfigByDatasetDisplayName(String dataset, String displayName)
    throws ConfigurationException {
    checkUpdateException();

    DatasetConfig view = null;
    for (int i = 0; i < dsviews.size(); ++i) {

      DatasetConfig dsv = (DatasetConfig) dsviews.get(i);
      if (StringUtil.compare(dataset, dsv.getDataset()) == 0
        && StringUtil.compare(displayName, dsv.getDisplayName()) == 0)
        view = new DatasetConfig(dsv, false, true); //lazyLoaded copy
    }

    return view;
  }

  /**
   * DatabaseDSConfigAdaptor objects do not contain child adaptors
   * @return null
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getAdaptorByName(java.lang.String)
   */
  public DSConfigAdaptor getAdaptorByName(String adaptorName) throws ConfigurationException {
    // DatabaseDSConfigAdaptor objects do not contain child adaptors
    return null;
  }

  /**
   * DatabaseDSConfigAdaptor objects do not contain child adaptors
   * return empty String[]
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getAdaptorNames()
   */
  public String[] getAdaptorNames() throws ConfigurationException {
    return new String[0];
  }

  /**
   * DatabaseDSConfigAdaptor objects do not contain child adaptors
   * return empty DSConfigAdaptor[]
   * @see org.ensembl.mart.lib.config.LeafDSConfigAdaptor#getLeafAdaptors()
   */
  public DSConfigAdaptor[] getLeafAdaptors() throws ConfigurationException {
    // DatabaseDSConfigAdaptor objects do not contain child adaptors
    return new DSConfigAdaptor[0];
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetNames()
   */
  public String[] getDatasetNames(boolean includeHidden) throws ConfigurationException {
    checkUpdateException();
    ArrayList names = new ArrayList();
    
    for (Iterator iter = dsviews.iterator(); iter.hasNext();) {
        DatasetConfig dsv = (DatasetConfig) iter.next();

        if (includeHidden || ( (dsv.getVisible() != null) &&  (Integer.valueOf(dsv.getVisible()).intValue() > 0) ))
            names.add(dsv.getDataset());
    }
    
    return (String[]) names.toArray(new String[names.size()]);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetNames(java.lang.String)
   */
  public String[] getDatasetNames(String adaptorName, boolean includeHidden) throws ConfigurationException {
    checkUpdateException();
    if (adaptorName.equals(this.adaptorName))
      return getDatasetNames(includeHidden);
    else
      return new String[0];
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigDisplayNamesByDataset(java.lang.String)
   */
  public String[] getDatasetConfigDisplayNamesByDataset(String dataset) throws ConfigurationException {
    checkUpdateException();
    List names = new ArrayList();

    for (int i = 0; i < dsviews.size(); ++i) {
      DatasetConfig dsv = (DatasetConfig) dsviews.get(i);
      if (StringUtil.compare(dataset, dsv.getDataset()) == 0)
        names.add(dsv.getDisplayName());
    }

    return (String[]) names.toArray(new String[names.size()]);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getDatasetConfigInternalNamesByDataset(java.lang.String)
   */
  public String[] getDatasetConfigInternalNamesByDataset(String dataset) throws ConfigurationException {
    checkUpdateException();
    List names = new ArrayList();

    for (int i = 0; i < dsviews.size(); ++i) {
      DatasetConfig dsv = (DatasetConfig) dsviews.get(i);
      if (StringUtil.compare(dataset, dsv.getDataset()) == 0)
        names.add(dsv.getInternalName());
    }

    return (String[]) names.toArray(new String[names.size()]);
  }

  /**
   * return name, defaults to "user@host:port/databaseName"
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
   * DatabaseDSConfigAdaptor objects do not contain child adaptors
   * return false
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#supportsAdaptor(java.lang.String)
   */
  public boolean supportsAdaptor(String adaptorName) throws ConfigurationException {
    return false;
  }

  /**
   * Get the underlying DataSource for this DatabaseDSConfigAdaptor
   * @return DetailedDataSource
   */
  public DetailedDataSource getDataSource() {
    return dataSource;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getNumDatasetConfigs()
   */
  public int getNumDatasetConfigs(boolean visibleOnly) {
    try {
      checkUpdateException();
      int ret = 0;
      for (Iterator iter = dsviews.iterator(); iter.hasNext();) {
        DatasetConfig dsv = (DatasetConfig) iter.next();

        if (visibleOnly)
          if ( (dsv.getVisible() != null) &&  (Integer.valueOf(dsv.getVisible()).intValue() > 0) )
            ret++;
          else
            continue;
        else
          ret++;
      }
      return ret;
    } catch (ConfigurationException e) {
      if (logger.isLoggable(Level.FINE))
        logger.fine("Recieved Exception during update Thread: " + updateException + "\nReturning 0\n");
      return 0;
    }
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#getNumDatasetConfigsByDataset(java.lang.String)
   */
  public int getNumDatasetConfigsByDataset(String dataset) {
    int ret = 0;
    try {
      checkUpdateException();

      for (int i = 0; i < dsviews.size(); ++i) {
        DatasetConfig dsv = (DatasetConfig) dsviews.get(i);
        if (StringUtil.compare(dataset, dsv.getDataset()) == 0)
          ret++;
      }
    } catch (ConfigurationException e) {
      if (logger.isLoggable(Level.FINE))
        logger.fine("Recieved Exception during update Thread: " + updateException + "\nReturning 0\n");
    }
    return ret;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.config.DSConfigAdaptor#containsDatasetConfig(org.ensembl.mart.lib.config.DatasetConfig)
   */
  public boolean containsDatasetConfig(DatasetConfig dsv) throws ConfigurationException {
    checkUpdateException();
    return dsviews.contains(dsv);
  }

  private void checkUpdateException() throws ConfigurationException {
    if (updateThread != null && updateThread != Thread.currentThread()) {
      //if this is the main thread, and the updateThread is still active, we need to wait here
      try {
        if (updateThread.isAlive()) {
          if (logger.isLoggable(Level.FINE))
            logger.fine("Waiting for Update thread to finish\n");
          updateThread.join();
        }
      } catch (InterruptedException e1) {
        updateException = new ConfigurationException("Update Thread was interrupted: " + e1.getMessage() + "\n", e1);
      } finally {
        //all getters use this to check if there was an exception in the underlying update Thread
        if (updateException != null) {
          ConfigurationException e = updateException;
          updateException = null;
          updateThread = null;
          throw e;
        }
      }
    }
    return;
  }

  /* (non-Javadoc)
   * @see java.lang.Runnable#run()
   */
  public void run() {
    try {
      String[] datasets = dbutils.getAllDatasetNames(user,martUser);
      for (int i = 0, n = datasets.length; i < n; i++) {
        String dataset = datasets[i];
        
        // note all keying is on datasetID rather than internalName now
        String[] inms = dbutils.getAllDatasetIDsForDataset(user, dataset);

        if (datasetNameMap.containsKey(dataset)) {
          //dataset is loaded, check for update of its datasetview
          Vector maps = (Vector) datasetNameMap.get(dataset);
          HashMap inameMap = (HashMap) maps.get(INAME_INDEX);

          for (int k = 0, m = inms.length; k < m; k++) {
            String iname = inms[k];

            if (logger.isLoggable(Level.FINE))
              logger.fine("Checking for dataset " + dataset + " internamName " + iname + "\n");

            if (inameMap.containsKey(iname))
              checkMemoryForUpdate(dataset, inameMap, iname);
            else if (!ignoreCache && cache.cacheExists(dataset, iname))
              loadCacheOrUpdate(dataset, iname);
            else
              loadFromDatabase(dataset, iname);
          }
        } else if ( !ignoreCache ) {
            //not already loaded, check for its datasetviews in cache
            for (int k = 0, m = inms.length; k < m; k++) {
              String iname = inms[k];

              if (logger.isLoggable(Level.FINE))
                logger.fine("Checking for dataset " + dataset + " internamName " + iname + "\n");

              if (!ignoreCache && cache.cacheExists(dataset, iname))
                loadCacheOrUpdate(dataset, iname);
              else {
                //load datasetview from database
                if (logger.isLoggable(Level.FINE))
                  logger.fine("Dataset " + dataset + " internalName " + iname + " not in cache, loading from database\n");

                loadFromDatabase(dataset, iname);
              }
            }
        } else {
          //load dataset from database
          if (logger.isLoggable(Level.FINE))
            logger.fine("Dataset " + dataset + " not in cache, loading from database\n");

          for (int k = 0, m = inms.length; k < m; k++) {
            String iname = inms[k];
            loadFromDatabase(dataset, iname);
          }
        }
      }
    } catch (ConfigurationException e) {
      updateException = e;
    }
  }
  
  /**
   * Removes cached config files from filesystem.
   */
  public void clearCache() {
      try {
        cache.clearCache();
      } catch (ConfigurationException e) {
        e.printStackTrace();
      }
    
  }
}
