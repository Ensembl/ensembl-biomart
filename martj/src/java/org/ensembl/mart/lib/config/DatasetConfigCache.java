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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

/**
 * Object to cache DatasetConfiguration objects to the file system.
 * Uses a combination of the client user Preferences, and a files in
 * the home directory of the user under .martj_preferences.   
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class DatasetConfigCache {

  private Logger logger = Logger.getLogger(DatasetConfigCache.class.getName());

  private final String XMLDIR = System.getProperty("user.home") + File.separator + ".martj_preferences";
  private final String NAMESEPARATOR = "__";
  private final String NAMEPREFIX = ".";
  private final String XMLENDING = ".xml";
  private final String DNAMEKEY = "displayName";
  private final String DESCKEY = "description";
  private final String TYPEKEY = "type";
  private final String VISIBLEKEY = "visible";
  private final String VISIBLEFILTERPAGEKEY = "visibleFilterPage";
  private final String VERSIONKEY = "version";
  private final String DIGESTKEY = "MD5";
  private final String XMLKEY = "XML";

  private Preferences xmlPrefs = null;
  private DSConfigAdaptor caller = null;
  private String[] keys = null;
  private DatasetConfigXMLUtils dscutils = null;

  /**
   * Constructs a cache for a given DatasetConfigAdaptor. Passing
   * a key[] allows the cache to specify a key specific enough for any
   * DSConfigAdaptor implementation.
   * @param caller - DSConfigAdaptor that needs caching
   * @param keys - String[] list of keys to use in creating the cache.
   * @param dscutils - DatasetConfigXMLUtils object to read and write XML
   */
  public DatasetConfigCache(DSConfigAdaptor caller, String[] keys, DatasetConfigXMLUtils dscutils) {
    this.caller = caller;
    this.keys = keys;
    this.dscutils = dscutils;
    initCache();
  }

  private void initCache() {
    if (xmlPrefs == null) {
      xmlPrefs = Preferences.userNodeForPackage(caller.getClass());
      for (int i = 0, n = keys.length; i < n; i++) {
        String key = keys[i];
        xmlPrefs = xmlPrefs.node(key);
      }
    }
  }

  /**
   * Clears the cache for this node, deleting any xml files in $HOME/.martj_preferences
   * @throws ConfigurationException for underlying exceptions
   */
  public void clearCache() throws ConfigurationException {
    initCache();
    try {
      //find any xml files associated with this cache and delete them
      String[] datasets = xmlPrefs.childrenNames();
      for (int i = 0, n = datasets.length; i < n; i++) {
        String dataset = datasets[i];
        
        String[] inames = xmlPrefs.node(dataset).childrenNames();
        for (int j = 0, m = inames.length; j < m; j++) {
          String iname = inames[j];
          deleteFile(dataset, iname);
          xmlPrefs.node(dataset).node(iname).removeNode();
        }
        xmlPrefs.node(dataset).removeNode();
      }
      
      //remove the base directory if it is empty
      File baseDir = initCacheDir();
      if (baseDir.isDirectory() && baseDir.list().length < 1)
        baseDir.delete();
        
      xmlPrefs.flush();
    } catch (BackingStoreException e) {
      throw new ConfigurationException("Caught BackingStoreException clearing cache: " + e.getMessage(), e);
    }
  }

  private File initCacheDir() {
    File baseDir = new File(XMLDIR);
    if (!baseDir.isDirectory())
      baseDir.mkdir();
    return baseDir;
  }

  private File getFile(String dataset, String iname) {
    File baseDir = initCacheDir();
    String fileName = NAMEPREFIX;
    
    for (int i = 0, n = keys.length; i < n; i++) {
      String key = keys[i];
      fileName += key + NAMESEPARATOR;
    }
    
    fileName += dataset + NAMESEPARATOR + iname + XMLENDING;
    File ret = new File(baseDir, fileName);
    return ret; 
  }
  
  private void deleteFile(String dataset, String internalName) {
    String xmlloc = pathTo(dataset, internalName);
    if (xmlloc != null) {
      File xmlfile = new File(xmlloc);
      if (xmlfile.exists())
        xmlfile.delete();
    }
  }
  
  /**
   * Adds a DatasetConfig to the cache. This stores a file to $HOME/.martj_prefs/dataset/internalName.xml,
   * and stores the identifying values of the DatasetConfig to the preferences object to allow lazy loading, as well
   * as its file location and md5sum to check if it is up to date with the original source.
   * @param dsc - DatasetConfig to cache
   * @throws ConfigurationException for underlying BackingStoreExceptions and OutputStream exceptions
   */
  public void addDatasetConfig(DatasetConfig dsc) throws ConfigurationException {
    initCache();

    String iname = dsc.getInternalName();
    String dname = dsc.getDisplayName();
    String dataset = dsc.getDataset();
    String desc = dsc.getDescription();
    byte[] digest = dsc.getMessageDigest();
    String type = dsc.getType();
    String visible = dsc.getVisible();
    String version = dsc.getVersion();

    deleteFile(dataset, iname);
    
    File xmlFile = getFile(dataset, iname);
    
    dscutils.writeDatasetConfigToFile(dsc, xmlFile);
    
    //hidden datasets may have null display names
    if (dname != null)
      xmlPrefs.node(dataset).node(iname).put(DNAMEKEY, dname);
    
    if (desc != null)
      xmlPrefs.node(dataset).node(iname).put(DESCKEY, desc);
    
    if (type != null)
        xmlPrefs.node(dataset).node(iname).put(TYPEKEY, type);
    
    if (visible != null)
        xmlPrefs.node(dataset).node(iname).put(VISIBLEKEY, visible);
    
    if (version != null)
        xmlPrefs.node(dataset).node(iname).put(VERSIONKEY, version);
    
    xmlPrefs.node(dataset).node(iname).put(XMLKEY, xmlFile.getAbsolutePath());
    xmlPrefs.node(dataset).node(iname).putByteArray(DIGESTKEY, digest);
  }

  /**
   * Removes all information for a DatasetConfig object specified by dataset and internalName from the cache, 
   * including its associated file in $HOME/.martj_preferences.
   * @param dataset - dataset for DatasetConfig to be removed
   * @param iname - internalname for the DatasetConfig to be removed
   * @throws ConfigurationException for underlying exceptions
   */
  public void removeDatasetConfig(String dataset, String iname) throws ConfigurationException {
    initCache();

    if (cacheExists(dataset, iname)) {
      try {
        deleteFile(dataset, iname);
        xmlPrefs.node(dataset).node(iname).removeNode(); //removes this node entirely
        if (xmlPrefs.node(dataset).childrenNames().length == 0)
          xmlPrefs.node(dataset).removeNode(); //removes the dataset node, if empty
        xmlPrefs.flush();
      } catch (BackingStoreException e) {
        throw new ConfigurationException(
          "Caught BackingStoreException removing DatasetConfig from preferences node "
            + dataset
            + " internalName "
            + iname
            + " "
            + e.getMessage()
            + "\nAssuming it doesnt exist\n");
      }
    }
  }

  /**
   * Returns a DatasetConfig for the given dataset and internalName.
   * @param dataset -- dataset for required DatasetConfig
   * @param iname -- internalName for required DatasetConfig
   * @param adaptor -- DSConfigAdaptor to set as the underlying DSConfigAdaptor for the returned DatasetConfig object
   *                   Note, in order to satisfy the contract for the DatasetConfig lazyLoad system, if this is passed null,
   *                   the system will fully load the resulting DatasetConfig from the xml file before returning it.
   * @return DatasetConfig for given dataset and internalName
   * @throws ConfigurationException for underlying exceptions
   */
  public DatasetConfig getDatasetConfig(String dataset, String iname, DSConfigAdaptor adaptor)
    throws ConfigurationException {
    initCache();
    DatasetConfig dsv = null;

    try {
      if (xmlPrefs.nodeExists(dataset)) {
        if (xmlPrefs.node(dataset).nodeExists(iname)) {

          byte[] digest = xmlPrefs.node(dataset).node(iname).getByteArray(DIGESTKEY, null);

          if (adaptor == null) {

            dscutils.setFullyLoadMode(true); //temporarily
            dsv = dscutils.getDatasetConfigForXMLStream(getXMLStream(dataset, iname));
            dscutils.setFullyLoadMode(false);            
          } else {
            String displayName = xmlPrefs.node(dataset).node(iname).get(DNAMEKEY, null);
            String description = xmlPrefs.node(dataset).node(iname).get(DESCKEY, null);
            String type = xmlPrefs.node(dataset).node(iname).get(TYPEKEY, null);
            String visible = xmlPrefs.node(dataset).node(iname).get(VISIBLEKEY, null);
            String version = xmlPrefs.node(dataset).node(iname).get(VERSIONKEY, null);
			String visibleFilterPage = xmlPrefs.node(dataset).node(iname).get(VISIBLEFILTERPAGEKEY, null);
			
            dsv = new DatasetConfig(iname, displayName, dataset, description, type, visible,visibleFilterPage,version,"","","","","","","","","","","");
            
            dsv.setDSConfigAdaptor(adaptor);
          }

          if (digest != null)
            dsv.setMessageDigest(digest);
        }
      }
    } catch (BackingStoreException e) {
      throw new ConfigurationException(
        "Caught BackingStoreException getting DatasetConfig from preferences node "
          + dataset
          + " internalName "
          + iname
          + " "
          + e.getMessage());
    } catch (ConfigurationException e) {
      throw e;
    }

    return dsv;
  }

  /**
   * lazyLoads a given DatasetConfig object from its cache, if present.
   * @param dsv -- DatasetConfig to be lazyLoaded.
   * @throws ConfigurationException for all underlying exceptions that prevent the DatasetView from being lazyLoaded.
   */
  public void lazyLoadWithCache(DatasetConfig dsv) throws ConfigurationException {
    initCache();

    String dataset = dsv.getDataset();
    String iname = dsv.getInternalName();

    if (cacheExists(dataset, iname)) {
      try {
        InputStream xmlinput = getXMLStream(dataset, iname);
        dscutils.loadDatasetConfigWithDocument(
          dsv,
          dscutils.getDocumentForXMLStream(xmlinput));
        xmlinput.close();
      } catch (ConfigurationException e) {
        throw e;
      } catch (IOException e) {
        if (logger.isLoggable(Level.FINE))
          logger.fine(
            "Caught IOException closing Stream: " + e.getMessage() + "\nAssuming datasetview was properly lazyLoaded\n");
      }
    } else
      throw new ConfigurationException("Cache does not exist for " + dataset + " " + iname + "\n");
  }

  private String pathTo(String dataset, String iname) {
    return xmlPrefs.node(dataset).node(iname).get(XMLKEY, null);
  }
  
  private InputStream getXMLStream(String dataset, String iname) throws ConfigurationException {
    FileInputStream ret = null;

    String xmlloc = pathTo(dataset, iname);
    if (xmlloc == null) {
      throw new ConfigurationException(
        "Could not retrieve cache information for dataset "
          + dataset
          + " internalName "
          + iname
          + " does not appear to be cached!\n");
    } else {
      File xmlfile = new File(xmlloc);
      try {
        ret = new FileInputStream(xmlfile);
      } catch (FileNotFoundException e) {
        throw new ConfigurationException(
          "Could not retrieve cache information for dataset "
            + dataset
            + " internalName "
            + iname
            + " does not appear to be cached!\n"
            + e.getMessage()
            + "\n",
          e);
      }
    }
    return ret;
  }

  /**
   * Determine if cache exists for a specified DatasetConfig, given its dataset and internalName.
   * @param dataset -- dataset for required DatasetConfig
   * @param iname -- optional internalName for required DatasetConfig. If null, only checks for existence of dataset cache
   * @return boolean, true if cache exists for this dataset, and optional internalName, false otherwise
   * @throws ConfigurationException for underlying exceptions
   */
  public boolean cacheExists(String dataset, String iname) throws ConfigurationException {
    initCache();
    boolean ret = false;

    try {
      ret = xmlPrefs.nodeExists(dataset);

      if (ret && iname != null)
        ret = xmlPrefs.node(dataset).nodeExists(iname);
    } catch (BackingStoreException e) {
      throw new ConfigurationException(
        "Recieved BackingStoreException determining existence of cache for "
          + dataset
          + " "
          + iname
          + "\n"
          + e.getMessage(),
        e);
    }

    return ret;
  }

  /**
   * Determines if the cache for a specified DatasetConfig is up to date for a given source MD5SUM MessageDigest.
   * If this returns a false value, then this method actually removes any cache for the given dataset and internalName before returning.
   * @param sourceDigest -- byte[] MD5SUM MessageDigest
   * @param dataset -- dataset for required DatasetConfig
   * @param iname -- internalName for required DatasetConfig
   * @return boolean, true if cache exists, and digest in cache matches the given sourceDigest, false otherwise
   *         This will also return false if, for some reason, the cached digest cannot be retrieved.
   * @throws ConfigurationException
   */
  public boolean cacheUpToDate(byte[] sourceDigest, String dataset, String iname) throws ConfigurationException {
    boolean ret = cacheExists(dataset, iname);

    if (ret) {
      byte[] cacheDigest = xmlPrefs.node(dataset).node(iname).getByteArray(DIGESTKEY, new byte[0]);
      //if the cache cannot return the digest for some reason, it should return an empty byte[]

      ret = MessageDigest.isEqual(cacheDigest, sourceDigest);
    }

    if (!ret) {
      removeDatasetConfig(dataset, iname);
    }

    return ret;
  }
}
