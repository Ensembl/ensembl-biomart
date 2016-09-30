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

package org.ensembl.mart.shell;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.BooleanFilter;
import org.ensembl.mart.lib.DetailedDataSource;
import org.ensembl.mart.lib.FieldAttribute;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.IDListFilter;
import org.ensembl.mart.lib.InputSourceUtil;
import org.ensembl.mart.lib.InvalidQueryException;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.SequenceDescription;
import org.ensembl.mart.lib.config.AttributeCollection;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.CompositeDSConfigAdaptor;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DSConfigAdaptor;
import org.ensembl.mart.lib.config.DatabaseDSConfigAdaptor;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.DatasetConfigIterator;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterPage;
import org.ensembl.mart.lib.config.MultiDSConfigAdaptor;
import org.ensembl.mart.lib.config.RegistryDSConfigAdaptor;
import org.ensembl.mart.lib.config.URLDSConfigAdaptor;

/**
 * <p>Library allowing client code to parse Mart Query Language (MQL)
 * querries into Query Objects using MQLtoQuery, or parse Query objects into MQL querries using QueryToMQL.  
 * 
 * <!-- This is a css style for indenting lines below.  It is placed here to allow the first line to be used in the package description. -->  
 * <STYLE>
 * <!--
 * 
 * P.indent_small {
 *   text-indent: 0.2in;
 * }
 * 
 * P.indent_big {
 *   text-indent: 0.5in;
 * }
 * -->
 * </STYLE>
 * 
 * <p>Mart Query Language (MQL) is a derivative of the Structured Query Language (SQL)
 * used to query relational databases.  It follows the following format (items in angle brackets are optional):</p>
 * <p class="indent_big">&lt; using datasetConfigName &lt; dataSourceName &gt; &gt; </p>
 * <p class="indent_big">get</p>
 * <p class="indent_big">&lt; attribute_list &gt;</p>
 * <p class="indent_big">&lt; sequence sequence_request &gt;</p>
 * <p class="indent_big">&lt; where filter_list &gt;</p>
 * <p class="indent_big">&lt; limit integer &gt;</p>
 * <br>
 * <p>- attribute_list is a comma-separated list of mart attributes.  These must match the internal_name of attributes in the MartConfiguration for the mart being querried.
 *    attribute_list can be omitted for sequence_requests, otherwise, at least one attribute must be specified. Specifying attributes with
 *    a sequence_request requests that those attributes (if available) are included, either as fields in tabulated output, or in the description portion of the 
 *    header in fasta output.</p>
 * <p>- sequence_request follows the pattern:</p>
 * <p class="indent_big">&lt;left_flank_length_integer+&gt;sequence_name&lt;+right_flank_length_integer&gt;</p>
 * <p class="indent_small">sequence_name must match the name of a sequence available from the system (see the SequenceDescription documentation below).
 * <p class="indent_small">If specified with a left and/or right flank_length_integer (eg., 10+gene_exon, gene_exon+10, or 10+gene_exon+10)<p>
 * <p class="indent_small">then the sequences will return the requested flanking sequence (although some sequences are not flankable).</p>
 * <p>- dataset_name must match the internal_name of a dataset made available by the MartConfiguration made available for the mart being querried.</p>
 * <p>- filter_list is a comma-separated list of filter_requests.  filter_requests must match one of the following formats:</p>
 * <p class="indent_big">- filter_name excluded|only  -- specifies that objects should be returned only if they match (only),</p>
 * <p class="indent_big">or only if they do not match (excluded) this filter.</p>
 * <p class="indent_big">- filter_name =|!=|&gt;|&gt;=|&lt;|&lt;= value</p>
 * <p class="indent_big">- filter_name in url -- specifies that the system should harvest the items in the specified url,</p>
 * <p class="indent_big">and return objects only if they satisfy the condition of filter_name in (items in the url).</p>
 * <p class="indent_big">The url must return a list of items, one per line.</p>
 * <p class="indent_big">- filter_name in (comma-separated list of items) -- note the list must be enclosed in perentheses.</p>
 * <p class="indent_big">- filter_name in (nested query) -- Nested query can be a MQL query, but it cannot include sequence_requests, or into requests.</p>
 * <p class="indent_big">It also must only return one attribute, matching filter_name.</p>
 * <p>filter_name must match the internal_name for a filter specified in the MartConfiguration for the mart being querried.  Also, in some cases, the filter_name
 *    must be further qualified with a filter_set_name prepended with a period (eg, filter_set_name.filter_name = value).  This is only the case when the filter
 *    is part of a filter_set, as specified in the MartConfiguration.</p>   
 * <br>
 * <p>- if a limit request is specified, this adds a limit integer clause to the actual SQL executed against the mart database.  Note that this does not necessarily
 *    limit the number of records returned to the specified integer.  It limits the number of mart focus objects querried, for which attributes are returned.
 *    If there is a one-many, many-many, or many-one relationship between the mart focus object, and the attribute being requested, then the number of records returned
 *    will reflect this.</p>
 * <br>
 * <p>Note, the minimal MQL request would be 'get attribute_name'.  The minimal sequence request would be 'get sequence sequence_name'.</p>
 * <p>MQL differs from SQL in not requiring (or even allowing) multiple datasets, table ALLQUALIFIERS, or joins.  You just have to specify the attributes/sequence that you want, the dataset to query, and filters to apply.</p>
 * <p>This makes simple queries which are not that hard at the SQL level, even more simple.</p>
 * <p>Because the Mart-Explorer engine resolves some filters to complex sub querries, it makes more complex underlying querries just as simple.</p>
 * <p>Finally, it makes querries for things like sequences (which are impossible using SQL) just as simple.</p>  
 * <p>MQL statements can be written on one line, or separated with newlines and whitespace to make them easier to read</p>
 * <br>
 * <p>The default output settings are tab - separated, tabulated output
 * to System.out.  Client programs can over ride these settings using the appropriate setter methods
 *      but, once set, they remain in effect for the entire session, until another call to the
 *      setter(s) is(are) made).</p>
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @see org.ensembl.mart.lib.SequenceDescription
 * @see org.ensembl.mart.lib.config.MartConfiguration
 */
public class MartShellLib {

  /**
   * Create a MartShellLib object with an empty adaptorManager,
   * to be managed with add/remove/update commands.
   */
  public MartShellLib() {
      //dont ignoreCache, dont includeHiddenMembers
      adaptorManager = new RegistryDSConfigAdaptor(false, true);
  }
  
  /**
   * Create a MartShellLib object with a previously populated
   * adaptorManager.
   * @param adaptor RegistryDSConfigAdaptor object
   */
  public MartShellLib(RegistryDSConfigAdaptor adaptor) {
    adaptorManager = adaptor;
  }

  /**
   * Add a MartRegistry to this MartShellLib instance. Optionally, instruct
   * the system to bypass the caching system and fully load all DatasetConfig objects into
   * memory (This option is not used by MartShell, but is available for users who wish
   * to run this on a server with adequate memory).
   * @param confFile - String path to a configuration file, either absolute, or relative to the ClASSPATH
   * @param loadFully - boolean, if false, all DatasetConfig objects are cached to the filesystem. If true
   *                    all DatasetConfig objects are fully loaded into memory, with no lazy loading.
   * @throws ConfigurationException if confFile is not correctly parsed into a proper URL, and for all Configuration
   *         loading exceptions.
   * @throws MalformedURLException if confFile is a malformed URL.
   */
  public synchronized void addMartRegistry(String confFile, boolean loadFully) throws ConfigurationException, MalformedURLException {
    URL confURL = InputSourceUtil.getURLForString(confFile);

    if (confURL == null)
      throw new ConfigurationException("Could not parse " + confFile + " into a URL\n");

    //dont ignoreCache, dont includeHiddenMembers
    RegistryDSConfigAdaptor adaptor = new RegistryDSConfigAdaptor(confURL, false, loadFully, true);
    //see notes to adaptorManager for boolean settings
    harvestAdaptorsFrom(adaptor);
  }

  /**
   * Converts the Query into an MQL string.
   * 
   * @param query Query object to be transformed into MQL
   * @param datasetConfigquery Query object to be transformed into MQL
   * @return String MQL statement
   * throws InvalidQueryException for all underlying exceptions
   */
  public String QueryToMQL(Query query, DatasetConfig datasetConfig) throws InvalidQueryException {

    StringBuffer mqlbuf = new StringBuffer();
    boolean success = getUsingClause(query, datasetConfig, mqlbuf);

    if (success)
      success = getGetClause(query, datasetConfig, mqlbuf.append(" "));

    if (success && (query.getType() == Query.SEQUENCE))
      getSequenceClause(query, mqlbuf.append(" "));

    if (success && (query.getTotalFilterCount() > 0))
      success = getWhereClause(query, datasetConfig, mqlbuf.append(" "));

    if (success && query.hasLimit())
      mqlbuf.append(" ").append("limit ").append(query.getLimit());

    if (!success)
      throw new InvalidQueryException("Could not compile MQL from Query\n" + MQLError + "\n");

    return mqlbuf.toString();
  }

  /**
   * Creates a Mart Query Language command from a Query object.
   * 
   * @param query Query object to be transformed into MQL
   * @return String MQL statement
   * throws InvalidQueryException for all underlying exceptions
   */
  public String QueryToMQL(Query query) throws InvalidQueryException, ConfigurationException {

    //TODO: this is currently broken.  Now need to store the DatasetConfig dataset and internalName in the Query for this to work
    String datasetName = query.getDataset();

    //	get datasetName first
    if (datasetName == null)
      throw new InvalidQueryException("Recieved null DatasetName from query provided\n");

    if (!adaptorManager.supportsDataset(datasetName))
      throw new InvalidQueryException(
        "DatasetConfig " + datasetName + " is not supported by the martConfiguration provided\n");

    DatasetConfig dataset = adaptorManager.getDatasetConfigByDatasetInternalName(datasetName, null); //TODO:broken

    return QueryToMQL(query, dataset);
  }

  private boolean getUsingClause(Query query, DatasetConfig dataset, StringBuffer mqlbuf) {
    boolean success = true;

    mqlbuf.append(USINGQSTART).append(" ").append(dataset.getInternalName());

    return success;
  }

  private boolean getGetClause(Query query, DatasetConfig dataset, StringBuffer mqlbuf) {
    Attribute[] attributes = query.getAttributes();
    mqlbuf.append(GETQSTART);

    if (attributes.length == 0) {
      if (query.getType() == Query.SEQUENCE)
        return true;
      else {
        MQLError = "Empty attributes, no Sequence.";
        return false;
      }
    }

    mqlbuf.append(" ");
    boolean success = true;

    for (int i = 0, n = attributes.length;(success && i < n); i++) {
      if (i > 0)
        mqlbuf.append(", ");

      Attribute attribute = attributes[i];
      String fname = attribute.getField();
      String tconstraint = attribute.getTableConstraint();

      if (dataset.supportsAttributeDescription(fname, tconstraint))
        mqlbuf.append(dataset.getAttributeDescriptionByFieldNameTableConstraint(fname, tconstraint).getInternalName());
      else {
        success = false;
        MQLError = "Could not map attribute " + attribute.getField() + " " + attribute.getTableConstraint();
      }
    }

    return success;
  }

  private void getSequenceClause(Query query, StringBuffer mqlbuf) {
    mqlbuf.append("sequence ");

    SequenceDescription seqd = query.getSequenceDescription();
    String seqtype = seqd.getSeqType();

    int lflank = seqd.getLeftFlank();
    if (lflank > 0)
      mqlbuf.append(lflank).append("+");

    mqlbuf.append(seqtype);

    int rflank = seqd.getRightFlank();
    if (rflank > 0)
      mqlbuf.append("+").append(rflank);
  }

  private boolean getWhereClause(Query query, DatasetConfig datasetconfig, StringBuffer mqlbuf) {
    boolean success = true;

    mqlbuf.append("where ");

    Filter[] filters = query.getFilters();
    for (int i = 0, n = filters.length;(success && i < n); i++) {
      if (i > 0)
        mqlbuf.append(" and ");

      Filter filter = filters[i];

      if (filter instanceof BasicFilter)
        success = mapBasicFilter((BasicFilter) filter, datasetconfig, mqlbuf);
      else if (filter instanceof BooleanFilter)
        success = mapBooleanFilter((BooleanFilter) filter, datasetconfig, mqlbuf);
      else
        success = mapIDListFilter((IDListFilter) filter, datasetconfig, mqlbuf);

      if (!success)
        MQLError = "Could not map filter " + filter.getField() + " " + filter.getTableConstraint();
    }

    return success;
  }

  private boolean mapBooleanFilter(BooleanFilter filter, DatasetConfig datasetconfig, StringBuffer mqlbuf) {
    String field = filter.getField();
    String tableConstraint = filter.getTableConstraint();
    String filterCondition = filter.getQualifier();

    if (!datasetconfig.supportsFilterDescription(field, tableConstraint, filterCondition))
      return false;

    FilterDescription fdesc =
      datasetconfig.getFilterDescriptionByFieldNameTableConstraint(field, tableConstraint, filterCondition);
    String filterName = fdesc.getInternalNameByFieldNameTableConstraint(field, tableConstraint, filterCondition);

    mqlbuf.append(filterName);

    if (filterCondition.equals(BooleanFilter.isNULL) || filterCondition.equals(BooleanFilter.isNotNULL_NUM))
      mqlbuf.append(" excluded");
    else
      mqlbuf.append(" only");

    return true;
  }

  private boolean mapIDListFilter(IDListFilter filter, DatasetConfig datasetconfig, StringBuffer mqlbuf) {
    String field = filter.getField();
    String tableConstraint = filter.getTableConstraint();
    String filterCondition = filter.getQualifier();

    if (!datasetconfig.supportsFilterDescription(field, tableConstraint, filterCondition))
      return false;

    boolean success = true;
    FilterDescription fdesc =
      datasetconfig.getFilterDescriptionByFieldNameTableConstraint(field, tableConstraint, filterCondition);
    String filterName = fdesc.getInternalNameByFieldNameTableConstraint(field, tableConstraint, filterCondition);

    mqlbuf.append(filterName).append(" in ");

    //String handler = filter.getHandler();
	String handler = null;
    if (handler.equals(IDListFilter.FILE)) {
      mqlbuf.append(filter.getFile());
    } else if (handler.equals(IDListFilter.URL)) {
      mqlbuf.append(filter.getUrl());
    } else if (handler.equals(IDListFilter.SUBQUERY)) {
      Query subq = filter.getSubQuery();
      mqlbuf.append(subq.getQueryName());

      try {
        mqlbuf.insert(0, QueryToMQL(subq) + " as " + subq.getQueryName() + ";");
      } catch (Exception e) {
        success = false;
        MQLError = ("Could not map subquery:\n" + subq + "\n" + e);
      }
    } else {
      String[] ids = filter.getIdentifiers();
      mqlbuf.append("(");

      for (int i = 0, n = ids.length; i < n; i++) {
        if (i > 0)
          mqlbuf.append(", ");
        mqlbuf.append(ids[i]);
      }

      mqlbuf.append(")");
    }

    return success;
  }

  private boolean mapBasicFilter(BasicFilter filter, DatasetConfig datasetconfig, StringBuffer mqlbuf) {
    String field = filter.getField();
    String tableConstraint = filter.getTableConstraint();
    String filterCondition = filter.getQualifier();
    if (!datasetconfig.supportsFilterDescription(field, tableConstraint, filterCondition))
      return false;

    FilterDescription fdesc =
      datasetconfig.getFilterDescriptionByFieldNameTableConstraint(field, tableConstraint, filterCondition);
    mqlbuf
      .append(fdesc.getInternalNameByFieldNameTableConstraint(field, tableConstraint, filterCondition))
      .append(" ")
      .append(filter.getQualifier())
      .append(" ")
      .append(filter.getValue());

    return true;
  }

  public void setMaxCharCount(int max) {
    maxcharcount = max;
  }

  /**
   *  Allows users to store MQL to use as subqueries in other MQL.
   * 
   * @param key - String name to refer to this stored Command in later queries.
   * @param mql - String mql subquery.
   */
  public synchronized void addStoredMQLCommand(String key, String mql) {
    storedCommands.put(key, mql);
  }

  /**
   * Remove a stored MQL statement with its key.
   * @param key -- key for stored MQL command
   */
  public synchronized void removeStoredMQLCommand(String key) {
    if (storedCommands.containsKey(key)) {
      storedCommands.remove(key);
    }
  }

  /**
   * Returns the actual MQL statement stored for a particular key, or null if not stored
   * @param key -- key for a stored MQL command
   * @return String MQL command
   */
  public synchronized String describeStoredMQLCommand(String key) {
    return storedCommands.getProperty(key);
  }

  /**
   * Returns a query object for a stored procedure.  This only works for
   * queries without bind variables.
   * @param key -- key for a MQL command
   * @return Query parsed from stored MQL command
   * @throws InvalidQueryException for any query parsing Exceptions
   */
  public synchronized Query StoredMQLCommandToQuery(String key) throws InvalidQueryException {
    return MQLtoQuery(describeStoredMQLCommand(key));
  }

  /**
   * Get a Set containing all stored MQL Procedure keys.
   * @return Set
   */
  public synchronized Set getStoredMQLCommandKeys() {
    return storedCommands.keySet();
  }

  public String[] listDatasets(String[] toks) throws ConfigurationException {
    if (adaptorManager.getDatasetNames(false).length == 0)
      return new String[] { "No Datasets Loaded\n" };

    List retList = new ArrayList();

    if (toks.length == 3) {
      //list datasets all|sourceName
      String reqName = toks[2];

      if (reqName.equalsIgnoreCase(LISTALLREQ)) {
        //list datasets all

        String[] sources = adaptorManager.getAdaptorNames();
        for (int i = 0, n = sources.length; i < n; i++) {
          String source = sources[i];

          DSConfigAdaptor adaptor = adaptorManager.getAdaptorByName(source);

          String[] datasets = adaptor.getDatasetNames(false);
          for (int j = 0, m = datasets.length; j < m; j++) {
            retList.add( /*canonicalizeMartName( source )*/ source + "." + datasets[j] + "\n"); // CHANGED
          }
        }
      } else {
        //list datasets sourceName
        if (!adaptorManager.supportsAdaptor( reqName /*deCanonicalizeMartName( reqName )*/ )) // CHANGED
          throw new ConfigurationException(reqName + " is not a valid Mart Source to list Datasets\n");

        String[] datasets = adaptorManager.getAdaptorByName( reqName /*deCanonicalizeMartName( reqName )*/ ).getDatasetNames(false); //CHANGED
        for (int i = 0, n = datasets.length; i < n; i++) {
          retList.add(reqName + "." + datasets[i] + "\n");
        }
      }
    } else if (toks.length == 2) {
      //list datasets (relative to envMart)
      if (envMart == null)
        throw new ConfigurationException("Must set environmental Mart to list Datasets relative to it\n");

      String reqName = envMart.getName();
      String[] datasets = adaptorManager.getAdaptorByName(reqName).getDatasetNames(false);
      for (int i = 0, n = datasets.length; i < n; i++) {
        retList.add(datasets[i] + "\n");
      }
    } else
      throw new ConfigurationException("Invalid List Datasets Request\n");

    String[] ret = new String[retList.size()];
    retList.toArray(ret);
    Arrays.sort(ret);
    return ret;
  }

  public String[] listDatasetConfigs(String[] toks) throws ConfigurationException {
    if (adaptorManager.getNumDatasetConfigs(true) == 0)
      return new String[] { "No DatasetConfigs Loaded\n" };

    List retList = new ArrayList();

    if (toks.length == 3) {
      //list datasetconfigs all|sourcename
      String reqName = toks[2];

      if (reqName.equalsIgnoreCase(LISTALLREQ)) {
        //list datasetconfigs all

        String[] sources = adaptorManager.getAdaptorNames();
        for (int i = 0, n = sources.length; i < n; i++) {
          String source = sources[i];
          DSConfigAdaptor adaptor = adaptorManager.getAdaptorByName(source);

          String[] datasets = adaptor.getDatasetNames(false);
          for (int j = 0, m = datasets.length; j < m; j++) {
            String dataset = datasets[j];
            String[] configs = adaptor.getDatasetConfigInternalNamesByDataset(dataset);

            for (int k = 0, l = configs.length; k < l; k++) {
              String config = configs[k];
              retList.add( source /*canonicalizeMartName( source )*/ + "." + dataset + "." + config + "\n"); // CHANGED
            }
          }
        }
      } else {
        //list datasetconfigs sourcename
        if (!adaptorManager.supportsAdaptor( reqName /*deCanonicalizeMartName( reqName )*/ ))  // CHANGED
          throw new ConfigurationException("Source " + reqName + " is not a valid Mart Source\n");

        DSConfigAdaptor adaptor = adaptorManager.getAdaptorByName(reqName /*deCanonicalizeMartName( reqName )*/ );  // CHANGED

        String[] datasets = adaptor.getDatasetNames(false);
        for (int j = 0, m = datasets.length; j < m; j++) {
          String[] configs = adaptor.getDatasetConfigInternalNamesByDataset(datasets[j]);

          for (int k = 0, l = configs.length; k < l; k++) {
            retList.add(reqName + "." + datasets[j] + "." + configs[k] + "\n");
          }
        }

      }
    } else if (toks.length == 2) {
      //list datasetconfigs (relative to envMart and envDataset
      if (envMart == null)
        throw new ConfigurationException("Must set environmental Mart to list DatasetConfigs to it\n");
      if (envDataset == null)
        throw new ConfigurationException("Must set environmental Dataset to list DatasetConfigs to it\n");

      String[] configs =
        adaptorManager.getAdaptorByName(envMart.getName()).getDatasetConfigInternalNamesByDataset(
          envDataset.getInternalName());
      for (int i = 0, n = configs.length; i < n; i++) {
        retList.add(configs[i] + "\n");
      }
    } else
      throw new ConfigurationException("Invalid list datasetconfigs command recieved\n");

    String[] ret = new String[retList.size()];
    retList.toArray(ret);
    Arrays.sort(ret);
    return ret;
  }

  public String[] listProcedures() {
    if (getStoredMQLCommandKeys().size() == 0)
      return new String[] { "No Procedures Stored\n" };

    Set names = getStoredMQLCommandKeys();
    String[] ret = new String[names.size()];

    int i = 0;
    for (Iterator iter = names.iterator(); iter.hasNext();) {
      String name = (String) iter.next();
      ret[i] = name + "\n";
      i++;
    }
    Arrays.sort(ret);
    return ret;
  }

  public String[] listMarts() throws ConfigurationException {
    if (adaptorManager.getAdaptorNames().length == 0)
      throw new ConfigurationException("No Marts have been loaded\n");

    String[] names = adaptorManager.getAdaptorNames();
    ArrayList martNames = new ArrayList();
    
    for (int i = 0, n = names.length; i < n; i++) {
      if (adaptorManager.getAdaptorByName( names[i] ).getNumDatasetConfigs(true) > 0)
        martNames.add( names[i] /*canonicalizeMartName( names[i] )*/ + "\n" ); // CHANGED
    }

    String[] ret = new String[martNames.size()];
    martNames.toArray(ret);    
    Arrays.sort(ret);
    
    return ret;
  }

  public String[] listFilters() throws InvalidQueryException, ConfigurationException {
    if (envDataset == null)
      throw new InvalidQueryException("Must set the environmental Dataset to list filters\n");

    int blen = 3; //3 filters/line

    List columns = new ArrayList();
    String[] buffer = new String[blen];

    int[] maxlengths = new int[] { 0, 0, 0 };

    List names = envDataset.getFilterCompleterNames();
    Collections.sort(names);

    int pos = 0;
    for (Iterator iter = names.iterator(); iter.hasNext();) {
      String name = (String) iter.next();

      if (pos == blen) {
        columns.add(buffer);
        buffer = new String[blen];
        pos = 0;
      }
      buffer[pos] = name;
      if (name.length() > maxlengths[pos])
        maxlengths[pos] = name.length();
      pos++;
    }

    if (pos > 0)
      columns.add(buffer);

    return formatColumns(columns, maxlengths);
  }

  public String[] listAttributes() throws ConfigurationException, InvalidQueryException {
    if (envDataset == null)
      throw new InvalidQueryException("Must set the environmental Dataset to list attributes\n");

    int blen = 3; //3 atts/line
    List columns = new ArrayList();
    String[] buffer = new String[blen];

    int[] maxlengths = new int[] { 0, 0, 0 };

    List names = envDataset.getAttributeCompleterNames();
    Collections.sort(names);

    int pos = 0;
    for (Iterator iter = names.iterator(); iter.hasNext();) {
      String name = (String) iter.next();

      if (pos == blen) {
        columns.add(buffer);
        buffer = new String[blen];
        pos = 0;
      }
      buffer[pos] = name;

      if (name.length() > maxlengths[pos])
        maxlengths[pos] = name.length();
      pos++;
    }

    if (pos > 0)
      columns.add(buffer);

    return formatColumns(columns, maxlengths);
  }

  private String[] formatColumns(List columns, int[] maxlengths) {
    int[] pos = new int[] { 0, 0, 0 }; // position matrix, change pos[0] to increase leftmost padding

    int maxtotal = 0;
    for (int i = 0, n = maxlengths.length; i < n; i++) {
      maxtotal += maxlengths[i];
    }

    int minSpace = 5; //default
    if (maxtotal < maxcharcount)
      minSpace = (maxcharcount - maxtotal) / (maxlengths.length - 1);

    //calculate positions 2 onward
    for (int i = 1, n = maxlengths.length; i < n; i++) {
      pos[i] = pos[i - 1] + maxlengths[i - 1] + minSpace;
    }

    List lines = new ArrayList();

    for (Iterator iter = columns.iterator(); iter.hasNext();) {
      String[] lc = (String[]) iter.next();
      StringBuffer thisLine = new StringBuffer();
      int len = thisLine.length();

      for (int i = 0, n = lc.length; i < n; i++) {
        if (lc[i] != null) {
          while (len < pos[i]) {
            thisLine.append(" ");
            len++;
          }
          thisLine.append(lc[i]);
          len = thisLine.length();
        }
      }
      thisLine.append("\n");
      lines.add(thisLine.toString());
    }

    String[] ret = new String[lines.size()];
    lines.toArray(ret);
    return ret;
  }

  public String DescribeMart(String name) throws InvalidQueryException {
    DetailedDataSource reqMart = null;

    try {
      if (name == null) {
        if (envMart == null)
          throw new InvalidQueryException("Invalid describe Mart command recieved, must set environmental Mart with 'set' or 'use', or explicitly supply martName to describe\n");

        reqMart = envMart;
      } else {
        if (!adaptorManager.supportsAdaptor( name /*deCanonicalizeMartName( name )*/ ) )  // CHANGED
          throw new InvalidQueryException(MARTREQ + " " + name + " has not been stored\n");

        reqMart = adaptorManager.getAdaptorByName(name /* deCanonicalizeMartName( name ) */).getDataSource();  // CHANGED

        //this could (but probably wont) be a file sourcename
        if (reqMart == null)
          throw new InvalidQueryException("Source " + name + "is a file Source\n");
      }
    } catch (InvalidQueryException e) {
      throw e;
    } catch (ConfigurationException e) {
      throw new InvalidQueryException("Caught ConfigurationException describing Mart " + name + "\n");
    }

    String ret =
      "Mart: "
        + name
        + " HOST: "
        + reqMart.getHost()
        + " USER: "
        + reqMart.getUser()
        + " MART NAME: "
        + reqMart.getDatabaseName();

    return ret;
  }

  public String[] DescribeDataset(String dsetname) throws ConfigurationException, InvalidQueryException {
    DatasetConfig dset = null;

    if (dsetname == null) {
      if (envDataset == null)
        throw new InvalidQueryException("Invalid describe dataset command, please set the environmental Dataset and Mart with either 'use' or 'set'\n");
      dset = envDataset;
    } else {
      //TODO: getLocalConfigFor(dsetname); dset = localDataset (reference, so no further datasets pulled into memory)
      dset = getDatasetConfigFor(dsetname);
    }

    List lines = new ArrayList();

    //filters first
    FilterPage[] fpages = dset.getFilterPages();
    for (int i = 0, n = fpages.length; i < n; i++) {
      FilterPage page = fpages[i];
      lines.add("The following filters can be applied in the same query\n");
      lines.add("\n");

      List names = page.getCompleterNames();
      for (int j = 0, m = names.size(); j < m; j++) {
        String name = (String) names.get(j);

        lines.add(DescribeFilter(page.getFilterDescriptionByInternalName(name)));
        lines.add("\n");
      }
    }

    //attributes
    AttributePage[] apages = dset.getAttributePages();
    for (int i = 0, n = apages.length; i < n; i++) {
      AttributePage page = apages[i];
      lines.add("\n");
      lines.add("\n");
      lines.add("The following Attributes can be querried together\n");
      lines.add(
        "numbers in perentheses denote groups of attributes that have limits on the number that can be queried together\n");
      lines.add("\n");

      List groups = page.getAttributeGroups();
      for (Iterator iter = groups.iterator(); iter.hasNext();) {
        Object obj = iter.next();

        if (obj instanceof AttributeGroup) {
          AttributeGroup group = (AttributeGroup) obj;
          AttributeCollection[] cols = group.getAttributeCollections();

          for (int j = 0, m = cols.length; j < m; j++) {
            AttributeCollection collection = cols[j];

            List atts = collection.getAttributeDescriptions();
            int maxSelect = collection.getMaxSelect();
            for (Iterator iterator = atts.iterator(); iterator.hasNext();) {
              Object element = iterator.next();
              String tmp = DescribeAttribute(element);

              if (maxSelect > 0)
                lines.add(tmp + " (" + maxSelect + ")");
              else
                lines.add(tmp);
              lines.add("\n");
            }
          }
        }
      }
    }

    String[] ret = new String[lines.size()];
    lines.toArray(ret);
    dset = null; //for gc
    return ret;
  }

  public String DescribeFilter(String name) throws InvalidQueryException {
    if (envMart == null)
      throw new InvalidQueryException("Must set environmental Mart with a 'use' or 'set' command for describe filter to work\n");

    if (envDataset == null)
      throw new InvalidQueryException("Must set environmental Dataset with a 'use' or 'set' command for describe filter to work\n");

    if (!(envDataset.containsFilterDescription(name)))
      throw new InvalidQueryException(
        "Filter " + name + " is not supported by Environmental Dataset " + envDataset.getDataset() + "\n");

    return DescribeFilter(envDataset.getFilterDescriptionByInternalName(name));
  }

  private String DescribeFilter(FilterDescription desc) throws InvalidQueryException {
    String name = desc.getInternalName();

    List quals = desc.getCompleterQualifiers(name);
    StringBuffer qual = new StringBuffer();
    for (int k = 0, l = quals.size(); k < l; k++) {
      if (k > 0)
        qual.append(", ");
      String element = (String) quals.get(k);
      qual.append(element);
    }

    String qualifiers = qual.toString();
    String displayName = desc.getDisplayname(name);

    return name + " - " + displayName + " (" + qualifiers + ")";
  }

  public String DescribeAttribute(String name) throws InvalidQueryException {
    if (envDataset == null)
      throw new InvalidQueryException("Must set environmental Dataset with a 'use' or 'set' command for describe attribute to work\n");

    if (envMart == null)
      throw new InvalidQueryException("Must set environmental Mart with a 'use' or 'set' command for describe attribute to work\n");

    if (!envDataset.containsAttributeDescription(name))
      throw new InvalidQueryException(
        "Attribute " + name + " is not supported by environmental Dataset " + envDataset.getInternalName() + "\n");

    String tmp = DescribeAttribute(envDataset.getAttributeDescriptionByInternalName(name));
    return tmp;
  }

  private String DescribeAttribute(Object attributeo) {
    if (attributeo instanceof AttributeDescription) {
      AttributeDescription desc = (AttributeDescription) attributeo;

      String iname = desc.getInternalName();
      String displayName = desc.getDisplayName();
      return iname + " - " + displayName;
    } else
      //dsattributedescription, if ever implimented
      return null;
  }

  /**
   * Add a Mart to MartShellLib.  Interface must collect all necessary connection paramaters.
   * @param martDatabaseType
   * @param martHost
   * @param martPort
   * @param martDatabase
   * @param martUser
   * @param martPass
   * @param martDriver
   * @param sourceKey - name to reference Mart in queries
   * @return boolean, true if the Database settings resulted in a successful connection, false otherwise
   * @throws InvalidQueryException
   * @see org.ensembl.mart.lib.DetailedDataSource for further information about parameter meanings
   */
  public void addMart(
    String martDatabaseType,
    String martHost,
    String martPort,
    String martDatabase,
	String martSchema,
    String martUser,
    String martPass,
    String martDriver,
    String sourceKey)
    throws InvalidQueryException {

    DetailedDataSource ds =
      new DetailedDataSource(
        martDatabaseType,
        martHost,
        martPort,
        martDatabase,
		martSchema,
        martUser,
        martPass,
        DetailedDataSource.DEFAULTPOOLSIZE,
        martDriver,
        sourceKey);

    Connection conn = null;
    try {
      conn = ds.getConnection();
      addMart(ds);
    } catch (SQLException e) {
      throw new InvalidQueryException("Could not connect to Database with given settings, please try again\n", e);
    } finally {
      DetailedDataSource.close(conn);
    }
  }

  public void addMart(DetailedDataSource ds) throws InvalidQueryException {
    try {
      DatabaseDSConfigAdaptor adaptor = new DatabaseDSConfigAdaptor(ds, ds.getUser(), ds.getMartUser(), false, false, true, true);
      //see notes for adaptorManager for boolean settings
      adaptor.setName(ds.getName());
      adaptorManager.add(adaptor);
    } catch (ConfigurationException e) {
      throw new InvalidQueryException("Problem creating Mart " + e.getMessage(), e);
    }

    //for convenience, set envMart to the latest added Mart
    envMart = ds;
  }

  public void addDatasets(StringTokenizer toks) throws InvalidQueryException {
    if (toks.countTokens() == 2) {
      toks.nextToken(); // ignore from

      String source = toks.nextToken();

      try {
        URL regURL = InputSourceUtil.getURLForString(source);
        RegistryDSConfigAdaptor regadaptor = new RegistryDSConfigAdaptor(regURL, false, false, true);
        // see notes for adaptorManager for boolean settings

        harvestAdaptorsFrom(regadaptor);
      } catch (MalformedURLException e) {
        throw new InvalidQueryException(
          "Recieved MalformedURLException parsing " + source + " into a URL " + e.getMessage() + "\n",
          e);
      } catch (ConfigurationException e) {
        throw new InvalidQueryException(
          "Recieved ConfigurationException loading DatasetConfigs from " + source + " " + e.getMessage() + "\n",
          e);
      }
    } else
      throw new InvalidQueryException("Recieved invalid add DatasetConfigs command.\n");
  }

  //recursively harvest DB and URL adaptors from composite/registry adaptors
  public void harvestAdaptorsFrom(DSConfigAdaptor adaptor) throws ConfigurationException {
    if (adaptor instanceof CompositeDSConfigAdaptor) {
      DSConfigAdaptor[] adaptors = adaptor.getLeafAdaptors();

      //returns all leaf adaptors (DatabaseDSConfigAdaptor, URLDSConfigAdaptor) from a CompositeDSViewAdaptor
      for (int i = 0, n = adaptors.length; i < n; i++)
        addLeafAdaptor(adaptors[i]);
    } else
      addLeafAdaptor(adaptor);
  }

  private void addLeafAdaptor(DSConfigAdaptor adaptor) throws ConfigurationException {
    if (adaptor instanceof DatabaseDSConfigAdaptor) {
      //test the connection
      try {
        ((DSConfigAdaptor) adaptor).getDataSource().getConnection();
        adaptorManager.add(adaptor);
      } catch (SQLException e) {
        //ignore this one, it cant connect to its underlying database
      }
    } else
      adaptorManager.add(adaptor);
  }

  public void addDatasetConfig(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      String source = toks.nextToken();
      String userName = null;

      if (toks.hasMoreTokens()) {
        if (toks.nextToken().equals("as"))
          userName = toks.nextToken();
      }

      try {
        URL dsvURL = InputSourceUtil.getURLForString(source);
        URLDSConfigAdaptor adaptor = new URLDSConfigAdaptor(dsvURL, false, true);
        // see notes for adaptorManager for boolean settings
        //dont ignoreCache, dont includeHiddenMembers

        if (userName == null) {
          CompositeDSConfigAdaptor fileAdaptor = null;
          if (adaptorManager.supportsAdaptor(DEFAULTURLADAPTORNAME)) {
            fileAdaptor = (CompositeDSConfigAdaptor) adaptorManager.getAdaptorByName(DEFAULTURLADAPTORNAME);
            adaptorManager.remove(fileAdaptor);
          } else {
            fileAdaptor = new CompositeDSConfigAdaptor();
            fileAdaptor.setName(DEFAULTURLADAPTORNAME);
          }

          fileAdaptor.add(adaptor);
          adaptorManager.add(fileAdaptor);
        } else {
          adaptor.setName(userName);
          adaptorManager.add(adaptor);
        }
      } catch (MalformedURLException e) {
        throw new InvalidQueryException(
          "Recieved MalformedURLException parsing " + source + " into a URL " + e.getMessage() + "\n",
          e);
      } catch (ConfigurationException e) {
        throw new InvalidQueryException(
          "Recieved ConfigurationException loading DatasetConfig from " + source + " " + e.getMessage() + "\n",
          e);
      }

    } else
      throw new InvalidQueryException("Recieved invalid add DatasetConfig command.\n");
  }

  public void removeProcedure(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      String name = toks.nextToken();
      removeStoredMQLCommand(name);
    } else
      throw new InvalidQueryException("Recieved invalid remove Procedure command.\n");
  }

  public void removeMart(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      String name = toks.nextToken();

      try {
        //If a DSConfigAdaptor has been created with this Mart, remove it
        if (adaptorManager.supportsAdaptor(name))
          adaptorManager.remove(adaptorManager.getAdaptorByName(name));
        else
          throw new InvalidQueryException("Unknown Mart " + name + "\n");
      } catch (ConfigurationException e) {
        throw new InvalidQueryException(
          "Caught ConfigurationException removing adaptor for Mart " + name + "\n" + e.getMessage(),
          e);
      } catch (InvalidQueryException e) {
        throw e;
      }
    } else
      throw new InvalidQueryException("Recieved invalid remove Mart command.\n");
  }

  public void removeDataset(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      String name = toks.nextToken();

      //special case where x.y must be of sourcename.datasetname
      String[] nametoks = name.split("\\.");

      try {
        if (nametoks.length == 2) {
          if (!(adaptorManager.supportsAdaptor(nametoks[0]) && adaptorManager.supportsDataset(nametoks[1])))
            throw new InvalidQueryException(
              "Cannot remove dataset with name: "
                + name
                + " must remove it with datasetname relative to the environmental mart, or sourcename.datasetname explicitly\n");

          DSConfigAdaptor adaptor = adaptorManager.getAdaptorByName(nametoks[0]);
          if (adaptor == null)
            throw new InvalidQueryException("Nothing loaded for Mart " + nametoks[0] + "\n");

          if (adaptor.supportsDataset(nametoks[1])) {
            if (adaptor instanceof MultiDSConfigAdaptor) {
              MultiDSConfigAdaptor dbadaptor = (MultiDSConfigAdaptor) adaptor;

              DatasetConfigIterator dsvs = dbadaptor.getDatasetConfigsByDataset(nametoks[1]);
              while (dsvs.hasNext())
                dbadaptor.removeDatasetConfig((DatasetConfig) dsvs.next());

              if (dbadaptor.getNumDatasetConfigs(true) < 1)
                adaptorManager.remove(dbadaptor);
            } else
              adaptorManager.remove(adaptor);
          } else
            throw new InvalidQueryException("Mart " + nametoks[0] + " does not support dataset " + nametoks[1] + "\n");

        } else if (nametoks.length == 1) {
          if (envMart == null)
            throw new InvalidQueryException("Must set environmental Mart to remove datasets relative to it.");

          DSConfigAdaptor adaptor = adaptorManager.getAdaptorByName(envMart.getName());

          if (adaptor.supportsDataset(nametoks[0])) {
            if (adaptor instanceof MultiDSConfigAdaptor) {
              MultiDSConfigAdaptor dbadaptor = (MultiDSConfigAdaptor) adaptor;

              DatasetConfigIterator dsvs = dbadaptor.getDatasetConfigsByDataset(nametoks[1]);
              while (dsvs.hasNext()) {
                dbadaptor.removeDatasetConfig((DatasetConfig) dsvs.next());
              }

              if (dbadaptor.getNumDatasetConfigs(true) < 1)
                adaptorManager.remove(dbadaptor);
            } else
              adaptorManager.remove(adaptor);
          } else
            throw new InvalidQueryException(
              "Dataset " + nametoks[0] + " is not supported by environmental Mart " + envMart.getName() + "\n");
        } else
          throw new InvalidQueryException(
            "Cannot remove dataset with name "
              + name
              + " must remove it with datasetname relative to the environmental mart, or sourcename.datasetname explicitly\n");
      } catch (ConfigurationException e) {
        throw new InvalidQueryException(
          "Caught ConfigurationException removing dataset " + name + " " + e.getMessage(),
          e);
      } catch (InvalidQueryException e) {
        throw e;
      }
    } else
      throw new InvalidQueryException("Invalid remove dataset command.\n");
  }

  public void removeDatasets(StringTokenizer toks) throws InvalidQueryException {
    if (toks.countTokens() == 2) {
      toks.nextToken(); // skip from
      String source = toks.nextToken();

      try {
        if (adaptorManager.supportsAdaptor(source))
          adaptorManager.remove(adaptorManager.getAdaptorByName(source));
        else
          throw new InvalidQueryException("Source " + source + " has not been loaded.\n");
      } catch (ConfigurationException e) {
        throw new InvalidQueryException(
          "Caught ConfigurationException removing DatasetConfigs from source " + source + "\n" + e.getMessage(),
          e);
      }
    } else if (toks.countTokens() == 1) {
      if (envMart == null)
        throw new InvalidQueryException("Environmental Mart not set, no datasets to remove\n");

      try {
        if (adaptorManager.supportsAdaptor(envMart.getName()))
          adaptorManager.remove(adaptorManager.getAdaptorByName(envMart.getName()));
        else
          throw new InvalidQueryException("envMart: " + envMart.getName() + " does not have any datasets loaded\n");
      } catch (ConfigurationException e) {
        throw new InvalidQueryException(
          "Caught ConfigurationException removing envMart " + envMart.getName() + " Datasets\n" + e.getMessage(),
          e);
      } catch (InvalidQueryException e) {
        throw e;
      }
    } else
      throw new InvalidQueryException("Recieved invalid remove DatasetConfigs comand.\n");
  }

  public void removeDatasetConfig(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      String name = toks.nextToken();

      try {

        //special case where it does not allow remove dataset sourcename.datasetname (eg. must explicitly use sourcename.datasetname.default to remove default)
        String[] nametoks = name.split("\\.");
        if (nametoks.length == 2
          && adaptorManager.supportsAdaptor( nametoks[0] /*deCanonicalizeMartName( nametoks[0] )*/)  // CHANGED
          && adaptorManager.supportsDataset(nametoks[1]))
          throw new InvalidQueryException(
            "Cannot remove default datasetconfig for dataset with relative name: "
              + name
              + " must remove it with sourcename.datasetname.configname explicitly\n");

        adaptorManager.removeDatasetConfig(getDatasetConfigFor(name));
      } catch (ConfigurationException e) {
        throw new InvalidQueryException(
          "Caught ConfigurationException removing DatasetConfig " + name + " " + e.getMessage(),
          e);
      } catch (InvalidQueryException e) {
        throw e;
      }
    } else
      throw new InvalidQueryException("Recieved invalid remove DatasetConfig command.\n");
  }

  public void updateDatasets(StringTokenizer toks) throws InvalidQueryException {
    try {
      if (toks.hasMoreTokens()) {
        toks.nextToken(); // skip from
        String source = toks.nextToken();

        if (adaptorManager.supportsAdaptor( source /*deCanonicalizeMartName( source )*/ )) // CHANGED
          adaptorManager.getAdaptorByName( source /*deCanonicalizeMartName( source )*/ ).update();  // CHANGED
        else
          throw new InvalidQueryException("Invalid Mart Source " + source + " passed in update datasets request\n");
      } else {
        if (envMart == null)
          throw new InvalidQueryException("Environment Mart not set, cannot update datasets\n");
        adaptorManager.getAdaptorByName(envMart.getName()).update();
      }
    } catch (ConfigurationException e) {
      throw new InvalidQueryException("Could not update DatasetConfigs, " + e.getMessage() + "\n", e);
    }
  }

  public void updateDataset(StringTokenizer toks) throws InvalidQueryException {
    if (toks.hasMoreTokens()) {
      String name = toks.nextToken();

      try {
        String[] nametoks = name.split("\\.");
        if (nametoks.length == 2) {
          //sourcename.datasetname
          if (!adaptorManager.supportsAdaptor(nametoks[0] /*deCanonicalizeMartName( nametoks[0] )*/ ))  // CHANGED
            throw new InvalidQueryException("Source " + nametoks[0] + " is not a valid mart source\n");

          DSConfigAdaptor adaptor = adaptorManager.getAdaptorByName( nametoks[0] /*deCanonicalizeMartName( nametoks[0] )*/ );  // CHANGED

          if (!adaptor.supportsDataset(nametoks[1]))
            throw new InvalidQueryException(
              "Dataset " + nametoks[1] + " is not supported by Mart Source " + nametoks[0] + "\n");

          adaptor.update();
        } else if (nametoks.length == 1) {
          //datasetname relative to environmental Mart
          if (envMart == null)
            throw new InvalidQueryException(
              "Must set environmental Mart to update datasetconfigs with relative name " + nametoks[0] + "\n");

          if (!adaptorManager.getAdaptorByName(envMart.getName()).supportsDataset(nametoks[0]))
            throw new InvalidQueryException(
              "Mart " + envMart.getName() + " does not support dataset " + nametoks[0] + "\n");

          adaptorManager.getAdaptorByName(envMart.getName()).update();
        } else
          throw new InvalidQueryException(
            "Recieved invalid update DatasetConfig command update datasetconfig " + name + "\n");
      } catch (ConfigurationException e) {
        throw new InvalidQueryException("Could not update DatasetConfig " + name + " " + e.getMessage(), e);
      }
    } else
      throw new InvalidQueryException("Recieved invalid update DatasetConfig command.\n");
  }

  public DatasetConfig getDatasetConfigFor(String name) throws InvalidQueryException {
    DatasetConfig ret = null;
    DatasetRequest dsetreq = new DatasetRequest(name, this);

    try {
      ret =
        adaptorManager.getAdaptorByName(dsetreq.mart).getDatasetConfigByDatasetInternalName(
          dsetreq.dataset,
          dsetreq.datasetconfig);
    } catch (ConfigurationException e) {
      throw new InvalidQueryException("Could not parse " + name + " for DatasetConfig : " + e.getMessage() + "\n");
    }

    if (ret == null)
      throw new InvalidQueryException("Could not manipulate DatasetConfig " + name + "\n");

    return ret;
  }

  /**
   * Allows Completer to set the localDataset and usingLocalDataset values with a string to be parsed.
   * Does not set localDatasetConfig if it is not changed.
   * @param name -- name to be parsed into a DatasetConfig, either with absolute path, or relative to envMart or envDataset.
   */
  protected void setLocalDatasetFor(String name) throws InvalidQueryException {
    DatasetConfig dset = getDatasetConfigFor(name);
    if (localDataset == null || !localDataset.equals(dset))
      localDataset = dset;
  }

  public void setEnvMart(String name) throws InvalidQueryException {
    if (name == null) {
      //unset command
      envMart = null;
    } else {
      try {
        if (!adaptorManager.supportsAdaptor( name /*deCanonicalizeMartName( name )*/)) //CHANGED
          throw new InvalidQueryException("Invalid Mart name " + name + " recieved in set Mart command\n");

        DetailedDataSource ds = adaptorManager.getAdaptorByName( name /*deCanonicalizeMartName( name )*/ ).getDataSource(); //CHANGED

        if (ds == null)
          throw new InvalidQueryException("Source " + name + " is a File Source\n");
        envMart = ds;
      } catch (ConfigurationException e) {
        throw new InvalidQueryException("Caught ConfigurationException setting Mart to " + name + "\n");
      } catch (InvalidQueryException e) {
        throw e;
      }
    }
  }

  public void setEnvDataset(String command) throws InvalidQueryException {
    if (command == null) {
      //unset command
      envDataset = null;
    } else {
      DatasetRequest dsrq = new DatasetRequest(command, this);

      try {
        envDataset = adaptorManager.getAdaptorByName(dsrq.mart).getDatasetConfigByDatasetInternalName(dsrq.dataset, dsrq.datasetconfig);

        if (envMart == null || !(envMart.getName().equals(dsrq.mart)))
          envMart = adaptorManager.getAdaptorByName(dsrq.mart).getDataSource();
      } catch (ConfigurationException e) {
        throw new InvalidQueryException("Could not parse set Dataset command " + command + ": " + e.getMessage() + "\n");
      }
    }
  }

  public String showEnvMart() {
    if (envMart == null)
      return " Mart not set\n";
    else
      return " Mart HOST: "
        + envMart.getHost()
        + " USER: "
        + envMart.getUser()
        + " MART NAME: "
        + envMart.getDatabaseName()
        + "\n";
  }

  public String showEnvDataset() {
    if (envDataset == null)
      return " Environmental DataSet not set\n";
    else {
      //determine if it is a URLDSConfigAdaptor DatasetConfig
      DSConfigAdaptor adaptor = envDataset.getAdaptor();
      if (adaptor != null && adaptor instanceof URLDSConfigAdaptor)
        return adaptor.getName() + "." + envDataset.getDataset() + "\n";
      else
        return " Dataset " + envDataset.getDataset() + "\n";
    }
  }

  public String showEnvDataSetConfig() {
    if (envDataset == null)
      return " Environmental DataSetConfig not set\n";
    else
      return " DatasetConfig " + envDataset.getDataset() + "." + envDataset.getInternalName() + "\n";
  }

  /** 
   * Creates a Query object from a Mart Query Language command.
   * 
   * @param mql - String MQL command to parse into a Query object
   * @return Query object
   * @throws InvalidQueryException for all underlying exceptions (MQL syntax errors, DatasetConfig/Attributes/Sequences/Filters not found, etc.)
   */
  public synchronized Query MQLtoQuery(String newquery) throws InvalidQueryException {
    try {
      //reset MartCompleter induced state, if any. Also reduces the number of DatasetConfig objects held in memory
      usingLocalDataset = false;
      localDataset = null;

      boolean start = true;
      boolean getClause = false;
      boolean usingClause = false;
      boolean domainSpecificClause = false;
      boolean whereClause = false;
      boolean limitClause = false;
      boolean sortClause = false;
      boolean inList = false;
      boolean inBind = false;
      boolean inQuotedValue = false;
      boolean whereFilterName = false;
      boolean whereFilterCond = false;
      boolean whereFilterVal = false;
      boolean validQuery = false;

      DatasetConfig thisDatasetConfig = null;

      if (logger.isLoggable(Level.FINE))
        logger.fine("Recieved Query " + newquery + "\n");

      Query query = new Query();
      currentFpage = null;
      currentApage = null;
      atts = new ArrayList();
      filtNames = new ArrayList();
      maxSelects = new Hashtable();

      String filterName = null;
      String filterCondition = null;
      StringBuffer filterValue = new StringBuffer();
      StringBuffer storedCommand = new StringBuffer();
      List listFilterValues = new ArrayList();
      String domainSpecificKeyword = null;

      StringTokenizer cTokens = new StringTokenizer(newquery, " ");

      if (cTokens.countTokens() < 2)
        throw new InvalidQueryException("\nInvalid Query Recieved " + newquery + "\n");

      while (cTokens.hasMoreTokens()) {
        String thisToken = cTokens.nextToken();
        if (start) {
          if (!(thisToken.equalsIgnoreCase(GETQSTART) || thisToken.equalsIgnoreCase(USINGQSTART)))
            throw new InvalidQueryException(
              "Invalid Query Recieved, should begin with either 'using' or 'get': " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(GETQSTART)) {
            start = false;
            getClause = true;
          } else {
            start = false;
            usingClause = true;
          }
        } else if (usingClause) {
          if (domainSpecificHandlerAvailable(thisToken))
            throw new InvalidQueryException(
              "Invalid Query Recieved, domain specific clause before " + GETQSTART + " clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QWHERE))
            throw new InvalidQueryException(
              "Invalid Query Recieved, where clause before " + GETQSTART + " clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QLIMIT))
            throw new InvalidQueryException(
              "Invalid Query Recieved, limit clause before " + GETQSTART + " clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QSORT))
            throw new InvalidQueryException(
              "Invalid Query Recieved, sortBy clause before " + GETQSTART + " clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(GETQSTART)) {
            usingClause = false;
            getClause = true;
          } else {
            if (thisDatasetConfig != null)
              throw new InvalidQueryException(
                "Invalid Query Recieved, DatasetConfig already set, attempted to set again: " + newquery + "\n");
            else {
              DatasetRequest dsrq = new DatasetRequest(thisToken, this);

              try {
                if (envMart == null || !(envMart.getName().equals(dsrq.mart)))
                  query.setDataSource(adaptorManager.getAdaptorByName(dsrq.mart).getDataSource());

                thisDatasetConfig =
                  adaptorManager.getAdaptorByName(dsrq.mart).getDatasetConfigByDatasetInternalName(dsrq.dataset, dsrq.datasetconfig);
                query.setDataset(thisDatasetConfig.getDataset());
              } catch (ConfigurationException e1) {
                throw new InvalidQueryException("Could not set parse using request " + thisToken + "\n");
              }

              if (logger.isLoggable(Level.FINE)) {
                logger.fine("setting local dataset to " + thisDatasetConfig.getDataset() + "\n");
                if (query.getDataSource() != null)
                  logger.fine("setting Mart to " + query.getDataSource().getName() + "\n");
              }
            }

          }
        } else if (getClause) {
          // set dataset and update query with starbases, or throw an exception if dataset not set
          if (thisDatasetConfig == null) {
            if (envDataset == null) {
              throw new InvalidQueryException(
                "Invalid Query Recieved, did not set DatasetConfig: "
                  + newquery
                  + "\nEither set environmental DatasetConfig with 'set' or 'use', or use a 'using' clause in your MQL\n");
            } else
              thisDatasetConfig = envDataset;
          }

          query.setDataset(thisDatasetConfig.getDataset());
          query.setMainTables(thisDatasetConfig.getStarBases());
          query.setPrimaryKeys(thisDatasetConfig.getPrimaryKeys());

          //favor using DataSource over envMart
          if (query.getDataSource() == null) {
            if (envMart != null)
              query.setDataSource(envMart);
            else
              throw new InvalidQueryException(
                "Invalid Query Recieved, could not get a Mart from a 'using' clause, or the environment: "
                  + newquery
                  + "\n");
          }

          if (thisToken.equalsIgnoreCase(GETQSTART) || thisToken.equalsIgnoreCase(USINGQSTART))
            throw new InvalidQueryException(
              "Invalid Query Recieved, "
                + GETQSTART
                + " clause in the middle of a "
                + GETQSTART
                + " clause: "
                + newquery
                + "\n");
          else if (thisToken.equalsIgnoreCase(QLIMIT)) {
            if (!validQuery)
              throw new InvalidQueryException("Recieved invalid Query " + newquery + "\ncheck for a dangling comma\n");

            validQuery = false;
            getClause = false;
            limitClause = true;
          } else if (thisToken.equalsIgnoreCase(QSORT)) {
            if (!validQuery)
              throw new InvalidQueryException("Recieved invalid Query " + newquery + "\ncheck for a dangling comma\n");

            if (!advancedFeaturesOn)
              throw new InvalidQueryException("sortBy request not allowed unless advancedFeatures set\n");
                          
            validQuery = false;
            getClause = false;
            sortClause = true;
          } else if (domainSpecificHandlerAvailable(thisToken)) {
            validQuery = false;
            domainSpecificKeyword = thisToken;
            getClause = false;
            domainSpecificClause = true;
          } else if (thisToken.equalsIgnoreCase(QWHERE)) {
            if (logger.isLoggable(Level.FINE))
              logger.fine("Recieved where clause after attributes, query is valid: " + validQuery + " \n");

            if (!validQuery)
              throw new InvalidQueryException("Recieved invalid Query " + newquery + "\ncheck for a dangling comma\n");

            validQuery = false;
            getClause = false;
            whereClause = true;
            whereFilterName = true;
          } else {
            if (thisToken.endsWith(",")) {
              if (logger.isLoggable(Level.FINE))
                logger.fine(thisToken + " Comma, setting validQuery to false\n");

              thisToken = thisToken.substring(0, thisToken.length() - 1);
              validQuery = false;
            } else {
              if (logger.isLoggable(Level.FINE))
                logger.fine(thisToken + " Not comma, setting validQuery to true\n");
              validQuery = true;
            }

            StringTokenizer attToks = new StringTokenizer(thisToken, ",");
            while (attToks.hasMoreTokens())
              query = addAttribute(query, thisDatasetConfig, attToks.nextToken().trim());
          }
        } else if (domainSpecificClause) {
          if (thisToken.equalsIgnoreCase(GETQSTART) || thisToken.equalsIgnoreCase(USINGQSTART))
            throw new InvalidQueryException(
              "Invalid Query Recieved, " + GETQSTART + " clause in the middle of a sequence clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QLIMIT)) {
            if (!validQuery)
              throw new InvalidQueryException(
                "Recieved invalid Query " + newquery + "\ncheck for an incomplete Domain Specific Request\n");

            validQuery = false;
            domainSpecificClause = false;
            limitClause = true;
          } else if (thisToken.equalsIgnoreCase(QSORT)) {
            if (!validQuery)
              throw new InvalidQueryException("Recieved invalid Query " + newquery + "\ncheck for a dangling comma\n");

            if (!advancedFeaturesOn)
              throw new InvalidQueryException("sortBy request not allowed unless advancedFeatures set\n");
              
            validQuery = false;
            getClause = false;
            sortClause = true;
          } else if (thisToken.equalsIgnoreCase(QWHERE)) {
            if (!validQuery)
              throw new InvalidQueryException(
                "Recieved invalid Query " + newquery + "\ncheck for an incomplete Domain Specific Request\n");

            validQuery = false;
            domainSpecificClause = false;
            whereClause = true;
            whereFilterName = true;
          } else {
            query = modifyQueryForDomainSpecificKeyword(domainSpecificKeyword, query, thisDatasetConfig, thisToken);
            validQuery = true;
          }
        } else if (whereClause) {
          if (thisToken.equalsIgnoreCase(QLIMIT)) {
            if (!validQuery)
              throw new InvalidQueryException(
                "Recieved invalid Query "
                  + newquery
                  + "\ncheck for a dangling filter delimiter "
                  + FILTERDELIMITER
                  + "\n");

            validQuery = false;
            whereClause = false;
            limitClause = true;
          } else if (thisToken.equalsIgnoreCase(QSORT)) {
            if (!validQuery)
              throw new InvalidQueryException(
                "Recieved invalid Query "
                  + newquery
                  + "\ncheck for a dangling filter delimiter "
                  + FILTERDELIMITER
                  + "\n");

            if (!advancedFeaturesOn)
              throw new InvalidQueryException("sortBy request not allowed unless advancedFeatures set\n");
              
            validQuery = false;
            whereClause = false;
            sortClause = true;
          } else if (thisToken.equalsIgnoreCase(GETQSTART) || thisToken.equalsIgnoreCase(USINGQSTART))
            throw new InvalidQueryException(
              "Invalid Query Recieved, " + GETQSTART + " clause after where clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QWHERE))
            throw new InvalidQueryException(
              "Invalid Query Recieved, where clause after where clause: " + newquery + "\n");

          else if (thisToken.equalsIgnoreCase(FILTERDELIMITER) && !inQuotedValue) {
            whereFilterCond = false;
            whereFilterVal = false;
            whereFilterName = true;
            validQuery = false;
          } else if (whereFilterName) {
            if (thisToken.matches("[^>=<]+([>=<]+)[^>=<]*")) {
              Pattern pat = Pattern.compile("[^>=<]+([>=<]+)[^>=<]*");
              //one or more non qualifier characters followed immediately by a qualifier, followed by zero or more non qualifier characters
              Matcher m = pat.matcher(thisToken);

              m.find(); // know its there, just have to find it
              filterCondition = m.group(1);
              whereFilterCond = false;

              StringTokenizer filtToks = new StringTokenizer(thisToken, filterCondition);

              if (filtToks.countTokens() == 2) {
                filterName = filtToks.nextToken();
                whereFilterName = false;
                
                String valTok = filtToks.nextToken();
                whereFilterVal = true;
                
                if (valTok.startsWith(QUOTE) || valTok.startsWith(QUOTEESCSTART)) {
                  if (valTok.startsWith(QUOTEESCSTART))
                    valTok = valTok.substring(2);
                  else
                    valTok = valTok.substring(1);
                  
                  inQuotedValue = true;
                  
                  if (valTok.endsWith(QUOTE) || valTok.endsWith(QUOTEESCEND)) {
                    valTok = valTok.substring(0, valTok.length() - 1);
                    query = addBasicFilter(query, thisDatasetConfig, filterName, filterCondition, valTok);
                    validQuery = true;
                    
                    inQuotedValue = false;
                    filterValue = new StringBuffer();
                    filterName = null;
                    filterCondition = null;
                    whereFilterVal = false;
                  } else
                    filterValue.append(valTok);
                } else {
                  query =
                    addBasicFilter(query, thisDatasetConfig, filterName, filterCondition, valTok);
                  validQuery = true;

                  filterValue = new StringBuffer();
                  filterName = null;
                  filterCondition = null;
                  whereFilterVal = false;
                }
              } else {
                filterName = filtToks.nextToken();
                whereFilterName = false;
                whereFilterCond = false;
                whereFilterVal = true;
              }
            } else {
              filterName = thisToken;
              whereFilterName = false;
              whereFilterCond = true;
              whereFilterVal = false;
            }
          } else if (whereFilterCond) {
            if (BOOLEANQUALIFIERS.contains(thisToken)) {
              query = addBooleanFilter(query, thisDatasetConfig, filterName, thisToken);
              validQuery = true;

              filterValue = new StringBuffer();
              filterName = null;
              filterCondition = null;
              whereFilterName = false;
              whereFilterCond = false;
              whereFilterVal = false;
            } else if (thisToken.matches("([>=<]+)([^>=<]+)")) {
              Pattern p = Pattern.compile("([>=<]+)([^>=<]+)");
              Matcher m = p.matcher(thisToken);
              m.find();
              filterCondition = m.group(1);
              whereFilterCond = false;
              
              if (!ALLQUALIFIERS.contains(filterCondition))
                throw new InvalidQueryException(
                  "Recieved invalid FilterCondition " + filterCondition + " in " + newquery + "\n");

              String valTok = m.group(2);            
              whereFilterVal = true;
                
              if (valTok.startsWith(QUOTE) || valTok.startsWith(QUOTEESCSTART)) {
                if (valTok.startsWith(QUOTEESCSTART))
                  valTok = valTok.substring(2);
                else  
                  valTok = valTok.substring(1);                  
                
                inQuotedValue = true;
                whereFilterName = false;
                whereFilterCond = false;
                  
                if (valTok.endsWith(QUOTE) || valTok.endsWith(QUOTEESCEND)) {
                  valTok = valTok.substring(0, valTok.length() - 1);
                  query = addBasicFilter(query, thisDatasetConfig, filterName, filterCondition, valTok);
                  validQuery = true;
                    
                  inQuotedValue = false;
                  filterValue = new StringBuffer();
                  filterName = null;
                  filterCondition = null;
                  whereFilterVal = false;
                } else
                  filterValue.append(valTok);
              } else {
                query = addBasicFilter(query, thisDatasetConfig, filterName, filterCondition, valTok);
                validQuery = true;

                filterValue = new StringBuffer();
                filterName = null;
                filterCondition = null;
                whereFilterName = false;
                whereFilterCond = false;
                whereFilterVal = false;
              }
            } else {
              filterCondition = thisToken;
              if (!ALLQUALIFIERS.contains(filterCondition))
                throw new InvalidQueryException(
                  "Recieved invalid FilterCondition " + filterCondition + " in " + newquery + "\n");

              whereFilterCond = false;
              whereFilterVal = true;
            }
          } else if (whereFilterVal) {
            if (thisToken.startsWith(QUOTE) || thisToken.startsWith(QUOTEESCSTART)) {
              String tok = null;
              if (thisToken.startsWith(QUOTEESCSTART))
                tok = thisToken.substring(2);
              else
                tok = thisToken.substring(1);

              inQuotedValue = true;
              whereFilterName = false;
              whereFilterCond = false;

              if (thisToken.endsWith(QUOTE) || thisToken.endsWith(QUOTEESCEND)) {
                tok = tok.substring(0, tok.length() - 1);
                query = addBasicFilter(query, thisDatasetConfig, filterName, filterCondition, tok);
                validQuery = true;
                
                inQuotedValue = false;
                filterValue = new StringBuffer();
                filterName = null;
                filterCondition = null;
                whereFilterVal = false;
              } else
                filterValue.append(tok);
            } else if (inQuotedValue) {
              if (thisToken.endsWith(QUOTE) || thisToken.endsWith(QUOTEESCEND)) {
                filterValue.append(" ").append(thisToken.substring(0, thisToken.length() - 1));

                query = addBasicFilter(query, thisDatasetConfig, filterName, filterCondition, filterValue.toString());
                validQuery = true;

                inQuotedValue = false;
                filterValue = new StringBuffer();
                filterName = null;
                filterCondition = null;
                whereFilterName = false;
                whereFilterCond = false;
                whereFilterVal = false;
              } else
                filterValue.append(" ").append(thisToken);
            } else if (thisToken.equals(LSTART)) {
              inList = true;
            } else if (thisToken.startsWith(LSTART)) {
              inList = true;

              String tmp = thisToken.substring(1);

              if (tmp.indexOf(LEND) > 0) {
                inList = false;

                tmp = tmp.substring(0, tmp.indexOf(LEND));
                StringTokenizer toks = new StringTokenizer(tmp, ",");
                while (toks.hasMoreTokens())
                  listFilterValues.add(toks.nextToken().trim());

                query = addListFilter(query, thisDatasetConfig, filterName, listFilterValues);
                validQuery = true;

                filterValue = new StringBuffer();
                filterName = null;
                filterCondition = null;
                whereFilterName = false;
                whereFilterCond = false;
                whereFilterVal = false;
                listFilterValues = new ArrayList();
              } else {
                if (tmp.indexOf(",") >= 0) {
                  StringTokenizer toks = new StringTokenizer(tmp, ",");

                  while (toks.hasMoreTokens())
                    listFilterValues.add(toks.nextToken().trim());
                } else {
                  listFilterValues.add(tmp);
                }
              }
            } else if (inList) {
              if (thisToken.indexOf(LEND) >= 0) {
                inList = false;
                String tmp = thisToken.substring(0, thisToken.indexOf(LEND));
                if (tmp.length() > 0) {
                  StringTokenizer toks = new StringTokenizer(tmp, ",");

                  while (toks.hasMoreTokens())
                    listFilterValues.add(toks.nextToken().trim());
                }
                query = addListFilter(query, thisDatasetConfig, filterName, listFilterValues);
                validQuery = true;

                filterValue = new StringBuffer();
                filterName = null;
                filterCondition = null;
                whereFilterName = false;
                whereFilterCond = false;
                whereFilterVal = false;
                listFilterValues = new ArrayList();
              } else {
                StringTokenizer toks = new StringTokenizer(thisToken, ",");

                while (toks.hasMoreTokens())
                  listFilterValues.add(toks.nextToken().trim());
              }
            } else if (thisToken.indexOf(LSTART) > 0) {
              if (thisToken.endsWith(LEND)) {
                // storedCommand with bindValues, no whitespaces
                query = addListFilter(query, thisDatasetConfig, filterName, thisToken);
                validQuery = true;

                filterValue = new StringBuffer();
                filterName = null;
                filterCondition = null;
                whereFilterName = false;
                whereFilterCond = false;
                whereFilterVal = false;
                inBind = false;
                storedCommand = new StringBuffer();
              } else {
                //append to storedCommand
                inBind = true;
                if (storedCommand.length() > 0)
                  storedCommand.append(" ");
                storedCommand.append(thisToken);
              }
            } else if (inBind) {
              if (thisToken.endsWith(LEND)) {
                // add storedCommand
                query =
                  addListFilter(
                    query,
                    thisDatasetConfig,
                    filterName,
                    storedCommand.append(" ").append(thisToken).toString());
                validQuery = true;

                filterValue = new StringBuffer();
                filterName = null;
                filterCondition = null;
                whereFilterName = false;
                whereFilterCond = false;
                whereFilterVal = false;
                inBind = false;
                storedCommand = new StringBuffer();
              } else {
                //append to storedCommand
                if (storedCommand.length() > 0)
                  storedCommand.append(" ");
                storedCommand.append(thisToken);
              }
            } else {
              if (filterCondition.equalsIgnoreCase("in")) {
                if (thisToken.indexOf(":") >= 0) {
                  //url
                  try {
                    query = addListFilter(query, thisDatasetConfig, filterName, new URL(thisToken));
                    validQuery = true;
                  } catch (Exception e) {
                    throw new InvalidQueryException(
                      "Error adding url filter " + filterName + " " + thisToken + " " + e.getMessage(),
                      e);
                  }
                } else if (storedCommands.containsKey(thisToken)) {
                  //storedCommand without bindvalues
                  query = addListFilter(query, thisDatasetConfig, filterName, thisToken);
                  validQuery = true;
                } else {
                  //file
                  query = addListFilter(query, thisDatasetConfig, filterName, new File(thisToken));
                  validQuery = true;
                }
              } else {
                query = addBasicFilter(query, thisDatasetConfig, filterName, filterCondition, thisToken);
                validQuery = true;
              }

              filterValue = new StringBuffer();
              filterName = null;
              filterCondition = null;
              whereFilterName = false;
              whereFilterCond = false;
              whereFilterVal = false;
            }
          } else
            throw new InvalidQueryException(
              "Invalid Query Recieved, invalid filter statement in where clause: " + newquery + "\n");
        } else if (sortClause) {
          if (thisToken.equalsIgnoreCase(GETQSTART) || thisToken.equalsIgnoreCase(USINGQSTART))
            throw new InvalidQueryException(
              "Invalid Query Recieved, " + GETQSTART + " clause in limit clause: " + newquery + "\n");
          else if (domainSpecificHandlerAvailable(thisToken))
            throw new InvalidQueryException(
              "Invalid Query Recieved, domain specific clause in limit clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QWHERE))
            throw new InvalidQueryException("Invalid Query Recieved, where clause in limit clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QLIMIT)) {
            if (!validQuery)
              throw new InvalidQueryException(
                "Recieved invalid Query " + newquery + "\ncheck for an incomplete Domain Specific Request\n");

            validQuery = false;
            sortClause = false;
            limitClause = true;
          } else {
            if (thisToken.endsWith(",")) {
              if (logger.isLoggable(Level.FINE))
                logger.fine(thisToken + " Comma, setting validQuery to false\n");

              thisToken = thisToken.substring(0, thisToken.length() - 1);
              validQuery = false;
            } else {
              if (logger.isLoggable(Level.FINE))
                logger.fine(thisToken + " Not comma, setting validQuery to true\n");
              validQuery = true;
            }

            StringTokenizer attToks = new StringTokenizer(thisToken, ",");
            while (attToks.hasMoreTokens())
              query = addSortAttribute(query, thisDatasetConfig, attToks.nextToken().trim());
            validQuery = true;
          }
        } else if (limitClause) {
          if (thisToken.equalsIgnoreCase(GETQSTART) || thisToken.equalsIgnoreCase(USINGQSTART))
            throw new InvalidQueryException(
              "Invalid Query Recieved, " + GETQSTART + " clause in limit clause: " + newquery + "\n");
          else if (domainSpecificHandlerAvailable(thisToken))
            throw new InvalidQueryException(
              "Invalid Query Recieved, domain specific clause in limit clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QWHERE))
            throw new InvalidQueryException("Invalid Query Recieved, where clause in limit clause: " + newquery + "\n");
          else if (thisToken.equalsIgnoreCase(QSORT))
            throw new InvalidQueryException("Invalid Query Recieved, sortBy clause in limit clause: " + newquery + "\n");
          else {
            if (query.getLimit() > 0)
              throw new InvalidQueryException("Invalid Query Recieved, attempt to set limit twice: " + newquery + "\n");
            else {
              query.setLimit(Integer.parseInt(thisToken));
              validQuery = true;
            }
          }
        }
        // else not needed, as these are the only states present
      }

      if (!validQuery)
        throw new InvalidQueryException(
          "Recieved invalid query "
            + newquery
            + "\ncheck for dangling commas between attributes, an incomplete domain specific request, a dangling filter delimeter "
            + FILTERDELIMITER
            + " between filter requests,\nor an incomplete limit request\n");

      if (query.getAttributes().length == 0 && query.getSequenceDescription() == null)
        throw new InvalidQueryException(
          "Invalid Query Recieved, no attributes or sequence description found " + newquery + "\n");

      //GenericHandler requires the query to have a DatasetConfig 
      query.setDatasetConfig(thisDatasetConfig);
      return query;
    } catch (NumberFormatException e) {
      throw new InvalidQueryException("Recieved NumberFormatException parsing MQL " + e.getMessage(), e);
    } catch (InvalidQueryException e) {
      throw e;
    }
  }

  private Query modifyQueryForDomainSpecificKeyword(
    String domainSpecificKeyword,
    Query query,
    DatasetConfig dset,
    String thisToken)
    throws InvalidQueryException {
    // can either add keywords here, or replace it with a Plugin Module
    return addSequenceDescription(query, dset, thisToken);
  }

  private boolean domainSpecificHandlerAvailable(String keyword) throws InvalidQueryException {
    //modify this to add other domainSpecific keywords, or just replace it with a module
    if (keyword.equalsIgnoreCase(QSEQUENCE))
      return true;
    return false;
  }

  private boolean domainSpecificSubQueryAllowed(String keyword) {
    //  modify this to add other domainSpecific keywords, or just replace it with a module
    return !QSEQUENCE.equalsIgnoreCase(keyword);
  }

  private synchronized Filter getIDFilterForSubQuery(
    String fieldName,
    String tableConstraint,
    String key,
    String storedCommandName)
    throws InvalidQueryException {

    String bindValues = null;
    if (storedCommandName.indexOf(LSTART) > 0) {
      bindValues = storedCommandName.substring(storedCommandName.indexOf(LSTART) + 1, storedCommandName.indexOf(LEND));
      storedCommandName = storedCommandName.substring(0, storedCommandName.indexOf(LSTART));
    }

    if (!storedCommands.containsKey(storedCommandName))
      throw new InvalidQueryException(storedCommandName + " is not available as a stored MQL Command\n");

    String nestedQuery = storedCommands.getProperty(storedCommandName);

    if ((bindValues != null) && (bindValues.length() > 0)) {
      List bindVariables = new ArrayList();
      StringTokenizer vtokens = new StringTokenizer(bindValues, ",");
      while (vtokens.hasMoreTokens())
        bindVariables.add(vtokens.nextToken().trim());

      Pattern bindp = Pattern.compile("\\?");
      Matcher bindm = bindp.matcher(nestedQuery);

      StringBuffer qbuf = new StringBuffer();
      int bindIter = 0;
      while (bindm.find()) {
        bindm.appendReplacement(qbuf, (String) bindVariables.get(bindIter));
        bindIter++;
      }
      bindm.appendTail(qbuf);
      nestedQuery = qbuf.toString();
    }

    //validate, then call parseQuery on the subcommand
    String[] tokens = nestedQuery.split("\\s");

    for (int i = 1, n = tokens.length; i < n; i++) {
      String tok = tokens[i];
      if (domainSpecificHandlerAvailable(tok) && !domainSpecificSubQueryAllowed(tok))
        throw new InvalidQueryException(
          "Invalid Nested Query Recieved: domain specific statement " + tok + " is not allowed " + nestedQuery + "\n");
      //else not needed
    }

    nestedLevel++;

    if (nestedLevel > MAXNESTING) {
      nestedLevel--;
      throw new InvalidQueryException("Only " + MAXNESTING + " levels of nested Query are allowed\n");
    }

    Query subQuery = null;
    try {
      subQuery = MQLtoQuery(nestedQuery);
    } catch (Exception e) {
      nestedLevel--;
      throw new InvalidQueryException("Could not parse Nested Query : " + e.getMessage(), e);
    }

    subQuery.setQueryName(storedCommandName);

    Filter f = null;
    //if (handler != null)
    //  f = new IDListFilter(fieldName, tableConstraint, key, subQuery, handler);
    //else
      f = new IDListFilter(fieldName, tableConstraint, key, subQuery);

    nestedLevel--;
    return f;
  }

  private Query addSequenceDescription(Query inquery, DatasetConfig dset, String seqrequest)
    throws InvalidQueryException {
    currentApage = dset.getAttributePageByInternalName("sequences");
    for (int i = 0, n = atts.size(); i < n; i++) {
      String element = (String) atts.get(i);

      if (!currentApage.containsAttributeDescription(element))
        throw new InvalidQueryException(
          "Cannot request attribute " + element + " together with sequences in the same query.\n");
    }

    Query newQuery = new Query(inquery);

    String seqDescription = null;
    int left = 0;
    int right = 0;

    StringTokenizer tokens = new StringTokenizer(seqrequest, SEQDELIMITER, true);
    int n = tokens.countTokens();
    switch (n) {
      case 5 :
        // left+type+right
        left = Integer.parseInt(tokens.nextToken());
        tokens.nextToken(); // skip plus
        seqDescription = tokens.nextToken();
        tokens.nextToken();
        right = Integer.parseInt(tokens.nextToken());
        break;
      case 3 :
        // left+type || type+right
        String tmpl = tokens.nextToken();
        tokens.nextToken();
        String tmpr = tokens.nextToken();

        if (dset.containsAttributeDescription(tmpl)) {
          seqDescription = tmpl;
          right = Integer.parseInt(tmpr);
        } else if (dset.containsAttributeDescription(tmpr)) {
          left = Integer.parseInt(tmpl);
          seqDescription = tmpr;
        } else {
          throw new InvalidQueryException("Invalid sequence request recieved: " + seqrequest + "\n");
        }
        break;
      case 1 :
        // type
        seqDescription = seqrequest;
        break;
    }
    
	AttributeDescription attrDesc = currentApage.getAttributeDescriptionByInternalName(seqDescription);
	String seqDs = attrDesc.getPointerDataset();
	if (seqDs==null || "".equals(seqDs)) seqDs = dset.getDataset();
    newQuery.setSequenceDescription(new SequenceDescription(dset.getDataset(), seqDs, seqDescription, adaptorManager, left, right));
    return newQuery;
  }

  private Query addAttribute(Query inquery, DatasetConfig dset, String attname) throws InvalidQueryException {
    checkAttributeValidity(dset, attname);

    Query newQuery = new Query(inquery);
    AttributeDescription attdesc = (AttributeDescription) dset.getAttributeDescriptionByInternalName(attname);
    Attribute attr = null;
    if (attdesc.getPointerDataset()!=null && !"".equals(attdesc.getPointerDataset())) {
        //placeholder sequence attribute
        attr = new FieldAttribute(attdesc.getInternalName(), attdesc.getTableConstraint(), attdesc.getKey());
    }
    else
      attr = new FieldAttribute(attdesc.getField(), attdesc.getTableConstraint(), attdesc.getKey());
    
    newQuery.addAttribute(attr);

    return newQuery;
  }

  private Query addSortAttribute(Query inquery, DatasetConfig dset, String attname) throws InvalidQueryException {
    checkAttributeValidity(dset, attname);

    Query newQuery = new Query(inquery);
    AttributeDescription attdesc = (AttributeDescription) dset.getAttributeDescriptionByInternalName(attname);
    Attribute attr = null;
    attr = new FieldAttribute(attdesc.getField(), attdesc.getTableConstraint(), attdesc.getKey());
    
    newQuery.addSortByAttribute(attr);

    return newQuery;
  }
  
  private void checkAttributeValidity(DatasetConfig dset, String attname) throws InvalidQueryException {
    if (!dset.containsAttributeDescription(attname))
      throw new InvalidQueryException(
        "Attribute " + attname + " is not found in this mart for dataset " + dset.getInternalName() + "\n");

    //check page
    if (currentApage == null) {
      currentApage = dset.getPageForAttribute(attname);
    } else {
      if (!currentApage.containsAttributeDescription(attname)) {
        currentApage = dset.getPageForAttribute(attname);

        for (int i = 0, n = atts.size(); i < n; i++) {
          String element = (String) atts.get(i);

          if (!currentApage.containsAttributeDescription(element))
            throw new InvalidQueryException(
              "Cannot request attributes "
                + attname
                + " and "
                + element
                + " together in the same query.  Use 'describe dataset "
                + dset.getInternalName()
                + "' for a list of attributes that can be selected together\n");
        }
      }
    }

    //check maxSelect
    AttributeCollection collection = currentApage.getCollectionForAttributeDescription(attname);
    String colname = collection.getInternalName();
    int maxSelect = collection.getMaxSelect();

    if (maxSelect > 0) {
      if (maxSelects.containsKey(colname)) {
        int oldMax = ((Integer) maxSelects.get(colname)).intValue();
        oldMax++;
        if (oldMax > maxSelect)
          throw new InvalidQueryException(
            "You cannot select more than " + maxSelect + " attributes from AttributeCollection " + colname + "\n");
        maxSelects.put(colname, new Integer(oldMax));
      } else
        maxSelects.put(colname, new Integer(1));
    }

    atts.add(attname);
  }

  private Query addBooleanFilter(Query inquery, DatasetConfig dset, String filterName, String filterCondition)
    throws InvalidQueryException {
    checkFilterValidity(dset, filterName);

    FilterDescription fdesc = dset.getFilterDescriptionByInternalName(filterName);
    String thisType = fdesc.getType(filterName);

    if (!thisType.startsWith("boolean"))
      throw new InvalidQueryException(
        filterName + " is not a boolean filter, cannot process with " + filterCondition + "\n");

    if (!BOOLEANQUALIFIERS.contains(filterCondition))
      throw new InvalidQueryException(filterCondition + " is not valid for a boolean filter\n");

    String thisCondition = null;
    if (thisType.equals("boolean_num"))
      thisCondition = BOOLEAN_NUMCONDITIONS[BOOLEANQUALIFIERS.indexOf(filterCondition)];
    else
      thisCondition = BOOLEAN_CONDITIONS[BOOLEANQUALIFIERS.indexOf(filterCondition)];

    //String handler = fdesc.getHandler(filterName);

    Query newQuery = new Query(inquery);
    newQuery.addFilter(
      new BooleanFilter(
        fdesc.getField(filterName),
        fdesc.getTableConstraint(filterName),
        fdesc.getKey(filterName),
        thisCondition
        //handler)
        ));
    return newQuery;
  }

  private Query addBasicFilter(
    Query inquery,
    DatasetConfig dset,
    String filterName,
    String filterCondition,
    String filterValue)
    throws InvalidQueryException {
    checkFilterValidity(dset, filterName);

    FilterDescription fdesc = dset.getFilterDescriptionByInternalName(filterName);

    Query newQuery = new Query(inquery);
    //if (fdesc.getHandler(filterName) != null) {
    //  newQuery.addFilter(
    //    new BasicFilter(
    //      fdesc.getField(filterName),
    //      fdesc.getTableConstraint(filterName),
    //      fdesc.getKey(filterName),
    //      filterCondition,
    //      filterValue,
    //      fdesc.getHandler(filterName)));
    //} else {
      newQuery.addFilter(
        new BasicFilter(
          fdesc.getField(filterName),
          fdesc.getTableConstraint(filterName),
          fdesc.getKey(filterName),
          filterCondition,
          filterValue));
    //}
    return newQuery;
  }

  private Query addListFilter(Query inquery, DatasetConfig dset, String filterName, List filterValues)
    throws InvalidQueryException {
    checkFilterValidity(dset, filterName);

    FilterDescription fdesc = dset.getFilterDescriptionByInternalName(filterName);

    Query newQuery = new Query(inquery);

    //if (fdesc.getHandler(filterName) != null)
    //  newQuery.addFilter(
    //    new IDListFilter(
    //      fdesc.getField(filterName),
    //      fdesc.getTableConstraint(filterName),
     //     fdesc.getKey(filterName),
     //     (String[]) filterValues.toArray(new String[filterValues.size()]),
     //     fdesc.getHandler(filterName)));
    //else
      newQuery.addFilter(
        new IDListFilter(
          fdesc.getField(filterName),
          fdesc.getTableConstraint(filterName),
          fdesc.getKey(filterName),
          (String[]) filterValues.toArray(new String[filterValues.size()])));

    return newQuery;
  }

  private Query addListFilter(Query inquery, DatasetConfig dset, String filterName, File fileloc)
    throws InvalidQueryException {
    checkFilterValidity(dset, filterName);

    FilterDescription fdesc = dset.getFilterDescriptionByInternalName(filterName);

    Query newQuery = new Query(inquery);

    //if (fdesc.getHandler(filterName) != null)
    //  newQuery.addFilter(
    //    new IDListFilter(
    //      fdesc.getField(filterName),
    //      fdesc.getTableConstraint(filterName),
    //      fdesc.getKey(filterName),
    //      fileloc,
    //      fdesc.getHandler(filterName)));
    //else
      newQuery.addFilter(
        new IDListFilter(
          fdesc.getField(filterName),
          fdesc.getTableConstraint(filterName),
          fdesc.getKey(filterName),
          fileloc));

    return newQuery;
  }

  private Query addListFilter(Query inquery, DatasetConfig dset, String filterName, URL urlLoc)
    throws InvalidQueryException {
    checkFilterValidity(dset, filterName);

    FilterDescription fdesc = dset.getFilterDescriptionByInternalName(filterName);

    Query newQuery = new Query(inquery);

    //if (fdesc.getHandler(filterName) != null)
    //  newQuery.addFilter(
    //    new IDListFilter(
    //      fdesc.getField(filterName),
    //      fdesc.getTableConstraint(filterName),
    //      fdesc.getKey(filterName),
    //      urlLoc,
    //      fdesc.getHandler(filterName)));
    //else
      newQuery.addFilter(
        new IDListFilter(
          fdesc.getField(filterName),
          fdesc.getTableConstraint(filterName),
          fdesc.getKey(filterName),
          urlLoc));

    return newQuery;
  }

  private Query addListFilter(Query inquery, DatasetConfig dset, String filterName, String storedQueryName)
    throws InvalidQueryException {
    checkFilterValidity(dset, filterName);

    FilterDescription fdesc = dset.getFilterDescriptionByInternalName(filterName);

    Query newQuery = new Query(inquery);

    // subquery will overwrite page states, need to return them to original after it is parsed
    AttributePage bakApage = currentApage;
    FilterPage bakFpage = currentFpage;
    currentApage = null;
    currentFpage = null;

    newQuery.addFilter(
      getIDFilterForSubQuery(
        fdesc.getField(filterName),
        fdesc.getTableConstraint(filterName),
        fdesc.getKey(filterName),
        //fdesc.getHandler(filterName),
        storedQueryName));

    currentApage = bakApage;
    currentFpage = bakFpage;
    return newQuery;
  }

  private void checkFilterValidity(DatasetConfig dset, String filterName) throws InvalidQueryException {
    if (!dset.containsFilterDescription(filterName))
      throw new InvalidQueryException(
        "Filter " + filterName + " not supported by mart dataset " + dset.getInternalName() + "\n");

    if (currentFpage == null)
      currentFpage = dset.getPageForFilter(filterName);
    else {
      if (!currentFpage.containsFilterDescription(filterName)) {
        currentFpage = dset.getPageForFilter(filterName);

        for (int i = 0, n = filtNames.size(); i < n; i++) {
          String element = (String) filtNames.get(i);
          if (!currentFpage.containsFilterDescription(element))
            throw new InvalidQueryException(
              "Cannot use filters "
                + filterName
                + " and "
                + element
                + " together in the same query.  Use 'describe dataset "
                + dset.getInternalName()
                + "' to get a list of filters that can be used in the same query.\n");
        }
      }
    }

    filtNames.add(filterName);
  }

  public void setAdvancedFeatures(boolean toggle) {
    advancedFeaturesOn = toggle;
  }

  /**
   * Takes a canonicalized martName, and replaces each
   * underscore with a single whitespace.
   * 
   * @param String Canonicalized Mart Name
   * @return String deCanonicalized Mart Name
   */
  public String deCanonicalizeMartName(String cMartName) {
	  
	  //return cMartName.replaceAll("\\_", " ");
	  return cMartName;
  }
  
  /**
   * Takes an uncanonicalized martName, and replaces each
   * individual whitespace with a single underscore.
   * 
   * @param String uncanonicalized Mart Name
   * @return String canonicalized Mart Name
   */
  public String canonicalizeMartName(String cMartName) {
	  return cMartName.replaceAll(" ", "_");
  }
  
  private boolean advancedFeaturesOn = false;
  private int maxcharcount = 0;

  private String MQLError = null;

  //these allow the MartShellLib to act as controller to the MartShell and MartCompleter
  protected RegistryDSConfigAdaptor adaptorManager = null;
  //dont ignore cache, dont validate, and dont include hidden Members (these are for MartEditor only)
  protected DatasetConfig envDataset = null;
  protected DetailedDataSource envMart = null;
  protected DatasetConfig localDataset = null;
  protected boolean usingLocalDataset = false;

  private AttributePage currentApage = null;
  // keeps track of the AttributePage
  private FilterPage currentFpage = null; // keeps track of the FilterPage
  //	will hold max-select keyed by collection.internalName
  private Hashtable maxSelects = new Hashtable();
  // will hold all previously selected UIAttributeDescriptions, for page constraint validation during addAttribute
  private List atts = new ArrayList();
  private List filtNames = new ArrayList();

  // query instructions
  public static final String GETQSTART = "get";
  public static final String USINGQSTART = "using";
  public static final String QSEQUENCE = "sequence";
  public static final String QWHERE = "where";
  public static final String QLIMIT = "limit";
  public static final String QSORT = "sortBy";
  public static final char LISTSTARTCHR = '(';
  private final String LSTART = String.valueOf(LISTSTARTCHR);
  private final String QUOTE = "'";
  private final String QUOTEESCSTART = "q(";
  private final String QUOTEESCEND = ")";
  public static final char LISTENDCHR = ')';
  private final String LEND = String.valueOf(LISTENDCHR);
  private final String SEQDELIMITER = "+";
  public static final String FILTERDELIMITER = "and";
  private final String DEFAULTURLADAPTORNAME = "userfiles";
  public static final String DEFAULTDATASETCONFIGNAME = "default";
  private final String LISTALLREQ = "all";
  private final String MARTREQ = "Mart";

  protected final List availableCommands =
    Collections.unmodifiableList(Arrays.asList(new String[] { USINGQSTART, GETQSTART }));

  // variables for subquery
  private int nestedLevel = 0;
  private final int MAXNESTING = 1;
  // change this to allow deeper nesting of queries inside queries

  //Pattern for stored Command
  public static final Pattern STOREPAT = Pattern.compile("(.*)\\s+(a|A)(s|S)\\s+(\\w+)$", Pattern.DOTALL);

  public static List ALLQUALIFIERS =
    Arrays.asList(new String[] { "=", "!=", "<", ">", "<=", ">=", "only", "excluded", "in", "like" });
  //TODO: need to make MartShell get a union set of all possible qualifiers from DatasetConfig

  public static List BOOLEANQUALIFIERS = Arrays.asList(new String[] { "only", "excluded" });
  public final String[] BOOLEAN_NUMCONDITIONS = { BooleanFilter.isNotNULL_NUM, BooleanFilter.isNULL_NUM };
  public final String[] BOOLEAN_CONDITIONS = { BooleanFilter.isNotNULL, BooleanFilter.isNULL };

  private Logger logger = Logger.getLogger(MartShellLib.class.getName());
  private Properties storedCommands = new Properties();
}