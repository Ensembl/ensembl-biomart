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

//TODO: Either needs to have a DatasetConfig associated with it, or a dataset and internalName, before being sent to QueryToMQL
package org.ensembl.mart.lib;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.util.StringUtil;

/**
 * A mart query object. Instances of this class specify all of the parameters
 * necessary for execution against a mart instance. Also any 
 * QueryChangeListeners, added with <code>addQueryChangeListener(listener)</code>,
 * are notified of changes in the query's state such as the addition of an attribute or remval of
 * a filter.
 * 
 * <p>If the log level is set to >= FINE then the query is written to the log
 * after it's state is changed but before calling the listeners.</p>
 * 
 * 
 * TODO addXXX(int, o) -> add(o,int)
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @see Attribute
 * @see SequenceDescription 
 * @see Filter
 * @see QueryListener
 * 
 */

public class Query {

	private final static Logger logger = Logger.getLogger(Query.class.getName());

	private List listeners = new ArrayList();

	/**
	 * enums over query types
	 * clients can set type using the constant
	 * and test / get results as well
	 */
	public final static int ATTRIBUTE = 1;
	public final static int SEQUENCE = 2;

	public Query() {
	}

	public Query(Query oq) {

		initialise(oq);
	}

	/**
	 * Inititise this query by removing all current properties
	 * and copying all the properties from oq.
	 * 
	 * @param oq
	 * @throws InvalidQueryException
	 */
	public synchronized void initialise(Query oq) {

		setDataset(oq.getDataset());
		setDataSource(oq.getDataSource());
            
		try {
            removeAllAttributes();
        } catch (InvalidQueryException e3) {
            // ignore, oq would have thrown if there was a problem
        }
        
		if (oq.getAttributes().length > 0) {
			Attribute[] oatts = oq.getAttributes();
			//    TODO copy attribute and filter by value rather than reference
			for (int i = 0; i < oatts.length; ++i)
                    addAttribute(oatts[i]);
		}

        removeAllFilters();
        
		if (oq.getFilters().length > 0) {
			Filter[] ofilts = oq.getFilters();
			//    TODO copy attribute and filter by value rather than reference
			for (int i = 0; i < ofilts.length; ++i)
              addFilter(ofilts[i]);
		}
            
		if (oq.getMainTables().length > 0) {
			String[] oStars = oq.getMainTables();
			String[] nStars = new String[oStars.length];
			System.arraycopy(oStars, 0, nStars, 0, oStars.length);

			setMainTables(nStars);
		} else {
			setMainTables(null);
		}

		if (oq.getPrimaryKeys().length > 0) {
			String[] oPkeys = oq.getPrimaryKeys();
			String[] nPkeys = new String[oPkeys.length];
			System.arraycopy(oPkeys, 0, nPkeys, 0, oPkeys.length);

			setPrimaryKeys(nPkeys);
		} else {
			setPrimaryKeys(null);
		}

		limit = oq.getLimit();
    
    if (oq.hasSort()) {
      Attribute[] sortAtts = oq.getSortByAttributes();
      for (int i = 0, n = sortAtts.length; i < n; i++) {
        addSortByAttribute(sortAtts[i]);        
      }
    }
    
	// TODO copy other querytypes?
	if (oq.querytype == Query.SEQUENCE)
            setSequenceDescription(
            	new SequenceDescription(oq.getSequenceDescription()));
	}

	/**
	 * returns the query type (one of ATTRIBUTE or SEQUENCE)
	 * @return int querytype
	 */
	public int getType() {
		return querytype;
	}

	/**
	 * Determines if the specified attribute object is 
	 * contained within the attribute array of the Query.
	 * 
	 * @param attribute attribute to look for
	 * @return boolean true if attribute is present, otherwise false.
	 */
	public boolean hasAttribute(Attribute attribute) {
		return attributes.contains(attribute);
	}

	/**
	 * Determines if the specified filter object is 
	 * contained within the filter array of the Query.
	 * 
	 * @param filter filter to look for
	 * @return boolean true if filter is present, otherwise false.
	 */
	public boolean hasFilter(Attribute filter) {
		return filters.contains(filter);
	}

	/**
	 * Adds attribute to the end of the attributes array. 
	 * @param attribute item to be added.
	 * @throws InvalidQueryException
	 * @throws IllegalArgumentException if attribute is 
	 * null or already added.
	 */
	public synchronized void addAttribute(Attribute attribute) {
		  addAttribute(attributes.size(), attribute);
	}

	/**
	 * remove a Filter object from the list of Attributes
	 * 
	 * @param Filter filter
	 * @throws InvalidQueryException
	 */
	public synchronized void removeFilter(Filter filter) {
		int index = filters.indexOf(filter);
		if (index > -1) {
			filters.remove(index);
			log();
			for (int i = 0; i < listeners.size(); ++i)
				 ((QueryListener) listeners.get(i)).filterRemoved(this, index, filter);
		}
	}

	/**
	 * get all Attributes as an Attribute :ist
	 * 
	 * @return Attribute[] attributes
	 */
	public Attribute[] getAttributes() {
		Attribute[] a = new Attribute[attributes.size()];
		attributes.toArray(a);
		return a;
	}

	/**
	 * get all Filter objects as a Filter[] Array
	 * 
	 * @return Filters[] filters
	 */
	public Filter[] getFilters() {
		Filter[] f = new Filter[filters.size()];
		filters.toArray(f);
		return f;
	}

	/**
	 * Allows the retrieval of a specific Filter object with a specified field name.
	 * 
	 * @param name - name of the fieldname for this Filter.
	 * @return Filter object named by given field name.
	 */
	public Filter getFilterByName(String name) {
		for (int i = 0, n = filters.size(); i < n; i++) {
			Filter element = (Filter) filters.get(i);
			if (element.getField().equals(name))
				return element;
		}
		return null;
	}

	/**
	 * Add filter to end of filter list. 
	 * 
	 * @param Filter filter to be added.
	 * @throws InvalidQueryException
	 * @throws IllegalArgumentException if filter is 
	 * null or already added.
	 */
	public synchronized void addFilter(Filter filter) {
		  addFilter(filters.size(), filter);
	}

	/**
	 * Add filter to the filters array at the specified index. 
	 * 
	 * @param index position where to insert the filter in the filters array.
	 * @param filter filter to be added
	 * @throws IllegalArgumentException if attribute is 
	 * null or already added.
	 */
	public synchronized void addFilter(int index, Filter filter) {

		if (filter == null)
			throw new IllegalArgumentException("Can not add a null filter");
		if (filters.contains(filter))
			throw new IllegalArgumentException("Filter already present: " + filter);

		filters.add(index, filter);
		log();
		for (int i = 0; i < listeners.size(); ++i)
			 ((QueryListener) listeners.get(i)).filterAdded(this, index, filter);
	}

	/**
	 * Remove Attribute if present, otherwise do nothing.
	 * 
	 * @param Attribute attribute to be removed.
	 * @throws InvalidQueryException
	 */
	public synchronized void removeAttribute(Attribute attribute) {
		int index = attributes.indexOf(attribute);
		if (index > -1) {
			attributes.remove(index);
			log();
			for (int i = 0; i < listeners.size(); ++i)
				((QueryListener) listeners.get(i)).attributeRemoved(
					this,
					index,
					attribute);
		}
	}

	/**
	 * Sets a SequenceDescription to the Query, and sets querytype = SEQUENCE. 
	* @param s A SequenceDescription object.
	 * @throws InvalidQueryException
	*/
	public synchronized void setSequenceDescription(SequenceDescription s) {

		SequenceDescription oldSequenceDescription = this.sequenceDescription;
		this.sequenceDescription = s;
	    
    if (s==null)
      this.querytype = Query.ATTRIBUTE;
    else
      this.querytype = Query.SEQUENCE;
   
    log();

		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).sequenceDescriptionChanged(
				this,
				oldSequenceDescription,
				this.sequenceDescription);
	}

	private void migrateAttributes() throws InvalidQueryException {
	    Attribute[] oAttributes = getAttributes();
	    removeAllAttributes();
	    
	    for (int i = 0, n = oAttributes.length; i < n; i++) {
	        if (attributes.size() == 0) {
	    	    //must get the attribute, so that the sequenceDescription initializes properly,
	            //but must then add the exportables before this (first) attribute
	            Attribute sa = sequenceDescription.getAttribute(oAttributes[i]);
	    	    Attribute[] eAtts = sequenceDescription.getFinalLink();
                
                if (eAtts == null)
                    throw new InvalidQueryException("Sequence type " + sequenceDescription.getSeqType() + " is not supported\n");
                
	    	    for (int j = 0, m = eAtts.length; j < m; j++) {
                    addAttribute(eAtts[j]);
                }
	    	    addAttribute(sa);
	        } else
	          addAttribute( sequenceDescription.getAttribute(oAttributes[i]) );
	    }
	}

	private void migrateFilters() throws InvalidQueryException {
	    Filter[] oFilters = getFilters();
        
        // if there are no filters added, this will just be a filterless subquery
	    if (oFilters.length > 0) {
	        removeAllFilters();            
	        IDListFilter lastFilter = null;
	        for (int i = 0, n = oFilters.length; i < n; i++) {
	            lastFilter = sequenceDescription.getFilter(oFilters[i]);
                
                //add it back if this is a non subquery based sequence query
                if (lastFilter == null)
                    addFilter(oFilters[i]);
	        }
            
            if (lastFilter != null)
	          addFilter(lastFilter);
	    }
	}
	
	public void initializeForSequence() throws InvalidQueryException {
	    sequenceDescription.setSubQuery(this);
	    
	    //a valid sequence query must have one attribute
	    //in order to function properly.  Throw if not.
	    if (attributes.size() < 1)
	        throw new InvalidQueryException("Sequence Queries must contain at least one header attribute\n");
	    
	    migrateAttributes();
	    migrateFilters();
        
	    if (sequenceDescription.getFinalDatasetName() != null) {
	        setDataset(sequenceDescription.getFinalDatasetName());
	        setDatasetConfig(sequenceDescription.getFinalDataset());
	        setDataSource(sequenceDescription.getFinalDataSource());
	        setMainTables(sequenceDescription.getStructureMainTables());
	        setPrimaryKeys(sequenceDescription.getStructurePrimaryKeys());
	        //this query is now a structure query with a core subQuery, not the original core query
	    }
	}
	
	/**
	 * returns the SequenceDescription for this Query.
	* @return SequenceDescription
	*/
	public SequenceDescription getSequenceDescription() {
		return sequenceDescription;
	}

	/**
	 * get the primaryKeys of the Query
	 * @return String primaryKeys
	 */
	public String[] getPrimaryKeys() {
		return primaryKeys;
	}

	/**
	 * set the primaryKeys for the Query
	 * @param String primaryKeys
	 */
	public synchronized void setPrimaryKeys(String[] primaryKeys) {
		String[] old = this.primaryKeys;
		this.primaryKeys = primaryKeys;
		log();
		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).primaryKeysChanged(
				this,
				old,
				this.primaryKeys);
	}

	/**
	 * get the starBases for the Query
	 * @return String starBases
	 */
	public String[] getMainTables() {
		return starBases;
	}

	/**
	 * set the starBases for the Query
	 * @param String starBases
	 */
	public synchronized void setMainTables(String[] starBases) {
		String[] old = this.starBases;
		this.starBases = starBases;

		log();

		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).starBasesChanged(
				this,
				old,
				this.starBases);
	}

	/**
	 * Set a limit for the Query.
	 * @param inlimit - int limit to add to the Query, should be >=0 where
   * 0 mean no limit
	 */
	public synchronized void setLimit(int inlimit) {
		//assert inlimit > -1 : "invalid limit should be >=0 but is " + inlimit;
		int old = this.limit;
		this.limit = inlimit;
		log();
		for (int i = 0; i < listeners.size(); ++i)
			 ((QueryListener) listeners.get(i)).limitChanged(this, old, this.limit);
	}
	
  /**
	 * Determine if the Query has a limit > 0.
	 * @return true if limit > 0, false if not
	 */
	public boolean hasLimit() {
		return (limit > 0);
	}
	/**
	 * Returns the limit for the Query. limit == 0 means no limit
	 * @return limit
	 */
	public int getLimit() {
		return limit;
	}

	/**
	 * Returns the total number of filters of all types added to the Query
	 * @return int count of all filters added
	 */
	public int getTotalFilterCount() {
		return filters.size();
	}

	/**
	 * returns a description of the Query for logging purposes
	 * 
	 * @return String description (primaryKeys=primaryKeys\nstarBases=starBases\nattributes=attributes\nfilters=filters)
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
    buf.append(" datasetConfig=").append( datasetConfig==null ? "unset" : datasetConfig.getInternalName() );
    buf.append(" dataset=").append(dataset);
		buf.append(", dataSource=").append(dataSource);
		buf.append(", starBases=[").append(StringUtil.toString(starBases));
		buf.append("], primaryKeys=[").append(StringUtil.toString(primaryKeys));
		buf.append("], querytype=").append(querytype);
		buf.append(", attributes=").append(attributes);
		buf.append(", filters=").append(filters);

		if (sequenceDescription != null)
			buf.append(", sequencedescription=").append(sequenceDescription);

    if (hasSort) {
      buf.append(", sortBy=").append(sortAttributes);
    }
    
		buf.append(", limit=").append(limit);
		buf.append("]");

		return buf.toString();
	}

	/**
	 * Allows Equality Comparisons manipulation of Query objects
	 * Mainly for testing of copy constructor
	 */
	public boolean equals(Object o) {
		return o instanceof Query && hashCode() == ((Query) o).hashCode();
	}

	public int hashCode() {
		int tmp = 0;
		if (querytype == Query.SEQUENCE) {
			tmp = (31 * tmp) + sequenceDescription.hashCode();
			tmp = (31 * tmp) + querytype;
		}

		for (int i = 0, n = starBases.length; i < n; i++)
			tmp = (31 * tmp) + starBases[i].hashCode();

		for (int i = 0, n = primaryKeys.length; i < n; i++)
			tmp = (31 * tmp) + primaryKeys[i].hashCode();

		for (int i = 0, n = attributes.size(); i < n; i++) {
			FieldAttribute element = (FieldAttribute) attributes.get(i);
			tmp = (31 * tmp) + element.hashCode();
		}

		for (int i = 0, n = filters.size(); i < n; i++)
			tmp = (31 * tmp) + filters.get(i).hashCode();

		tmp *= 31;
		if (datasetConfig != null)
			tmp += datasetConfig.hashCode();

		tmp *= 31;
		if (queryName != null)
			tmp += queryName.hashCode();

		return tmp;
	}

	private List attributes = new Vector();
	private List filters = new Vector();

	private String queryName;
	private int querytype = Query.ATTRIBUTE;
	// default to ATTRIBUTE, over ride for SEQUENCE
	private SequenceDescription sequenceDescription;
	private String[] primaryKeys;
	private String[] starBases;
	private DatasetConfig datasetConfig;
	private int limit = 0; // add a limit clause to the SQL with an int > 0

	/**
	 * Datasource this query applies to.
	 */
	private DetailedDataSource dataSource;

	/**
	 * Name of dataset this query applies to.
	 */
	private String dataset;

	public QueryListener[] getQueryChangeListeners() {
		return (QueryListener[]) listeners.toArray(
			new QueryListener[listeners.size()]);
	}

	public synchronized void removeQueryChangeListener(QueryListener listener) {
		listeners.remove(listener);
	}

	/**
	 * Convenience method that removes all attributes from the query. Notifies listeners.
	 * @throws InvalidQueryException
	 */
	public synchronized void removeAllAttributes() throws InvalidQueryException {

		Attribute[] attributes = getAttributes();

		for (int i = 0, n = attributes.length; i < n; i++) {
			removeAttribute(attributes[i]);
		}

	}

	/**
	 * Removes all Filters from the query. Each removed Filter will
	 * generate a separate property change event.
	 * @throws InvalidQueryException
	 */
	public synchronized void removeAllFilters() {

		Filter[] filters = getFilters();

		for (int i = 0, n = filters.length; i < n; i++) {
			removeFilter(filters[i]);
		}

	}

	/**
	 * Removes all QueryChangeListeners. Note: does not notify
	 * listeners that they have been removed.
	 */
	public synchronized void removeAllQueryChangeListeners() {
		listeners.clear();
	}

	/**
	 * Replace the oldFilter with the new one. 
	 * @param oldFilter
	 * @param newFilter
	 * @throws RuntimeException if oldFilter is not currently in the query.
	 * TODO remove replaceFilter() + listener method.
	 */
	public synchronized void replaceFilter(Filter oldFilter, Filter newFilter) {

		int index = filters.indexOf(oldFilter);
		if (index == -1)
			throw new IllegalArgumentException(
				"Old filter can not be removed because not in query: " + oldFilter);

		filters.remove(index);
		filters.add(index, newFilter);
    log();

		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).filterChanged(
				this,
				index,
        oldFilter,
				newFilter);

	}

	public DetailedDataSource getDataSource() {
		return dataSource;
	}

	/**
	 * Sets the value and notifies listeners.
	 * @param dataSource new dataSource.
	 */
	public synchronized void setDataSource(DetailedDataSource dataSource) {
		DetailedDataSource oldDatasource = this.dataSource;
		this.dataSource = dataSource;
		log();
		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).datasourceChanged(
				this,
				oldDatasource,
				this.dataSource);
	}

	public String getDataset() {
		return dataset;
	}

	/**
	 * Sets the value and propagates a PropertyChange event to listeners. The 
	 * property name is in the event is "dataset". No event is propagated 
	 * if the parameter is equal to the current dataset.
	 * @param dataset new dataset.
	 */
	public synchronized void setDataset(String datasetName) {

		if (this.dataset == datasetName
			|| datasetName != null
			&& datasetName.equals(this.dataset))
			return;

		String oldDatasetName = this.dataset;
		this.dataset = datasetName;

		log();

		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).datasetChanged(
				this,
				oldDatasetName,
				this.dataset);
	}

	/**
	 * Returns the name that has been set for this Query, null if not set
	 * @return String Query name
	 */
	public String getQueryName() {
		return queryName;
	}

	/**
	 * @param string -- String name to apply to this Query.
	 */
	public synchronized void setQueryName(String queryName) {

		if (this.queryName != null && this.queryName.equals(queryName))
			return;

		String old = this.queryName;
		this.queryName = queryName;

		log();

		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).queryNameChanged(
				this,
				old,
				this.queryName);
	}

	/**
	 * @param listener
	 */
	public synchronized void addQueryChangeListener(QueryListener listener) {        
		listeners.add(listener);
	}

	/**
	 * Adds attribute at the specified index. 
	 * @param index position in array to add attribute.
	 * @param attribute item to be added to attributes array.
	 * @throws IllegalArgumentException if attribute is 
	 * null or already added.
	 */
	public synchronized void addAttribute(int index, Attribute attribute) {

		if (attribute == null)
			throw new IllegalArgumentException("Can not add a null attribute");
//		if (attributes.contains(attribute))
//			throw new IllegalArgumentException(
//				"attribute already present: " + attribute);

		attributes.add(index, attribute);
		log();
		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).attributeAdded(this, index, attribute);

	}

	/**
	 * Cause the result of toString() to be printed to the log if 
	 * logging level for this class is >= FINE. Useful for debugging
	 * and test purposes. Called automatically by all the state changing
	 * methods such as addFilter(...) and setDataset(...).
	 */
	public void log() {
		if (logger.isLoggable(Level.FINE))
			logger.fine(this.toString());
	}

	/**
	 * Unsets all property values.
	 */
	public synchronized void clear() {

		setDataSource(null);
		setDataset(null);
		setDatasetConfig(null);

		try {
            removeAllAttributes();
            removeAllFilters();
        } catch (InvalidQueryException e) {
            // ignore
        }

		setLimit(0);

		setPrimaryKeys(null);
		setMainTables(null);
	}

	public DatasetConfig getDatasetConfig() {
		return datasetConfig;
	}

	public synchronized void setDatasetConfig(DatasetConfig datasetConfig) {
		DatasetConfig old = this.datasetConfig;
		this.datasetConfig = datasetConfig;
		log();
		for (int i = 0; i < listeners.size(); ++i)
			((QueryListener) listeners.get(i)).datasetConfigChanged(
				this,
				old,
				datasetConfig);
	}
  
  //special sortby feature, only an advanced MartShell feature, so no need to link with the QueryListener
  private boolean hasSort = false;
  private List sortAttributes = null;
  
  public synchronized void addSortByAttribute(Attribute sortAtt) {
    if (!hasSort) {
      hasSort = true;
      sortAttributes = new ArrayList();
    }
    sortAttributes.add(sortAtt);
  }

  public synchronized void removeSortBy() {
    hasSort = false;
    sortAttributes = null;
  }
  
  public boolean hasSort() {
    return hasSort;
  }
  
  public Attribute[] getSortByAttributes() {
    if (!hasSort)
      return null;
    
    Attribute[] ret = new Attribute[sortAttributes.size()];
    sortAttributes.toArray(ret);
    return ret;
  }
}
