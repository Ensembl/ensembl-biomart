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
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

/**
 * <p>Stores the configuration information for a particular Mart. </p>
 * 
 * <p>The Mart Configuration Hierarchy consists of:</p>
 * <ul>
 *    <li><p>MartConfiguration: information for a particular mart database. It contains a List of
 *                                Datasets.</p>
 * 
 *         <ul>
 *           <li><p>DatasetConfig: contains the information for a particular star within a mart.
 *                          Holds lists of AttributePages and FilterPages</p>
 *               <ul>
 *               <li><p>AttributePage: contains a List of AttributeCollections.</p>
 *                <ul>
 *                    <li><p>AttributeCollection: contains a List of UIAttributes.</p>
 *                       <ul>
 *                           <li><p>AttributeDescription: holds all of the information needed by the UI
 *                                        for displaying an attribute and its information, and the 
 *                                        Attribute it needs to add to a mart Query.</p>
 *                       </ul>
 *                
 *                   <li><p>FilterPage:  contains a List of FilterCollections</p>
 *                     <ul>
 *                         <li><p>FilterCollection: contains a List of FilterDescription/MapFilterDescription objects</p>
 *                            <ul>
 *                                <li><p>FilterDescription: holds all of the information needed by the UI for displaying a filter,
 *                                             and the information that it needs to add a filter to a mart Query</p>
 *                                <li><p>MapFilterDescription: holds all of the information needed by the UI for displaying a Domain Specific Filter,
 *                                             and the information that it needs to add a Domain Specific Filter to a mart Query</p> 
 *                            </ul>
 *                </ul>
 *         </ul>
 * </ul>
 * <br>
 * <p>This framework allows for flexibility in the display and use of attributes and filters.  A UI for a simple mart would contain
 * one dataset containing one FilterPage and one AttributePage.  Each FilterPage and AttributePage would contain
 * one relevant Collection.  The resulting UI would be fairly simple, having one set of filters and one set of attributes
 * with no other information.  More complex marts could group attributes/filters into categories using their relevant
 * Collection objects, or even separate Collections of attributes or filters into separate Pages.  For example, the 
 * EnsMart Mart-Explorer implementation uses Pages to group collections of attributes into functional groups that 
 * cannot be combined with attributes from other Pages in a single Query.</p>  
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class MartConfiguration extends BaseNamedConfigurationObject {

	private FilterDescription layout = null;
	private int thisRank = 0;
	private TreeMap datasets = new TreeMap();
	private Hashtable datasetNameMap = new Hashtable();
	
	/**
	 * Constructs a MartConfiguration for a particular mart database,
	 * named by internalName.
	 * 
	 * @param internalName String name of the mart database for this configuration
	 */
	public MartConfiguration(String martname) throws ConfigurationException {
		this(martname, "", "");
	}

	/**
	 * Constructs a MartConfiguration for a particular mart database,
	 * named by internalName.  May also have a displayName to display in a UI, and
	 * a description.
	 * 
	 * @param internalName String name of the mart database for this configuration.  May not be null.
	 * @param displayName String name to display in a UI.
	 * @param description String description of the mart database for this configuration
	 * @throws ConfigurationException when internalName is null.
	 */
	public MartConfiguration(String internalName, String displayName, String description) throws ConfigurationException {

    super( internalName, displayName, description );
    		
	}

	/**
	 * Add a single DatasetConfig to the MartConfiguration.
	 * 
	 * @param d A DatasetConfig object
	 * @see DatasetConfig
	 */
	public void addDataset(DatasetConfig d) {
		Integer rankInt = new Integer(thisRank);
		datasets.put(rankInt, d);
		datasetNameMap.put(d.getInternalName(), rankInt);
		thisRank++;
	}

	/**
	 * Set a group of Datasets at once.
	 * Note, subsequent calls to setDatasets or addDataset will add
	 * datasets to what have been added before.
	 *  
	 * @param d DatasetConfig[] Array of Datasets.
	 */
	public void setDatasets(DatasetConfig[] d) {
		for (int i = 0, n = d.length; i < n; i++) {
			Integer rankInt = new Integer(thisRank);
			datasets.put(rankInt, d[i]);
			datasetNameMap.put(d[i].getInternalName(), rankInt);
			thisRank++;
		}
	}

	/**
	 * Returns an Array of DatasetConfig objects, in the order they were added.
	 * 
	 * @return DatasetConfig[]
	 */
	public DatasetConfig[] getDatasets() {
		DatasetConfig[] d = new DatasetConfig[datasets.size()];
		datasets.values().toArray(d);
		return d;
	}

	/**
	 * Returns a particular DatasetConfig based on a supplied DatasetConfig internalName.
	 * 
	 * @param internalName String internalName of the DatasetConfig
	 * @return DatasetConfig with the provided internalName, or null if not found
	 */
	public DatasetConfig getDatasetByName(String internalName) {
		if (datasetNameMap.containsKey(internalName))
			return (DatasetConfig) datasets.get((Integer) datasetNameMap.get(internalName));
		else
			return null;
	}

	/**
	 * Check for whether the MartConfiguration contains a particular DatasetConfig named by internalName.
	 * 
	 * @param internalName String internalName of a DatasetConfig
	 * @return boolean, true if the MartConfiguration contains the DatasetConfig named by the given internalName
	 */
	public boolean containsDataset(String internalName) {
		return datasetNameMap.containsKey(internalName);
	}

	/**
	 * Get the layout FilterDescription Object, which instructs the UI on how to display the available datasets to the user.
	 * @return FilterDescription layout
	 */
	public FilterDescription getLayout() {
		return layout;
	}

	/**
	 * Set the Layout, which instructs the UI on how to display the available datasets to the user.
	 * @param FilterDescription description
	 */
	public void setLayout(FilterDescription description) {
		layout = description;
	}

  /**
   * Returns a List of potential dataset internalNames for the MartCompleter command
   * completion system.
   * @return List of internalNames
   */
  public List getCompleterNames() {
  	List names = new ArrayList();
  	
  	for (Iterator iter = datasets.values().iterator(); iter.hasNext();) {
			DatasetConfig dset = (DatasetConfig) iter.next();
			names.add(dset.getInternalName());
		}
  	return names;
  }
  
	/**
	 * String representation of a MartConfiguration useful in debugging output.
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
		buf.append( super.toString() );
		buf.append(", datasets=").append(datasets);
    buf.append(", layout=").append( layout );
		buf.append("]");

		return buf.toString();
	}

	/**
	 * Allows equality comparisons of MartConfiguration objects
	 */
	public boolean equals(Object o) {
		return o instanceof MartConfiguration && hashCode() == o.hashCode();
	}

	/* (non-Javadoc)
	 * @see org.ensembl.mart.lib.config.BaseNamedConfigurationObject#isBroken()
	 */
	public boolean isBroken() {
		// TODO Auto-generated method stub
		return false;
	}  
}
