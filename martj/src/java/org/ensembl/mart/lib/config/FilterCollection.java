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

/**
 * Container for a group of Mart FilterDescriptions.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class FilterCollection extends BaseNamedConfigurationObject {

  private boolean hasBrokenFilters = false;
  // FilterDescriptions
  private List filters = new ArrayList();
  private Hashtable filterNameMap = new Hashtable();

  //cache one FilterDescription for call to containsFilterDescription or getFiterDescriptionByInternalName
  private FilterDescription lastFilt = null;

  //cache one FilterDescription for call to supports
  private FilterDescription lastSupportFilt = null;
  
	protected final String enableSelectAllKey = "enableSelectAll";
  private final String[] titles = new String[] { 
			 enableSelectAllKey	 
  };
  
  /**
   * Copy Constructor. Constructs an exact copy of an existing
   * FilterCollection.
   * @param fc FilterCollection to copy.
   */
  public FilterCollection(FilterCollection fc) {
  	super(fc);
  	
  	List fds = fc.getFilterDescriptions();
  	for (int i = 0, n = fds.size(); i < n; i++) {
      Object fd = fds.get(i);
      if (fd instanceof FilterDescription)
        addFilterDescription( new FilterDescription( (FilterDescription) fd) );
      //else not needed      
    }  
  }
  
  /**
   * Empty Constructor should only be used by DatasetConfigEditor.
   *
   */
  public FilterCollection() {
    super();
    
    for (int i = 0, n = titles.length; i < n; i++) {
      setAttribute(titles[i], null); //establishes the order of the keys, and adds all possible attribute titles to getXMLAttributeTitles, even if never set in future
    }
  }
  
	/**
	 * Constructor for a FilterCollection named by intenalName
	 * 
	 * @param internalName String name to internally represent the FilterCollection.  Must not be null.
	 * @throws ConfigurationException when paremeter requirements are not met
	 */
	public FilterCollection(String internalName) throws ConfigurationException {
		this(internalName, "", "", "");
	}

	/**
	 * Constructor for a FilterCollection named by intenalName, with a displayName, and description.
	 * 
	 * @param internalName String name to internally represent the FilterCollection.  Must not be null or empty.
	 * @param displayName String name to represent the FilterCollection. 
	 * @param description String description of the FilterCollection.
	 * @throws ConfigurationException when paremeters are null or empty
	 */
	public FilterCollection(String internalName, String displayName, String description, String enableSelectAll) throws ConfigurationException {

		super(internalName, displayName, description);
		setAttribute(enableSelectAllKey, enableSelectAll);
	}

	/**
	 * Add a FilterDescription object to this FilterCollection.
	 * 
	 * @param f a FilterDescription object
	 */
	public void addFilterDescription(FilterDescription f) {
		filters.add(f);
		filterNameMap.put(f.getInternalName(), f);
	}

  /**
   * Remove a FilterDescription from the FilterCollection.
   * @param f -- FilterDescription to remove.
   */
  public void removeFilterDescription(FilterDescription f) {
    filterNameMap.remove(f.getInternalName());
    filters.remove(f);
    // fix to stop containsFilterDescription breaking after a remove
    if (lastFilt != null && f.getInternalName().equals(lastFilt.getInternalName()))
    	lastFilt = null;
  }
  
  /**
   * Insert a FilterDescription at a specific position within the List of FilterDescriptions within this FilterCollection.
   * FilterDescription Objects occuring at or after the given position will be shifted right.
   * @param position -- position at which to insert the FilterDescription 
   * @param f -- FilterDescription to be inserted.
   */
  public void insertFilterDescription(int position, FilterDescription f) {
    filters.add(position, f);
    filterNameMap.put(f.getInternalName(), f);
  }
  
  /**
   * Insert a FilterDescription before a specific FilterDescription, named by internalName.
   * @param internalName -- internalName of FilterDescription before which the given FilterDescription should be inserted.
   * @param f -- FilterDescription to insert.
   * @throws ConfigurationException when the FilterCollection does not contain a FilterDescription named by internalName.
   */
  public void insertFilterDescriptionBeforeFilterDescription(String internalName, FilterDescription f) throws ConfigurationException {
    if (!filterNameMap.containsKey(internalName))
      throw new ConfigurationException("FilterCollection does not contain FilterDescription " + internalName + "\n");
    insertFilterDescription( filters.indexOf( filterNameMap.get(internalName) ), f );
  }
  
  /**
   * Insert a FilterDescription after a specific FilterDescription, named by internalName.
   * @param internalName -- internalName of FilterDescription after which the given FilterDescription should be inserted.
   * @param f -- FilterDescription to insert.
   * @throws ConfigurationException when the FilterCollection does not contain a FilterDescription named by internalName.
   */
  public void insertFilterDescriptionAfterFilterDescription(String internalName, FilterDescription f) throws ConfigurationException {
    if (!filterNameMap.containsKey(internalName))
      throw new ConfigurationException("FilterCollection does not contain FilterDescription " + internalName + "\n");
    insertFilterDescription( filters.indexOf( filterNameMap.get(internalName) ) + 1, f );
  }
  
	/**
	 * Add a group of FilterDescription objects in one call.
	 * Note, subsequent calls to addFilterDescription and addFilterDescriptions will add to what has
	 * been added previously.
	 * 
	 * @param f an array of FilterDescription objects.
	 */
	public void addFilterDescriptions(FilterDescription[] f) {
		for (int i = 0, n = f.length; i < n; i++) {
			filters.add(f[i]);
			filterNameMap.put(f[i].getInternalName(), f[i]);
		}
	}

	/**
	 * Returns a List of FilterDescription objects, 
	 * in the order they were added.
	 * 
	 * @return List of FilterDescription objects
	 */
	public List getFilterDescriptions() {
		return new ArrayList(filters);
	}

	/**
	 * Returns a specific FilterDescription, named by internalName, or
	 * containing an Option named by internalName.
	 * 
	 * @param internalName String name of the requested FilterDescription
	 * @return FilterDescription requested, or null.
	 */
	public FilterDescription getFilterDescriptionByInternalName(String internalName) {
		if (containsFilterDescription(internalName))
			return lastFilt;
		else
			return null;
	}

	/**
	 * Check if this FilterCollection contains a specific FilterDescription object.
	 * 
	 * @param internalName String name of the requested FilterDescription
	 * @return boolean, true if FilterCollection contains the FilterDescription, false if not.
	 */
	public boolean containsFilterDescription(String internalName) {
		boolean contains = false;
		if (lastFilt == null) {
			contains = filterNameMap.containsKey(internalName);

			if (contains)
				lastFilt = (FilterDescription) filterNameMap.get(internalName);
			else if ( ( internalName.indexOf(".") > 0 ) && !( internalName.endsWith(".") ) ) {
				String[] testNames = internalName.split("\\.");
				String testRefName = testNames[0]; // x in x.y
				String testIname = testNames[1]; // y in x.y

        		if (filterNameMap.containsKey(testIname)) {
        			// y is an actual filter, with its values stored in a PushOption in another Filter					
					lastFilt = (FilterDescription) filterNameMap.get(testIname);
					contains = true;
				} 
				else {
					// y may be a Filter stored in a PushOption within another Filter
					for (Iterator iter = filters.iterator(); iter.hasNext();) {
						FilterDescription element = (FilterDescription) iter.next();

						if (element.containsOption(testRefName)) {
							Option superOption = element.getOptionByInternalName(testRefName);
							
							PushAction[] pos = superOption.getPushActions();
							for (int i = 0, n = pos.length; i < n; i++) {
								PushAction po = pos[i];
								if (po.containsOption(testIname)) {
									lastFilt = element;
									contains = true;
									break;
								}
							}
						}
						
						if (contains)
						  break;
					}
				}
			} 
			else {
				for (Iterator iter = filters.iterator(); iter.hasNext();) {
					FilterDescription element = (FilterDescription) iter.next();
					if (element.containsOption(internalName)) {
						lastFilt = element; 
						//lastFilt = new FilterDescription(element.getOptionByInternalName(internalName));
						contains = true;
						break;
					}
				}
			}
		} 
		else {// lastFilt != null
			if (lastFilt.getInternalName().equals(internalName))
				contains = true;
			else if (lastFilt.containsOption(internalName))
				contains = true;
			else if ( (internalName.indexOf(".") > 0) &&  !(internalName.endsWith(".")) && lastFilt.getInternalName().equals( internalName.split("\\.")[1] ) )
				contains = true;
			else if (lastFilt.getInternalName().matches("\\w+\\." + internalName)){
					contains = true;
					internalName = lastFilt.getInternalName();  
			}	
			else {
				lastFilt = null;
				contains = containsFilterDescription(internalName);
			}
		}
		return contains;
	}

	/**
	 * Get a FilterDescription for a given field and tableConstraint.  Useful for mapping the field and tableConstraint from a Filter
	 * object added to a Query object back to its MartConfiguration FilterDescription.
	 * @param field -- String mart database field
	 * @param tableConstraint -- String mart database tableConstraint
   * @param qualifier -- Filter qualifier
	 * @return FilterDescription supporting the given field and tableConstraint, or null.
	 */
	public FilterDescription getFilterDescriptionByFieldNameTableConstraint(String field, String tableConstraint, String qualifier) {
		if (supports(field, tableConstraint, qualifier))
			return lastSupportFilt;
		else
			return null;
	}

	/**
	 * Determine if this FilterCollection contains a FilterDescription supporting a given field and tableConstraint.
	 * @param field - String field of a mart database table
	 * @param TableConstraint -- String tableConstraint of a mart database table
   * @param qualifier -- Filter qualifier
	 * @return boolean, true if a FilterDescription contained within this collection supports the field and tableConstraint, false otherwise.
	 */
	public boolean supports(String field, String TableConstraint, String qualifier) {
		boolean supports = false;

		if (lastSupportFilt == null) {
			for (Iterator iter = filters.iterator(); iter.hasNext();) {
				Object element = iter.next();

				if (element instanceof FilterDescription) {
					if (((FilterDescription) element).supports(field, TableConstraint,qualifier)) {
						lastSupportFilt = (FilterDescription) element;
						supports = true;
						break;
					}
				}
			}
		} else {
			if (lastSupportFilt.supports(field, TableConstraint, qualifier))
				supports = true;
			else {
				lastSupportFilt = null;
				supports = supports(field, TableConstraint,qualifier);
			}
		}
		return supports;
	}

	/**
	 * Returns a List of possible internalNames to add to the MartCompleter command completion system.
	 * internalNames may be of the form x.y.  Also, internalNames that are not of the form x.y, but are found to be equal to y in
	 * another internalName of form x.y will not be added as potential completers. 
	 * @return List of potential completer names
	 */
	public List getCompleterNames() {
		List names = new ArrayList();

		for (Iterator iter = filters.iterator(); iter.hasNext();) {
			FilterDescription element = (FilterDescription) iter.next();
            if (element.getHidden() != null && element.getHidden().equals("true")) continue;
            if (element.getDisplay() != null && element.getDisplay().equals("true")) continue;
			names.addAll(element.getCompleterNames());
		}

		return names;
	}

	/**
	 * Allows MartShell to get all values associated with a given internalName (which may be of form x.y).
	 * Behaves differently than getFilterDescriptionByInternalName when internalName is x.y and y is the name of
	 * an actual filterDescription.
	 * @param internalName
	 * @return List of values to complete
	 */
	public List getCompleterValuesByInternalName(String internalName) {
		if (internalName.indexOf(".") > 0)
			return getFilterDescriptionByInternalName(internalName.split("\\.")[0]).getCompleterValues(internalName);
		else
			return getFilterDescriptionByInternalName(internalName).getCompleterValues(internalName);
	}

	/**
	 * Allows MartShell to get all qualifiers associated with a given internalName (which may be of form x.y).
	 * Behaves differently than getFilterDescriptionByInternalName when internalName is x.y and y is the name of
	 * an actual filterDescription.
	 * @param internalName
	 * @return List of qualifiers to complete
	 */
	public List getFilterCompleterQualifiersByInternalName(String internalName) {
		if (internalName.indexOf(".") > 0 && !(internalName.endsWith(".")) && containsFilterDescription( internalName.split("\\.")[1] ) ) {
			String refname = internalName.split("\\.")[1];
			return getFilterDescriptionByInternalName(refname).getCompleterQualifiers(refname);
		} else
			return getFilterDescriptionByInternalName(internalName).getCompleterQualifiers(internalName);
	}

	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
		buf.append(super.toString());
		buf.append(", FilterDescriptions=").append(filters);
		buf.append("]");

		return buf.toString();
	}

	/**
	 * Allows Equality Comparisons manipulation of FilterCollection objects
	 */
	public boolean equals(Object o) {
		return o instanceof FilterCollection && hashCode() == o.hashCode();
	}

	public int hashCode() {
		int hashcode = super.hashCode();

		for (Iterator iter = filters.iterator(); iter.hasNext();) {
			Object element = iter.next();
			hashcode = (31 * hashcode) + element.hashCode();
		}

		return hashcode;
	}

  /**
   * set the hasBrokenFilters flag to true, meaning that one or more FilterDescription Objects (or their child Option Objects)
   * within this FilterCollection contain invalid field/tableConstraint references to a specific Mart instance.
   */
  public void setFiltersBroken() {
     hasBrokenFilters = true;
  }
  
  /**
   * Determine if this FilterCollection has FilterDescriptions that are broken.
   * @return boolean
   */
  public boolean hasBrokenFilters() {
  	return hasBrokenFilters;
  }
  
  /**
   * True if hasBrokenFilters is true.
   * @return boolean
   */
  public boolean isBroken() {
  	return hasBrokenFilters;
  }

  public boolean containsOnlyPointerFilters() {
      boolean ret = true;
      
      List filters = getFilterDescriptions();
      for (int i = 0, n = filters.size(); i < n; i++) {
          FilterDescription filt = (FilterDescription) filters.get(i);
          if (filt.getHidden() != null && filt.getHidden().equals("true")) continue;
          if (filt.getDisplay() != null && filt.getDisplay().equals("true")) continue;
          
          if (filt.getInternalName().indexOf('.') < 0) {
              ret = false;
              break;
          }
      }
      
      return ret;
  }
  
  public boolean containsOnlyFilterListFilterUploadFilters() {
      boolean ret = true;
      
      List filters = getFilterDescriptions();
      for (int i = 0, n = filters.size(); i < n; i++) {
          FilterDescription filt = (FilterDescription) filters.get(i);
          if (filt.getFilterList() == null || filt.getFilterList().length() < 1) {
              ret = false;
              break;
          }
      }
      
      return ret;
  }
	
	public void setEnableSelectAll(String value) {
		setAttribute(enableSelectAllKey, value);
	}

	public String getEnableSelectAll() {
		return getAttribute(enableSelectAllKey);
	}
}
