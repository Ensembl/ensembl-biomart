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
 * Container for a group of Mart FilterCollections.  Allows categorical grouping of collections
 * of filters.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class FilterGroup extends BaseNamedConfigurationObject {

  private boolean hasBrokenCollections = false;
  private List filterCollections = new ArrayList();
  private Hashtable filterCollectionNameMap = new Hashtable();

  //cache one FilterDescription for call to containsFilterDescription or getUIFiterDescriptionByName
  private FilterDescription lastFilt = null;

  //cache one FilterCollection for call to getCollectionForFilter
  private FilterCollection lastColl = null;

  //cache one FilterDescription for call to supports
  private FilterDescription lastSupportingFilter = null;
  
  /**
   * Copy constructor.  Constructs a new FilterGroup which is an exact copy
   * of an existing FilterGroup.
   * @param fg FilterGroup to copy.
   */
  public FilterGroup(FilterGroup fg) {
  	super(fg);
  	
  	FilterCollection[] fcs = fg.getFilterCollections();
  	for (int i = 0, n = fcs.length; i < n; i++) {
      addFilterCollection( new FilterCollection( fcs[i] ) );
    }
  }
  
  public FilterGroup() {
    super();
  }
  
	/**
	 * Constructor for a FilterGroup represented internally by internalName.
	 * 
	 * @param internalName name to internally represent the FilterGroup
	 * @throws ConfigurationException when internalName is null or empty
	 */
	public FilterGroup(String internalName) throws ConfigurationException {
		this(internalName, "", "");
	}

	/**
	 * Constructor for a FilterGroup named internally by internalName, with a displayName, and a description.
	 * 
	 * @param internalName String name to internally represent the filterGroup. Must not be null.
	 * @param displayName
	 * @param description
	 * @throws ConfigurationException when internalName is null or empty.
	 */
	public FilterGroup(String internalName, String displayName, String description) throws ConfigurationException {
		super(internalName, displayName, description);
	}

	/**
	 * Add a FilterCollection to the FilterGroup
	 * 
	 * @param f a FilterCollection object
	 */
	public void addFilterCollection(FilterCollection f) {
		filterCollections.add(f);
		filterCollectionNameMap.put(f.getInternalName(), f);
	}

  /**
   * Remove a FilterCollection from this FilterGroup.
   * @param f -- FilterCollection to be removed.
   */
  public void removeFilterCollection(FilterCollection f) {
    filterCollectionNameMap.remove(f.getInternalName());
    filterCollections.remove(f);
  }
  
  /**
   * Insert a FilterCollection at a specific position within the List of FilterCollections.
   * FilterCollection Objects occuring at or after the specified position are shifted right.
   * @param position -- position at which to insert the FilterCollection
   * @param f -- FilterCollection to insert.
   */
  public void insertFilterCollection(int position, FilterCollection f) {
    filterCollections.add(position, f);
    filterCollectionNameMap.put(f.getInternalName(), f);
  }
  
  /**
   * Insert a FilterCollection before a specific FilterCollection, named by internalName.
   * @param internalName -- internalName of FilterCollection before which the given FilterCollection should be inserted.
   * @param f -- FilterCollection to be inserted.
   * @throws ConfigurationException when the FilterGroup does not contain a FilterCollection named by internalName.
   */
  public void insertFilterCollectionBeforeFilterCollection(String internalName, FilterCollection f) throws ConfigurationException {
    if (!filterCollectionNameMap.containsKey(internalName))
      throw new ConfigurationException("FilterGroup does not contain a FilterCollection named by " + internalName +"\n");
    insertFilterCollection( filterCollections.indexOf( filterCollectionNameMap.get(internalName) ) , f);
  }
  
  /**
   * Insert a FilterCollection after a specific FilterCollection, named by internalName.
   * @param internalName -- internalName of FilterCollection after which the given FilterCollection should be inserted.
   * @param f -- FilterCollection to be inserted.
   * @throws ConfigurationException when the FilterGroup does not contain a FilterCollection named by internalName.
   */
  public void insertFilterCollectionAfterFilterCollection(String internalName, FilterCollection f) throws ConfigurationException {
    if (!filterCollectionNameMap.containsKey(internalName))
      throw new ConfigurationException("FilterGroup does not contain a FilterCollection named by " + internalName +"\n");
    insertFilterCollection( filterCollections.indexOf( filterCollectionNameMap.get(internalName) ) + 1, f);
  }
  
	/**
	 * Add a group of FilterCollection objects in one call.  Note, subsequent calls
	 * to addFilterCollection or addFilterCollections will add to what was previously added.
	 * 
	 * @param f an Array of FilterCollection objects
	 */
	public void addFilterCollections(FilterCollection[] f) {
		for (int i = 0, n = f.length; i < n; i++) {
			filterCollections.add(f[i]);
			filterCollectionNameMap.put(f[i].getInternalName(), f[i]);
		}
	}

	/**
	 * Returns an array of FilterCollection objects, in the order they were added
	 * 
	 * @return Array of FilterCollection objects
	 */
	public FilterCollection[] getFilterCollections() {
		FilterCollection[] fc = new FilterCollection[filterCollections.size()];
		filterCollections.toArray(fc);
		return fc;
	}

	/**
	 * Returns a particular FilterCollection named by internalName
	 * 
	 * @param internalName String name of the requested FilterCollection
	 * 
	 * @return a FilterCollection object, or null
	 */
	public FilterCollection getFilterCollectionByName(String internalName) {
		if (filterCollectionNameMap.containsKey(internalName))
			return (FilterCollection) filterCollectionNameMap.get(internalName);
		else
			return null;
	}

	/**
	 * Check if a FilterGroup contains a given FilterCollection, of name internalName
	 * 
	 * @param internalName String name of the requested FilterCollection
	 * @return boolean true if FilterGroup contains the FilterCollection, false if not
	 */
	public boolean containsFilterCollection(String internalName) {
		return filterCollectionNameMap.containsKey(internalName);
	}

	/**
		* Convenience method for non graphical UI.  Allows a call against the FilterGroup for a particular FilterDescription object.
		* Note, it is best to first call containsFilterDescription, as there is a caching system to cache a FilterDescription during a call 
		* to containsFilterDescription.
		* 
		* @param internalName name of the requested FilterDescription
		* @return requested FilterDescription, or null.
		*/
	public FilterDescription getFilterDescriptionByInternalName(String internalName) {
		if (containsFilterDescription(internalName))
			return lastFilt;
		else
			return null;
	}

	/**
		* Convenience method for non graphical UI.  Can determine if the FilterGroup contains a specific FilterDescription object.
		*  As an optimization for initial calls to containsFilterDescription with an immediate call to getFilterDescriptionByInternalName if
		*  found, this method caches the FilterDescription it has found.
		* 
		* @param internalName name of the requested FilterDescription object
		* @return boolean, true if found, false if not.
		*/
	public boolean containsFilterDescription(String internalName) {
		boolean contains = false;

		if (lastFilt == null) {
			if ( ( internalName.indexOf(".") > 0 ) && !( internalName.endsWith(".") ) ) {
				String[] refs = internalName.split("\\.");
				if (refs.length > 1 && containsFilterDescription( refs[1] ) )
					contains = true;
			}

			if (!contains) {
				for (Iterator iter = (Iterator) filterCollections.iterator(); iter.hasNext();) {
					FilterCollection collection = (FilterCollection) iter.next();
					
					if (collection.containsFilterDescription(internalName)) {
						lastFilt = collection.getFilterDescriptionByInternalName(internalName);
						contains = true;
						break;
					}
				}
			}
		} else {
			if ( lastFilt.getInternalName().equals(internalName) )
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
	 * Convenience method to get all FilterDescription objects 
	 * contained in all FilterCollections in this FilterGroup.
	 * 
	 * @return List of FilterDescription objects.
	 */
	public List getAllFilterDescriptions() {
		List filts = new ArrayList();

		for (Iterator iter = filterCollections.iterator(); iter.hasNext();) {
			FilterCollection fc = (FilterCollection) iter.next();

			filts.addAll(fc.getFilterDescriptions());
		}

		return filts;
	}

	/**
	 * Returns the FilterCollection for a particular FilterDescription
	 * based on its internalName.
	 * 
	 * @param internalName - String internalName of the Filter Description for which the collection is being requested.
	 * @return FilterCollection for the FilterDescription provided, or null
	 */
	public FilterCollection getCollectionForFilter(String internalName) {
		if (!containsFilterDescription(internalName))
			return null;
		else if (lastColl == null) {
			for (Iterator iter = filterCollections.iterator(); iter.hasNext();) {
				FilterCollection fc = (FilterCollection) iter.next();

				if (fc.containsFilterDescription(internalName)) {
					lastColl = fc;
					break;
				}
			}
			return lastColl;
		} else {
			if (lastColl.getInternalName().equals(internalName))
				return lastColl;
			else {
				lastColl = null;
				return getCollectionForFilter(internalName);
			}
		}
	}

	/**
	 * Get a FilterDescription object that supports a given field and tableConstraint.  Useful for mapping from a Filter object
	 * added to a Query back to its FilterDescription.
	 * @param field -- String field of a mart database table
	 * @param tableConstraint -- String tableConstraint of a mart database
   * @param qualifier -- Filter qualifier
	 * @return FilterDescription object supporting the given field and tableConstraint, or null.
	 */
	public FilterDescription getFilterDescriptionByFieldNameTableConstraint(String field, String tableConstraint, String qualifier) {
		if (supports(field, tableConstraint, qualifier))
			return lastSupportingFilter;
		else
			return null;
	}

	/**
	 * Determine if this FilterGroup contains a FilterDescription that supports a given field and tableConstraint.
	 * Calling this method will cache any FilterDescription that supports the field and tableConstraint, and this will
	 * be returned by a getFilterDescriptionByFieldNameTableConstraint call.
	 * @param field -- String field of a mart database table
	 * @param tableConstraint -- String tableConstraint of a mart database
   * @param qualifier -- Filter qualifier
	 * @return boolean, true if the FilterGroup contains a FilterDescription supporting a given field, tableConstraint, false otherwise.
	 */
	public boolean supports(String field, String tableConstraint, String qualifier) {
		boolean supports = false;

		if (lastSupportingFilter == null) {
			for (Iterator iter = filterCollections.iterator(); iter.hasNext();) {
				FilterCollection element = (FilterCollection) iter.next();
				if (element.supports(field, tableConstraint, qualifier)) {
					lastSupportingFilter = element.getFilterDescriptionByFieldNameTableConstraint(field, tableConstraint, qualifier);
					supports = true;
					break;
				}
			}
		} else {
			if (lastSupportingFilter.supports(field, tableConstraint, qualifier))
				supports = true;
			else {
				lastSupportingFilter = null;
				supports = supports(field, tableConstraint,qualifier);
			}
		}
		return supports;
	}

	/**
	 * Retruns a List of possible Completion names for filters to the MartCompleter command completion system.
	 * @return List possible completions
	 */
	public List getCompleterNames() {
		List names = new ArrayList();

		for (Iterator iter = filterCollections.iterator(); iter.hasNext();) {
			FilterCollection element = (FilterCollection) iter.next();
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

	/**
	 * debug output
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
		buf.append(super.toString());
		buf.append(", filterCollections=").append(filterCollections);
		buf.append("]");

		return buf.toString();
	}

	/**
	 * Allows Equality Comparisons manipulation of FilterGroup objects
	 */
	public boolean equals(Object o) {
		return o instanceof FilterGroup && hashCode() == o.hashCode();
	}

	public int hashCode() {
		int tmp = super.hashCode();

		for (Iterator iter = filterCollections.iterator(); iter.hasNext();) {
			FilterCollection element = (FilterCollection) iter.next();
			tmp = (31 * tmp) + element.hashCode();
		}

		return tmp;
	}

  /**
   * Set the hasBrokenCollections flag to true, meaning
   * one or more FilterCollection objects contain FilterDescriptions
   * with invalid field, tableConstraint, or Options.
   */
  public void setCollectionsBroken() {
    hasBrokenCollections = true;    
  }
  
  /**
   * Determine if this FilterGroup has broken Collections
   * @return boolean
   */
  public boolean hasBrokenCollections() {
  	return hasBrokenCollections;
  }
  
  /**
   * True if hasBrokenCollections is true.
   * @return boolean
   */
  public boolean isBroken() {
  	return hasBrokenCollections;
  }

public boolean containsOnlyPointerFilters() {
   boolean contains = true;
   FilterCollection[] cols = getFilterCollections();
   for (int i = 0, n = cols.length; i < n; i++) {
     if (cols[i].getHidden() != null && cols[i].getHidden().equals("true")) continue;
     if (cols[i].getDisplay() != null && cols[i].getDisplay().equals("true")) continue;
     
     if (!cols[i].containsOnlyPointerFilters()) {
         contains = false;
         break;
     }
   }
   return contains;
}
}
