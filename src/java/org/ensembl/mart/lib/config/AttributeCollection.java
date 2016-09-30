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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class AttributeCollection extends BaseNamedConfigurationObject {

  private Logger logger = Logger.getLogger(AttributeCollection.class.getName());
  
  /**
   * The default maxSelect is 0, meaning no limit 
   */
  public final int DEFAULTMAXSELECT = 0;
  private boolean hasBrokenAttributes = false;
  
  private List AttributeDescriptions = new ArrayList();
  private Hashtable attributeDescriptionNameMap = new Hashtable();
  
  private List AttributeLists = new ArrayList();
  private Hashtable attributeListNameMap = new Hashtable();

  //cache one AttributeDescription for call to supports/getAttributeDescriptionByFieldNameTableConstraint
  private AttributeDescription lastSupportingAttribute = null;

  private final String maxSelectKey = "maxSelect";
	protected final String enableSelectAllKey = "enableSelectAll";
private final String[] titles = new String[] { 
		 maxSelectKey,
		 enableSelectAllKey
};

  /**
   * Copy constructor.  Constructs a new AttributeCollection which
   * is an exact copy of an existing AttributeCollection.
   * @param ac AttributeCollection to copy
   */
  public AttributeCollection(AttributeCollection ac) {
  	super(ac);
  	
  	List ads = ac.getAttributeDescriptions();
  	for (int i = 0, n = ads.size(); i < n; i++) {
      Object ad = ads.get(i);
      if (ad instanceof AttributeDescription)
        addAttributeDescription( new AttributeDescription( (AttributeDescription) ad ) );
      //else not needed      
    } 
  	ads = ac.getAttributeLists();
  	for (int i = 0, n = ads.size(); i < n; i++) {
        Object ad = ads.get(i);
        if (ad instanceof AttributeList)
          addAttributeList( new AttributeList( (AttributeList) ad ) );
        //else not needed      
      }   
  }
  /**
   * Empty Constructor should only be used by DatasetConfigEditor.
   *
   */
  public AttributeCollection() {
    super();
    
    for (int i = 0, n = titles.length; i < n; i++) {
      setAttribute(titles[i], null); //establishes the order of the keys, and adds all possible attribute titles to getXMLAttributeTitles, even if never set in future
    }
  }
  
	/**
	 * Constructor for an AttributeCollection named by internalName, with a type.
	 * 
	*  @param internalName String name to internally represent the AttributeCollection.  Must not be null
	 * @param maxSelect String maximum allowable combined attribute selections. 0 means no limit.
	 * @throws ConfigurationException when the required values are null or empty.
	 */
	public AttributeCollection(String internalName) throws ConfigurationException {
		this(internalName, "0", "", "", "");
	}

	/**
	 * Constructor for a AttributeCollection named by internalName, with a displayName, type and maxSelect value.
	 * May have description description.
	 * 
	 * @param internalName String name to internally represent the AttributeCollection.  Must not be null
	 * @param maxSelect String maximum allowable combined attribute selections. 0 means no limit.
	 * @param displayName String name to represent the AttributeCollection.
	 * @param description String description of the AttributeCollection
	 * @throws ConfigurationException if required parameters are null or empty, and if Integer.parseInt(maxSelect) throws a NumberFormatException .
	 */
	public AttributeCollection(String internalName, String maxSelect, String displayName, String description, String enableSelectAll) throws ConfigurationException {
    super( internalName, displayName, description);
    setAttribute(maxSelectKey, maxSelect);
    setAttribute(enableSelectAllKey, enableSelectAll);
	}

  /**
   * Set the maxSelect value for this AttributeCollection
   * @param maxSelect -- String value to limit selections of Attributes in groups. 0 means no limit.
   */
  public void setMaxSelect(String maxSelect){
  	setAttribute(maxSelectKey, maxSelect);
  }
  

  public String getMaxSelectString()  {
  	return getAttribute(maxSelectKey);
  }
	/**
	 * Returns the maxSelect value for attributes in this AttributeCollection.
	 * If the value for maxSelect provided is not a valid int (eg.
	 * Integer.parseInt( maxSelect) throws a NumberFormatException)
	 * this method returns DEFAULTMAXSELECT.
	 * 
	 * @return int maxSelect value
	 */
	public int getMaxSelect()  {
		try {
			 return Integer.parseInt( getAttribute(maxSelectKey) );
		} catch (NumberFormatException e) {
			if (logger.isLoggable(Level.FINE))
			  logger.fine("maxSelect value " + getAttribute(maxSelectKey) + " could not be parsed into an int: " + e.getMessage());
			return DEFAULTMAXSELECT;
		}
	}

	/**
	 * Add a AttributeDescription to the AtttributeCollection.
	 * 
	 * @param a a AttributeDescription object.
	 */
	public void addAttributeDescription(AttributeDescription a) {
		AttributeDescriptions.add(a);
		attributeDescriptionNameMap.put(a.getInternalName(), a);
	}

  /**
   * Remove an AttributeDescription from this AttributeCollection.
   * @param a -- AttributeDescription to be removed.
   */
  public void removeAttributeDescription(AttributeDescription a) {
    attributeDescriptionNameMap.remove(a.getInternalName());
    AttributeDescriptions.remove(a);
  }
  
  /**
   * Insert an AttributeDescription at a particular position within the AttributeCollection.
   * AttributeDescriptions set at or after the given position are shift right.
   * @param position -- position at which to insert the given AttributeDescription
   * @param a -- AttributeDescription to insert
   */
  public void insertAttributeDescription(int position, AttributeDescription a) {
    AttributeDescriptions.add(position, a);
    attributeDescriptionNameMap.put(a.getInternalName(), a);
  }
  
  /**
   * Insert an AttributeDescription before a specific AttributeDescription, named by internalName.
   * @param internalName -- AttributeDescription before which the given AttributeDescription should be inserted.
   * @param a -- AttributeDescription to insert.
   * @throws ConfigurationException when the AttributeCollection does not contain an AttributeDescription named by internalName.
   */
  public void insertAttributeDescriptionBeforeAttributeDescription(String internalName, AttributeDescription a) throws ConfigurationException {
    if (!attributeDescriptionNameMap.containsKey(internalName))
      throw new ConfigurationException("AttributeCollection does not contain AttributeDescription " + internalName + "\n");
    insertAttributeDescription( AttributeDescriptions.indexOf( attributeDescriptionNameMap.get(internalName) ), a );
  }
  
  /**
   * Insert an AttributeDescription after a specific AttributeDescription, named by internalName.
   * @param internalName -- AttributeDescription after which the given AttributeDescription should be inserted.
   * @param a -- AttributeDescription to insert.
   * @throws ConfigurationException when the AttributeCollection does not contain an AttributeDescription named by internalName.
   */
  public void insertAttributeDescriptionAfterAttributeDescription(String internalName, AttributeDescription a) throws ConfigurationException {
    if (!attributeDescriptionNameMap.containsKey(internalName))
      throw new ConfigurationException("AttributeCollection does not contain AttributeDescription " + internalName + "\n");
    insertAttributeDescription( AttributeDescriptions.indexOf( attributeDescriptionNameMap.get(internalName) ) + 1, a );
  }
  
	/**
	 * Add a group of AttributeDescription objects in one call.  Note, subsequent calls to addAttributeDescription or addAttributeDescriptions
   * will add to what was added before.
	 * 
	 * @param a an Array of AttributeDescription objects.
	 */
	public void addAttributeDescriptions(AttributeDescription[] a) {
		for (int i = 0, n = a.length; i < n; i++) {
			AttributeDescriptions.add(a[i]);
			attributeDescriptionNameMap.put(a[i].getInternalName(), a[i]);
		}
	}
	
	/**
	 * Returns a List of AttributeDescription objects, in the order they were added.
	 * 
	 * @return List of AttributeDescription objects.
	 */
	public List getAttributeDescriptions() {
    //return a copy
		return new ArrayList(AttributeDescriptions);
	}


	/**
		* Get a specific AttributeDescription, named by internalName.
		*  
		* @param internalName name of the requested AttributeDescription
		* @return AttributeDescription requested, or null
		*/
	public AttributeDescription getAttributeDescriptionByInternalName(String internalName) {
		if ( containsAttributeDescription(internalName) )
			return (AttributeDescription) attributeDescriptionNameMap.get(internalName);
		else
			return null;
	}

	/**
		* Check if this AttributeCollection contains a specific AttributeDescription named
		* by internalName.
		*  
		* @param internalName name of the requested AttributeDescription object
		* @return boolean, true if found, false if not.
		*/
	public boolean containsAttributeDescription(String internalName) {
  	return attributeDescriptionNameMap.containsKey(internalName);
	}
  
  /**
   * Retrieve a specific AttributeDescription that supports a given field and tableConstraint.
   * @param field
   * @param tableConstraint
   * @return AttributeDescription supporting the field and tableConstraint, or null
   */
  public AttributeDescription getAttributeDescriptionByFieldNameTableConstraint(String field, String tableConstraint) {
  	if (supports(field, tableConstraint))
  	  return lastSupportingAttribute;
  	else
  	  return null;
  }
  
  /**
   * Determine if this AttributeCollection supports a given field and tableConstraint.  Caches the first supporting AttributeDescription
   * that it finds, for subsequent call to getAttributeDescriptionByFieldNameTableConstraint.
   * @param field
   * @param tableConstraint
   * @return boolean, true if an AttributeDescription contained in this AttributeCollection supports the field and tableConstraint, false otherwise
   */
  public boolean supports(String field, String tableConstraint) {
  	boolean supports = false;
  	
  	for (Iterator iter = AttributeDescriptions.iterator(); iter.hasNext();) {
			AttributeDescription element = (AttributeDescription) iter.next();
			
			if (element.supports(field, tableConstraint)) {
				lastSupportingAttribute = element;
				supports = true;
				break;
			}
		}
  	return supports;
  }
  
	/**
	 * Add a AttributeList to the AtttributeCollection.
	 * 
	 * @param a a AttributeList object.
	 */
	public void addAttributeList(AttributeList a) {
		AttributeLists.add(a);
		attributeListNameMap.put(a.getInternalName(), a);
	}

/**
 * Remove an AttributeList from this AttributeCollection.
 * @param a -- AttributeList to be removed.
 */
public void removeAttributeList(AttributeList a) {
  attributeListNameMap.remove(a.getInternalName());
  AttributeLists.remove(a);
}

/**
 * Insert an AttributeList at a particular position within the AttributeCollection.
 * AttributeLists set at or after the given position are shift right.
 * @param position -- position at which to insert the given AttributeList
 * @param a -- AttributeList to insert
 */
public void insertAttributeList(int position, AttributeList a) {
  AttributeLists.add(position, a);
  attributeListNameMap.put(a.getInternalName(), a);
}

/**
 * Insert an AttributeList before a specific AttributeList, named by internalName.
 * @param internalName -- AttributeList before which the given AttributeList should be inserted.
 * @param a -- AttributeList to insert.
 * @throws ConfigurationException when the AttributeCollection does not contain an AttributeList named by internalName.
 */
public void insertAttributeListBeforeAttributeList(String internalName, AttributeList a) throws ConfigurationException {
  if (!attributeListNameMap.containsKey(internalName))
    throw new ConfigurationException("AttributeCollection does not contain AttributeList " + internalName + "\n");
  insertAttributeList( AttributeLists.indexOf( attributeListNameMap.get(internalName) ), a );
}

/**
 * Insert an AttributeList after a specific AttributeList, named by internalName.
 * @param internalName -- AttributeList after which the given AttributeList should be inserted.
 * @param a -- AttributeList to insert.
 * @throws ConfigurationException when the AttributeCollection does not contain an AttributeList named by internalName.
 */
public void insertAttributeListAfterAttributeList(String internalName, AttributeList a) throws ConfigurationException {
  if (!attributeListNameMap.containsKey(internalName))
    throw new ConfigurationException("AttributeCollection does not contain AttributeList " + internalName + "\n");
  insertAttributeList( AttributeLists.indexOf( attributeListNameMap.get(internalName) ) + 1, a );
}

	/**
	 * Add a group of AttributeList objects in one call.  Note, subsequent calls to addAttributeList or addAttributeLists
 * will add to what was added before.
	 * 
	 * @param a an Array of AttributeList objects.
	 */
	public void addAttributeLists(AttributeList[] a) {
		for (int i = 0, n = a.length; i < n; i++) {
			AttributeLists.add(a[i]);
			attributeListNameMap.put(a[i].getInternalName(), a[i]);
		}
	}
	
	/**
	 * Returns a List of AttributeList objects, in the order they were added.
	 * 
	 * @return List of AttributeList objects.
	 */
	public List getAttributeLists() {
  //return a copy
		return new ArrayList(AttributeLists);
	}


	/**
		* Get a specific AttributeList, named by internalName.
		*  
		* @param internalName name of the requested AttributeList
		* @return AttributeList requested, or null
		*/
	public AttributeList getAttributeListByInternalName(String internalName) {
		if ( containsAttributeList(internalName) )
			return (AttributeList) attributeListNameMap.get(internalName);
		else
			return null;
	}

	/**
		* Check if this AttributeCollection contains a specific AttributeList named
		* by internalName.
		*  
		* @param internalName name of the requested AttributeList object
		* @return boolean, true if found, false if not.
		*/
	public boolean containsAttributeList(String internalName) {
	return attributeListNameMap.containsKey(internalName);
	}

  
  /**
   * Returns a List of possible internalNames to add to the MartCompleter command completion system.
   * @return List of possible completions.
   */
  public List getCompleterNames() {
  	List names = new ArrayList();
  	
  	for (Iterator iter = AttributeDescriptions.iterator(); iter.hasNext();) {
			AttributeDescription element = (AttributeDescription) iter.next();
            if (element.getHidden() != null && element.getHidden().equals("true")) continue;
            if (element.getDisplay() != null && element.getDisplay().equals("true")) continue;
            
			//skip placeholders
			if ( !(element.getInternalName().indexOf('.') > 0) )
			  names.add(element.getInternalName());
		}
  	
  	for (Iterator iter = AttributeLists.iterator(); iter.hasNext();) {
			AttributeList element = (AttributeList) iter.next();
            if (element.getHidden() != null && element.getHidden().equals("true")) continue;
            if (element.getDisplay() != null && element.getDisplay().equals("true")) continue;
            
			//skip placeholders
			if ( !(element.getInternalName().indexOf('.') > 0) )
			  names.add(element.getInternalName());
		}
  	return names;
  }

  /**
   * Returns a List of possible internalNames to add to the MartCompleter command completion system.
   * @return List of possible completions.
   */
  public List getHiddenCompleterNames() {
  	List names = new ArrayList();  	
  	for (Iterator iter = AttributeDescriptions.iterator(); iter.hasNext();) {
			AttributeDescription element = (AttributeDescription) iter.next();			
  		    names.add(element.getInternalName());
		}
  	for (Iterator iter = AttributeLists.iterator(); iter.hasNext();) {
		AttributeList element = (AttributeList) iter.next();			
		    names.add(element.getInternalName());
	}
  	return names;
  }
  
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
		buf.append( super.toString() );
		buf.append(", AttributeDescriptions=").append(AttributeDescriptions);
		buf.append(", AttributeLists=").append(AttributeLists);
		buf.append("]");

		return buf.toString();
	}

  /**
	 * Allows Equality Comparisons manipulation of AttributeCollection objects
	 */
	public boolean equals(Object o) {
		return o instanceof AttributeCollection && hashCode() == ((AttributeCollection) o).hashCode();
	}

	public int hashCode() {
		int tmp = super.hashCode();
		
		for (Iterator iter = AttributeDescriptions.iterator(); iter.hasNext();) {
			AttributeDescription element = (AttributeDescription) iter.next();
			tmp = (31 * tmp) + element.hashCode();
		}
		for (Iterator iter = AttributeLists.iterator(); iter.hasNext();) {
			AttributeList element = (AttributeList) iter.next();
			tmp = (31 * tmp) + element.hashCode();
		}
		return tmp;
	}
  /**
   * Sets the hasBrokenAttributes flag to true, meaning that one or more AttributeDescription objects
   * contains invalid field or tableConstraint references to a particular Mart instance. 
   */
  public void setAttributesBroken() {
    hasBrokenAttributes = true;
  }
  
  /**
   * Determine if this AttributeCollection contains broken AttributeDescriptions
   * @return boolean, true if one or more AttributeDescription objects are broken, false otherwise
   */
  public boolean hasBrokenAttributes() {
  	return hasBrokenAttributes;
  }
  
  /**
   * True if hasBrokenAttributes is true.
   * @return boolean
   */
  public boolean isBroken() {
  	return hasBrokenAttributes;
  }
public boolean containsOnlyPointerAttributes() {
    boolean ret = true;
    
    List atts = getAttributeDescriptions();
    for (int i = 0, n = atts.size(); i < n; i++) {
        AttributeDescription element = (AttributeDescription) atts.get(i);
        if (element.getInternalName().indexOf('.') < 1) {
            ret = false;
            break;
        }
    }
    atts = getAttributeLists();
    for (int i = 0, n = atts.size(); i < n; i++) {
        AttributeList element = (AttributeList) atts.get(i);
        if (element.getInternalName().indexOf('.') < 1) {
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
