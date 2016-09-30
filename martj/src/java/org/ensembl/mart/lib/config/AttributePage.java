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
 * Container for a set of Mart AttributeCollections
 *   
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class AttributePage extends BaseNamedConfigurationObject {

  private boolean hasBrokenGroups = false;
  private List attributeGroups = new ArrayList();
  private Hashtable attGroupNameMap = new Hashtable();

  //cache one AttributeDescription object for call to containsAttributeDescription or getAttributeDescriptionByName
  private AttributeDescription lastAtt = null;
  private AttributeList lastAttList = null;

  //cache one AttributeDescription for call to supports/getAttributeDescriptionByFieldNameTableConstraint
  private AttributeDescription lastSupportingAttribute = null;

  //cache one AttributeGroup for call to getGroupForAttribute
  private AttributeGroup lastGroup = null;

  //cache one AttributeCollection for call to getCollectionForAttribute
  private AttributeCollection lastColl = null;
  private final String outFormatsKey = "outFormats";
  private final String maxSelectKey = "maxSelect";
  public final int DEFAULTMAXSELECT = 0;
  
  /**
   * Copy constructor. Constructs an exact copy of an existing AttributePage.
   * @param ap AttributePage to copy.
   */
  public AttributePage(AttributePage ap) {
  	super (ap);
  	
  	List agroups = ap.getAttributeGroups();
  	for (int i = 0, n = agroups.size(); i < n; i++) {
      Object group = agroups.get(i);
      addAttributeGroup( new AttributeGroup( (AttributeGroup) group ));
      
    }
  }
   
/**
 * Empty Constructor should really only be used by the DatasetConfigEditor
 */
	public AttributePage() {
		super();
		setAttribute(outFormatsKey, null);
		setAttribute(maxSelectKey, null);
	}

	/**
	 * Constructor for an AttributePage represented by internalName internally.
	 * 
	 * @param internalName String name to internally represent the AttributePage
	 * @throws ConfigurationException when the internalName is null or empty
	 */
	public AttributePage(String internalName) throws ConfigurationException {
		this(internalName, "", "", "","");
	}

	/**
	 * Constructor for an AttributePage named internally by internalName, with a 
	 * displayName and described by description.
	 * 
	 * @param internalName String name to internally represent the AttributePage.  Must not be null.
	 * @param displayName String name to represent the AttributePage
	 * @param description String description of the AttributePage
	 * @throws ConfigurationException when the internalName is null or empty
	 */
	public AttributePage(String internalName, String displayName, String description, String outFormats, String maxSelect) throws ConfigurationException {
		super(internalName, displayName, description);
		setAttribute(outFormatsKey, outFormats);
		setAttribute(maxSelectKey,maxSelect);
	}

	/**
	 * Add a single AttributeGroup to the AttributePage.
	 * 
	 * @param a An AttributeGroup object
	 */
	public void addAttributeGroup(AttributeGroup a) {
		attributeGroups.add(a);
		attGroupNameMap.put(a.getInternalName(), a);
	}

  /**
   * Remove an AttributeGroup from the AttributePage
   * @param a -- AttributeGroup to be removed.
   */
  public void removeAttributeGroup(AttributeGroup a) {
    attGroupNameMap.remove(a.getInternalName());
    attributeGroups.remove(a); 
  }
  
  /**
   * Insert an AttributeGroup at a particular position within the List of AttributeGroup/DSAttributeGroup objects
   * contained in the AttributePage. AttributeGroup/DSAttributeGroup objects at or after this position are shifted right.
   * @param position -- position to insert the given AttributeGroup
   * @param a -- AttributeGroup to insert.
   */
  public void insertAttributeGroup(int position, AttributeGroup a) {
    attributeGroups.add(position, a);
    attGroupNameMap.put(a.getInternalName(), a);
  }
  
  /**
   * Insert an AttributeGroup before a specific AttributeGroup/DSAttributeGroup, named by internalName.
   * @param internalName -- name of the AttributeGroup/DSAttributeGroup before which the given AttributeGroup should be inserted.
   * @param a -- AttributeGroup to insert.
   * @throws ConfigurationException when the AttributePage does not contain an AttributeGroup/DSAttributeGroup named by internalName.
   */
  public void insertAttributeGroupBeforeAttributeGroup(String internalName, AttributeGroup a) throws ConfigurationException {
    if (!attGroupNameMap.containsKey(internalName))
      throw new ConfigurationException("AttributePage does not contain AttributeGroup " + internalName + "\n");
    
    insertAttributeGroup( attributeGroups.indexOf( attGroupNameMap.get(internalName) ), a );
  }
  
  /**
   * Insert an AttributeGroup after a specific AttributeGroup/DSAttributeGroup, named by internalName.
   * @param internalName -- name of the AttributeGroup/DSAttributeGroup after which the given AttributeGroup should be inserted.
   * @param a -- AttributeGroup to insert.
   * @throws ConfigurationException when the AttributePage does not contain an AttributeGroup/DSAttributeGroup named by internalName.
   */
  public void insertAttributeGroupAfterAttributeGroup(String internalName, AttributeGroup a) throws ConfigurationException {
    if (!attGroupNameMap.containsKey(internalName))
      throw new ConfigurationException("AttributePage does not contain AttributeGroup " + internalName + "\n");
    
    insertAttributeGroup( attributeGroups.indexOf( attGroupNameMap.get(internalName) ) + 1, a );
  }
  
	/**
	 * Add a group of AttributeGroup objects at once.  Note, subsequent calls
	 * to addAttributeGroup or setAttributeGroup will add to what has already been added.
	 * 
	 * @param a an array of AttributeGroup objects
	 */
	public void addAttributeGroups(AttributeGroup[] a) {
		for (int i = 0, n = a.length; i < n; i++) {
			attributeGroups.add(a[i]);
			attGroupNameMap.put(a[i].getInternalName(), a[i]);
		}
	}


	/**
	 * Returns a List of AttributeGroup/DSAttributeGroup objects contained in the AttributePage, in the order they were added.
	 * 
	 * @return A List of AttributeGroup/DSAttributeGroup objects
	 */
	public List getAttributeGroups() {
    //return a copy
		return new ArrayList(attributeGroups);
	}

	/**
	 * Returns a specific AttributeGroup named by internalName.
	 * 
	 * @param internalName String name of the requested AttributeGroup
	 * @return an Object (either AttributeGroup or DSAttributeGroup), or null
	 */
	public Object getAttributeGroupByName(String internalName) {
		if (attGroupNameMap.containsKey(internalName))
			return attGroupNameMap.get(internalName);
		else
			return null;
	}

	/**
	 * Check whether the AttributePage contains a particular AttributeGroup named by internalName.
	 * 
	 * @param internalName String name of the AttributeGroup
	 * @return boolean, true if AttributePage contains AttributeGroup, false if not
	 */
	public boolean containsAttributeGroup(String internalName) {
		return attGroupNameMap.containsKey(internalName);
	}

	/**
		* Convenience method for non graphical UI.  Allows a call against the AttributePage for a particular AttributeDescription.
	 *  Note, it is best to first call containsAttributeDescription,  
		*  as there is a caching system to cache a AttributeDescription during a call to containsAttributeDescription.
		*  
		* @param internalName name of the requested AttributeDescription
		* @return AttributeDescription requested, or null
		*/
	public AttributeDescription getAttributeDescriptionByInternalName(String internalName) {
		if (containsAttributeDescription(internalName))
			return lastAtt;
		else
			return null;
	}

	/**
		* Convenience method for non graphical UI.  Can determine if the AttributePage contains a specific AttributeDescription.
		*  As an optimization for initial calls to containsAttributeDescription with an immediate call to getAttributeDescriptionByName if
		*  found, this method caches the AttributeDescription it has found.
		* 
		* @param internalName name of the requested AttributeDescription
		* @return boolean, true if found, false if not.
		*/
	public boolean containsAttributeDescription(String internalName) {
		boolean found = false;

		if (lastAtt == null) {
			for (Iterator iter = (Iterator) attributeGroups.iterator(); iter.hasNext();) {
				Object group = iter.next();
				if (group instanceof AttributeGroup && ((AttributeGroup) group).containsAttributeDescription(internalName)) {
					lastAtt = ((AttributeGroup) group).getAttributeDescriptionByInternalName(internalName);
					found = true;
					break;
				}
			}
		} else {
			if (lastAtt.getInternalName().equals(internalName))
				found = true;
			else {
				lastAtt = null;
				found = containsAttributeDescription(internalName);
			}
		}
		return found;
	}

	/**
		* Convenience method for non graphical UI.  Allows a call against the AttributePage for a particular AttributeList.
	 *  Note, it is best to first call containsAttributeList,  
		*  as there is a caching system to cache a AttributeList during a call to containsAttributeList.
		*  
		* @param internalName name of the requested AttributeList
		* @return AttributeList requested, or null
		*/
	public AttributeList getAttributeListByInternalName(String internalName) {
		if (containsAttributeList(internalName))
			return lastAttList;
		else
			return null;
	}

	/**
		* Convenience method for non graphical UI.  Can determine if the AttributePage contains a specific AttributeList.
		*  As an optimization for initial calls to containsAttributeList with an immediate call to getAttributeListByName if
		*  found, this method caches the AttributeList it has found.
		* 
		* @param internalName name of the requested AttributeList
		* @return boolean, true if found, false if not.
		*/
	public boolean containsAttributeList(String internalName) {
		boolean found = false;

		if (lastAttList == null) {
			for (Iterator iter = (Iterator) attributeGroups.iterator(); iter.hasNext();) {
				Object group = iter.next();
				if (group instanceof AttributeGroup && ((AttributeGroup) group).containsAttributeList(internalName)) {
					lastAttList = ((AttributeGroup) group).getAttributeListByInternalName(internalName);
					found = true;
					break;
				}
			}
		} else {
			if (lastAttList.getInternalName().equals(internalName))
				found = true;
			else {
				lastAttList = null;
				found = containsAttributeList(internalName);
			}
		}
		return found;
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
	 * Determine if this AttributePage supports a given field and tableConstraint.  Caches the first supporting AttributeDescription
	 * that it finds, for subsequent call to getAttributeDescriptionByFieldNameTableConstraint.
	 * @param field
	 * @param tableConstraint
	 * @return boolean, true if an AttributeDescription contained in this AttributePage supports the field and tableConstraint, false otherwise
	 */
	public boolean supports(String field, String tableConstraint) {
		boolean supports = false;

		for (Iterator iter = attributeGroups.iterator(); iter.hasNext();) {
			Object element = iter.next();
			if (element instanceof AttributeGroup) {
				AttributeGroup attgroup = (AttributeGroup) element;

				if (attgroup.supports(field, tableConstraint)) {
					lastSupportingAttribute = attgroup.getAttributeDescriptionByFieldNameTableConstraint(field, tableConstraint);
					supports = true;
					break;
				}
			}
		}
		return supports;
	}

	/**
	 * Convenience method. Returns all of the AttributeDescriptions contained in all of the AttributeGroups.
	 * 
	 * @return List of AttributeDescription objects
	 */
	public List getAllAttributeDescriptions() {
		List atts = new ArrayList();

		for (Iterator iter = attributeGroups.iterator(); iter.hasNext();) {
			Object ag = iter.next();

			if (ag instanceof AttributeGroup)
				atts.addAll(((AttributeGroup) ag).getAllAttributeDescriptions());
		}

		return atts;
	}

	/**
	 * Returns a AttributeGroup for a particular Attribute Description (AttributeDescription or UIDSAttributeDescription)
	 * based on its internalName.
	 * 
	 * @param internalName - String internalname for which a group is requested
	 * @return AttributeGroup containing Attribute Description with given internalName, or null.
	 */
	public AttributeGroup getGroupForAttributeDescription(String internalName) {
		if (!containsAttributeDescription(internalName))
			return null;
		else if (lastGroup == null) {
			for (Iterator iter = attributeGroups.iterator(); iter.hasNext();) {
				Object groupo = iter.next();

				if (groupo instanceof AttributeGroup) {

					AttributeGroup group = (AttributeGroup) groupo;

					if (group.containsAttributeDescription(internalName)) {
						lastGroup = group;
						break;
					}
				}
			}
			return lastGroup;
		} else {
			if (lastGroup.getInternalName().equals(internalName))
				return lastGroup;
			else {
				lastGroup = null;
				return getGroupForAttributeDescription(internalName);
			}
		}
	}

	/**
	 * Returns a AttributeCollection for a particular Attribute Description (AttributeDescription or UIDSAttributeDescription)
	 * based on its internalName.
	 * 
	 * @param internalName - String internalname for which a collection is requested
	 * @return AttributeCollection object containing Attribute Description with given internalName, or null.
	 */
	public AttributeCollection getCollectionForAttributeDescription(String internalName) {
		if (!containsAttributeDescription(internalName))
			return null;
		else if (lastColl == null) {
			lastColl = getGroupForAttributeDescription(internalName).getCollectionForAttributeDescription(internalName);
			return lastColl;
		} else {
			if (lastColl.getInternalName().equals(internalName))
				return lastColl;
			else {
				lastColl = null;
				return getCollectionForAttributeDescription(internalName);
			}
		}
	}

	/**
	 * Convenience method. Returns all of the AttributeLists contained in all of the AttributeGroups.
	 * 
	 * @return List of AttributeList objects
	 */
	public List getAllAttributeLists() {
		List atts = new ArrayList();

		for (Iterator iter = attributeGroups.iterator(); iter.hasNext();) {
			Object ag = iter.next();

			if (ag instanceof AttributeGroup)
				atts.addAll(((AttributeGroup) ag).getAllAttributeLists());
		}

		return atts;
	}

	/**
	 * Returns a AttributeGroup for a particular Attribute List (AttributeList or UIDSAttributeList)
	 * based on its internalName.
	 * 
	 * @param internalName - String internalname for which a group is requested
	 * @return AttributeGroup containing Attribute List with given internalName, or null.
	 */
	public AttributeGroup getGroupForAttributeList(String internalName) {
		if (!containsAttributeList(internalName))
			return null;
		else if (lastGroup == null) {
			for (Iterator iter = attributeGroups.iterator(); iter.hasNext();) {
				Object groupo = iter.next();

				if (groupo instanceof AttributeGroup) {

					AttributeGroup group = (AttributeGroup) groupo;

					if (group.containsAttributeList(internalName)) {
						lastGroup = group;
						break;
					}
				}
			}
			return lastGroup;
		} else {
			if (lastGroup.getInternalName().equals(internalName))
				return lastGroup;
			else {
				lastGroup = null;
				return getGroupForAttributeList(internalName);
			}
		}
	}

	/**
	 * Returns a AttributeCollection for a particular Attribute List (AttributeList or UIDSAttributeList)
	 * based on its internalName.
	 * 
	 * @param internalName - String internalname for which a collection is requested
	 * @return AttributeCollection object containing Attribute List with given internalName, or null.
	 */
	public AttributeCollection getCollectionForAttributeList(String internalName) {
		if (!containsAttributeList(internalName))
			return null;
		else if (lastColl == null) {
			lastColl = getGroupForAttributeList(internalName).getCollectionForAttributeList(internalName);
			return lastColl;
		} else {
			if (lastColl.getInternalName().equals(internalName))
				return lastColl;
			else {
				lastColl = null;
				return getCollectionForAttributeList(internalName);
			}
		}
	}
	
	/**
	 * Retruns a List of possible Completion names for filters to the MartCompleter command completion system.
	 * @return List possible completions
	 */
	public List getCompleterNames() {
		List names = new ArrayList();
		
		for (Iterator iter = attributeGroups.iterator(); iter.hasNext();) {
			Object group = iter.next();
            if (((BaseNamedConfigurationObject) group).getHidden() != null && ((BaseNamedConfigurationObject) group).getHidden().equals("true")) continue;
            if (((BaseNamedConfigurationObject) group).getDisplay() != null && ((BaseNamedConfigurationObject) group).getDisplay().equals("true")) continue;
              
			if (group instanceof AttributeGroup)
			  names.addAll( ( (AttributeGroup) group).getCompleterNames() );
		}
		return names;
	}
	
	/**
	 * debug output
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
		buf.append(super.toString());
		buf.append(", AttributeGroups=").append(attributeGroups);
		buf.append("]");

		return buf.toString();
	}

	/**
	 * Allows Equality Comparisons manipulation of AttributePage objects
	 */
	public boolean equals(Object o) {
		return o instanceof AttributePage && hashCode() == ((AttributePage) o).hashCode();
	}

	public int hashCode() {
		int tmp = super.hashCode();

		for (Iterator iter = attributeGroups.iterator(); iter.hasNext();) {
			Object element = iter.next();
		    tmp = (31 * tmp) + ((AttributeGroup) element).hashCode();
			
		}

		return tmp;
	}

  /**
   * Sets the hasBrokenGroups flag to true, meaning that one or more AttributeGroup Objects
   * contain broken AttributeDescriptions.
   */
  public void setGroupsBroken() {
    hasBrokenGroups = true;
  }
  
  /**
   * Determine if this AttributePage has broken AttributeGroups.
   * @return boolean, true if one or more AttributeGroup Objects contain broken AttributeDescriptions, false otherwise
   */
  public boolean hasBrokenGroups() {
  	return hasBrokenGroups;
  }
  
  /**
   * True if hasBrokenGroups is true.
   * @return boolean
   */
  public boolean isBroken() {
  	return hasBrokenGroups;
  }
  
  /**
   * Returns the outFormats for this att page.
   * 
   * @return key.
   */
  public String getOutFormats() {
	return getAttribute(outFormatsKey);
  }
	
  /**
   * @param key - outFormats for this att page
   */
  public void setOutFormats(String outFormats) {
	 setAttribute(outFormatsKey, outFormats);
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
			return DEFAULTMAXSELECT;
		}
	}

public boolean containsOnlyPointerAttributes() {
    boolean ret = true;
    
    List groups = getAttributeGroups();
    for (int i = 0, n = groups.size(); i < n; i++) {
        AttributeGroup element = (AttributeGroup) groups.get(i);
        if (!element.containsOnlyPointerAttributes()) {
            ret = false;
            break;
        }
    }
    
    return ret;
}  
  
}
