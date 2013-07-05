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
import java.util.Iterator;
import java.util.List;
import java.util.Collection;
import java.util.Properties;

/**
 * Base Object from which all Configuration objects inherit. Provides a properties object
 * to hold attribute keys and values, and setAttribute/getAttribute methods.  
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public abstract class BaseConfigurationObject implements Comparable {
  //Properties Object holds values from XML attributes keyed to AttributeTitle returned by getXMLAttributeTitles.
  protected Properties attributes = new Properties();
  protected List xmlTitles = new ArrayList();
  protected int[] requiredFields;
  //want to preserve the order of the titles for multiple calls to getXMLAttributeTitles

  /**
   * Copy constructor for all Configuration objects. Propogates all keys from the objects
   * attributes properties to the new Object.
   * @param bo
   */
  public BaseConfigurationObject(BaseConfigurationObject bo) {
	String[] boXMLTitles = bo.getXmlAttributeTitles();
	for (int i = 0; i < boXMLTitles.length; i++) {
		String key = boXMLTitles[i];
		if (bo.getAttribute(key) == null)
		  setAttribute(new String(key), null);
		else
		  setAttribute(new String(key), new String(bo.getAttribute(key)));		
	}
  }

  /**
   * Initializes the attributes properties.
   */
  public BaseConfigurationObject() {
    //doesnt do anything, except return an empty object
  }

  /**
   * Set the XML Attribute for a particular key. This method is primarily for DatasetConfigXMLUtils and DatasetConfigEditor.  Client code should
   * use the setXXX methods. Note, keys with null values are not added to the object. 
   * @param key - String key for this attribute
   * @param value - String value for this attribute
   */
  public void setAttribute(String key, String value) {
    if (!xmlTitles.contains(key))
      xmlTitles.add(key);
    if (value != null)
      attributes.setProperty(key, value);
  }

  /**
   * Get the value of an attribute for a given key. This method is primarily for DatasetConfigEditor.  Client code should
   * use the getXXX methods.
   * @param key- 
   * @return String attribute for key
   */
  public String getAttribute(String key) {
    return attributes.getProperty(key);
  }

  /**
   * Get the value of an attribute for a given key. This method is primarily for DatasetConfigEditor.  Client code should
   * use the getXXX methods.
   * @param key- 
   * @return String attribute for key
   */
  public String getKey(int index) {
    return (String)attributes.keySet().toArray(new String[0])[index];
  }

  /**
   * Get the XML Attribute Titles for this object. This is meant for use
   * by DatasetConfigEditor. Once called, the order of the strings in the return
   * list are preserved over successive calls, with any new attribute titles added on subsequent
   * calls to addAttribute appended to the end of the list.
   * @return String[] List of XMLAttribute Titles.
   */
  public String[] getXmlAttributeTitles() {

    String[] newTitles = new String[xmlTitles.size()];
    xmlTitles.toArray(newTitles);

    return newTitles;
  }
  

  public void setRequiredFields(int[] reqFields) {
	requiredFields = reqFields;
  }

  public int[] getRequiredFields() {
  	if (requiredFields == null){
  		int[] newRequiredFields = {0};
  		return newRequiredFields;
  	}
	return requiredFields;
  }

  /**
   * All Configuration Objects must impliment a flag to determine
   * if a validated version of an object (one that has been returned
   * by the DatasetConfig.validate() method) is broken in some way.
   * @return boolean, true if the Object contains broken members, false otherwise
   */
  public abstract boolean isBroken();

  public String toString() {
    StringBuffer buf = new StringBuffer();

    int i = 0;
    for (Iterator iter = xmlTitles.iterator(); iter.hasNext();) {
      if (i > 0)
        buf.append(",");

      String key = (String) iter.next();
      buf.append(" ").append(key).append("=").append(attributes.getProperty(key));
      i++;
    }

    return buf.toString();
  }

  /**
   * Allows Equality Comparisons manipulation of BaseConfigurationObject objects
   */
  public boolean equals(Object o) {
    return o instanceof BaseConfigurationObject && hashCode() == o.hashCode();
  }

  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  public int hashCode() {
    int tmp = 17;

    for (Iterator iter = attributes.values().iterator(); iter.hasNext();) {
      String value = (String) iter.next();

      tmp += (value != null) ? value.hashCode() : 0;
    }

    return tmp;
  }

  /**
   * allows any BaseConfigurationObject implimenting object to be compared to any other
   * BaseConfigurationObject implimenting object, based on their hashCode.
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(Object o) {
    return hashCode() - o.hashCode();
  }
}
