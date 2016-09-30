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
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Contains all of the information required by a UI to display a specific attribute, and create an Attribute object to add to a mart Query.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class DynamicDataset extends BaseNamedConfigurationObject {

  private Logger logger =
    Logger.getLogger(DynamicDataset.class.getName());

  /**
   * The default maxLength is 10
   */
  public final int DEFAULTMAXLENGTH = 10;

  private final String aliasesKey = "aliases";
  
  private int[] reqFields = {0,5};// rendered red in AttributeTable
  //private final String hiddenKey = "hidden";
  // helper field so that only setter/constructors will throw ConfigurationExceptions when string values are converted to integers

  /**
   * Copy constructor. Constructs an exact copy of an existing AttributeDescription.
   * @param a AttributeDescription to copy.
   */
  public DynamicDataset(DynamicDataset a) {
    super(a);
	setRequiredFields(reqFields);
  }

  /**
   * Empty Constructor should only be used by DatasetConfigEditor
   *
   */
  public DynamicDataset() {
    super();
    
    setAttribute(aliasesKey, null);
	
	setRequiredFields(reqFields);
  }

  /**
   * Constructs a AttributeDescription with just the internalName and field.
   * not used anywhere yet and should probably add tableConstraint and Key
   * @param internalName String name to internally represent the AttributeDescription. Must not be null or empty
   * @param field String name of the field in the mart for this Attribute. Must not be null or empty.
   * @throws ConfigurationException when values are null or empty.
   */
  public DynamicDataset(String internalName, String aliases)
    throws ConfigurationException {
    this(internalName, "", "", aliases);
  }
  /**
   * Constructor for an AttributeDescription.
   * 
   * @param internalName String name to internally represent the AttributeDescription. Must not be null or empty.
   * @param field String name of the field in the mart for this attribute.  Must not be null or empty.
   * @param displayName String name of the AttributeDescription.
   * @param maxLength Int maximum possible length of the field in the mart.
   * @param tableConstraint String base name of a specific table containing this UIAttribute.
   * @param aliases String name of the key to use with this attribute
   * @throws ConfigurationException when required parameters are null or empty
   */
  public DynamicDataset(
    String internalName,
    String displayName,
    String description,
    String aliases)
    throws ConfigurationException {

    super(internalName, displayName, description);

    setAttribute(aliasesKey, aliases);
	
	setRequiredFields(reqFields);
  }
  
  

  /**
   * @param tableConstraint - tableConstraint for the field
   */
  public void setAliases(String aliases) {
    setAttribute(aliasesKey, aliases);
  }

  /**
   * Returns the TableConstraint.
   * 
   * @return String tableConstraint.
   */
  public String getAliases() {
    return getAttribute(aliasesKey);
  }
  
  public void resolveText(BaseConfigurationObject to, BaseConfigurationObject from) {
	  String[] titles = from.getXmlAttributeTitles();
	  //String[] titles = (String[])from.attributes.keySet().toArray(new String[0]);
	  for (int x = 0 ; x < titles.length; x++) {
		  String key = titles[x];
		  if (key.equals("internalName")) continue; // Don't muck with these!
		  String value = from.getAttribute(key);
		  String oldVal = value;	  
		  if (value==null || value.equals("")) {
			  value = to.getAttribute(key);
		  } else {
			  if (value.indexOf('*')>=0 && this.getAliases()!=null && !this.getAliases().equals("")) {
			  	String[] pairs = this.getAliases().split(",");
			  	for (int i = 0; i < pairs.length; i++) {
			  		String[] parts= pairs[i].split("=");
			  		if (parts.length<2) {
			  			if (parts.length==1) value = value.replaceAll("\\*"+parts[0]+"\\*", "");
			  			continue;
			  		} 
			  		value = value.replaceAll("\\*"+parts[0]+"\\*", parts[1]);
			  	}
			  }
		  }
		  if (value!=null) to.setAttribute(key, value);
	  }
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();

    buf.append("[ DynamicDataset:");
    buf.append(super.toString());
    buf.append("]");

    return buf.toString();
  }

  /**
   * Allows Equality Comparisons of AttributeDescription objects
   */
  public boolean equals(Object o) {
    return o instanceof DynamicDataset
      && hashCode() == ((DynamicDataset) o).hashCode();
  }

  /**
   * True if one of hasBrokenField or hasBrokenTableConstraint is true.
   * @return boolean
   */
  public boolean isBroken() {
    return false;
  }
}
