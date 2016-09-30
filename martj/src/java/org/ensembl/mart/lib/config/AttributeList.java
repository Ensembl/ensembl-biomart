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

 /** 
  * Allows a FilterDescription Object to code whether to enable another FilterDescription Object
  * in the UI, possibly based on a particular value of the enabling FilterDescription.
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class AttributeList extends BaseNamedConfigurationObject {
  private final String attributesKey = "attributes";
  private final String defaultKey = "default";
  private final String imageURLKey = "imageURL";
  private int[] reqFields = {0,5};// rendered red in AttributeTable
   
	/**
	 * Copy Constructor. Constructs a new Importable that is a
	 * exact copy of an existing Importable.
	 * @param e Importable Object to copy.
	 */ 
  public AttributeList(AttributeList e) {
  	super(e);
	setAttribute(attributesKey, e.getAttributes());
	setAttribute(defaultKey, e.getDefault());
	setAttribute(imageURLKey, e.getImageURL());
  	setRequiredFields(reqFields); 
  }
  
  public AttributeList() {
  	super();
    
	setAttribute(attributesKey, null);
	setAttribute(defaultKey, null);
	setAttribute(imageURLKey, null);
	setRequiredFields(reqFields);
  }
    
  /**
   * Construct an Importable Object with a ref.
   * @param ref - String internalName of the FilterDescription to Importable.
   * @throws ConfigurationException when ref is null or empty.
   */
  public AttributeList(String internalName, String displayName, String description) throws ConfigurationException {
  	this(internalName, displayName, description, null, null);
  }
  
  /**
   * Construct an Importable Object with a ref, and a valueCondition.
   * @param ref - String internalName of the FilterDescription to Importable.
   * @param valueCondition - String Condition for Value of the Enabling FilterDescription required for it to Importable the referent FilterDescription.
   * @throws ConfigurationException when ref is null or empty.
   */
  public AttributeList(String internalName, String displayName, String description, String attributes, String d) throws ConfigurationException {
  	super(internalName, displayName, description);
  	
	setAttribute(attributesKey, attributes);
	setAttribute(defaultKey, d);
	setAttribute(imageURLKey, null);
	setRequiredFields(reqFields);
  }


	/**
	 * Get the Reference for this Importable.  Refers to the internalName of a FilterDescription to Importable.
	 * @return String internalName of the referring FilterDescription.
	 */
	public String getDefault() {
		return getAttribute(defaultKey);
	}

	/**
	 * Get the ValueCondition, if set.
	 * @return String valueCondition
	 */
	public String getAttributes() {
		return getAttribute(attributesKey);
	}

  /**
	* Set a value at which the referenced Filter should be Importabled.
	* @param valueCondition -- value at which the referenced Filter should be Importabled.
	*/
   public void setDefault(String valueCondition) {
		 setAttribute(defaultKey, valueCondition);
   }	
	
  /**
   * Set a value at which the referenced Filter should be Importabled.
   * @param valueCondition -- value at which the referenced Filter should be Importabled.
   */
  public void setAttributes(String valueCondition) {
		setAttribute(attributesKey, valueCondition);
  }

	/**
	 * Get the ValueCondition, if set.
	 * @return String valueCondition
	 */
	public String getImageURL() {
		return getAttribute(imageURLKey);
	}

	/**
	* Set a value at which the referenced Filter should be Importabled.
	* @param valueCondition -- value at which the referenced Filter should be Importabled.
	*/
	public void setImageURL(String imageURL) {
		 setAttribute(imageURLKey, imageURL);
	}	
  
  public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
		buf.append(super.toString());
		buf.append("]");

		return buf.toString();
	}
	
	/**
	 * Allows Equality Comparisons manipulation of Importable objects
	 */
	public boolean equals(Object o) {
		return o instanceof AttributeList && hashCode() == o.hashCode();
	}

	/**
	 * always false
	 */
	public boolean isBroken() {
		return false;
	}
}
