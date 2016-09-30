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
public class AttributeDescription extends BaseNamedConfigurationObject {

  private Logger logger =
    Logger.getLogger(AttributeDescription.class.getName());

  /**
   * The default maxLength is 10
   */
  public final int DEFAULTMAXLENGTH = 10;

  private final String datasetLinkKey = "datasetLink";
  private final String fieldKey = "field";
  private final String tableConstraintKey = "tableConstraint";
  private final String keyKey = "key";
  private final String sourceKey = "source";
  private final String homepageURLKey = "homepageURL";
  private final String linkoutURLKey = "linkoutURL";
  private final String imageURLKey = "imageURL";
  private final String maxLengthKey = "maxLength";
  private final String defaultKey = "default";
  private final String pointerDatasetKey = "pointerDataset";
  private final String pointerInterfaceKey = "pointerInterface";
  private final String pointerAttributeKey = "pointerAttribute";
  private final String pointerFilterKey = "pointerFilter";
  private final String checkForNullsKey = "checkForNulls";
  private final String pipeDisplayKey = "pipeDisplay";

  private int[] reqFields = {0,5,8,9};// rendered red in AttributeTable
  //private final String hiddenKey = "hidden";
  // helper field so that only setter/constructors will throw ConfigurationExceptions when string values are converted to integers


	
	private List specificAttributeContents = new ArrayList();
	private Hashtable specificAttributeContentNameMap = new Hashtable();

  private boolean hasBrokenField = false;
  private boolean hasBrokenTableConstraint = false;

  /**
   * Copy constructor. Constructs an exact copy of an existing AttributeDescription.
   * @param a AttributeDescription to copy.
   */
  public AttributeDescription(AttributeDescription a) {
    super(a);
	setRequiredFields(reqFields);
    
    SpecificAttributeContent[] sf = (SpecificAttributeContent[])a.getSpecificAttributeContents().toArray(new SpecificAttributeContent[0]);
    for (int i = 0, n = sf.length; i < n; i++) {
      addSpecificAttributeContent( new SpecificAttributeContent( sf[i] ) );
    }
  }

  /**
   * Empty Constructor should only be used by DatasetConfigEditor
   *
   */
  public AttributeDescription() {
    super();
    
    setAttribute(fieldKey, null);
    setAttribute(datasetLinkKey, null);
    setAttribute(maxLengthKey, null);
    setAttribute(tableConstraintKey, null);
	setAttribute(keyKey, null);
    setAttribute(sourceKey, null);
    setAttribute(homepageURLKey, null);
    setAttribute(linkoutURLKey, null);
	setAttribute(imageURLKey, null);
	setAttribute(defaultKey, null);
	setAttribute(pointerDatasetKey, null);
	setAttribute(pointerInterfaceKey, null);
	setAttribute(pointerAttributeKey, null);
	setAttribute(pointerFilterKey, null);
	setAttribute(checkForNullsKey, null);
	setAttribute(pipeDisplayKey, null);
	setRequiredFields(reqFields);
  }

  /**
   * Constructs a AttributeDescription with just the internalName and field.
   * not used anywhere yet and should probably add tableConstraint and Key
   * @param internalName String name to internally represent the AttributeDescription. Must not be null or empty
   * @param field String name of the field in the mart for this Attribute. Must not be null or empty.
   * @throws ConfigurationException when values are null or empty.
   */
  public AttributeDescription(String internalName, String field)
    throws ConfigurationException {
    this(internalName, field, "", "0", "", "", "", "", "", "", "", "", "", "", "", "", "","","");
  }
  /**
   * Constructor for an AttributeDescription.
   * 
   * @param internalName String name to internally represent the AttributeDescription. Must not be null or empty.
   * @param field String name of the field in the mart for this attribute.  Must not be null or empty.
   * @param displayName String name of the AttributeDescription.
   * @param maxLength Int maximum possible length of the field in the mart.
   * @param tableConstraint String base name of a specific table containing this UIAttribute.
   * @param key String name of the key to use with this attribute
   * @param description String description of this UIAttribute.
   * @param source String source for the data for this UIAttribute.
   * @param homePageURL String Web Homepage for the source.
   * @param linkoutURL String Base for a link to a specific entry in a source website.
   * @param default attribute for a dataset if set to true.
   * @throws ConfigurationException when required parameters are null or empty
   */
  public AttributeDescription(
    String internalName,
    String field,
    String displayName,
    String maxLength,
    String tableConstraint,
    String key,
    String description,
    String source,
    String homePageURL,
    String linkoutURL,
    String imageURL,
    String datasetLink,
    String defaultString,
    String pointerDataset,
    String pointerInterface,
    String pointerAttribute,
    String pointerFilter,
    String checkForNulls,
    String pipeDisplay)
    throws ConfigurationException {

    super(internalName, displayName, description);

    if (field == null || field.equals(""))
      throw new ConfigurationException("UIAttributeDescriptions require a field");

    setAttribute(fieldKey, field);
    setAttribute(maxLengthKey, maxLength);
    setAttribute(tableConstraintKey, tableConstraint);
	setAttribute(keyKey, key);
    setAttribute(sourceKey, source);
    setAttribute(homepageURLKey, homePageURL);
    setAttribute(linkoutURLKey, linkoutURL);
	setAttribute(imageURLKey, imageURL);
    setAttribute(datasetLinkKey, datasetLink);
	setAttribute(defaultKey, defaultString);
	setAttribute(pointerDatasetKey, pointerDataset);
	setAttribute(pointerInterfaceKey, pointerInterface);
	setAttribute(pointerAttributeKey, pointerAttribute);
	setAttribute(pointerFilterKey, pointerFilter);
	setAttribute(checkForNullsKey, checkForNulls);
	setAttribute(pipeDisplayKey, pipeDisplay);
	setRequiredFields(reqFields);
  }

  
  /**
   * Add a dynamicImportableContent to the AttributeDescription.
   * 
   * @param a dynamicImportableContent object.
   */
  public void addSpecificAttributeContent(SpecificAttributeContent a) {
	  specificAttributeContents.add(a);
	  specificAttributeContentNameMap.put(a.getInternalName(),a);
  }

  /**
   * Add a dynamicFilterContent to the AttributeDescription.
   * 
   * @param a dynamicFilterContent object.
   */
  public SpecificAttributeContent getSpecificAttributeContent(String name) {
	  return (SpecificAttributeContent)specificAttributeContentNameMap.get(name);
  }
  
  /**
   * Add a dynamicFilterContent to the AttributeDescription.
   * 
   * @param a dynamicFilterContent object.
   */
  public List getSpecificAttributeContents() {
	  return this.specificAttributeContents;
  }

  /**
   * Remove an dynamicFilterContent from this AttributeDescription.
   * @param a -- dynamicFilterContent to be removed.
   */
  public void insertSpecificAttributeContent(int index, SpecificAttributeContent a) {
	specificAttributeContents.add(index,a);
	specificAttributeContentNameMap.put(a.getInternalName(),a);
  }

  /**
   * Remove an dynamicFilterContent from this AttributeDescription.
   * @param a -- dynamicFilterContent to be removed.
   */
  public void removeSpecificAttributeContent(SpecificAttributeContent a) {
	specificAttributeContents.remove(a);
	specificAttributeContentNameMap.remove(a.getInternalName());
  }

  
  /**
   * @param homePageURL - url to homepage for the data source
   */
  public void setHomepageURL(String homePageURL) {
    setAttribute(homepageURLKey, homePageURL);
  }

  /**
   * @return homepageURL
   */
  public String getHomepageURL() {
    return getAttribute(homepageURLKey);
  }

  /**
   * @param tableConstraint - tableConstraint for the field
   */
  public void setTableConstraint(String tableConstraint) {
    setAttribute(tableConstraintKey, tableConstraint);
  }

  /**
   * Returns the TableConstraint.
   * 
   * @return String tableConstraint.
   */
  public String getTableConstraint() {
    return getAttribute(tableConstraintKey);
  }

  /**
   * @param String datasetLink
   */
  public void setDatasetLink(String datasetLink) {
	setAttribute(datasetLinkKey, datasetLink);
  }

  /**
   * Returns the datasetLink.
   * 
   * @return String datasetLink.
   */
  	public String getDatasetLink() {
		return getAttribute(datasetLinkKey);
  	}



	/**
	 * Returns the join field key.
	 * 
	 * @return key.
	 */
	public String getDefault() {
	  return getAttribute(defaultKey);
	}
	
	/**
	 * @param key - join field key for the field
	 */
	public void setDefault(String defaultString) {
	   setAttribute(defaultKey, defaultString);
	}

	  /**
	   * Returns the join field key.
	   * 
	   * @return key.
	   */
	  public String getKey() {
		return getAttribute(keyKey);
	  }

	/**
	 * @param key - join field key for the field
	 */
  public void setKey(String key) {
	setAttribute(keyKey, key);
  }

  /**
   * @param pointerDataset - pointer dataset, used for placeholder attributes
   */
  public void setPointerDataset(String pointerDataset) {
	setAttribute(pointerDatasetKey, pointerDataset);
  }

  /**
   * Returns the pointerDataset.
   * 
   * @return String pointerDataset
   */
  public String getPointerDataset() {
	return getAttribute(pointerDatasetKey);
  }

  /**
   * @param pointerInterface - pointer interface, used for placeholder attributes
   */
  public void setPointerInterface(String pointerInterface) {
	setAttribute(pointerInterfaceKey, pointerInterface);
  }

  /**
   * Returns the pointerInterface.
   * 
   * @return String pointerInterface
   */
  public String getPointerInterface() {
	return getAttribute(pointerInterfaceKey);
  }
  
  /**
   * @param pointerAttribute - pointer attribute, used for placeholder attributes
   */
  public void setPointerAttribute(String pointerAttribute) {
	setAttribute(pointerAttributeKey, pointerAttribute);
  }

  /**
   * Returns the pointerDataset.
   * 
   * @return String pointerDataset
   */
  public String getPointerAttribute() {
	return getAttribute(pointerAttributeKey);
  }

  /**
   * @param pointerfilter - pointer filter, used for placeholder filters
   */
  public void setPointerFilter(String pointerFilter) {
	setAttribute(pointerFilterKey, pointerFilter);
  }

  /**
   * Returns the pointerDataset.
   * 
   * @return String pointerFilter
   */
  public String getPointerFilter() {
	return getAttribute(pointerFilterKey);
  }
  
  /**
   * @param checkForNulls - 
   */
  public void setCheckForNulls(String checkForNulls) {
	setAttribute(checkForNullsKey, checkForNulls);
  }

  /**
   * Returns the pointerDataset.
   * 
   * @return String checkForNulls
   */
  public String getCheckForNulls() {
	return getAttribute(checkForNullsKey);
  }


  /**
   * @param field - field in mart table
   */
  public void setField(String field) {
    setAttribute(fieldKey, field);
  }

  /**
   * Returns the field.
   * 
   * @return String field
   */
  public String getField() {
    return getAttribute(fieldKey);
  }

  /**
   * @param maxLength - String maximum length of the table field
   */
  public void setMaxLength(String maxLength){
    setAttribute(maxLengthKey, maxLength);
  }

  /**
   * Returns the maxLength. If the value for maxLength
   * is not a valid integer (eg, a NumberFormatException is
   * thrown by Integer.parseInt( maxLength )) this method will
   * return DEFAULTMAXLENGTH
   * 
   * @return int MaxLength.
   */
  public int getMaxLength() {
    try {
      return Integer.parseInt(getAttribute(maxLengthKey));
    } catch (NumberFormatException e) {
      if (logger.isLoggable(Level.INFO))
        logger.info(
          "Could not parse maxLength value to integer: " + e.getMessage());
      return DEFAULTMAXLENGTH;
    }
  }

  /**
   * @param source - String name of data source
   */
  public void setSource(String source) {
    setAttribute(sourceKey, source);
  }

  /**
   * Returns the source.
   * 
   * @return String source
   */
  public String getSource() {
    return getAttribute(sourceKey);
  }


  
  /**
   * @param LinkoutURL - String base for HTML link references
   */
  public void setLinkoutURL(String linkoutURL) {
    setAttribute(linkoutURLKey, linkoutURL);
  }

  /**
   * Returns the linkoutURL.
   * @return String linkoutURL.
   */
  public String getLinkoutURL() {
    return getAttribute(linkoutURLKey);
  }
  
  /**
   * @param ImageURL - String base for HTML link references
   */
  public void setImageURL(String imageURL) {
	setAttribute(imageURLKey, imageURL);
  }

  /**
   * Returns the imageURL.
   * @return String imageURL.
   */
  public String getImageURL() {
	return getAttribute(imageURLKey);
  }

  /**
   * Determine if this AttributeDescription supports a given field and tableConstraint. Useful for mapping Query Attribute Objects
   * back to their corresponding MartConfiguration AttributeDescription.
   * @param field -- String field of the mart datbase table
   * @param tableConstraint -- String constraining the field to a particular table or table type
   * @return boolean, true if given field and given tableConstraint matches underlying values for AttributeDescription 
   */
  public boolean supports(String field, String tableConstraint) {
    boolean f =
      getAttribute(fieldKey) != null && getAttribute(fieldKey).equals(field);
    boolean tc =
      (tableConstraint == null)
        ? getAttribute(tableConstraintKey) == null
        : getAttribute(tableConstraintKey) != null
        && getAttribute(tableConstraintKey).equals(tableConstraint);

    return f && tc;
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();

    buf.append("[ AttributeDescription:");
    buf.append(super.toString());
    buf.append("]");

    return buf.toString();
  }

  /**
   * Allows Equality Comparisons of AttributeDescription objects
   */
  public boolean equals(Object o) {
    return o instanceof AttributeDescription
      && hashCode() == ((AttributeDescription) o).hashCode();
  }

  /**
   * set the hasBrokenField flag to true, eg. the field
   * does not refer to an existing field in a particular Mart Dataset instance.
   *
   */
  public void setFieldBroken() {
    hasBrokenField = true;
  }

  /**
   * Determine if this AttributeDescription has a broken field reference.
   * @return boolean, true if field is broken, false otherwise
   */
  public boolean hasBrokenField() {
    return hasBrokenField;
  }

  /**
   * set the hasBrokenTableConstraint flag to true, eg. the tableConstraint
   * does not refer to an existing table in a particular Mart Dataset instance.
   *
   */
  public void setTableConstraintBroken() {
    hasBrokenTableConstraint = true;
  }

  /**
   * Determine if this AttributeDescription has a broken tableConstraint reference.
   * @return boolean, true if tableConstraint is broken, false otherwise
   */
  public boolean hasBrokenTableConstraint() {
    return hasBrokenTableConstraint;
  }

  /**
   * True if one of hasBrokenField or hasBrokenTableConstraint is true.
   * @return boolean
   */
  public boolean isBroken() {
    return hasBrokenField || hasBrokenTableConstraint;
  }
}
