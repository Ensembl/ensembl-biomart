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

/**
 * Basic Object from which all named Configuration Objects inherit.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public abstract class BaseNamedConfigurationObject extends
		BaseConfigurationObject {

	protected final String internalNameKey = "internalName";
	protected final String displayNameKey = "displayName";
	protected final String descriptionKey = "description";
	protected final String hiddenKey = "hidden";
	protected final String displayKey = "hideDisplay";
	protected final String useDefaultKey = "useDefault";

	protected int templateDrivenFlag;

	private final String[] titles = new String[] { internalNameKey,
			displayNameKey, descriptionKey, hiddenKey, displayKey, useDefaultKey };

	/**
	 * Determines if string is an invalid attribute value.
	 * 
	 * @param s
	 * @return true if string is null or the empty string.
	 */
	public static final boolean isInvalid(String s) {
		return s == null && "".equals(s);
	}

	/**
	 * Determines if string is a valid attribute value.
	 * 
	 * @param s
	 * @return true if string is not null and not empty
	 */
	public static final boolean valid(String s) {
		return s != null && !"".equals(s);
	}

	/**
	 * Copy constructor. Creates an exact copy of an existing object.
	 * 
	 * @param bo
	 *            - BaseNamedConfigurationObject to copy.
	 */
	public BaseNamedConfigurationObject(BaseNamedConfigurationObject bo) {
		super(bo);
	}

	public BaseNamedConfigurationObject() {
		super();

		for (int i = 0, n = titles.length; i < n; i++) {
			setAttribute(titles[i], null); // establishes the order of the keys,
											// and adds all possible attribute
											// titles to getXMLAttributeTitles,
											// even if never set in future
		}
	}

	public BaseNamedConfigurationObject(String internalName,
			String displayName, String description)
			throws ConfigurationException {
		super();

		if (internalName == null || internalName.equals(""))
			throw new ConfigurationException(
					"Configuration Object must contain an internalName\n");

		setAttribute(internalNameKey, internalName);
		setAttribute(displayNameKey, displayName);
		setAttribute(descriptionKey, description);
		setAttribute(hiddenKey, null);
		setAttribute(displayKey, null);
		setAttribute(useDefaultKey, "true");
	}

	/**
	 * Sets the templateDriven flag for this DatasetConfig object
	 * 
	 * @param flag
	 *            -- 1 means the dataset config is template driven for all atts
	 *            except table, field etc.
	 */
	public void setTemplateDrivenFlag(int flag) {
		templateDrivenFlag = flag;
	}

	/**
	 * gets the templateDriven flag for this DatasetConfig object
	 */
	public int getTemplateDrivenFlag() {
		return templateDrivenFlag;
	}

	/**
	 * Returns the Description
	 * 
	 * @return String description
	 */
	public String getDescription() {
		return attributes.getProperty(descriptionKey);
	}

	/**
	 * Returns the displayName
	 * 
	 * @return String displayName
	 */
	public String getDisplayName() {
		return attributes.getProperty(displayNameKey);
	}

	/**
	 * Returns the internalName
	 * 
	 * @return String internalName
	 */
	public String getInternalName() {
		return attributes.getProperty(internalNameKey);
	}

	/**
	 * Returns the hidden status
	 * 
	 * @return String hidden
	 */
	public String getHidden() {
		return attributes.getProperty(hiddenKey);
	}
	
	public String getUseDefault() {
		return attributes.getProperty(useDefaultKey);
	}


	/**
	 * Returns the display status
	 * 
	 * @return String display
	 */
	public String getDisplay() {
		return attributes.getProperty(displayKey);
	}

	/**
	 * Sets the description for this object
	 * 
	 * @param string
	 */
	public void setDescription(String description) {
		setAttribute(descriptionKey, description);
	}

	/**
	 * Sets the displayName for this object
	 * 
	 * @param string
	 */
	public void setDisplayName(String displayName) {
		if (displayName == null)
			displayName = "";
		setAttribute(displayNameKey, displayName);
	}

	public void setUseDefault(String useDefault) {
		if (useDefault == null)
			useDefault = "true";
		setAttribute(useDefaultKey, useDefault);
	}
	
	/**
	 * Sets the internalName for this object
	 * 
	 * @param string
	 */
	public void setInternalName(String internalName) {
		setAttribute(internalNameKey, internalName);
	}

	/**
	 * Sets the hidden for this object
	 * 
	 * @param string
	 */
	public void setHidden(String hidden) {
		setAttribute(hiddenKey, hidden);
	}

	/**
	 * Sets the display for this object
	 * 
	 * @param string
	 */
	public void setDisplay(String display) {
		setAttribute(displayKey, display);
	}

	public int hashCode() {
		return super.hashCode();
	}

	public boolean equals(Object o) {
		return o instanceof BaseNamedConfigurationObject
				&& o.hashCode() == hashCode();
	}

	public String toString() {
		return super.toString();
	}

	public abstract boolean isBroken();

	public boolean isUseDefault() {
		return "true".equals(this.getUseDefault());
	}

}
