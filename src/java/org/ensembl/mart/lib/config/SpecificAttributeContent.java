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
 * Contains all of the information required by a UI to display a specific attribute, and create an Attribute object to add to a mart Query.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class SpecificAttributeContent extends AttributeDescription {
  /**
   * Copy constructor. Constructs an exact copy of an existing AttributeDescription.
   * @param a AttributeDescription to copy.
   */
  public SpecificAttributeContent(SpecificAttributeContent a) {
    super(a);
  }

  /**
   * Empty Constructor should only be used by DatasetConfigEditor
   *
   */
  public SpecificAttributeContent() {
    super();
  }

  /**
   * Constructs a AttributeDescription with just the internalName and field.
   * not used anywhere yet and should probably add tableConstraint and Key
   * @param internalName String name to internally represent the AttributeDescription. Must not be null or empty
   * @param field String name of the field in the mart for this Attribute. Must not be null or empty.
   * @throws ConfigurationException when values are null or empty.
   */
  public SpecificAttributeContent(String internalName, String field)
    throws ConfigurationException {
	  super(internalName, field);
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
  public SpecificAttributeContent(
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

    super(internalName,
    	     field,
    	     displayName,
    	     maxLength,
    	     tableConstraint,
    	     key,
    	     description,
    	     source,
    	     homePageURL,
    	     linkoutURL,
    	     imageURL,
    	     datasetLink,
    	     defaultString,
    	     pointerDataset,
    	     pointerInterface,
    	     pointerAttribute,
    	     pointerFilter,
    	     checkForNulls,
    	     pipeDisplay);
  }

  }
