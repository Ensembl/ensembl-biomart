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
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;


/**
 * Contains all of the information necessary for the UI to display the information for a specific filter,
 * and add this filter as a Filter to a Query.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class SpecificOptionContent extends Option {

  /**
   * Copy Constructor. Constructs a new FilterDescription which is an
   * exact copy of the given FilterDescription.
   * @param fd - FilterDescription to be copied.
   */
  public SpecificOptionContent(SpecificOptionContent fd) {
    super( fd );
  }

  /**
   * Special Copy constructor that allows an Option to be converted
   * to a FilterDescription based upon their common fields. This is
   * a destructive action, in that not all fields in an Option are 
   * represented in a FilterDescription, and FilterDescription objects
   * do not contain PushAction objects, whild Option objects do not
   * contain Enable or Disable objects.  For this reason, this method
   * is strictly reserved for use for the DatasetConfigEditor application,
   * to facilitate conversions between these objects, with subsequent editing by the user. 
   * @param o - Option to be converted to a FilterDescription.
   */
  public SpecificOptionContent(Option o) {
  	super(o);
  }
  
	/**
	 * Empty Constructor should only be used by DatasetConfigEditor
	 *
	 */
	public SpecificOptionContent() {
		super();
	}

	/**
	 * Constructor for a FilterDescription named by internalName internally, with a field, type, 
   * and legalQualifiers. not used anywhere yet and should probably add tableConstraint and Key
   * 
	 * @param internalName String internal name of the FilterDescription. Must not be null or empty.
	 * @param field String name of the field to reference in the mart.
	 * @param type String type of filter.  Must not be null or empty.
	 * @param legalQualifiers String, comma-separated list of legalQualifiers to use in a MartShell MQL
	 * @throws ConfigurationException when required values are null or empty, or when a filterSetName is set, but no filterSetReq is submitted.
	 */
	public SpecificOptionContent(String internalName, String isSelectable) throws ConfigurationException {
		this(internalName, isSelectable, "", "", "", "", "", "", "", "", "", "", "", null, "","","","", "", "", "", "", "","", "", "", "", "", "", "","","");
	}

	/**
	 * Constructor for a fully defined FilterDescription
	 * 
	 * @param internalName String internal name of the FilterDescription. Must not be null or empty.
	 * @param field String name of the field to reference in the mart.
	 * @param type String type of filter.  Must not be null or empty.
	 * @param qualifier String qualifier to apply to a filter for this Filter.
	 * @param legal_qualifiers String, comma-separated list of legalQualifiers to use in a MartShell MQL
	 * @param displayName String name to display in a UI
	 * @param tableConstraint String table basename to constrain SQL field
	 * @param key String join field key
	 * @param description String description of the Filter
	 * 
	 * @throws ConfigurationException when required values are null or empty
	 * @see SpecificOptionContent
	 */

	public SpecificOptionContent(
		String internalName,
		String isSelectable,
		String displayName,
		String description,
		String field,
		String tableConstraint,
		String key,
		String value,
		String ref,
		String type,
		String qualifier,
		String legalQualifiers,
		String otherFilters,
		String buttonURL,
		String regexp,
		String defaultValue,
		String defaultOn,
		String filterList,
		String attributePage,
		String attribute,
		String colForDisplay,
		String pointerDataset,
		String pointerInterface,
		String pointerFilter,
		String displayType,
		String multipleValues,
		String graph,
		String style,
		String autoCompletion,
		String dependsOnType,
		String dependsOn,
		String checkForNulls)
		throws ConfigurationException {

		super(
				internalName,
				isSelectable,
				displayName,
				description,
				field,
				tableConstraint,
				key,
				value,
				ref,
				type,
				qualifier,
				legalQualifiers,
				otherFilters,
				buttonURL,
				regexp,
				defaultValue,
				defaultOn,
				filterList,
				attributePage,
				attribute,
				colForDisplay,
				pointerDataset,
				pointerInterface,
				pointerFilter,
				displayType,
				multipleValues,
				graph,
				style,
				autoCompletion,
				dependsOnType,
				dependsOn,
				checkForNulls);
	}

}
