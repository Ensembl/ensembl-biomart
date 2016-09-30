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
public class FilterDescription extends QueryFilterSettings {

	private Hashtable uiOptionNameMap = new Hashtable();
	private List uiOptions = new ArrayList();
	private boolean hasOptions = false;

	private List Enables = new ArrayList();
	private List Disables = new ArrayList();
	private List PushActions = new ArrayList();
	
	private List specificFilterContents = new ArrayList();
	private Hashtable specificFilterContentNameMap = new Hashtable();
	
	private boolean hasBrokenOptions = false;
	
	//cache one supporting Option for call to supports
	Option lastSupportingOption = null;

  /**
   * Copy Constructor. Constructs a new FilterDescription which is an
   * exact copy of the given FilterDescription.
   * @param fd - FilterDescription to be copied.
   */
  public FilterDescription(FilterDescription fd) {
    super( fd );
    
    Option[] os = fd.getOptions();
    for (int i = 0, n = os.length; i < n; i++) {
      addOption( new Option( os[i] ) );
    }
    
    SpecificFilterContent[] sf = (SpecificFilterContent[])fd.getSpecificFilterContents().toArray(new SpecificFilterContent[0]);
    for (int i = 0, n = sf.length; i < n; i++) {
      addSpecificFilterContent( new SpecificFilterContent( sf[i] ) );
    }
    
	//PushAction[] pa = fd.getPushActions();
	//for (int i = 0, n = pa.length; i < n; i++) {
	//   addPushAction( new PushAction( pa[i] ) );
	//}
    
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
  public FilterDescription(Option o) {
  	super(o);
		
		//need to remove some Option specific attributes
		attributes.remove("ref");
		attributes.remove("isSelectable");
		
		Option[] os = o.getOptions();
		for (int i = 0, n = os.length; i < n; i++) {
			addOption( new Option( os[i] ) );
		}
  }
  
	/**
	 * Empty Constructor should only be used by DatasetConfigEditor
	 *
	 */
	public FilterDescription() {
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
	public FilterDescription(String internalName, String field, String type, String legalQualifiers) throws ConfigurationException {
		this(internalName, field, type, "", legalQualifiers, "", "", null, "", "", "", "", "", "", "", "", "", "", "", "","", "", "", "", "", "","","","");
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
	 * @see FilterDescription
	 */
	public FilterDescription(
		String internalName,
		String field,
		String type,
		String qualifier,
		String legalQualifiers,
		String displayName,
		String tableConstraint,
		String key,
		String description,
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

		super(internalName, displayName, description, field, null, tableConstraint, key, type, qualifier, 
			legalQualifiers, otherFilters, buttonURL, regexp, defaultValue, defaultOn, filterList, attributePage, 
			attribute, colForDisplay,pointerDataset,pointerInterface,pointerFilter,displayType,multipleValues,
			graph,style,autoCompletion,dependsOnType,dependsOn,checkForNulls);

		if (type == null || type.equals(""))
			throw new ConfigurationException("FilterDescription requires a type.");
	}

	  
	  /**
	   * Add a dynamicImportableContent to the AttributeDescription.
	   * 
	   * @param a dynamicImportableContent object.
	   */
	  public void addSpecificFilterContent(SpecificFilterContent a) {
		  specificFilterContents.add(a);
		  specificFilterContentNameMap.put(a.getInternalName(),a);
	  }

	  /**
	   * Add a dynamicFilterContent to the AttributeDescription.
	   * 
	   * @param a dynamicFilterContent object.
	   */
	  public SpecificFilterContent getSpecificFilterContent(String name) {
		  return (SpecificFilterContent)specificFilterContentNameMap.get(name);
	  }
	  
	  /**
	   * Add a dynamicFilterContent to the AttributeDescription.
	   * 
	   * @param a dynamicFilterContent object.
	   */
	  public List getSpecificFilterContents() {
		  return this.specificFilterContents;
	  }

	  /**
	   * Remove an dynamicFilterContent from this AttributeDescription.
	   * @param a -- dynamicFilterContent to be removed.
	   */
	  public void insertSpecificFilterContent(int index, SpecificFilterContent a) {
		specificFilterContents.add(index,a);
		specificFilterContentNameMap.put(a.getInternalName(),a);
	  }

	  /**
	   * Remove an dynamicFilterContent from this AttributeDescription.
	   * @param a -- dynamicFilterContent to be removed.
	   */
	  public void removeSpecificFilterContent(SpecificFilterContent a) {
		specificFilterContents.remove(a);
		specificFilterContentNameMap.remove(a.getInternalName());
	  }


	/**
	 * Returns the description, given an internalName which may, in some cases, map to an Option instead of this FilterDescription.
	 * @param internalName -- internalName of either this FilterDescription, or an Option contained within this FilterDescription
	 * @return String description
	 */
	public String getDescription(String internalName) {
		if ( getAttribute( internalNameKey ).equals(internalName))
			return  getAttribute(descriptionKey);
		else {
			if (uiOptionNameMap.containsKey(internalName))
				return ((Option) uiOptionNameMap.get(internalName)).getDescription();
			else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
				// pushOption option
				String[] names = internalName.split("\\.");
				String optionIname = names[0];
				String refIname = names[1];

				if ( getAttribute( internalNameKey ).equals(refIname))
					return  getAttribute(descriptionKey);
				else if (uiOptionNameMap.containsKey(optionIname))
					return ((Option) uiOptionNameMap.get(optionIname)).getDescription(refIname);
				else
					return null; // nothing found
			} else
				return null; // nothing found
		}
	}




	/**
	 * Returns the displayName, given an internalName which may, in some cases, map to an Option instead of this FilterDescription.
	 * @param internalName -- internalName of either this FilterDescription, or an Option contained within this FilterDescription
	 * @return String displayName
	 */
	public String getDisplayname(String internalName) {
		if ( getAttribute(internalNameKey).equals(internalName))
			return  getAttribute(displayNameKey);
		else {
			if (uiOptionNameMap.containsKey(internalName))
				return ((Option) uiOptionNameMap.get(internalName)).getDisplayName();
			else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
				// pushOption option
				String[] names = internalName.split("\\.");
				String optionIname = names[0];
				String refIname = names[1];

				if ( getAttribute(internalNameKey).equals(refIname))
					return  getAttribute(displayNameKey);
				else if (uiOptionNameMap.containsKey(optionIname))
					return ((Option) uiOptionNameMap.get(optionIname)).getDisplayName(refIname);
				else
					return null; // nothing found
			} else
				return null; // nothing found
		}
	}

	/**
	 * Returns the field, given an internalName which may, in some cases, map to an Option instead of this FilterDescription.
	 * @param internalName -- internalName of either this FilterDescription, or an Option contained within this FilterDescription
	 * @return String field
	 */
	public String getField(String internalName) {
		if ( getAttribute(internalNameKey).equals(internalName))
			return  getAttribute(fieldKey);
		else {
			if (uiOptionNameMap.containsKey(internalName))
				return ((Option) uiOptionNameMap.get(internalName)).getField();
			else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
				// pushOption option
				String[] names = internalName.split("\\.");
				String optionIname = names[0];
				String refIname = names[1];

				if ( getAttribute(internalNameKey).equals(refIname))
					return  getAttribute(fieldKey);
				else if (uiOptionNameMap.containsKey(optionIname))
					return ((Option) uiOptionNameMap.get(optionIname)).getField(refIname);
				else
					return null; // nothing found
			} else
				return null; // nothing found
		}
	}

	/**
	 * Returns the type, given an internalName which may, in some cases, map to an Option instead of this FilterDescription.
	 * @param internalName -- internalName of either this FilterDescription, or an Option contained within this FilterDescription
	 * @return String type
	 */
	public String getType(String internalName) {
		if ( getAttribute(internalNameKey).equals(internalName))
			return  getAttribute(typeKey);
		else {
			if (uiOptionNameMap.containsKey(internalName))
				return ((Option) uiOptionNameMap.get(internalName)).getType();
			else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
				// pushOption option
				String[] names = internalName.split("\\.");
				String optionIname = names[0];
				String refIname = names[1];

				if ( getAttribute(internalNameKey).equals(refIname))
					return  getAttribute(typeKey);
				else if (uiOptionNameMap.containsKey(optionIname))
					return ((Option) uiOptionNameMap.get(optionIname)).getType(refIname);
				else
					return null; // nothing found
			} else
				return null; // nothing found
		}
	}

	/**
	 * Returns the tableConstraint, given an internalName which may, in some cases, map to an Option instead of this FilterDescription.
	 * @param internalName -- internalName of either this FilterDescription, or an Option contained within this FilterDescription
	 * @return String tableConstraint
	 */
	public String getTableConstraint(String internalName) {
		if ( getAttribute(internalNameKey).equals(internalName))
			return  getAttribute(tableConstraintKey);
		else {
			if (uiOptionNameMap.containsKey(internalName))
				return ((Option) uiOptionNameMap.get(internalName)).getTableConstraint();
			else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
				// pushOption option
				String[] names = internalName.split("\\.");
				String optionIname = names[0];
				String refIname = names[1];

				if ( getAttribute(internalNameKey).equals(refIname))
					return  getAttribute(tableConstraintKey);
				else if (uiOptionNameMap.containsKey(optionIname))
					return ((Option) uiOptionNameMap.get(optionIname)).getTableConstraint(refIname);
				else
					return null; // nothing found
			} else
				return null; // nothing found
		}
	}

	/**
	 * Returns the key, given an internalName which may, in some cases, map to an Option instead of this FilterDescription.
	 * @param internalName -- internalName of either this FilterDescription, or an Option contained within this FilterDescription
	 * @return String key
	 */
	public String getKey(String internalName) {
		if ( getAttribute(internalNameKey).equals(internalName))
			return  getAttribute(keyKey);
		else {
			if (uiOptionNameMap.containsKey(internalName))
				return ((Option) uiOptionNameMap.get(internalName)).getKey();
			else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
				// pushOption option
				String[] names = internalName.split("\\.");
				String optionIname = names[0];
				String refIname = names[1];

				if ( getAttribute(internalNameKey).equals(refIname))
					return  getAttribute(keyKey);
				else if (uiOptionNameMap.containsKey(optionIname))
					return ((Option) uiOptionNameMap.get(optionIname)).getKey(refIname);
				else
					return null; // nothing found
			} else
				return null; // nothing found
		}
	}



	/**
	 * Gets the legalQualifiers for the FilterDescription/Option named by internalName, which may be this particular FilterDescription,
	 * or a child Option (possibly occuring within in a PushAction).
	 * @param internalName -- String internalName of FilterDescription/Option for which legalQualifiers is desired.
	 * @return String legalQualifiers.
	 */
	public String getLegalQualifiers(String internalName) {
		if ( getAttribute(internalNameKey).equals(internalName))
			return  getAttribute(legalQualifiersKey);
		else {
			if (uiOptionNameMap.containsKey(internalName))
				return ((Option) uiOptionNameMap.get(internalName)).getLegalQualifiers();
			else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
				// pushOption option
				String[] names = internalName.split("\\.");
				String optionIname = names[0];
				String refIname = names[1];

				if (uiOptionNameMap.containsKey(optionIname))
					return ((Option) uiOptionNameMap.get(optionIname)).getLegalQualifiers(refIname);
				else
					return null; // nothing found
			} else
				return null; // nothing found
		}
	}

	/**
	 * Returns the internalName of the FilterDescription/Option that supports a given field and tableConstraint, which
	 * could be this particular FilterDescription, or a child Option (possibly occuring within a PushAction).
	 * @param field --  String field
	 * @param tableConstraint -- String table
   * @param qualifier -- Filter qualifier
	 * @return String internalName of supporting FilterDescription/Option
	 */
	public String getInternalNameByFieldNameTableConstraint(String field, String tableConstraint, String qualifier) {
		String ret = null;

		if (supports(field, tableConstraint,qualifier)) {
			if ( getAttribute(fieldKey) != null &&  getAttribute(fieldKey).equals(field) 
      &&  getAttribute(tableConstraintKey) != null &&  getAttribute(tableConstraintKey).equals(tableConstraint)
      // ignore qualifier if null
      &&  (qualifier==null || getAttribute(qualifierKey) != null &&  getAttribute(qualifierKey).equals(qualifier) )
            )
				ret = getAttribute(internalNameKey);
			else
				ret = lastSupportingOption.getInternalNameByFieldNameTableConstraint(field, tableConstraint, qualifier);
		}

		return ret;
	}
  
  /**
   * Returns the displayName of the FilterDescription/Option that supports a given field and tableConstraint, which
   * could be this particular FilterDescription, or a child Option (possibly occuring within a PushAction).
   * @param field --  String field
   * @param tableConstraint -- String table
   * @param qualifier -- Filter qualifier
   * @return String displayName of supporting FilterDescription/Option
   */
  public String getDisplayNameByFieldNameTableConstraint(String field, String tableConstraint, String qualifier) {
    String ret = null;

    if (supports(field, tableConstraint, qualifier)) {
      if ( getAttribute(fieldKey) != null &&  getAttribute(fieldKey).equals(field) 
      &&  getAttribute(tableConstraintKey) != null &&  getAttribute(tableConstraintKey).equals(tableConstraint)
      // ignore qualifier if it is null
      &&  (qualifier==null || getAttribute(qualifierKey) != null &&  getAttribute(qualifierKey).equals(qualifier)))
        ret = getAttribute(displayNameKey);
      else 
        ret = lastSupportingOption.getDisplayNameByFieldNameTableConstraint(field, tableConstraint, qualifier);
        
    }

    return ret;
  }

	/**
	 * Determine if this FilterDescription, or a child Option (possibly occuring within a PushAction)
	 * supports the given field and tableConstraint.  If the supporting Object is a child Option, this Option is cached for a subsequent call
   * to getInternalNameByFieldNameTableConstraint(String, String).
	 * @param field -- String field
	 * @param tableConstraint -- String tableConstraint
   * @param qualifier -- Filter qualifier, can be null if irrelevant
	 * @return boolean, true if this FilterDescription or a child Option supports the given FilterDescription, false otherwise.
	 */
	public boolean supports(String field, String tableConstraint, String qualifier) {
		boolean supports = super.supports(field, tableConstraint, qualifier);

		if (!supports) {
			if (lastSupportingOption == null) {
				for (Iterator iter = uiOptions.iterator(); iter.hasNext();) {
					Option element = (Option) iter.next();
					if (element.supports(field, tableConstraint, qualifier)) {
						lastSupportingOption = element;
						supports = true;
						break;
					}
				}
			} else {
				if (lastSupportingOption.supports(field, tableConstraint, qualifier))
					supports = true;
				else {
					lastSupportingOption = null;
					return supports(field, tableConstraint, qualifier);
				}
			}
		}

		return supports;
	}

	/**
	 * debug output
	 */
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[ FilterDescription:");
		buf.append(super.toString());
		buf.append(", Options=").append(uiOptions);
		buf.append("]");

		return buf.toString();
	}

	/**
	 * Allows Collections manipulation of FilterDescription objects
	 */
	public boolean equals(Object o) {
		return o instanceof FilterDescription && hashCode() == o.hashCode();
	}

	public int hashCode() {

			int hshcode = super.hashCode();

			for (Iterator iter = uiOptions.iterator(); iter.hasNext();) {
				Option option = (Option) iter.next();
				hshcode = (31 * hshcode) + option.hashCode();
			}

		return hshcode;
	}

	/**
	 * add an Option object to this FilterDescription.  Options are stored in the order that they are added.
	 * @param o - an Option object
	 */
	public void addOption(Option o) {
		uiOptions.add(o);
		uiOptionNameMap.put(o.getInternalName(), o);
		hasOptions = true;
	}

  /**
   * Remove an Option from the FilterDescription.
   * @param o -- Option to be removed
   */
  public void removeOption(Option o) {
    uiOptionNameMap.remove(o.getInternalName());
    uiOptions.remove(o);
    if (uiOptions.size() < 1)
     hasOptions = false;
  }
 
  /**
   * Remove Options from the FilterDescription.
   */
  public void removeOptions() {
  	//uiOptionNameMap.clear();
  	//uiOptions.clear();
	uiOptionNameMap = new Hashtable();
	uiOptions = new ArrayList();
	hasOptions = false;
  	//Option[] ops = getOptions();
  	//for (int i = 0; i < ops.length; i++){
  	//	removeOption(ops[i]);
  	//}
  }
  
  /**
   * Insert an Option at a specific position within the list of Options for this FilterDescription.
   * Options occuring at or after the given position are shifted right.
   * @param position -- position to insert the given Option
   * @param o -- Option to be inserted.
   */
  public void insertOption(int position, Option o) {
    uiOptions.add(position, o);
    uiOptionNameMap.put(o.getInternalName(), o);
    hasOptions = true;
  }

  
    
  /**
   * Insert an Option before a specified Option, named by internalName.
   * @param internalName -- String internalName of the Option before which the given Option should be inserted.
   * @param o -- Option to insert.
   * @throws ConfigurationException when the FilterDescription does not contain an Option named by internalName.
   */
  public void insertOptionBeforeOption(String internalName, Option o) throws ConfigurationException {
    if (!uiOptionNameMap.containsKey(internalName))
      throw new ConfigurationException("FilterDescription does not contain an Option " + internalName + "\n");
    insertOption( uiOptions.indexOf( uiOptionNameMap.get(internalName) ), o );
  }
  
  /**
   * Insert an Option after a specified Option, named by internalName.
   * @param internalName -- String internalName of the Option after which the given Option should be inserted.
   * @param o -- Option to insert.
   * @throws ConfigurationException when the FilterDescription does not contain an Option named by internalName.
   */
  public void insertOptionAfterOption(String internalName, Option o) throws ConfigurationException {
    if (!uiOptionNameMap.containsKey(internalName))
      throw new ConfigurationException("FilterDescription does not contain an Option " + internalName + "\n");
    insertOption( uiOptions.indexOf( uiOptionNameMap.get(internalName) ) + 1, o );
  }
  
  /**
   * Add a group of Option objects in one call.  Subsequent calls to
   * addOption or addOptions will add to what was added before, in the order that they are added.
   * @param o - an array of Option objects
   */
  public void addOptions(Option[] o) {
    for (int i = 0, n = o.length; i < n; i++) {
      uiOptions.add(o[i]);
      uiOptionNameMap.put(o[i].getInternalName(), o[i]);
    }
    hasOptions = true;
  }
    
	/**
	 * Determine if this FilterDescription contains an Option.  This only determines if the specified internalName
	 * maps to a specific Option in the FilterDescription during a shallow search.  It does not do a deep search
	 * within the Options.
	 * 
	 * @param internalName - String name of the requested Option
	 * @return boolean, true if found, false if not found.
	 */
	public boolean containsOption(String internalName) {
		return uiOptionNameMap.containsKey(internalName);
	}

	/**
	 * Get a specific Option named by internalName.  This does not do a deep search within Options.
	 * 
	 * @param internalName - String name of the requested Option.   * 
	 * @return Option object named by internalName
	 */
	public Option getOptionByInternalName(String internalName) {
		if (uiOptionNameMap.containsKey(internalName))
			return (Option) uiOptionNameMap.get(internalName);
		else
			return null;
	}

  /**
	 * Get all Option objects available as an array.  Options are returned in the order they were added.
	 * @return Option[]
	 */
	public Option[] getOptions() {
		Option[] ret = new Option[uiOptions.size()];
		uiOptions.toArray(ret);
		return ret;
	}

	/**
	 * Determine if this FilterCollection has Options Available.
	 * 
	 * @return boolean, true if Options are available, false if not.
	 */
	public boolean hasOptions() {
		return hasOptions;
	}


	/**
	 * Add an PushAction object to this FilterDescription, allowing it to Enable another FilterDescription in the GUI.
	 * @param e, PushAction Object to add.
	 */
	//public void addPushAction(PushAction e) {
	//	PushActions.add(e);
	//}

	/**
	   * Get all PushAction objects available as an array.  Options are returned in the order they were added.
	   * @return PushAction[]
	   */
	  //public PushAction[] getPushActions() {
	///	  PushAction[] ret = new PushAction[PushActions.size()];
	//	  PushActions.toArray(ret);
	//	  return ret;
	  //}


	/**
	 * Insert an PushAction object to this FilterDescription, allowing it to Enable another FilterDescription in the GUI.
	 * @param e, PushAction Object to add.
	 */
	//public void insertPushAction(int position, PushAction e) {
	//	PushActions.add(position, e);
	//}

  /**
   * Returns a List of String names that MartShell can display in its command completion system.
   * @return List of Strings
   */
	public List getCompleterNames() {
		List names = new ArrayList();

		if ( getAttribute(fieldKey) != null &&  getAttribute(fieldKey).length() > 0 &&  getAttribute(typeKey) != null &&  getAttribute(typeKey).length() > 0) {
			//add internalName, and any PushOption names that are found
			if (!names.contains(getInternalName()))
				names.add(getInternalName());

			for (Iterator iter = uiOptions.iterator(); iter.hasNext();) {
				Option element = (Option) iter.next();
				names.addAll(element.getCompleterNames());
			}
		} else {
			for (Iterator iter = uiOptions.iterator(); iter.hasNext();) {
				Option element = (Option) iter.next();
                if (element.getHidden() != null && element.getHidden().equals("true")) continue;
                if (element.getDisplay() != null && element.getDisplay().equals("true")) continue;
                
				String opfield = element.getField();
				String optype = element.getType();

				if (opfield != null && opfield.length() > 0 && optype != null && optype.length() > 0) {
					if (!names.contains(element.getInternalName()))
						names.add(element.getInternalName());
				} else {
					//try pushOptions
					names.addAll(element.getCompleterNames());
				}
			}
		}
		return names;
	}

  /**
   * Returns a List of String qualifiers that MartShell can display in its command completion system, for a given
   * internalName, which refers to this FilterDescription, or one of its child Options.
   * @param internalName -- name of the FilterDescription/child Option for which the qualifiers are desired.
   * @return List Strings
   */
	public List getCompleterQualifiers(String internalName) {
		List quals = new ArrayList();

		if ( getAttribute(internalNameKey).equals(internalName)) {
			//filterDescription has legalQualifiers
			if ( getAttribute(legalQualifiersKey) != null &&  getAttribute(legalQualifiersKey).length() > 0)
				quals.addAll(Arrays.asList( getAttribute(legalQualifiersKey).split(",")));
		} else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
			//PushOption Filter Option has legalQualifiers 
			String[] iname_info = internalName.split("\\.");
			String supername = iname_info[0];
			String refname = iname_info[1];

			Option superOption = getOptionByInternalName(supername);
			PushAction[] pos = superOption.getPushActions();

			for (int i = 0, n = pos.length; i < n; i++) {
				PushAction po = pos[i];

				if (po.containsOption(refname)) {
					Option[] os = po.getOptionByInternalName(refname).getOptions();

					for (int j = 0, l = os.length; j < l; j++) {
						Option option = os[j];
						String opquals = option.getLegalQualifiers();

						if (opquals != null && opquals.length() > 0) {
							List theseQs = Arrays.asList(opquals.split(","));
							for (int k = 0, m = theseQs.size(); k < m; j++) {
								String qual = (String) theseQs.get(j);
								if (!quals.contains(qual))
									quals.add(qual);
							}
						}
					}
				}
			}
		} else {
			//subOption has legalQualifiers
			if (containsOption(internalName)) {
				Option option = getOptionByInternalName(internalName);
				String opquals = option.getLegalQualifiers();

				if (opquals != null && opquals.length() > 0) {
					quals.addAll(Arrays.asList(opquals.split(",")));
				}
			}
		}

		return quals;
	}

  /**
   * Returns a List of String completer values for a given internalName referring to either this FilterDescription,
   * or one of its child Options.
   * @param internalName -- String name for FilterDescription/Option for which values are desired.
   * @return List Strings
   */
	public List getCompleterValues(String internalName) {
		List vals = new ArrayList();

		if ( getAttribute(internalNameKey).equals(internalName)) {
			Option[] myops = getOptions();

			for (int i = 0, n = myops.length; i < n; i++) {
				Option option = myops[i];
				String opvalue = option.getValue();

				if (opvalue != null && opvalue.length() > 0) {
					if (!vals.contains(opvalue))
						vals.add(opvalue);
				}
			}
		} else if ((internalName.indexOf(".") > 0) && !(internalName.endsWith("."))) {
			//PushOption Option either Filter Option with Value Options, or Value Options
			String[] iname_info = internalName.split("\\.");
			String supername = iname_info[0];
			String refname = iname_info[1];

			Option superOption = getOptionByInternalName(supername);
			PushAction[] pos = superOption.getPushActions();

			for (int i = 0, n = pos.length; i < n; i++) {
				PushAction po = pos[i];
				if (po.getRef().equals(refname)) {
					//value options
					Option[] suboptions = po.getOptions();
					for (int j = 0, m = suboptions.length; j < m; j++) {
						Option option = suboptions[j];
						String opvalue = option.getValue();

						if (opvalue != null && opvalue.length() > 0) {
							if (!vals.contains(opvalue))
								vals.add(opvalue);
						}
					}
				} else {
					//Option Filter with Value Options
					if (po.containsOption(refname)) {
						Option[] os = po.getOptionByInternalName(refname).getOptions();

						for (int j = 0, l = os.length; j < l; j++) {
							Option option = os[j];
							String opvalue = option.getValue();

							if (opvalue != null && opvalue.length() > 0) {
								if (!vals.contains(opvalue))
									vals.add(opvalue);
							}
						}
					}
				}
			}
		} else {
			if (containsOption(internalName)) {
				Option[] ops = getOptionByInternalName(internalName).getOptions();

				for (int i = 0, n = ops.length; i < n; i++) {
					Option option = ops[i];
					String opvalue = option.getValue();

					if (opvalue != null && opvalue.length() > 0) {
						if (!vals.contains(opvalue))
							vals.add(opvalue);
					}
				}
			}
		}

		return vals;
	}

	/**
	 * Recurses through options beneath all PushAction Objects to set option.parent.
	 * 
	 * <br>
	 * For optionPush->option1 option1.parent = optionPush.ref
	 * </br>
	 * 
	 * <br>
	 * For optionPush->option1->option1_1 : option1_1.parent = option1
	 * </bre>
	 * 
	 * @param d
	 */
	public void setParentsForAllPushOptionOptions(DatasetConfig d) throws ConfigurationException {

		setParentsForAllPushOptionOptions(d, getOptions());

	}

	/**
	 * Recurses through options looking for Options inside PushActions. Set the parent
	 * for these Options to the FieldDesscription specified by PushOption.ref.
	 * @param d
	 * @param options
	 */
	private void setParentsForAllPushOptionOptions(DatasetConfig dataset, Option[] options) throws ConfigurationException {

		for (int i = 0, n = options.length; i < n; i++) {

			Option option = options[i];
			PushAction[] pushOptions = option.getPushActions();

			for (int j = 0; j < pushOptions.length; j++) {

				PushAction pushOption = pushOptions[j];

				String targetName = pushOption.getRef();
				QueryFilterSettings parent = dataset.getFilterDescriptionByInternalName(targetName);
                
                // not relevant anymore
				//if (parent == null)
				//	throw new ConfigurationException(
				//		"OptionPush.ref = " + targetName + " Refers to a FilterDescription that is not in dataset " + dataset.getInternalName());

				// Assign the target FilterDescription to each option.
				Option[] options2 = pushOption.getOptions();
				for (int k = 0; k < options2.length; k++) {
					Option option2 = options2[k];
					option2.setParent(parent);
                    
					setParentsForAllPushOptionOptions(dataset, options2);  
					//}
					
				}
			}
		}
	}

	/**
	 * Same as getField(). Included to make FilterDescription implement QueryFilterSettings
	 * interface.
	 * @return field
	 */
	public String getFieldFromContext() {
		return  getField();
	}

	/**
	 * Same as getValue(). Included to make FilterDescription implement QueryFilterSettings
	 * interface.
	 * @return value
	 */
	public String getValueFromContext() {
		return getValue();
	}

	/**
	 * Same as getType(). Included to make FilterDescription implement QueryFilterSettings
	 * interface.
	 * @return type
	 */
	public String getTypeFromContext() {
		return getType();
	}


	/**
	 * Same as getQualifier.  Included to make FilterDescription impliment QueryFilterSettings
	 * interface.
	 * @return qualifier
	 */
	public String getQualifierFromContext() {
		return getQualifier();
	}

  /**
   * Same as getlegalQualifiers.  Included to make FilterDescription impliment QueryFilterSettings
   * interface.
   * @return legalQualifiers
   */
  public String getLegalQualifiersFromContext() {
    return getLegalQualifiers();
  }
  
	/**
	 * Same as getTableConstraint(). Included to make FilterDescription implement QueryFilterSettings
	 * interface.
	 * @return tableConstraint
	 */
	public String getTableConstraintFromContext() {
		return getTableConstraint();
	}

	/**
	 * Same as getKey(). Included to make FilterDescription implement QueryFilterSettings
	 * interface.
	 * @return key
	 */
	public String getKeyFromContext() {
		return getKey();
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
	 * Determine if this FilterDescription has a broken field reference.
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
	 * Determine if this FilterDescription has a broken tableConstraint reference.
	 * @return boolean, true if tableConstraint is broken, false otherwise
	 */
	public boolean hasBrokenTableConstraint() {
		return hasBrokenTableConstraint;
	}
	
	/**
	 * set the hasBrokenOptions flag to true, eg, one or more Option objects (possibly within PushAction
	 * Objects within an Option tree) contain broken fields or tableConstraints. 
	 *
	 */
	public void setOptionsBroken() {
		hasBrokenOptions = true;
	}
	
	/**
	 * Determine if this FilterDescription has Broken Options.
	 * @return boolean, true if one or more Options are broken, false otherwise.
	 */
	public boolean hasBrokenOptions() {
		return hasBrokenOptions;
	}
	
	/**
	 * True if one of hasBrokenField, hasBrokenTableConstraint, or hasBrokenOptions is true.
	 * @return boolean
	 */
	public boolean isBroken() {
		return hasBrokenField || hasBrokenTableConstraint || hasBrokenOptions;
	}
	public boolean isBrokenExceptOpts() {
		return hasBrokenField || hasBrokenTableConstraint;
	}
}
