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
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class Option extends QueryFilterSettings {

	private QueryFilterSettings parent;

	private final String refKey = "ref";
	private final String isSelectableKey = "isSelectable"; 
	private int[] reqFields = {0};// rendered red in AttributeTable
 
	private boolean hasOptions = false;

	private boolean hasBrokenOptions = false;
	private boolean hasBrokenPushActions = false;
	
	//options can contain options
	private List uiOptions = new ArrayList();
	private Hashtable uiOptionNameMap = new Hashtable();
	private List pushActions = new ArrayList();
	
	private List specificOptionContents = new ArrayList();
	private Hashtable specificOptionContentNameMap = new Hashtable();

	// cache one Option per call to supports/getOptionByFieldNameTableConstraint
	private Option lastSupportingOption = null;

  /**
   * Copy Constructor.  Creates a copy of an existing Option.
   * @param o - Option to copy
   */
  public Option(Option o) {
    super(o);
  	Option[] os = o.getOptions();
  	for (int i = 0, n = os.length; i < n; i++) {
      addOption( new Option( os[i] ) );
    }

    
    SpecificOptionContent[] sf = (SpecificOptionContent[])o.getSpecificOptionContents().toArray(new SpecificOptionContent[0]);
    for (int i = 0, n = sf.length; i < n; i++) {
      addSpecificOptionContent( new SpecificOptionContent( sf[i] ) );
    }
    
    PushAction[] pas = o.getPushActions();
    for (int i = 0, n = pas.length; i < n; i++) {
      addPushAction( new PushAction(pas[i] ) );
    }
    if (o.getField() == null || o.getField().equals("")){
    	setRequiredFields(reqFields);
    }
  }
  
  /**
   * Add a dynamicImportableContent to the AttributeDescription.
   * 
   * @param a dynamicImportableContent object.
   */
  public void addSpecificOptionContent(SpecificOptionContent a) {
	  specificOptionContents.add(a);
	  specificOptionContentNameMap.put(a.getInternalName(),a);
  }

  /**
   * Add a dynamicFilterContent to the AttributeDescription.
   * 
   * @param a dynamicFilterContent object.
   */
  public SpecificOptionContent getSpecificOptionContent(String name) {
	  return (SpecificOptionContent)specificOptionContentNameMap.get(name);
  }
  
  /**
   * Add a dynamicFilterContent to the AttributeDescription.
   * 
   * @param a dynamicFilterContent object.
   */
  public List getSpecificOptionContents() {
	  return this.specificOptionContents;
  }

  /**
   * Remove an dynamicFilterContent from this AttributeDescription.
   * @param a -- dynamicFilterContent to be removed.
   */
  public void insertSpecificOptionContent(int index, SpecificOptionContent a) {
	specificOptionContents.add(index,a);
	specificOptionContentNameMap.put(a.getInternalName(),a);
  }

  /**
   * Remove an dynamicFilterContent from this AttributeDescription.
   * @param a -- dynamicFilterContent to be removed.
   */
  public void removeSpecificOptionContent(SpecificOptionContent a) {
	specificOptionContents.remove(a);
	specificOptionContentNameMap.remove(a.getInternalName());
  }

  
  public void resolveText(DynamicDataset ds) {
	  ds.resolveText(this, this);
      if (getTableConstraint()!=null && !getTableConstraint().equals("") && !getTableConstraint().equals("main")) {
		  setTableConstraint(ds.getInternalName()+"__"+getTableConstraint());
		  /*
          if (!getTableConstraint().startsWith(ds.getInternalName()+"__")) 
          	if (getTableConstraint().matches(".*__.*__.*"))
          		setTableConstraint(ds.getInternalName()+"__"+getTableConstraint().split("__")[1]+"__"+getTableConstraint().split("__")[2]);
          	else
          		setTableConstraint(ds.getInternalName()+"__"+getTableConstraint().split("__")[0]+"__"+getTableConstraint().split("__")[1]);
          }
          */
      }
	  for (Iterator i = this.uiOptions.iterator(); i.hasNext(); )
		  ((Option)i.next()).resolveText(ds);
	  for (Iterator i = this.pushActions.iterator(); i.hasNext(); )
		  ((PushAction)i.next()).resolveText(ds);
  }

  /**
   * Special Copy constructor allowing a FilterDescription to
   * be converted to an Option based upon their common fields.
   * This is a destructive operation, in that not all fields and
   * child objects of the Option are supported by the FilterDescription,
   * and vice versa.  In particular, the isSelectable field is set to
   * true. For these reasons, this method is reserved for use by
   * the DatasetConfigEditor application to facilitate the conversion
   * between these two objects, with subsequent editing by the user.
   * @param fd - FilterDescription to be converted to an Option
   */
  public Option(FilterDescription fd) {
  	super(fd);

    setSelectable("true");
		Option[] os = fd.getOptions();
		for (int i = 0, n = os.length; i < n; i++) {
			addOption( new Option( os[i] ) );
		}  	  
  }
  
	/**
	 * Empty Constructor should only be used by DatasetConfigEditor.
	 *
	 */
	public Option() {
		super();
    
    setAttribute(isSelectableKey, null);
    setAttribute(refKey, null);
		//setRequiredFields(reqFields);
	}

	public Option(String internalName, String isSelectable) throws ConfigurationException {
		this(internalName, isSelectable, "", "", "", "", "", "", "", "", "", "", "", null, "","","","", "", "", "", "", "","", "", "", "", "", "", "","","");
	}

	public Option(
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

		super(internalName, displayName, description, field, value, tableConstraint, key, type, qualifier, 
			legalQualifiers, otherFilters, buttonURL, regexp, defaultValue, defaultOn, filterList, attributePage, 
			attribute, colForDisplay,pointerDataset,pointerInterface,pointerFilter,displayType,multipleValues,
			graph,style,autoCompletion,dependsOnType,dependsOn,checkForNulls);

    	setAttribute(isSelectableKey, isSelectable );
		setAttribute(refKey, ref);
		if (field == null || field.equals("")){
			setRequiredFields(reqFields);
		}
		
	}

	/**
	 * add an Option object to this Option.  Options are stored in the order that they are added.
	 * @param o - an Option object
	 */
	public void addOption(Option o) {
		uiOptions.add(o);
		uiOptionNameMap.put(o.getInternalName(), o);
		hasOptions = true;
	}

	/**
	 * Remove an Option from this Option.
	 * @param o -- Option to be removed.
	 */
	public void removeOption(Option o) {
		uiOptionNameMap.remove(o.getInternalName());
		uiOptions.remove(o);

		if (uiOptions.size() < 1)
			hasOptions = false;
	}

	/**
	 * Insert an Option at a specific position within the Option list for this Option.
	 * Options occuring at or after this position are shifted right.
	 * @param position -- position to insert the given Option
	 * @param o -- Option to insert.
	 */
	public void insertOption(int position, Option o) {
		uiOptions.add(position, o);
		uiOptionNameMap.put(o.getInternalName(), o);
		hasOptions = true;
	}

	/**
	 * Insert an Option before a specific Option in the list, named by internalName.
	 * @param internalName -- internalName of Option before which the given Option is to be inserted.
	 * @param o -- Option to be inserted
	 * @throws ConfigurationExction when the Option does not contain an Option named by internalName
	 */
	public void insertOptionBeforeOption(String internalName, Option o) throws ConfigurationException {
		if (!uiOptionNameMap.containsKey(internalName))
			throw new ConfigurationException("Option does not contain an Option " + internalName + "\n");
		insertOption(uiOptions.indexOf(uiOptionNameMap.get(internalName)), o);
	}

	/**
	 * Insert an Option after a specific Option in the list, named by internalName.
	 * @param internalName -- internalName of Option after which the given Option is to be inserted.
	 * @param o -- Option to be inserted
	 * @throws ConfigurationExction when the Option does not contain an Option named by internalName
	 */
	public void insertOptionAfterOption(String internalName, Option o) throws ConfigurationException {
		if (!uiOptionNameMap.containsKey(internalName))
			throw new ConfigurationException("Option does not contain an Option " + internalName + "\n");
		insertOption(uiOptions.indexOf(uiOptionNameMap.get(internalName)) + 1, o);
	}

	/**
	 * Add a group of Option objects in one call.  Subsequent calls to
	 * addOption or setOptions will add to what was added before, in the order that they are added.
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
	 * Get all Option objects available as an array.  Options are returned in the order they were added.
	 * @return Option[]
	 */
	public Option[] getOptions() {
		Option[] ret = new Option[uiOptions.size()];
		uiOptions.toArray(ret);
		return ret;
	}

	/**
	 * Determine if this Option contains an Option.  This only determines if the specified internalName
	 * maps to a specific Option in the Option during a shallow search.  It does not do a deep search
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
	 * @param internalName - String name of the requested Option.
	 * @return Option object named by internalName
	 */
	public Option getOptionByInternalName(String internalName) {
		if (uiOptionNameMap.containsKey(internalName))
			return (Option) uiOptionNameMap.get(internalName);
		else
			return null;
	}
  
  /**
   * Sets the isSelectable attribute to the string value
   * of this flag.  The value of his should be a one of "true" or "false".
   * @param isSelectable - String isSelectable value
   */
  public void setSelectable(String isSelectable) {
  	setAttribute(isSelectableKey, isSelectable);
  }
  
	/**
	 * Determine if this Option is Selectable in the UI.
	 * @return boolean, true if selectable, false otherwise
	 */
	public boolean isSelectable() {
		return Boolean.valueOf( getAttribute( isSelectableKey) ).booleanValue();
	}

	/**
	 * Determine if this Option has underlying Options.
	 * @return boolean, true if this Option has underlying options, false if not.
	 */
	public boolean hasOptions() {
		return hasOptions;
	}

	public String getDisplayName(String refIname) {
		if (uiOptionNameMap.containsKey(refIname))
			return ((Option) uiOptionNameMap.get( attributes.getProperty(internalNameKey) )).getDisplayName(refIname);
		else {
			if (pushActions.size() < 1)
				return null;
			else {
				for (int i = 0, n = pushActions.size(); i < n; i++) {
					PushAction element = (PushAction) pushActions.get(i);
					if (element.containsOption(refIname))
						return element.getOptionByInternalName(refIname).getDisplayName();
				}
				return null; // nothing found
			}
		}
	}

	public String getDescription(String refIname) {
		if (uiOptionNameMap.containsKey(refIname))
			return ((Option) uiOptionNameMap.get( attributes.getProperty(internalNameKey) )).getDescription(refIname);
		else {
			if (pushActions.size() < 1)
				return null;
			else {
				for (int i = 0, n = pushActions.size(); i < n; i++) {
					PushAction element = (PushAction) pushActions.get(i);
					if (element.containsOption(refIname))
						return element.getOptionByInternalName(refIname).getDescription();
				}
				return null; // nothing found
			}
		}
	}

	/**
	 * Returns the field for this Option, or the field for a child Option (possibly within a PushAction) 
	 * of this Option, named by refIname
	 * @param refIname -- name of Option for which Field is desired.
	 * @return String field
	 */
	public String getField(String refIname) {
		if (uiOptionNameMap.containsKey(refIname))
			return ((Option) uiOptionNameMap.get( attributes.getProperty(internalNameKey) )).getField(refIname);
		else {
			if (pushActions.size() < 1)
				return null;
			else {
				for (int i = 0, n = pushActions.size(); i < n; i++) {
					PushAction element = (PushAction) pushActions.get(i);
					if (element.containsOption(refIname))
						return element.getOptionByInternalName(refIname).getField();
				}
				return null; // nothing found
			}
		}
	}

	/**
	 * Returns the tableConstraint for this Option, or a child Option (possibly within a PushAction),
	 * named by refIname.
	 * @param refIname -- name of Option for which tableConstraint is desired.
	 * @return String tableConstraint
	 */
	public String getTableConstraint(String refIname) {
		if (uiOptionNameMap.containsKey(refIname))
			return ((Option) uiOptionNameMap.get( attributes.getProperty(internalNameKey) )).getTableConstraint(refIname);
		else {
			if (pushActions.size() < 1)
				return null;
			else {
				for (int i = 0, n = pushActions.size(); i < n; i++) {
					PushAction element = (PushAction) pushActions.get(i);
					if (element.containsOption(refIname))
						return element.getOptionByInternalName(refIname).getTableConstraint();
				}
				return null; // nothing found
			}
		}
	}

	/**
	* Returns the Key for this Option, or a child Option (possibly within a PushAction),
	* named by refIname.
	* @param refIname -- name of Option for which Key is desired.
	* @return String tableConstraint
	*/
	public String getKey(String refIname) {
			if (uiOptionNameMap.containsKey(refIname))
				return ((Option) uiOptionNameMap.get( attributes.getProperty(internalNameKey) )).getKey(refIname);
			else {
				if (pushActions.size() < 1)
					return null;
				else {
					for (int i = 0, n = pushActions.size(); i < n; i++) {
						PushAction element = (PushAction) pushActions.get(i);
						if (element.containsOption(refIname))
							return element.getOptionByInternalName(refIname).getKey();
					}
					return null; // nothing found
				}
			}
		}


	/**
	 * Searches each PushAction for potential completer names.  If it contains Options acting as Filters 
	 * (eg, to be pushed to some other FilterDescription), adds 'internalName.option.getInternalName()' to the list of potential completer names.
	 * If it references another FilterDescription, and contains value Options, adds 'internalName.pushOptions.getRef()' to the list.
	 * @return List of potential completer names
	 */
	public List getCompleterNames() {
		List names = new ArrayList();
        return names;
        
//  disable pushOptions for now
//		for (int i = 0, n = pushActions.size(); i < n; i++) {
//			PushAction element = (PushAction) pushActions.get(i);
//			Option[] ops = element.getOptions();
//
//			for (int j = 0, o = ops.length; j < o; j++) {
//				Option option = ops[j];
//				String completer = null;
//
//				if (option.getField() != null && option.getField().length() > 0 && option.getType() != null && option.getType().length() > 0) {
//					//push option filter, should get superoption.subotion as name
//					completer =  attributes.getProperty(internalNameKey)  + "." + option.getInternalName();
//				} else if (option.getValue() != null && option.getValue().length() > 0) {
//					//push option value, should get superoption.pushoptionref as name
//					completer =  attributes.getProperty(internalNameKey)  + "." + element.getRef();
//				} // else not needed
//
//				if (!(completer == null || names.contains(completer)))
//					names.add(completer);
//			}
//		}
//		return names;
	}

	/**
	 * Sets the ref for this Option.
	 * @param ref -- String ref, which refers to another FilterDescription or Option internalName.
	 */
	public void setRef(String ref) {
		setAttribute(refKey, ref);
	}

	/**
	 * Get the Ref for this Option.
	 * @return String ref.
	 */
	public String getRef() {
		return getAttribute(refKey);
	}

	/**
	 * Get all PushOption objects available as an array.  OptionPushes are returned in the order they were added.
	 * @return PushOption[]
	 */
	public PushAction[] getPushActions() {
		return (PushAction[]) pushActions.toArray(new PushAction[pushActions.size()]);
	}

	/**
	 * Add a PushAction to this Option.
	 * @param PushAction object to be added.
	 */
	public void addPushAction(PushAction optionPush) {
		pushActions.add(optionPush);
	}

	/**
	 * Insert a PushAction at a specific position within the Option list for this Option.
	 * Options occuring at or after this position are shifted right.
	 * @param position -- position to insert the given Option
	 * @param o -- PushAction to insert.
	 */
	public void insertPushAction(int position, PushAction o) {
		pushActions.add(position, o);
	}

  /**
   * Add a group of PushAction objects in one call.
   * @param pushactions  Array of PushActions
   */
  public void addPushActions(PushAction[] pushactions) {
    pushActions.addAll(Arrays.asList(pushactions));
  }
  
	/**
	 * Remove a PushAction from this Option.
	 * @param pa -- PushAction to be removed.
	 */
	public void removePushAction(PushAction pa) {
		pushActions.remove(pa);
	}

	/**
	 * Get the legalQualifiers for this Option, or a child Option (possibly in a PushAction) named by refIname.
	 * @param refIname -- internalName of Option for which legalQualifiers is desired.
	 * @return String legalQualifiers.
	 */
	public String getLegalQualifiers(String refIname) {
		if (uiOptionNameMap.containsKey(refIname))
			return ((Option) uiOptionNameMap.get( attributes.getProperty(internalNameKey) )).getLegalQualifiers(refIname);
		else {
			if (pushActions.size() < 1)
				return null;
			else {
				for (int i = 0, n = pushActions.size(); i < n; i++) {
					PushAction element = (PushAction) pushActions.get(i);
					if (element.containsOption(refIname))
						return element.getOptionByInternalName(refIname).getLegalQualifiers();
				}
				return null; // nothing found
			}
		}
	}

	/**
	 * Get the type for this Option, or a child Option (possibly within a PushAction) named by
	 * refIname.
	 * @param refIname -- String name of Option for which type is desired.
	 * @return String type.
	 */
	public String getType(String refIname) {
		if (uiOptionNameMap.containsKey(refIname))
			return ((Option) uiOptionNameMap.get( attributes.getProperty(internalNameKey) )).getType(refIname);
		else {
			if (pushActions.size() < 1)
				return null;
			else {
				for (int i = 0, n = pushActions.size(); i < n; i++) {
					PushAction element = (PushAction) pushActions.get(i);
					if (element.containsOption(refIname))
						return element.getOptionByInternalName(refIname).getType();
				}
				return null; // nothing found
			}
		}
	}

	/**
	 * Determine if an Option supports a given field and tableConstraint.
	 * 
	 * @param field -- String mart database field
	 * @param tableConstraint -- String mart database table
   * @param qualifier - filter qualifier
	 * @return boolean, true if the field and tableConstraint for this Option match the given field and tableConstraint, false otherwise
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

				if (!supports) {
					for (int i = 0, n = pushActions.size(); i < n; i++) {
						PushAction element = (PushAction) pushActions.get(i);
						if (element.supports(field, tableConstraint, qualifier)) {
							lastSupportingOption = element.getOptionByFieldNameTableConstraint(field, tableConstraint, qualifier);
							supports = true;
							break;
						}
					}
				}
			} else {
				if (lastSupportingOption.supports(field, tableConstraint, qualifier))
					supports = true;
				else {
					lastSupportingOption = null;
					supports = supports(field, tableConstraint, qualifier);
				}
			}
		}
		return supports;
	}

	/**
	 * Get an Option by its field and tableConstraint.
	 * @param field -- Field for desired Option
	 * @param tableConstraint -- tableConstraint for desired Option.
   * @param qualifier - filter qualifier
	 * @return Option supporting this field and tableConstraint (eg, getOptionByFieldNameTableConstraint(f,t).supports(f,t) will always be true).
	 */
	public Option getOptionByFieldNameTableConstraint(String field, String tableConstraint, String qualifier) {
		if (supports(field, tableConstraint, qualifier))
			return lastSupportingOption;
		else
			return null;
	}

	/**
	 * Get the internalName of an Option by a given field and tableConstraint.
	 * @param field -- field for Option for which internalName is desired
	 * @param tableConstraint -- tableConstraint for Option for which internalName is desired
   * @param qualifier -- filter qualifier
	 * @return String internalName
	 */
	public String getInternalNameByFieldNameTableConstraint(String field, String tableConstraint, String qualifier) {
		if (getAttribute(fieldKey) != null && getAttribute(fieldKey).equals(field) 
    && getAttribute(tableConstraintKey) != null && getAttribute(tableConstraintKey).equals(tableConstraint)
    &&  getAttribute(qualifier) != null &&  getAttribute(qualifier).equals(qualifier))
			return  attributes.getProperty(internalNameKey) ;
		else {
			for (int i = 0, n = pushActions.size(); i < n; i++) {
				PushAction element = (PushAction) pushActions.get(i);
				if (element.supports(field, tableConstraint, qualifier)) {
					return  attributes.getProperty(internalNameKey)  + "." + element.getOptionInternalNameByFieldNameTableConstraint(field, tableConstraint, qualifier);
				}
			}
		}

		return null;
	}

  /**
   * Get the displayName of an Option by a given field and tableConstraint.
   * @param field -- field for Option for which displayName is desired
   * @param tableConstraint -- tableConstraint for Option for which displayName is desired
   * @param qualifier - filter qualifier
   * @return String displayName
   */
  public String getDisplayNameByFieldNameTableConstraint(String field, String tableConstraint, String qualifier) {
    if (getAttribute(fieldKey) != null && getAttribute(fieldKey).equals(field) && getAttribute(tableConstraintKey) != null && getAttribute(tableConstraintKey).equals(tableConstraint))
      return  attributes.getProperty(displayNameKey) ;
    else {
      for (int i = 0, n = pushActions.size(); i < n; i++) {
        PushAction element = (PushAction) pushActions.get(i);
        if (element.supports(field, tableConstraint, qualifier)) {
          return  attributes.getProperty(displayNameKey)  + "." + element.getOptionDisplayNameByFieldNameTableConstraint(field, tableConstraint, qualifier);
        }
      }
    }

    return null;
  }
  
	/**
		* Debug output
		*/
	public String toString() {
		StringBuffer buf = new StringBuffer();

		buf.append("[");
		buf.append(super.toString());
 	  buf.append(", options=").append(uiOptions);
		buf.append(", pushActions=").append( pushActions );
		buf.append("]");

		return buf.toString();
	}
	/**
		* Allows Equality Comparisons manipulation of Option objects
		*/
	public boolean equals(Object o) {
		return o instanceof Option && hashCode() == o.hashCode();
	}

	/* (non-Javadoc)
		* @see java.lang.Object#hashCode()
		*/
	public int hashCode() {

		int hashcode = super.hashCode();

		for (Iterator iter = uiOptions.iterator(); iter.hasNext();) {
			hashcode = (31 * hashcode) + iter.next().hashCode();
		}

		for (Iterator iter = pushActions.iterator(); iter.hasNext();) {
			hashcode = (31 * hashcode) + iter.next().hashCode();
		}

		return hashcode;
	}

	public void setParent(QueryFilterSettings parent) {
		this.parent = parent;
	}

	public QueryFilterSettings getParent() {
		return parent;
	}

	/**
	 * Returns field based on context. 
	 * @return field if set otherwise getParent().getFieldFromContext().
	 */
	public String getFieldFromContext() {
		if (valid( getAttribute(fieldKey) ))
			return  getAttribute(fieldKey) ;
		else
			return getParent().getFieldFromContext();
	}

	/**
	 * Returns value based on context. 
	 * @return value if set otherwise getParent().getValueFromContext().
	 */
	public String getValueFromContext() {
		if (valid( getAttribute(valueKey) ))
			return  getAttribute(valueKey) ;
		else
			return getParent().getValueFromContext();
	}

	/**
	 * Returns type based on context. 
	 * @return type if set otherwise getParent().getTypeFromContext().
	 */
	public String getTypeFromContext() {
		if (valid( getAttribute(typeKey) ))
			return  getAttribute(typeKey) ;
		else
			return getParent().getTypeFromContext();
	}
  
  /**
   * Returns the legalQualifiers based on context.
   * @return legalQualifiers if set, otherwise getParent().getLegalQualifiersFromContext().
   */
  public String getLegalQualifiersFromContext() {
    if (valid( getAttribute(legalQualifiersKey) ))
      return  getAttribute(legalQualifiersKey) ;
    else
      return getParent().getLegalQualifiersFromContext();
  }


	/**
	 * Returns tableContraint based on context. 
	 * @return tableConstraint if set otherwise getParent().getTableConstraintFromContext().
	 */
	public String getTableConstraintFromContext() {
		if (valid( getAttribute(tableConstraintKey) ))
			return  getAttribute(tableConstraintKey) ;
		else
			return getParent().getTableConstraintFromContext();
	}

	/**
	 * Returns key based on context. 
	 * @return key if set otherwise getParent().getKeyFromContext().
	 */
	public String getKeyFromContext() {
		if (valid( getAttribute(keyKey) ))
			return  getAttribute(keyKey) ;
		else
			return getParent().getKeyFromContext();
	}
	
	/**
	 * Returns the qualifier based on context.
	 * @return qualifier if set otherwise getParent().getQualifierFromContext().
	 */
	public String getQualifierFromContext() {
		if (valid( getAttribute(qualifierKey) ))
			return  getAttribute(qualifierKey) ;
		else
			return getParent().getQualifierFromContext();
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
	 * Determine if this Option has Broken Options.
	 * @return boolean, true if one or more Options are broken, false otherwise.
	 */
	public boolean hasBrokenOptions() {
		return hasBrokenOptions;
	}
	
	/**
	 * set the hasBrokenPushActions flag to true, eg, one or more PushAction objects (possibly within PushAction
	 * Objects within an subOption tree) contain Options with broken fields or tableConstraints. 
	 *
	 */
	public void setPushActionsBroken() {
		hasBrokenPushActions = true;
	}
	
	/**
	 * Determine if this Option has Broken PushActions.
	 * @return boolean, true if one or more PushActions are broken, false otherwise.
	 */
	public boolean hasBrokenPushActions() {
		return hasBrokenPushActions;
	}
	
	/**
	 * True if one of hasBrokenField, hasBrokenTableConstraint, hasBrokenOptions, hasBrokenPushActions is true.
	 * @return boolean
	 */
	public boolean isBroken() {
		return hasBrokenField || hasBrokenTableConstraint || hasBrokenOptions || hasBrokenPushActions;
	}
}
