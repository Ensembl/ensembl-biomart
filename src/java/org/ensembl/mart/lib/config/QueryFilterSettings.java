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
	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.ensembl.mart.lib.config;


/**
 * @author craig
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public abstract class QueryFilterSettings extends BaseNamedConfigurationObject {

	protected final String otherFiltersKey = "otherFilters";
	protected final String fieldKey = "field";
	protected final String valueKey = "value";
  protected final String tableConstraintKey = "tableConstraint";
	protected final String keyKey = "key";
	protected final String typeKey = "type";
	protected final String qualifierKey = "qualifier";
	protected final String legalQualifiersKey = "legal_qualifiers";
	protected final String buttonURLKey = "buttonURL";
	protected final String regexpKey = "regexp";
	protected final String defaultValueKey = "defaultValue";
	protected final String defaultOnKey = "defaultOn";
	protected final String filterListKey = "filterList";
	protected final String attributePageKey = "setAttributePage";
	protected final String attributeKey = "setAttribute";
	protected final String colForDisplayKey = "colForDisplay";
	protected final String pointerDatasetKey = "pointerDataset";
	protected final String pointerInterfaceKey = "pointerInterface";
	protected final String pointerFilterKey = "pointerFilter";
	protected final String displayTypeKey = "displayType";
	protected final String multipleValuesKey = "multipleValues";
	protected final String graphKey = "graph";
	protected final String styleKey = "style";
	protected final String autoCompletionKey = "autoCompletion";
	protected final String dependsOnTypeKey = "dependsOnType";
	protected final String dependsOnKey = "dependsOn";
	protected final String checkForNullsKey = "checkForNulls";
	
    //protected final String hiddenKey = "hidden";
	private int[] reqFields = {0,5,7,8,10};// rendered red in AttributeTable
	   
  private final String[] titles = new String[] { fieldKey, 
                                                 valueKey,
                                                 tableConstraintKey,
                                                 keyKey,
                                                 typeKey,
                                                 qualifierKey,
                                                 legalQualifiersKey,
                                                 otherFiltersKey,
                                                 buttonURLKey,
                                                 regexpKey,
                                                 defaultValueKey,
                                                 defaultOnKey,
                                                 filterListKey,
                                                 attributePageKey,
                                                 attributeKey,
                                                 colForDisplayKey,
                                                 pointerDatasetKey,
                                                 pointerInterfaceKey,
                                                 pointerFilterKey,
												 displayTypeKey,
												 multipleValuesKey,
												 graphKey,
											 	 styleKey,
												 autoCompletionKey,
												 dependsOnTypeKey,
												 dependsOnKey,
												 checkForNullsKey	 
  };

	/**
	 * Copy constructor.  Creates an exact copy of an existing object.
	 * @param bo - BaseNamedConfigurationObject to copy.
	 */
	public QueryFilterSettings(BaseNamedConfigurationObject bo) {
		super(bo);
		setRequiredFields(reqFields);
	}

	/**
	 * Empty Constructor should only be used by DatasetConfigEditor
	 *
	 */
	public QueryFilterSettings() {
		super();
		setRequiredFields(reqFields);
    
    for (int i = 0, n = titles.length; i < n; i++) {
      setAttribute(titles[i], null); //establishes the order of the keys, and adds all possible attribute titles to getXMLAttributeTitles, even if never set in future
    }
	}

  /**
   * Get all Option objects available as an array.  Options are returned in the order they were added.
   * @return Option[]
   */
  public abstract Option[] getOptions();

  public abstract void removeOption(final Option option);

	/**
	 * @param internalName
	 * @param displayName
	 * @param description
	 * @throws ConfigurationException
	 */
	public QueryFilterSettings(String internalName, String displayName, String description)
		throws ConfigurationException {
			this(internalName, displayName, description, "", "", null, "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "","", "", "", "", "","");
	}

  public QueryFilterSettings(String internalName, String displayName, String description, String field, 
  	String value, String tableConstraint, String key, String type, String qualifier, String legalQualifiers, 
  	String otherFilters, String buttonURL, String regexp, String defaultValue, String defaultOn, String filterList, 
  	String attributePage, String attribute, String colForDisplay, String pointerDataset, String pointerInterface,
	String pointerFilter, String displayType, String multipleValues, String graph, String style, String autoCompletion,
	String dependsOnType, String dependsOn, String checkForNulls) throws ConfigurationException {
		super(internalName, displayName, description);
		
    	setAttribute(fieldKey, field);
    	setAttribute(valueKey, value);
		setAttribute(tableConstraintKey, tableConstraint);
	    setAttribute(keyKey, key);
		setAttribute(typeKey, type);
		setAttribute(qualifierKey, qualifier);
		setAttribute(legalQualifiersKey, legalQualifiers);
		setAttribute(otherFiltersKey, otherFilters);
		setAttribute(buttonURLKey, buttonURL);
		setAttribute(regexpKey, regexp);
		setAttribute(defaultValueKey, defaultValue);
		setAttribute(defaultOnKey, defaultOn);
		setAttribute(filterListKey, filterList);
		setAttribute(attributePageKey,attributePage);
		setAttribute(attributeKey,attribute);
		setAttribute(colForDisplayKey,colForDisplay);
		setAttribute(pointerDatasetKey, pointerDataset);
		setAttribute(pointerInterfaceKey, pointerInterface);
		setAttribute(pointerFilterKey, pointerFilter);
		setAttribute(displayTypeKey, displayType);
		setAttribute(multipleValuesKey, multipleValues);
		setAttribute(graphKey, graph);
		setAttribute(styleKey, style);
		setAttribute(autoCompletionKey, autoCompletion);
		setAttribute(dependsOnTypeKey, dependsOnType);
		setAttribute(dependsOnKey, dependsOn);
		setAttribute(checkForNullsKey, checkForNulls);
	    setRequiredFields(reqFields);
  }
  
	public void setField(String field) {
		setAttribute(fieldKey, field);
	}
	
	public String getField() {
		return getAttribute(fieldKey);
	}
	
	public void setRegExp(String field) {
		setAttribute(regexpKey, field);
	}
	
	public String getRegExp() {
		return getAttribute(regexpKey);
	}	
	
	public void setDefaultValue(String field) {
		setAttribute(defaultValueKey, field);
	}
	
	public String getDefaultValue() {
		return getAttribute(defaultValueKey);
	}		

	public void setDefaultOn(String field) {
		setAttribute(defaultOnKey, field);
	}
	
	public String getDefaultOn() {
		return getAttribute(defaultOnKey);
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
	 * @param pointerFilter - pointer filter, used for placeholder attributes
	 */
	public void setPointerFilter(String pointerFilter) {
	  setAttribute(pointerFilterKey, pointerFilter);
	}

	/**
	 * Returns the pointerFilter.
	 * 
	 * @return String pointerFilter
	 */
	public String getPointerFilter() {
	  return getAttribute(pointerFilterKey);
	}



	public void setFilterList(String field) {
		setAttribute(filterListKey, field);
	}
	
	public String getFilterList() {
		return getAttribute(filterListKey);
	}	

	public void setOtherFilters(String otherFilters) {
		setAttribute(otherFiltersKey, otherFilters);
	}
	
	public String getOtherFilters() {
		return getAttribute(otherFiltersKey);
	}

	public void setAttributePage(String attributePage) {
		setAttribute(attributePageKey, attributePage);
	}
	
	public String getAttributePage() {
		return getAttribute(attributePageKey);
	}
	
	public void setAttribute(String attribute) {
		setAttribute(attributeKey, attribute);
	}
	
	public String getAttribute() {
		return getAttribute(attributeKey);
	}
	
	public void setColForDisplay(String colForDisplay) {
		setAttribute(colForDisplayKey, colForDisplay);
	}
	
	public String getColForDisplay() {
		return getAttribute(colForDisplayKey);
	}

	public abstract String getFieldFromContext();

	public void setTableConstraint(String tableConstraint) {
		setAttribute(tableConstraintKey, tableConstraint);
	}

	public String getTableConstraint() {
		return getAttribute(tableConstraintKey);
	}

	
	//public void setHidden(String hidden) {
	//  setAttribute(hiddenKey, hidden);
	//}

	//public String getHidden() {
	//  return getAttribute(hiddenKey);
	//}


	public void setKey(String key) {
		setAttribute(keyKey, key);
	}

	public String getKey() {
		return getAttribute(keyKey);
	}

	public abstract String getTableConstraintFromContext();
	
	public abstract String getKeyFromContext();
	
	public void setDisplayType(String value) {
		setAttribute(displayTypeKey, value);
	}

	public String getDisplayType() {
		return getAttribute(displayTypeKey);
	}
	
	public void setMultipleValues(String value) {
		setAttribute(multipleValuesKey, value);
	}

	public String getMultipleValues() {
		return getAttribute(multipleValuesKey);
	}
	
	public void setGraph(String value) {
		setAttribute(graphKey, value);
	}

	public String getGraph() {
		return getAttribute(graphKey);
	}
	
	public void setStyle(String value) {
		setAttribute(styleKey, value);
	}

	public String getStyle() {
		return getAttribute(styleKey);
	}
	
	public void setAutoCompletion(String value) {
		setAttribute(autoCompletionKey, value);
	}

	public String getAutoCompletion() {
		return getAttribute(autoCompletionKey);
	}
	
	public void setDependsOnType(String value) {
		setAttribute(dependsOnTypeKey, value);
	}

	public String getDependsOnType() {
		return getAttribute(dependsOnTypeKey);
	}
	
	public void setDependsOn(String value) {
		setAttribute(dependsOnKey, value);
	}

	public String getDependsOn() {
		return getAttribute(dependsOnKey);
	}
	
	public void setCheckForNulls(String value) {
		setAttribute(checkForNullsKey, value);
	}

	public String getCheckForNulls() {
		return getAttribute(checkForNullsKey);
	}
	
	public void setType(String type) {
		setAttribute(typeKey, type);
	}

	public String getType() {
		return getAttribute(typeKey);
	}

	public abstract String getTypeFromContext();

	public void setQualifier(String qualifier) {
		setAttribute(qualifierKey, qualifier);
	}

	public String getQualifier() {
		return getAttribute(qualifierKey);
	}

	public abstract String getQualifierFromContext();

	public void setLegalQualifiers(String legalQualifiers) {
		setAttribute(legalQualifiersKey, legalQualifiers);
	}

	public String getLegalQualifiers() {
		return getAttribute(legalQualifiersKey);
	}
	public abstract String getLegalQualifiersFromContext();

  public abstract String getValueFromContext();
  
	public boolean supports(String field, String tableConstraint, String qualifier) {
		boolean supports = false;
		
		//if field is null, this Object cannot support any field x tableConstraint combination 
		if (getAttribute(fieldKey) != null) {
				supports = getAttribute(fieldKey).equals(field);
				
				if (supports) {
					String tableConstraintK = getAttribute(tableConstraintKey);
					
					//if the field matches, it only depends on the tableConstraints if the given or the object has a non null tableConstraint
					if (tableConstraint != null || tableConstraintK != null) {
						supports = tableConstraintK != null && tableConstraint != null && tableConstraintK.equals(tableConstraint);
            
            			if (qualifier!=null && supports) {
              				String qualifierK = getAttribute(qualifierKey); 
              				supports = qualifierK!=null && qualifierK.equals(qualifier);
            			}
					}
				}
		}
		
		return supports;
	}

	protected boolean hasBrokenField = false;
	protected boolean hasBrokenTableConstraint = false;
	/**
		 * set the hasBrokenField flag to true, eg. the field
		 * does not refer to an existing field in a particular Mart Dataset instance.
		 *
		 */
	public void setFieldBroken() {
		hasBrokenField = true;
	}

	/**
		 * Determine if this Option has a broken field reference.
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
		 * Determine if this Option has a broken tableConstraint reference.
		 * @return boolean, true if tableConstraint is broken, false otherwise
		 */
	public boolean hasBrokenTableConstraint() {
		return hasBrokenTableConstraint;
	}

	public void setValue(String value) {
		setAttribute(valueKey, value);
	}

	public String getValue() {
		return getAttribute(valueKey);
	}

	/**
	   * Returns specific option matching field and value. Does a deep search through options and push actions.
	   * @param field field in target option
	   * @param value value in target option
	   * @return option if found, otherwise null
	   */
	public Option getOptionByFieldNameValue(String field, String value) {
    
	  Option o = null;
    
	  Option[] os = getOptions();
	  for (int i = 0; o==null && i < os.length; i++) {
			
      Option option = os[i];
      
      String f = option.getFieldFromContext(); 
      String v = option.getValueFromContext();
			
      if ( f!=null && f.equals(field ) && v!=null && v.equals(value) ) {
	      o = option;
      }  

	    if ( o==null ) {
        PushAction[] pas = option.getPushActions();
        for (int j = 0; o==null && i < pas.length; i++) {
          Option[] ospa = pas[i].getOptions();
          for (int k = 0; o==null && k < ospa.length; k++) {
						Option optionpa = ospa[k];
						o = optionpa.getOptionByFieldNameValue( field, value );
					}
        }
        
	    }
    
      if ( o==null ) {
	      o = option.getOptionByFieldNameValue( field, value);
      }		
    }
	  return o;
	}
}
