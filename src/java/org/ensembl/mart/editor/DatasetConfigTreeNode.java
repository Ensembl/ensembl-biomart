/*
	Copyright (C) 2003 EBI, GRL

	This library is free software; you can redistribute it and/or
	modify it under the terms of the GNU Lesser General Public
	License as published by the Free Software Foundation; either
	version 2.1 of the License, or (at your option) any later version.

	This library is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
	Lesser General Public License for more details.d

	You should have received a copy of the GNU Lesser General Public
	License along with this library; if not, write to the Free Software
	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.ensembl.mart.editor;

import java.util.Iterator;
import java.util.List;

import javax.swing.tree.DefaultMutableTreeNode;

import org.ensembl.mart.lib.config.AttributeCollection;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributeList;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.BaseNamedConfigurationObject;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.DynamicDataset;
import org.ensembl.mart.lib.config.Exportable;
import org.ensembl.mart.lib.config.FilterCollection;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterGroup;
import org.ensembl.mart.lib.config.FilterPage;
import org.ensembl.mart.lib.config.Importable;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.lib.config.PushAction;
import org.ensembl.mart.lib.config.SpecificAttributeContent;
import org.ensembl.mart.lib.config.SpecificFilterContent;
import org.ensembl.mart.lib.config.SpecificOptionContent;

//import org.ensembl.mart.lib.config.SeqModule;
 

/**
 * Class DatasetConfigTreeNode extends DefaultMutableTreeNode.
 *
 * <p>This class is written so that the tree node is aware of the datasetconfig etc objects
 * </p>
 *
 * @author <a href="mailto:katerina@ebi.ac.uk">Katerina Tzouvara</a>
 * //@see org.ensembl.mart.config.DatasetConfig
 */

public class DatasetConfigTreeNode extends DefaultMutableTreeNode {

  /**
   * Each Node stores all child objects in a single Vector, but some DatasetConfigTree
   * userObjects store heterogenous groups of children in different lists in a particular 
   * order. This method calculates the index adjustment to apply to any node Vector index 
   * to get the DatasetConfigTree userObject index, which could be 0 for any DatasetConfigTree
   * object that only stores one type of child object.  In general, for a DatasetConfigTree
   * userObject which stores separate lists of objects a, b, and c in order, the relationship
   * between the node Vector index (V) and the individual object index within the
   * parent DatasetConfigTree userObject is:
   * (a) V[i] = a[i]
   * (b) V[i] = b[i - (a.length)]
   * (c) V[i] = c[i - (a.length + b.length)] 
   * @param parent - DatasetConfigTree userObject into which dropnode is to be dropped 
   * @param child - DatasetConfigTree userObject object for which an index inside parent is needed 
   * @return adjustment for any index of child inside parent.
   */
  protected static int getHeterogenousOffset(Object parent, Object child) {
    int hetOffset = -1;
    
	if (child instanceof org.ensembl.mart.lib.config.Importable) {
		DatasetConfig dsc = (DatasetConfig) parent;
		hetOffset = 
			((dsc.getTemplateFlag()==null && dsc.getTemplateFlag().equals("1"))?1:0);
	}
    else if (child instanceof org.ensembl.mart.lib.config.Exportable) {
      //Exportables go after Importables within a DatasetConfig
	  DatasetConfig dsc = (DatasetConfig) parent;
      hetOffset = ((dsc.getTemplateFlag()==null && dsc.getTemplateFlag().equals("1"))?1:0)
		+ dsc.getImportables().length;
    } else if (child instanceof org.ensembl.mart.lib.config.FilterPage) {
      //FilterPages go after Importables and Exportables within a DatasetConfig
      DatasetConfig dsc = (DatasetConfig) parent;
      hetOffset = ((dsc.getTemplateFlag()==null && dsc.getTemplateFlag().equals("1"))?1:0)
		+ dsc.getImportables().length + dsc.getExportables().length;
    } else if (child instanceof org.ensembl.mart.lib.config.AttributePage) {
      //AttributePages go after Importables, Exportables, and FilterPages within a DatasetConfig
      DatasetConfig dsc = (DatasetConfig) parent;
      hetOffset = ((dsc.getTemplateFlag()==null && dsc.getTemplateFlag().equals("1"))?1:0)
		+ dsc.getImportables().length + dsc.getExportables().length + dsc.getFilterPages().length;
    //} //else if (child instanceof org.ensembl.mart.lib.config.Disable) {
      //Disables go after Enables within a FilterDescription
      //FilterDescription fdesc = (FilterDescription) parent;
      //hetOffset = fdesc.getEnables().length;
    } 
    //else if (child instanceof org.ensembl.mart.lib.config.Option) {
      //if (parent instanceof org.ensembl.mart.lib.config.FilterDescription) {
        //Options go after Enables and Disables within a FilterDescription
        //FilterDescription fdesc = (FilterDescription) parent;
        
      //} else {
        //Options go first within an Option
        //hetOffset = 0;
      //}
    //}
    else if (child instanceof org.ensembl.mart.lib.config.PushAction) {
      //PushActions go after Options within an Option
      Option op = (Option) parent;
      hetOffset = op.getOptions().length;
    } else {
      //for all others insert at top of list
      hetOffset = 0;
    }
    return hetOffset;
  }
  
	protected String name;

	public DatasetConfigTreeNode(String name) {
		this.name = name;
	}

	public DatasetConfigTreeNode(String name, Object obj) {
		this.name = name;
		this.setUserObject(obj);
	}

	public void setName(String newName) {
		name = newName;
	}

	public String toString() {
		return name;
	}

	public void setUserObject(Object obj) {
		//System.out.println("ADDING TEST " + obj);
		super.setUserObject(obj);
		//System.out.println("--- SET USER OBJECT CALLED FOR "+obj.getClass().getName());
		String nodeObjectClass = obj.getClass().getName();
		if (nodeObjectClass.equals("org.ensembl.mart.lib.config.DatasetConfig")) {
			setName("DatasetConfig: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			DatasetConfig dsv = (DatasetConfig) obj;

			// Is it a template?
			if (dsv.getTemplateFlag()!=null && dsv.getTemplateFlag().equals("1")) {
				// Find out what DSs it uses.
			DatasetConfigTreeNode fpNode = new DatasetConfigTreeNode("PartitionTable: Datasets");
			fpNode.setUserObject(new BaseNamedConfigurationObject() {
				public boolean isBroken() { return false; }
			});
			List datasetAtts = dsv.getDynamicDatasets();
			for (int a = 0; a < datasetAtts.size(); a++){
				DynamicDataset ds = (DynamicDataset)datasetAtts.get(a);
				DatasetConfigTreeNode fp = new DatasetConfigTreeNode("Partition: "+ds.getInternalName());
				fp.setUserObject(ds);
				fpNode.add(fp);
			}
			this.add(fpNode);
			}
			
			Importable[] imps = dsv.getImportables();
			for (int i = 0; i < imps.length; i++) {
				Importable importable = imps[i];
				String impName = importable.getLinkName();
				DatasetConfigTreeNode impNode = new DatasetConfigTreeNode("Importable:" + impName);
				impNode.setUserObject(importable);
				this.add(impNode);
				
			}
			Exportable[] exps = dsv.getExportables();
			for (int i = 0; i < exps.length; i++) {
				Exportable exportable = exps[i];
				String expName = exportable.getLinkName();
				DatasetConfigTreeNode expNode = new DatasetConfigTreeNode("Exportable:" + expName);
				expNode.setUserObject(exportable);
				this.add(expNode);
											
			}
			
			FilterPage[] fpages = dsv.getFilterPages();
			for (int i = 0; i < fpages.length; i++) {
				if (fpages[i].getClass().getName().equals("org.ensembl.mart.lib.config.FilterPage")) {
					FilterPage fp = fpages[i];
					String fpName = fp.getInternalName();
					DatasetConfigTreeNode fpNode = new DatasetConfigTreeNode("FilterPage:" + fpName);
					fpNode.setUserObject(fp);

					this.add(fpNode);
					List groups = fp.getFilterGroups();
					for (int j = 0; j < groups.size(); j++) {
						if (groups.get(j).getClass().getName().equals("org.ensembl.mart.lib.config.FilterGroup")) {
							FilterGroup fiGroup = (FilterGroup) groups.get(j);
							String grName = fiGroup.getInternalName();
							DatasetConfigTreeNode grNode = new DatasetConfigTreeNode("FilterGroup:" + grName);
							grNode.setUserObject(fiGroup);
							FilterCollection[] collections = fiGroup.getFilterCollections();
							for (int z = 0; z < collections.length; z++) {
								FilterCollection fiCollection = collections[z];
								String colName = fiCollection.getInternalName();
								DatasetConfigTreeNode colNode = new DatasetConfigTreeNode("FilterCollection:" + colName);
								colNode.setUserObject(fiCollection);
								List descriptions = fiCollection.getFilterDescriptions();
								for (int y = 0; y < descriptions.size(); y++) {
									FilterDescription fiDescription = (FilterDescription) descriptions.get(y);
									String desName = fiDescription.getInternalName();
									DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Filter:" + desName);
									desNode.setUserObject(fiDescription);
									
								for (Iterator r = fiDescription.getSpecificFilterContents().iterator(); r.hasNext(); ) {
									SpecificFilterContent filtAtt = (SpecificFilterContent)r.next();
									String dynName = filtAtt.getInternalName();
										DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificFilterContent:" + dynName);
										dynNode.setUserObject(filtAtt);
										
										Option[] options = filtAtt.getOptions();
									
										for (int k = 0; k < options.length; k++) {
											Option option = options[k];
											String optionName = option.getInternalName();
											DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
											optionNode.setUserObject(option);

											// specific option contents
											for (Iterator r2 = option.getSpecificOptionContents().iterator(); r2.hasNext(); ) {
												SpecificOptionContent optAtt = (SpecificOptionContent)r2.next();
												String dynName2 = optAtt.getInternalName();
												DatasetConfigTreeNode dynNode2 = new DatasetConfigTreeNode("SpecificOptionContent:" + dynName2);
												dynNode2.setUserObject(optAtt);
											}
											
										   // code for options within options ie for expression menus
																				Option[] subOptions = option.getOptions();
																				for (int m = 0; m < subOptions.length; m++) {
																					Option op = subOptions[m];
																					String paoptionName = op.getInternalName();
																					DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																					subOptionNode.setUserObject(op);
																				}
																				// new code to cycle through push actions
																				PushAction[] pushActions = option.getPushActions();
																				for (int l = 0; l < pushActions.length; l++) {
																					PushAction pa = pushActions[l];
																					DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
																					pushActionNode.setUserObject(pa);

																					Option[] paOptions = pa.getOptions();
																					for (int m = 0; m < paOptions.length; m++) {
																						Option op = paOptions[m];
																						String paoptionName = op.getInternalName();
																						DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																						paOptionNode.setUserObject(op);
																					}

																				}
																				//end of new code
										}
									}
																				
									Option[] options = fiDescription.getOptions();
									
									for (int k = 0; k < options.length; k++) {
										Option option = options[k];
										String optionName = option.getInternalName();
										DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
										optionNode.setUserObject(option);

										// code for options within options ie for expression menus
										Option[] subOptions = option.getOptions();
										for (int m = 0; m < subOptions.length; m++) {
											Option op = subOptions[m];
											String paoptionName = op.getInternalName();
											DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
											subOptionNode.setUserObject(op);
										}
										// new code to cycle through push actions
										PushAction[] pushActions = option.getPushActions();
										for (int l = 0; l < pushActions.length; l++) {
											PushAction pa = pushActions[l];
											DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
											pushActionNode.setUserObject(pa);

											Option[] paOptions = pa.getOptions();
											for (int m = 0; m < paOptions.length; m++) {
												Option op = paOptions[m];
												String paoptionName = op.getInternalName();
												DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
												paOptionNode.setUserObject(op);
											}

										}
										//end of new code
									}
								}
							}
						}
					}
				}
			}
			AttributePage[] apages = dsv.getAttributePages();
			for (int i = 0; i < apages.length; i++) {
				if (apages[i].getClass().getName().equals("org.ensembl.mart.lib.config.AttributePage")) {
					AttributePage ap = apages[i];
					String apName = ap.getInternalName();
					DatasetConfigTreeNode apNode = new DatasetConfigTreeNode("AttributePage:" + apName);
					apNode.setUserObject(ap);
					this.add(apNode);
					List groups = ap.getAttributeGroups();
					for (int j = 0; j < groups.size(); j++) {
						if (groups.get(j).getClass().getName().equals("org.ensembl.mart.lib.config.AttributeGroup")) {
							AttributeGroup atGroup = (AttributeGroup) groups.get(j);
							String grName = atGroup.getInternalName();
							DatasetConfigTreeNode grNode = new DatasetConfigTreeNode("AttributeGroup:" + grName);
							grNode.setUserObject(atGroup);
							AttributeCollection[] collections = atGroup.getAttributeCollections();
							for (int z = 0; z < collections.length; z++) {
								AttributeCollection atCollection = collections[z];
								String colName = atCollection.getInternalName();
								DatasetConfigTreeNode colNode = new DatasetConfigTreeNode("AttributeCollection:" + colName);
								colNode.setUserObject(atCollection);
								List descriptions = atCollection.getAttributeDescriptions();
								for (int y = 0; y < descriptions.size(); y++) {
									AttributeDescription atDescription = (AttributeDescription) descriptions.get(y);
									String desName = atDescription.getInternalName();
									DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Attribute:" + desName);
									desNode.setUserObject(atDescription);
									
									for (Iterator r = atDescription.getSpecificAttributeContents().iterator(); r.hasNext(); ) {
										SpecificAttributeContent filtAtt = (SpecificAttributeContent)r.next();
										String dynName = filtAtt.getInternalName();
											DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificAttributeContent:" + dynName);
											dynNode.setUserObject(filtAtt);
										}
								}
								descriptions = atCollection.getAttributeLists();
								for (int y = 0; y < descriptions.size(); y++) {
									AttributeList atDescription = (AttributeList) descriptions.get(y);
									String desName = atDescription.getInternalName();
									DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("AttributeList:" + desName);
									desNode.setUserObject(atDescription);
								}
							}
						} 
					}

				}
			}
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.FilterPage")) {
			setName("FilterPage: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			FilterPage fp = (FilterPage) obj;
			List groups = fp.getFilterGroups();
			for (int j = 0; j < groups.size(); j++) {
				if (groups.get(j).getClass().getName().equals("org.ensembl.mart.lib.config.FilterGroup")) {
					FilterGroup fiGroup = (FilterGroup) groups.get(j);
					String grName = fiGroup.getInternalName();
					DatasetConfigTreeNode grNode = new DatasetConfigTreeNode("FilterGroup:" + grName);
					grNode.setUserObject(fiGroup);
					this.add(grNode);
					FilterCollection[] collections = fiGroup.getFilterCollections();
					for (int z = 0; z < collections.length; z++) {
						FilterCollection fiCollection = collections[z];
						String colName = fiCollection.getInternalName();
						DatasetConfigTreeNode colNode = new DatasetConfigTreeNode("FilterCollection:" + colName);
						colNode.setUserObject(fiCollection);
						List descriptions = fiCollection.getFilterDescriptions();
						for (int y = 0; y < descriptions.size(); y++) {
							FilterDescription fiDescription = (FilterDescription) descriptions.get(y);
							String desName = fiDescription.getInternalName();
							DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Filter:" + desName);
							desNode.setUserObject(fiDescription);

							for (Iterator r = fiDescription.getSpecificFilterContents().iterator(); r.hasNext(); ) {
					SpecificFilterContent filtAtt = (SpecificFilterContent)r.next();
								
								String dynName = filtAtt.getInternalName();
								DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificFilterContent:" + dynName);
								dynNode.setUserObject(filtAtt);
								
								Option[] options = filtAtt.getOptions();
							
								for (int k = 0; k < options.length; k++) {
									Option option = options[k];
									String optionName = option.getInternalName();
									DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
									optionNode.setUserObject(option);

									// specific option contents
									for (Iterator r2 = option.getSpecificOptionContents().iterator(); r2.hasNext(); ) {
										SpecificOptionContent optAtt = (SpecificOptionContent)r2.next();
										String dynName2 = optAtt.getInternalName();
										DatasetConfigTreeNode dynNode2 = new DatasetConfigTreeNode("SpecificOptionContent:" + dynName2);
										dynNode2.setUserObject(optAtt);
									}
								   // code for options within options ie for expression menus
																		Option[] subOptions = option.getOptions();
																		for (int m = 0; m < subOptions.length; m++) {
																			Option op = subOptions[m];
																			String paoptionName = op.getInternalName();
																			DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																			subOptionNode.setUserObject(op);
																		}
																		// new code to cycle through push actions
																		PushAction[] pushActions = option.getPushActions();
																		for (int l = 0; l < pushActions.length; l++) {
																			PushAction pa = pushActions[l];
																			DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
																			pushActionNode.setUserObject(pa);

																			Option[] paOptions = pa.getOptions();
																			for (int m = 0; m < paOptions.length; m++) {
																				Option op = paOptions[m];
																				String paoptionName = op.getInternalName();
																				DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																				paOptionNode.setUserObject(op);
																			}

																		}
																		//end of new code
								}
							}
																		
							Option[] options = fiDescription.getOptions();

							
							for (int k = 0; k < options.length; k++) {
								Option option = options[k];
								String optionName = option.getInternalName();
								DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
								optionNode.setUserObject(option);

								// code for options within options ie for expression menus
								Option[] subOptions = option.getOptions();
								for (int m = 0; m < subOptions.length; m++) {
									Option op = subOptions[m];
									String paoptionName = op.getInternalName();
									DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
									subOptionNode.setUserObject(op);
								}
								// new code to cycle through push actions
								PushAction[] pushActions = option.getPushActions();
								for (int l = 0; l < pushActions.length; l++) {
									PushAction pa = pushActions[l];
									DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
									pushActionNode.setUserObject(pa);
									Option[] paOptions = pa.getOptions();
									for (int m = 0; m < paOptions.length; m++) {
										Option op = paOptions[m];
										String paoptionName = op.getInternalName();
										DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
										paOptionNode.setUserObject(op);
									}
								}
								//end of new code
							}
						}
					}
				}
			}

		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.AttributePage")) {
			setName("AttributePage: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			AttributePage atPage = (AttributePage) obj;
			List groups = atPage.getAttributeGroups();
			for (int j = 0; j < groups.size(); j++) {
				if (groups.get(j).getClass().getName().equals("org.ensembl.mart.lib.config.AttributeGroup")) {
					AttributeGroup atGroup = (AttributeGroup) groups.get(j);
					String grName = atGroup.getInternalName();
					DatasetConfigTreeNode grNode = new DatasetConfigTreeNode("AttributeGroup:" + grName);
					grNode.setUserObject(atGroup);
					this.add(grNode);
					AttributeCollection[] collections = atGroup.getAttributeCollections();
					for (int z = 0; z < collections.length; z++) {
						AttributeCollection atCollection = collections[z];
						String colName = atCollection.getInternalName();
						DatasetConfigTreeNode colNode = new DatasetConfigTreeNode("AttributeCollection:" + colName);
						colNode.setUserObject(atCollection);
						List descriptions = atCollection.getAttributeDescriptions();
						for (int y = 0; y < descriptions.size(); y++) {
							AttributeDescription atDescription = (AttributeDescription) descriptions.get(y);
							String desName = atDescription.getInternalName();
							DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Attribute:" + desName);
							desNode.setUserObject(atDescription);
							
							for (Iterator r = atDescription.getSpecificAttributeContents().iterator(); r.hasNext(); ) {
								SpecificAttributeContent filtAtt = (SpecificAttributeContent)r.next();
								String dynName = filtAtt.getInternalName();
									DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificAttributeContent:" + dynName);
									dynNode.setUserObject(filtAtt);
								}
						}
						 descriptions = atCollection.getAttributeLists();
						for (int y = 0; y < descriptions.size(); y++) {
							AttributeList atDescription = (AttributeList) descriptions.get(y);
							String desName = atDescription.getInternalName();
							DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("AttributeList:" + desName);
							desNode.setUserObject(atDescription);
						}
					}
				} 
			}

		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.FilterGroup")) {
			setName("FilterGroup: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			FilterGroup fiGroup = (FilterGroup) obj;
			FilterCollection[] collections = fiGroup.getFilterCollections();
			for (int z = 0; z < collections.length; z++) {
				FilterCollection fiCollection = collections[z];
				String colName = fiCollection.getInternalName();
				DatasetConfigTreeNode colNode = new DatasetConfigTreeNode("FilterCollection:" + colName);
				colNode.setUserObject(fiCollection);
				this.add(colNode);
				List descriptions = fiCollection.getFilterDescriptions();
				for (int y = 0; y < descriptions.size(); y++) {
					FilterDescription fiDescription = (FilterDescription) descriptions.get(y);
					String desName = fiDescription.getInternalName();
					DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Filter:" + desName);
					desNode.setUserObject(fiDescription);

					for (Iterator r = fiDescription.getSpecificFilterContents().iterator(); r.hasNext(); ) {
						SpecificFilterContent filtAtt = (SpecificFilterContent)r.next();
						String dynName = filtAtt.getInternalName();
						DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificFilterContent:" + dynName);
						dynNode.setUserObject(filtAtt);
						
						Option[] options = filtAtt.getOptions();
					
						for (int k = 0; k < options.length; k++) {
							Option option = options[k];
							String optionName = option.getInternalName();
							DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
							optionNode.setUserObject(option);

							// specific option contents
							for (Iterator r2 = option.getSpecificOptionContents().iterator(); r2.hasNext(); ) {
								SpecificOptionContent optAtt = (SpecificOptionContent)r2.next();
								String dynName2 = optAtt.getInternalName();
								DatasetConfigTreeNode dynNode2 = new DatasetConfigTreeNode("SpecificOptionContent:" + dynName2);
								dynNode2.setUserObject(optAtt);
							}
						   // code for options within options ie for expression menus
																Option[] subOptions = option.getOptions();
																for (int m = 0; m < subOptions.length; m++) {
																	Option op = subOptions[m];
																	String paoptionName = op.getInternalName();
																	DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																	subOptionNode.setUserObject(op);
																}
																// new code to cycle through push actions
																PushAction[] pushActions = option.getPushActions();
																for (int l = 0; l < pushActions.length; l++) {
																	PushAction pa = pushActions[l];
																	DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
																	pushActionNode.setUserObject(pa);

																	Option[] paOptions = pa.getOptions();
																	for (int m = 0; m < paOptions.length; m++) {
																		Option op = paOptions[m];
																		String paoptionName = op.getInternalName();
																		DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																		paOptionNode.setUserObject(op);
																	}

																}
																//end of new code
						}
					}
																
					Option[] options = fiDescription.getOptions();
					
					for (int k = 0; k < options.length; k++) {
						Option option = options[k];
						String optionName = option.getInternalName();
						DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
						optionNode.setUserObject(option);
						// code for options within options ie for expression menus
						Option[] subOptions = option.getOptions();
						for (int m = 0; m < subOptions.length; m++) {
							Option op = subOptions[m];
							String paoptionName = op.getInternalName();
							DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
							subOptionNode.setUserObject(op);
						}
						// new code to cycle through push actions
						PushAction[] pushActions = option.getPushActions();
						for (int l = 0; l < pushActions.length; l++) {
							PushAction pa = pushActions[l];
							DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
							pushActionNode.setUserObject(pa);
							Option[] paOptions = pa.getOptions();
							for (int m = 0; m < paOptions.length; m++) {
								Option op = paOptions[m];
								String paoptionName = op.getInternalName();
								DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
								paOptionNode.setUserObject(op);
							}
						}
						//end of new code
					}
				}
			}
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.AttributeGroup")) {
			setName("AttributeGroup: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			AttributeGroup atGroup = (AttributeGroup) obj;
			AttributeCollection[] collections = atGroup.getAttributeCollections();
			for (int z = 0; z < collections.length; z++) {
				AttributeCollection atCollection = collections[z];
				String colName = atCollection.getInternalName();
				DatasetConfigTreeNode colNode = new DatasetConfigTreeNode("AttributeCollection:" + colName);
				this.add(colNode);
				colNode.setUserObject(atCollection);
				List descriptions = atCollection.getAttributeDescriptions();
				for (int y = 0; y < descriptions.size(); y++) {
					AttributeDescription atDescription = (AttributeDescription) descriptions.get(y);
					String desName = atDescription.getInternalName();
					DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Attribute:" + desName);
					desNode.setUserObject(atDescription);
					//colNode.add(desNode);
					
					for (Iterator r = atDescription.getSpecificAttributeContents().iterator(); r.hasNext(); ) {
						SpecificAttributeContent filtAtt = (SpecificAttributeContent)r.next();
						String dynName = filtAtt.getInternalName();
							DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificAttributeContent:" + dynName);
							dynNode.setUserObject(filtAtt);
						}
				}
				descriptions = atCollection.getAttributeLists();
				for (int y = 0; y < descriptions.size(); y++) {
					AttributeList atDescription = (AttributeList) descriptions.get(y);
					String desName = atDescription.getInternalName();
					DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("AttributeList:" + desName);
					desNode.setUserObject(atDescription);
					//colNode.add(desNode);
				}
			}
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.DSAttributeGroup")) {
			setName("DSAttributeGroup: " + ((BaseNamedConfigurationObject) obj).getInternalName());
//			DSAttributeGroup atGroup = (DSAttributeGroup) obj;

		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.FilterCollection")) {
			setName("FilterCollection: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			FilterCollection fiCollection = (FilterCollection) obj;
			List descriptions = fiCollection.getFilterDescriptions();
			for (int y = 0; y < descriptions.size(); y++) {
				FilterDescription fiDescription = (FilterDescription) descriptions.get(y);
				String desName = fiDescription.getInternalName();
				DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Filter:" + desName);
				desNode.setUserObject(fiDescription);
				this.add(desNode);

				for (Iterator r = fiDescription.getSpecificFilterContents().iterator(); r.hasNext(); ) {
					SpecificFilterContent filtAtt = (SpecificFilterContent)r.next();
					String dynName = filtAtt.getInternalName();
					DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificFilterContent:" + dynName);
					dynNode.setUserObject(filtAtt);
					
					Option[] options = filtAtt.getOptions();
				
					for (int k = 0; k < options.length; k++) {
						Option option = options[k];
						String optionName = option.getInternalName();
						DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
						optionNode.setUserObject(option);

						// specific option contents
						for (Iterator r2 = option.getSpecificOptionContents().iterator(); r2.hasNext(); ) {
							SpecificOptionContent optAtt = (SpecificOptionContent)r2.next();
							String dynName2 = optAtt.getInternalName();
							DatasetConfigTreeNode dynNode2 = new DatasetConfigTreeNode("SpecificOptionContent:" + dynName2);
							dynNode2.setUserObject(optAtt);
						}
					   // code for options within options ie for expression menus
															Option[] subOptions = option.getOptions();
															for (int m = 0; m < subOptions.length; m++) {
																Option op = subOptions[m];
																String paoptionName = op.getInternalName();
																DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																subOptionNode.setUserObject(op);
															}
															// new code to cycle through push actions
															PushAction[] pushActions = option.getPushActions();
															for (int l = 0; l < pushActions.length; l++) {
																PushAction pa = pushActions[l];
																DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
																pushActionNode.setUserObject(pa);

																Option[] paOptions = pa.getOptions();
																for (int m = 0; m < paOptions.length; m++) {
																	Option op = paOptions[m];
																	String paoptionName = op.getInternalName();
																	DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																	paOptionNode.setUserObject(op);
																}

															}
															//end of new code
					}
				}
				Option[] options = fiDescription.getOptions();

				
				for (int k = 0; k < options.length; k++) {
					Option option = options[k];
					String optionName = option.getInternalName();
					DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
					optionNode.setUserObject(option);

					// code for options within options ie for expression menus
					Option[] subOptions = option.getOptions();
					for (int m = 0; m < subOptions.length; m++) {
						Option op = subOptions[m];
						String paoptionName = op.getInternalName();
						DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
						subOptionNode.setUserObject(op);
					}

					// new code to cycle through push actions
					PushAction[] pushActions = option.getPushActions();
					for (int l = 0; l < pushActions.length; l++) {
						PushAction pa = pushActions[l];
						DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
						pushActionNode.setUserObject(pa);
						Option[] paOptions = pa.getOptions();
						for (int m = 0; m < paOptions.length; m++) {
							Option op = paOptions[m];
							String paoptionName = op.getInternalName();
							DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
							paOptionNode.setUserObject(op);
						}
					}
					//end of new code
				}
			}
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.AttributeCollection")) {
			setName("AttributeCollection: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			AttributeCollection atCollection = (AttributeCollection) obj;
			List descriptions = atCollection.getAttributeDescriptions();
			for (int y = 0; y < descriptions.size(); y++) {
				AttributeDescription atDescription = (AttributeDescription) descriptions.get(y);
				String desName = atDescription.getInternalName();
				DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Attribute:" + desName);
				desNode.setUserObject(atDescription);
				this.add(desNode);
				
				for (Iterator r = atDescription.getSpecificAttributeContents().iterator(); r.hasNext(); ) {
					SpecificAttributeContent filtAtt = (SpecificAttributeContent)r.next();
					String dynName = filtAtt.getInternalName();
						DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificAttributeContent:" + dynName);
						dynNode.setUserObject(filtAtt);
					}
			}
			descriptions = atCollection.getAttributeLists();
			for (int y = 0; y < descriptions.size(); y++) {
				AttributeList atDescription = (AttributeList) descriptions.get(y);
				String desName = atDescription.getInternalName();
				DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("AttributeList:" + desName);
				desNode.setUserObject(atDescription);
				this.add(desNode);
			}
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.FilterDescription")) {
			//System.out.println("FILT\t" + ((BaseNamedConfigurationObject) obj).getInternalName());
			setName("Filter: " + ((BaseNamedConfigurationObject) obj).getInternalName());

			//setName("FilterCollection: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			FilterDescription fiDescription = (FilterDescription) obj;
			for (Iterator r = fiDescription.getSpecificFilterContents().iterator(); r.hasNext(); ) {
				SpecificFilterContent filtAtt = (SpecificFilterContent)r.next();
				String dynName = filtAtt.getInternalName();
				DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificFilterContent:" + dynName);
				dynNode.setUserObject(filtAtt);
				this.add(dynNode);
				
				Option[] options = filtAtt.getOptions();
			
				for (int k = 0; k < options.length; k++) {
					Option option = options[k];
					String optionName = option.getInternalName();
					DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
					optionNode.setUserObject(option);


														// specific option contents
														for (Iterator r2 = option.getSpecificOptionContents().iterator(); r2.hasNext(); ) {
															SpecificOptionContent optAtt = (SpecificOptionContent)r2.next();
															String dynName2 = optAtt.getInternalName();
															DatasetConfigTreeNode dynNode2 = new DatasetConfigTreeNode("SpecificOptionContent:" + dynName2);
															dynNode2.setUserObject(optAtt);
														}
														// code for options within options ie for expression menus
														Option[] subOptions = option.getOptions();
														for (int m = 0; m < subOptions.length; m++) {
															Option op = subOptions[m];
															String paoptionName = op.getInternalName();
															DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
															subOptionNode.setUserObject(op);
														}
														// new code to cycle through push actions
														PushAction[] pushActions = option.getPushActions();
														for (int l = 0; l < pushActions.length; l++) {
															PushAction pa = pushActions[l];
															DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
															pushActionNode.setUserObject(pa);

															Option[] paOptions = pa.getOptions();
															for (int m = 0; m < paOptions.length; m++) {
																Option op = paOptions[m];
																String paoptionName = op.getInternalName();
																DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
																paOptionNode.setUserObject(op);
															}

														}
														//end of new code
				}
			}
														
			Option[] ops = fiDescription.getOptions();
			
			for (int y = 0; y < ops.length; y++) {
				Option option = (Option) ops[y];
				String desName = option.getInternalName();
				DatasetConfigTreeNode desNode = new DatasetConfigTreeNode("Option:" + desName);
				desNode.setUserObject(option);
				this.add(desNode);

				// code for options within options ie for expression menus
				Option[] subOptions = option.getOptions();
				for (int m = 0; m < subOptions.length; m++) {
					Option op = subOptions[m];
					String paoptionName = op.getInternalName();
					DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
					subOptionNode.setUserObject(op);
				}
				//				new code to cycle through push actions
				PushAction[] pushActions = option.getPushActions();
				for (int l = 0; l < pushActions.length; l++) {
					PushAction pa = pushActions[l];
					DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
					pushActionNode.setUserObject(pa);
					Option[] paOptions = pa.getOptions();
					for (int m = 0; m < paOptions.length; m++) {
						Option op = paOptions[m];
						String paoptionName = op.getInternalName();
						DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
						paOptionNode.setUserObject(op);
					}
				}
				//end of new code
			}
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.AttributeDescription")) {
			
			//checkUniqueness((BaseNamedConfigurationObject)obj);
			setName("Attribute: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			AttributeDescription ad = (AttributeDescription) obj;
			//System.out.println("ATT\t" + ((BaseNamedConfigurationObject) obj).getInternalName()+":"+ad.getDynamicAttributeContents().size());

			
			for (Iterator r = ad.getSpecificAttributeContents().iterator(); r.hasNext(); ) {
				SpecificAttributeContent filtAtt = (SpecificAttributeContent)r.next();
				String dynName = filtAtt.getInternalName();
					DatasetConfigTreeNode dynNode = new DatasetConfigTreeNode("SpecificAttributeContent:" + dynName);
					dynNode.setUserObject(filtAtt);
					this.add(dynNode);
				}
			
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.AttributeList")) {
			
			//checkUniqueness((BaseNamedConfigurationObject)obj);
			setName("AttributeList: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			
			
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.SpecificAttributeContent")) {
			setName("SpecificAttributeContent: " + ((BaseNamedConfigurationObject) obj).getInternalName() );
			SpecificAttributeContent dynAtt = (SpecificAttributeContent) obj;
			
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.SpecificOptionContent")) {
			setName("SpecificOptionContent: " + ((BaseNamedConfigurationObject) obj).getInternalName() );
			SpecificOptionContent dynAtt = (SpecificOptionContent) obj;
			
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.SpecificFilterContent")) {
			setName("SpecificFilterContent: " + ((BaseNamedConfigurationObject) obj).getInternalName() );
			SpecificFilterContent dynAtt = (SpecificFilterContent) obj;
			Option[] options = dynAtt.getOptions();
									
												for (int k = 0; k < options.length; k++) {
													Option option = options[k];
													String optionName = option.getInternalName();
													DatasetConfigTreeNode optionNode = new DatasetConfigTreeNode("Option: " + optionName);
													optionNode.setUserObject(option);
													this.add(optionNode);// TRY THIS

													// specific option contents
													for (Iterator r2 = option.getSpecificOptionContents().iterator(); r2.hasNext(); ) {
														SpecificOptionContent optAtt = (SpecificOptionContent)r2.next();
														String dynName2 = optAtt.getInternalName();
														DatasetConfigTreeNode dynNode2 = new DatasetConfigTreeNode("SpecificOptionContent:" + dynName2);
														dynNode2.setUserObject(optAtt);
													}
													// code for options within options ie for expression menus
													Option[] subOptions = option.getOptions();
													for (int m = 0; m < subOptions.length; m++) {
														Option op = subOptions[m];
														String paoptionName = op.getInternalName();
														DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
														subOptionNode.setUserObject(op);
													}
													// new code to cycle through push actions
													PushAction[] pushActions = option.getPushActions();
													for (int l = 0; l < pushActions.length; l++) {
														PushAction pa = pushActions[l];
														DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
														pushActionNode.setUserObject(pa);

														Option[] paOptions = pa.getOptions();
														for (int m = 0; m < paOptions.length; m++) {
															Option op = paOptions[m];
															String paoptionName = op.getInternalName();
															DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
															paOptionNode.setUserObject(op);
														}

													}
													//end of new code
												}
			
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.Exportable")) {
			//System.out.println("EXP\t" + ((BaseNamedConfigurationObject) obj).getInternalName());
			setName("Exportable: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			Exportable ad = (Exportable) obj;
			//System.out.println("ATT\t" + ((BaseNamedConfigurationObject) obj).getInternalName()+":"+ad.getDynamicAttributeContents().size());
			
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.Importable")) {
			//System.out.println("IMP\t" + ((BaseNamedConfigurationObject) obj).getInternalName());
			setName("Importable: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			Importable ad = (Importable) obj;
			//System.out.println("ATT\t" + ((BaseNamedConfigurationObject) obj).getInternalName()+":"+ad.getDynamicAttributeContents().size());
			
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.Enable")) {
			//setName("Enable");
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.Disable")) {
			setName("Disable");
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.DynamicDataset")) {
			setName("Partition: " + ((DynamicDataset) obj).getInternalName());
		}

		//else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.PushAction")) {
		//    setName("Push Action");
		//}

		else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.Option")) {
			//System.out.println("OP\t" + ((BaseNamedConfigurationObject) obj).getInternalName());
			setName("Option: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			Option op = (Option) obj;

			// specific option contents
			for (Iterator r2 = op.getSpecificOptionContents().iterator(); r2.hasNext(); ) {
				SpecificOptionContent optAtt = (SpecificOptionContent)r2.next();
				String dynName2 = optAtt.getInternalName();
				DatasetConfigTreeNode dynNode2 = new DatasetConfigTreeNode("SpecificOptionContent:" + dynName2);
				dynNode2.setUserObject(optAtt);
				this.add(dynNode2);
			}
			// code for options within options ie for expression menus
			Option[] subOptions = op.getOptions();
			for (int m = 0; m < subOptions.length; m++) {
				Option op2 = subOptions[m];
				String paoptionName = op2.getInternalName();
				DatasetConfigTreeNode subOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
				subOptionNode.setUserObject(op2);
				this.add(subOptionNode);
			}

			PushAction[] pushActions = op.getPushActions();
			for (int k = 0; k < pushActions.length; k++) {
				PushAction pa = pushActions[k];

				DatasetConfigTreeNode pushActionNode = new DatasetConfigTreeNode("PushAction");
				pushActionNode.setUserObject(pa);

				this.add(pushActionNode);
				Option[] paOptions = pa.getOptions();
				for (int m = 0; m < paOptions.length; m++) {
					Option paop = paOptions[m];
					String paoptionName = paop.getInternalName();
					DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option: " + paoptionName);
					paOptionNode.setUserObject(paop);
				}
			}
		} else if (nodeObjectClass.equals("org.ensembl.mart.lib.config.PushAction")) {
			setName("Push Action: " + ((BaseNamedConfigurationObject) obj).getInternalName());
			PushAction pa = (PushAction) obj;
			Option[] paOptions = pa.getOptions();
			for (int k = 0; k < paOptions.length; k++) {
				Option op = paOptions[k];
				String paoptionName = op.getInternalName();
				DatasetConfigTreeNode paOptionNode = new DatasetConfigTreeNode("Option" + paoptionName);
				paOptionNode.setUserObject(op);
				this.add(paOptionNode);
			}

		}
	}
}


