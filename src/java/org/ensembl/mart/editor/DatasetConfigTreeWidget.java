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

package org.ensembl.mart.editor;

import java.awt.Color;
import java.awt.Component;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.io.File;
import java.net.URL;
import java.util.List;

import javax.swing.ImageIcon;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTree;
import javax.swing.event.InternalFrameEvent;
import javax.swing.event.InternalFrameListener;
import javax.swing.tree.DefaultTreeCellRenderer;

import org.ensembl.mart.lib.config.AttributeCollection;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributeList;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.BaseConfigurationObject;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DSConfigAdaptor;
import org.ensembl.mart.lib.config.DatabaseDSConfigAdaptor;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.DatasetConfigIterator;
import org.ensembl.mart.lib.config.DynamicDataset;
import org.ensembl.mart.lib.config.Exportable;
import org.ensembl.mart.lib.config.FilterCollection;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterGroup;
import org.ensembl.mart.lib.config.FilterPage;
import org.ensembl.mart.lib.config.Importable;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.lib.config.SimpleDSConfigAdaptor;
import org.ensembl.mart.lib.config.URLDSConfigAdaptor;
import org.jdom.Document;



/**
 * DatasetConfigTreeWidget extends internal frame.
 *
 *
 * @author <a href="mailto:katerina@ebi.ac.uk">Katerina Tzouvara</a>
 * //@see org.ensembl.mart.config.DatasetConfig
 */
public class DatasetConfigTreeWidget extends JInternalFrame{

    private DatasetConfig datasetConfig = null;
    private DatasetConfigTree naiveTree = null;
    private static int openFrameCount = 0;
    private static final int xOffset = 10, yOffset = 10;
    private JDesktopPane desktop;
    private GridBagConstraints constraints;
    private DatasetConfigTree tree;
    private File file = null;
    private MartEditor editor;

	
    public DatasetConfigTreeWidget(File file, MartEditor editor, DatasetConfig dsv, String user, 
    	String dataset, String datasetID, String schema, String template, String settingsFlag){

        super("Dataset Tree " + (++openFrameCount),
                true, //resizable
                true, //closable
                true, //maximizable
                true);//iconifiable
        this.editor = editor;
        this.setDefaultCloseOperation(JInternalFrame.DO_NOTHING_ON_CLOSE);
        this.addInternalFrameListener(new CloseListener());
        try {
		  	DatasetConfig config = null;	
          	if (dsv == null){	
            	if (file == null) {
            		if (user == null){
            	  		if (schema == null){	
                    		config = new DatasetConfig("new", "new", "new");
                    		config.setDSConfigAdaptor(new SimpleDSConfigAdaptor(config)); //prevents lazyLoading
                    		config.addFilterPage(new FilterPage("new"));
                    		config.addAttributePage(new AttributePage("new"));
            	  		}
            	  
            	  		else{// naive
            	  			config = MartEditor.getDatabaseDatasetConfigUtils().getNaiveDatasetConfigFor(schema,dataset);
                    		config.setDSConfigAdaptor(new SimpleDSConfigAdaptor(config)); //prevents lazyLoading
                    		if (config.getFilterPages().length+config.getAttributePages().length==0) {
            	  				JOptionPane.showMessageDialog(null,"No usable tables were found.");
            	  				return;
                    		}
            	  			if (config.getPrimaryKeys().length == 0 || !config.getPrimaryKeys()[0].toLowerCase().endsWith("_key")){
            	  				JOptionPane.showMessageDialog(null,"Your main table must contain a primary key ending _key");
            	  				return;
            	  			}
            	  			config.setTemplate(template);
            	  	
							DatasetConfigAttributesTable attrTable = new DatasetConfigAttributesTable(
										config, this);
							tree = new DatasetConfigTree(config,
										this, attrTable);
				            setNaiveTree(tree);
							
							// THEN JUST OPEN UP TEMPLATE DOC
							DatasetConfig templateConfig = new DatasetConfig("template","",template+"_template","","","","","","","","","","","",template,"","","","");
							Document templateDocument = MartEditor.getDatabaseDatasetConfigUtils().getTemplateDocument(template);
							if (templateDocument==null) {
								templateConfig = new DatasetConfig(config,true,false);
								// Generate template document based on existing config.
								templateConfig.setInternalName("template");
								templateConfig.setDataset(template+"_template");
								templateConfig.setTemplate(template);
							} else {
								MartEditor.getDatasetConfigXMLUtils().loadDatasetConfigWithDocument(templateConfig, templateDocument);

								// first of all call getNewFiltsAtts so any extra atts in a new config get added to the template
								templateConfig = MartEditor.getDatabaseDatasetConfigUtils().getNewFiltsAtts(schema,config,false);	
							}
							if (templateConfig.getDynamicDataset(dataset)==null)
								templateConfig.addDynamicDataset(new DynamicDataset(dataset,null));
							templateConfig.setTemplateFlag("1");
							
							config = templateConfig;		
            	  	
							//int templateCount = MartEditor.getDatabaseDatasetConfigUtils().templateCount(template);
							//if (templateCount > 0)			            	  	
							//	config = MartEditor.getDatabaseDatasetConfigUtils().updateConfigToTemplate(config,0);
            	  		}
            		}
            		else{//Importing config
            			if (template != null){
            				// import template
            				//config = MartEditor.getDatabaseDatasetConfigUtils().getTemplateConfig(template);
							DatasetConfig templateConfig = new DatasetConfig("template","",template+"_template","","","","","","","","","","","",template,"","","","");
							Document templateDocument = MartEditor.getDatabaseDatasetConfigUtils().getTemplateDocument(template);
							MartEditor.getDatasetConfigXMLUtils().loadDatasetConfigWithDocument(templateConfig, templateDocument);
							templateConfig.setTemplateFlag("1");
							config = templateConfig;
            			}
            			else if (settingsFlag == null){
            				// have an indiviudal config just for read-only viewing
							DSConfigAdaptor adaptor = new DatabaseDSConfigAdaptor(MartEditor.getDetailedDataSource(),user, "", true, false, true, true);
							DatasetConfigIterator configs = adaptor.getDatasetConfigs();
							while (configs.hasNext()){
								DatasetConfig lconfig = (DatasetConfig) configs.next();
								if (lconfig.getDataset().equals(dataset) && lconfig.getDatasetID().equals(datasetID)){
									config = lconfig;
									break;
								}
							}	
            			}
            			else{
      					    // have an individual config without a template - generate template
					  		DSConfigAdaptor adaptor = new DatabaseDSConfigAdaptor(MartEditor.getDetailedDataSource(),user, "", true, false, true, false);
					  		DatasetConfigIterator configs = adaptor.getDatasetConfigs();
					  		while (configs.hasNext()){
            					DatasetConfig lconfig = (DatasetConfig) configs.next();
					  			if (lconfig.getDataset().equals(dataset) && lconfig.getDatasetID().equals(datasetID)){
					    			config = lconfig;
					    			break;
								}
					  		}
					  		
					  		//System.out.println("GOT IMPORTED CONFIG WITH TEMPLATE "+config.getTemplate());
					  		
							// convert config to latest version using xslt - ? whether to do
							config = MartEditor.getDatabaseDatasetConfigUtils().getXSLTransformedConfig(config);
							
							// now just create a template with same name as dataset and open up
							// if want to change to a more generic n1 template use the Set template option
							
							if (settingsFlag.equals("1")){
								template = dataset;
							}
							else if (settingsFlag.equals("0")){// the set template option
							
								// CHOOSE A TEMPLATE AS FOR NAIVE GENERATION
								int choice = JOptionPane.showConfirmDialog(null,"Create new template rather than use existing one?");
								if (choice == 0){// YES
								template = (String) JOptionPane.showInputDialog(null,"New template name",dataset);		
								}
								else if (choice == 1){// NO
									String[] templates = MartEditor.getDatabaseDatasetConfigUtils().getAllTemplateNames();							
									if(templates.length!=0){
     									template =
										  (String) JOptionPane.showInputDialog(
												null,
										  		"Choose one",
												"Template",
												JOptionPane.INFORMATION_MESSAGE,
												null,
												templates,null);
										if (template == null)
											  return;
									}
									else{
										JOptionPane.showMessageDialog(null,"No existing templates available. Create a new one");
										return;								
									}
								}
								else{// CANCEL
									return;
								}
							
							}
							
							config.setTemplate(template);
							DatasetConfigAttributesTable attrTable = new DatasetConfigAttributesTable(
										config, this);
							tree = new DatasetConfigTree(config,
										this, attrTable);
				            setNaiveTree(tree);
							
//							THEN JUST OPEN UP TEMPLATE DOC
							DatasetConfig templateConfig = new DatasetConfig("template","",template+"_template","","","","","","","","","","","",template,"","","","");
							Document templateDocument = MartEditor.getDatabaseDatasetConfigUtils().getTemplateDocument(template);
							if (templateDocument==null) {
								templateConfig = new DatasetConfig(config,true,false);
								// Generate template document based on existing config.
								templateConfig.setInternalName("template");
								templateConfig.setDataset(template+"_template");
								templateConfig.setTemplate(template);
								
								// Convert tableConstraint names.
								MartEditor.getDatabaseDatasetConfigUtils().stripTableConstraints(templateConfig);
							}
							else {								
								MartEditor.getDatasetConfigXMLUtils().loadDatasetConfigWithDocument(templateConfig, templateDocument);
							}							
							if (templateConfig.getDynamicDataset(dataset)==null)
								templateConfig.addDynamicDataset(new DynamicDataset(dataset,null));
							
							templateConfig.setTemplateFlag("1");
							
							config = templateConfig;	
														
            			}
            		}
            	} 
            	else {// open from file
                	URL url = file.toURL();
//            ignore cache, include hidden members
                	DSConfigAdaptor adaptor = new URLDSConfigAdaptor(url,true, true);

                // only config one in the file so get that one
                	config = (DatasetConfig) adaptor.getDatasetConfigs().next();
                	config.setDatasetID("");//always blank from file so gets sorted out by database during export
                	if (config.getTemplate() == null || config.getTemplate().equals(""))
                		config.setTemplate(config.getDataset());
            	}
          	}// end of dsv = null
          	else{
          		config = new DatasetConfig(dsv, true, false);
          	}
        
        	// convert config to latest version using xslt - COMMENTED OUT AS WAS TRANSFORMING TEMPLATE AND 
        	// CAUSING MEMORY ERRORS - ? IF NEEED THOUGH
        	//config = MartEditor.getDatabaseDatasetConfigUtils().getXSLTransformedConfig(config);
        	
			this.setTitle(schema + "." + config.getDataset());
            JFrame.setDefaultLookAndFeelDecorated(true);

            DatasetConfigAttributesTable attrTable = new DatasetConfigAttributesTable(
                    config, this);
            tree = new DatasetConfigTree(config,
                    this, attrTable);
  
			tree.setCellRenderer(new MyRenderer());        
            // for update         
            setDatasetConfig(config);
            
            JScrollPane treeScrollPane = new JScrollPane(tree);
            JScrollPane tableScrollPane = new JScrollPane(attrTable);
            JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
                    treeScrollPane, tableScrollPane);
            splitPane.setOneTouchExpandable(true);
            splitPane.setDividerLocation(350);

            //Provide minimum sizes for the two components in the split pane.
            Dimension minimumSize = new Dimension(350, 450);
            treeScrollPane.setMinimumSize(minimumSize);
            tableScrollPane.setMinimumSize(minimumSize);

            this.getContentPane().add(splitPane);

            //...Then set the window size or call pack...
            setSize(800, 400);
            
            
            // SHOULD BE ABLE TO GET RID OF BELOW
			//int templateCount = MartEditor.getDatabaseDatasetConfigUtils().templateCount(config.getTemplate());
			if (template == null){// && templateCount > 1){// flag non-template XMLs with a template origin
			
				//JOptionPane.showMessageDialog(null,"This config is under template control. You need to edit the template in the majority of cases");
			
				Importable[] imps = config.getImportables();
				for (int i = 0; i < imps.length; i++){
					imps[i].setTemplateDrivenFlag(1);
				}
				Exportable[] exps = config.getExportables();
				for (int i = 0; i < exps.length; i++){
					exps[i].setTemplateDrivenFlag(1);
				}
				
				FilterPage[] fpages = config.getFilterPages();	
				for (int i = 0; i < fpages.length; i++){
					FilterPage fpage = fpages[i];
					fpage.setTemplateDrivenFlag(1);
					List fgroups = fpage.getFilterGroups();	
					for (int j = 0; j < fgroups.size(); j++){
						FilterGroup fgroup = (FilterGroup) fgroups.get(j);
						fgroup.setTemplateDrivenFlag(1);
						FilterCollection[] fcolls = fgroup.getFilterCollections();	
						for (int k = 0; k < fcolls.length; k++){
							FilterCollection fcoll = fcolls[k];
							fcoll.setTemplateDrivenFlag(1);
							List fds = fcoll.getFilterDescriptions();
							for (int l = 0; l < fds.size(); l++){
								FilterDescription fd = (FilterDescription) fds.get(l);
								if (!fd.getInternalName().matches("\\w+\\.\\w+") 
									&& !fd.getInternalName().matches("\\w+\\.\\w+\\.\\w+"))
									fd.setTemplateDrivenFlag(1);
								Option[] ops = fd.getOptions();
								for (int m = 0; m < ops.length; m++){
									Option op = ops[m];
									if (op.getTableConstraint() != null)
										op.setTemplateDrivenFlag(1);
								}
							}
						}
					}		
				}
				AttributePage[] apages = config.getAttributePages();	
				for (int i = 0; i < apages.length; i++){
					AttributePage apage = apages[i];
					apage.setTemplateDrivenFlag(1);
					List agroups = apage.getAttributeGroups();	
					for (int j = 0; j < agroups.size(); j++){
						AttributeGroup agroup = (AttributeGroup) agroups.get(j);
						agroup.setTemplateDrivenFlag(1);
						AttributeCollection[] acolls = agroup.getAttributeCollections();	
						for (int k = 0; k < acolls.length; k++){
							AttributeCollection acoll = acolls[k];
							acoll.setTemplateDrivenFlag(1);
							List ads = acoll.getAttributeDescriptions();
							for (int l = 0; l < ads.size(); l++){
								AttributeDescription ad = (AttributeDescription) ads.get(l);
								if (!ad.getInternalName().matches("\\w+\\.\\w+") 
									&& !ad.getInternalName().matches("\\w+\\.\\w+\\.\\w+"))
									ad.setTemplateDrivenFlag(1);	
							}
							ads = acoll.getAttributeLists();
							for (int l = 0; l < ads.size(); l++){
								AttributeList ad = (AttributeList) ads.get(l);
								if (!ad.getInternalName().matches("\\w+\\.\\w+") 
									&& !ad.getInternalName().matches("\\w+\\.\\w+\\.\\w+"))
									ad.setTemplateDrivenFlag(1);	
							}
						}
					}		
				}
					
			}				
            //Set the window's location.
            setLocation(xOffset * openFrameCount, yOffset * openFrameCount);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Test purposes only. Creates a frame with a JTree containing
     * a presepecified DatasetConfig.dtd compatible configuration file.
     * @param args
     * @throws ConfigurationException
     */
    public static void main(String[] args) throws ConfigurationException {


    }

    /**
     * @return
     */
    public DatasetConfig getDatasetConfig() {
        return datasetConfig;
    }
    
	public MartEditor getEditor() {
		return editor;
	}

    public void addAttributesTable(JTable table) {
        add(this.getContentPane(), new JScrollPane(table), constraints, 1, 0, 1, 1);
    }

    public void add(Container cont, Component component, GridBagConstraints constraints, int x, int y, int w, int h) {
        constraints.gridx = x;
        constraints.gridy = y;
        constraints.gridwidth = w;
        constraints.gridheight = h;
        cont.add(component, constraints);
    }

    /**
     * @param tree
     */
    public void setNaiveTree(DatasetConfigTree tree) {
        naiveTree = tree;
    }

    /**
     * @param config
     */
    public void setDatasetConfig(DatasetConfig config) {
        clearDatasetConfig();
        datasetConfig = config;
        loadDatasetConfig();
    }

    /**
     * Loads the datasetConfig by creating a tree to represent it
     * and displaying it.
     */
    private void loadDatasetConfig() {


    }

    /**
     * Removes current dataset config if one is loaded, otherwise does nothing.
     */
    private void clearDatasetConfig() {

    }

    public void save(){
        tree.save();
    }

    public void save_as(){
           tree.save_as();
       }

	public void export() throws ConfigurationException{
		tree.export();
	}
	
	public void exportTemplate() throws ConfigurationException{
		if (naiveTree!=null) {
			if (!naiveTree.export()) return;
			naiveTree = null;
		}
		tree.exportTemplate();
	}
	
	public void validateTemplate() throws ConfigurationException{
		tree.validateTemplate();
	}

    public void cut(){
        tree.cut();
    }

    public void copy(){
        tree.copy();
    }

    public void paste(){
        tree.paste();
    }

	public void makeHidden(){
		tree.makeHidden();
	}


    public void insert(){
       // tree.insert();
    }

    public void delete(){
        tree.delete();
    }

    public void setFileChooserPath(File file){
        this.file = file;
        editor.setFileChooserPath(file);
    }

    public File getFileChooserPath(){
        return editor.getFileChooserPath();
    }

    /** Returns an ImageIcon, or null if the path was invalid. */
    protected static ImageIcon createImageIcon(String path) {
        java.net.URL imgURL = DatasetConfigTreeWidget.class.getClassLoader().getResource(path);
        if (imgURL != null) {
            return new ImageIcon(imgURL);
        } else {
            System.err.println("Couldn't find file: " + path);
            return null;
        }
    }

}

class CloseListener implements InternalFrameListener {
	public void internalFrameClosed(InternalFrameEvent e) {
	}
	public void internalFrameOpened(InternalFrameEvent e) {
	}
	public void internalFrameIconified(InternalFrameEvent e) {
	}
	public void internalFrameDeiconified(InternalFrameEvent e) {
	}
	public void internalFrameActivated(InternalFrameEvent e) {
	}
	public void internalFrameDeactivated(InternalFrameEvent e) {
	}
	public void internalFrameClosing(InternalFrameEvent e){
		try{
			DatasetConfigTreeWidget dw = (DatasetConfigTreeWidget) e.getInternalFrame();
			if (!MartEditor.getDatabaseDatasetConfigUtils().isDatasetConfigChanged(null,dw.getDatasetConfig())){// current and db out of synch
				int returnType = JOptionPane.showConfirmDialog(null,"Close?","Changes not exported",JOptionPane.OK_CANCEL_OPTION);
				if (returnType == 0){
					e.getInternalFrame().dispose();
				}
			}
			else{
				e.getInternalFrame().dispose();
			}
		}
		catch(Exception exc){
			// connection changed already
			e.getInternalFrame().dispose();
		}
	}
}

class MyRenderer extends DefaultTreeCellRenderer {

	public MyRenderer() {	
	}

	public Component getTreeCellRendererComponent(
						JTree tree,
						Object value,
						boolean sel,
						boolean expanded,
						boolean leaf,
						int row,
						boolean hasFocus) {

		if (isHidden(value)){
			setTextNonSelectionColor(Color.lightGray);
			setTextSelectionColor(Color.lightGray);
		} else{
		    setTextNonSelectionColor(Color.black);
			setTextSelectionColor(Color.black);
		}
		super.getTreeCellRendererComponent(
						tree, value, sel,
						expanded, leaf, row,
						hasFocus);			
		
		return this;
	}

	protected boolean isHidden(Object value) {
		DatasetConfigTreeNode node =
				(DatasetConfigTreeNode)value;
		BaseConfigurationObject nodeObject = (BaseConfigurationObject) node.getUserObject();		
		if (nodeObject.getAttribute("hidden") != null && nodeObject.getAttribute("hidden").equals("true")){
			return true;
		}

		return false;
	}
}
