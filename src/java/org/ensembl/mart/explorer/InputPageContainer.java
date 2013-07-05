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

package org.ensembl.mart.explorer;

import java.awt.CardLayout;
import java.util.logging.Logger;

import javax.swing.JPanel;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Contains all the input pages.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * 
 */
public class InputPageContainer
  extends JPanel
  implements TreeSelectionListener {

  private Query query;

  private final static Logger logger =
    Logger.getLogger(InputPageContainer.class.getName());

  private DatasetConfig datasetConfig;
  private CardLayout cardLayout = new CardLayout();

	private AdaptorManager adaptorManager;

	private DatasetConfigWidget datasetConfigWidget;

	private OutputSettingsPage outputSettingsPage;

  public InputPageContainer(
    Query query,
    QueryTreeView tree,
    AdaptorManager adaptorManager) {

    super();
    this.query = query;
    this.adaptorManager = adaptorManager;

    if (tree != null)
      tree.addTreeSelectionListener(this);
    setLayout(cardLayout);
    datasetConfigWidget = new DatasetConfigWidget(query, adaptorManager, this); 
    add(
      datasetConfigWidget,
      "DATASET_CONFIG");
    add(
      new DatasourceWidget(query, adaptorManager),
      TreeNodeData.DATASOURCE.getLabel());
    add(
      new DatasetWidget(query),
      TreeNodeData.DATASET.getLabel());
    add(
      new AttributesWidget(query, tree, adaptorManager),
      TreeNodeData.ATTRIBUTES.getLabel());
    add(
      new FiltersWidget(query, adaptorManager.getRootAdaptor(), tree),
      TreeNodeData.FILTERS.getLabel());
    
    outputSettingsPage = new OutputSettingsPage(query); 
    add(outputSettingsPage, TreeNodeData.FORMAT.getLabel());
  }

  public void setDatasetConfig(DatasetConfig datasetConfig) {
    this.datasetConfig = datasetConfig;
  }

  /**
   * Show input page corresponding to selected tree node.
   * Does nothing if query.datasetConfig==null because we should
   * only show the datasetConfigWidget in that case. 
   */
  public void valueChanged(TreeSelectionEvent e) {
  
    if ( query.getDatasetConfig()==null ) return;


    if (e.getNewLeadSelectionPath() != null
      && e.getNewLeadSelectionPath().getLastPathComponent() != null) {

      boolean advanced = adaptorManager.isAdvancedOptionsEnabled();
      
      DefaultMutableTreeNode node =
        (DefaultMutableTreeNode) e
          .getNewLeadSelectionPath()
          .getLastPathComponent();

      if (node != null) {

        TreeNodeData tnd = (TreeNodeData) node.getUserObject();

        
        if (tnd.getAttribute() != null)
          toFront(TreeNodeData.ATTRIBUTES);
        else if (tnd.getFilter() != null)
          toFront(TreeNodeData.FILTERS);
        // disable this the ability to select the dataset and 
        // datasource unless advanced is selected
        else if (tnd.getType()==TreeNodeData.DATASOURCE); 
        else if (tnd.getType()==TreeNodeData.DATASET); 
        else
          toFront(tnd.getType());
      }
    }
  }

  /**
   * Brings page corresponding to tdd to the front. Does nothing
   * if no such page exists.
   * @param tnd tree node is a key for an input page.
   */
  void toFront(TreeNodeData.Type tnd) {
    cardLayout.show(this, tnd.getLabel());
  }

	/**
	 * 
	 */
	public void openDatasetConfigMenu() {
		datasetConfigWidget.openDatasetConfigMenu();
	}

	/**
	 * @return
	 */
	public OutputSettingsPage getOutputSettingsPage() {
		return outputSettingsPage;
	}

}
