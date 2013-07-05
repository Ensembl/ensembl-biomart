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

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.Box;
import javax.swing.JFrame;

import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DSConfigAdaptor;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.DatasetConfigIterator;
import org.ensembl.mart.util.LoggingUtil;

/**
 * Widget showing available dataset configs represented as a menu tree. The user can select one of
 * these. 
 * 
 * <p>The first tier of the tree contains
 * adaptors, the second the datasets, and the optional third tier the internalNames. 
 * If adaptorManager.isOptionalDatasetConfigsEnabled()==true then the optional 
 * third tier is included. Otherwise only dataset configs with internalName=="default"
 * are displayed and they are shown as adaptor -> dataset. In this case the internalName
 * is not shown.
 * </p>
 */
public class DatasetConfigTree extends PopUpTreeCombo {

	private final static Logger logger =
		Logger.getLogger(DatasetConfigTree.class.getName());

	private Feedback feedback = new Feedback(this);

	private AdaptorManager manager;

	public DatasetConfigTree(AdaptorManager manager) {
		super("Dataset");
		this.manager = manager;
	}

	/**
	 * Update the tree's rootNode to reflect the currently available datasetConfigs.
	 * Structure: adaptor -> dataset [ -> internalName ]
	 * @see org.ensembl.mart.explorer.PopUpTreeCombo#update()
	 */
	public void update() {

		boolean optional = manager.isAdvancedOptionsEnabled();
		rootNode.removeAllChildren();
		logger.fine("optional=" + optional);

		try {
			DSConfigAdaptor[] adaptors = manager.getRootAdaptor().getLeafAdaptors();
			// TODO sort adaptors by name

			for (int i = 0; i < adaptors.length; i++) {

				DSConfigAdaptor adaptor = adaptors[i];

				// Skip composite adaptors
				if (adaptor.getLeafAdaptors().length > 0)
					continue;

				// skip adaptors which lack a "default" config
				// if we are only showing default configs.
				if (!optional && !containsDefaultConfig(adaptor))
					continue;
        
        if (adaptor.getNumDatasetConfigs(true)==0 )
          continue;

				LabelledTreeNode adaptorNode =
					new LabelledTreeNode(adaptor.getName(), null);

				rootNode.add(adaptorNode);

				try {

					String[] datasetNames = adaptor.getDatasetNames(false);
					Arrays.sort(datasetNames);

					for (int j = 0; j < datasetNames.length; j++) {

						String dataset = datasetNames[j];
						DatasetConfigIterator configs = adaptor.getDatasetConfigsByDataset(dataset);

						LabelledTreeNode datasetNode = null;

						while (configs.hasNext()) {

							DatasetConfig config = (DatasetConfig) configs.next();

							if (optional) {

								if (datasetNode == null) {
									datasetNode = new LabelledTreeNode(dataset, null);

									if (datasetNode != null)
										adaptorNode.add(datasetNode);
								}

								// adaptor -> dataset -> internalName
								datasetNode.add(
									new LabelledTreeNode(config.getInternalName(), config));

							} else {
								// adaptor -> dataset (using default datasetconfig only)
								if (isDefault(config)) {
									adaptorNode.add(
										new LabelledTreeNode(config.getDataset(), config));
									break;
								}
							}
						}
					}
				} catch (ConfigurationException e) {
					// do this try ... catch so that a problem with one adaptor won't prevent dataset configs from
					// others being loaded 
					feedback.warning(e);
				}

			}
		} catch (ConfigurationException e) {

			feedback.warning(e);
		}
	}

	public boolean isDefault(DatasetConfig config) {
		return "default".equals(config.getInternalName().toLowerCase());
	}

	/**
	 * @param adaptor
	 * @return
	 */
	private boolean containsDefaultConfig(DSConfigAdaptor adaptor) {
		boolean r = false;
		try {
			DatasetConfigIterator configs = adaptor.getDatasetConfigs();
			while (!r && configs.hasNext())
				if (isDefault((DatasetConfig) configs.next()))
					r = true;

		} catch (ConfigurationException e) {
			feedback.warning(e);
		}
		return r;
	}

	public static void main(String[] args) {

		LoggingUtil.setAllRootHandlerLevelsToFinest();
		logger.setLevel(Level.FINE);

		AdaptorManager am = new AdaptorManager();
		//QueryEditor.testDatasetConfigSettings();
		am.setAdvancedOptionsEnabled(true);

		final DatasetConfigTree pu = new DatasetConfigTree(am);
		// test the listener support
		pu.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.out.println("Selection changed to : " + pu.getSelectedLabel());
			}
		});
		Box p = Box.createVerticalBox();
		p.add(pu);
		JFrame f = new JFrame(DatasetConfigTree.class.getName() + " (Test Frame)");
		f.getContentPane().add(p);
		f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		//f.setSize(250, 100);
		f.pack();
		f.setVisible(true);
	}

	/**
   * Sets the selected node to the node where node.userObject==datasetConfig
	 * @param newDatasetConfig
	 */
	public void setSelectedUserObject(DatasetConfig datasetConfig) {
		Enumeration e = rootNode.breadthFirstEnumeration();

		while (e.hasMoreElements()) {
			LabelledTreeNode next = (LabelledTreeNode) e.nextElement();
      if ( next.getUserObject()==datasetConfig) {
        setSelected(next);
        break;
        } 
		}
	}

  
  
}
