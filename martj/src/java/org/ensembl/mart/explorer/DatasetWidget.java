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

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.logging.Logger;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.QueryListener;
import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Widget represents the currently selected dataset 
 * and enables the user to select another.
 * 
 */
public class DatasetWidget
  extends InputPage
  implements QueryListener, ChangeListener {

  private Logger logger = Logger.getLogger(DatasetWidget.class.getName());
  
  private static final String PREFERENCE_KEY = "DATASET_KEY";
  private Preferences prefs = Preferences.userNodeForPackage(this.getClass());

  private Feedback feedback = new Feedback(this);

  private LabelledComboBox combo = new LabelledComboBox("Dataset ");

  private JButton defaultButton = new JButton("Reset from dataset config");

  /**
   * @param query underlying model for this widget.
   */
  public DatasetWidget(Query query) {

    super(query, "Dataset ");

    defaultButton.setEnabled(query.getDatasetConfig() != null);
    defaultButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        doLoadDefaultDatasetName();
      }
    });

    combo.setPreferenceKey(PREFERENCE_KEY);
    combo.load(prefs);
    combo.setSelectedItem( query.getDataset()  );
    // Note: must add listener AFTER setSelected() otherwise
    // this.stateChanged() will be called when the first element 
    // in the preferences is the selected item.
    combo.addChangeListener(this);

    Box b = Box.createVerticalBox();
    b.add(combo, BorderLayout.NORTH);
    b.add(defaultButton);
    b.add(Box.createVerticalGlue());
    add(b);
  }


  private void doLoadDefaultDatasetName() {
    DatasetConfig dsv = query.getDatasetConfig(); 
    if ( dsv!=null )
      combo.setSelectedItem( dsv.getDataset() );
  }


  /**
   * Runs a test; an instance of this class is shown in a Frame.
   */
  public static void main(String[] args) throws Exception {
    Query q = new Query();
    DatasetWidget dw = new DatasetWidget(q);
    dw.setSize(950, 750);

    JFrame f = new JFrame(dw.getClass().getName() + " - test");
    f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    f.getContentPane().add(dw);
    f.pack();
    f.setVisible(true);

  }

  /**
   * Responds to a change in query.dataset. Updates the state of
   * this widget by changing the label.
   */
  public void datasetChanged(
    Query query,
    String oldDataset,
    String newDataset) {

    combo.setSelectedItem(newDataset);
  }

  /**
   * Responds to user changing the selected dataset by updating
   * query.dataset.
   * @see javax.swing.event.ChangeListener#stateChanged(javax.swing.event.ChangeEvent)
   */
  public void stateChanged(ChangeEvent e) {

    if (combo.getSelectedItem() == query.getDataset())
      return;
    
    query.setDataset((String) combo.getSelectedItem());
      
    
    combo.store(prefs, 10);
    // We need to reload the list from the prefs so that the current selection is
    // added to the drop down list.
    combo.load(prefs);
    try {
      prefs.flush();
    } catch (BackingStoreException e1) {
      e1.printStackTrace();
    }
    
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#datasetConfigChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.config.DatasetConfig, org.ensembl.mart.lib.config.DatasetConfig)
   */
  public void datasetConfigChanged(
    Query query,
    DatasetConfig oldDatasetConfig,
    DatasetConfig newDatasetConfig) {

    // set dataset to default value
    if ( query.getDataset()==null && query.getDatasetConfig()!=null ) 
      query.setDataset( newDatasetConfig.getDataset() );

    defaultButton.setEnabled(newDatasetConfig != null );
  }

}
