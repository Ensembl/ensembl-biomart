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

import java.util.Iterator;
import java.util.List;

import javax.swing.JLabel;
import javax.swing.JTabbedPane;

import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.AttributeCollection;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DSConfigAdaptor;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.DatasetConfigIterator;

/**
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class AttributesWidget extends InputPage {

  private JTabbedPane tabbedPane = new JTabbedPane();
  private AdaptorManager manager;
  private JLabel unavailableLabel =
    new JLabel("Unavailable. Choose DatasetConfig first.");
  private Feedback feedback = null;
  
  
  /**
   * Displays the attributes grouped according to query.datasetConfig.
   * If none are available if displays a message to that effect. 
   * @param query
   */
  public AttributesWidget(Query query, QueryTreeView tree, AdaptorManager manager) {
    super(query, null, tree);
    this.manager = manager;
    feedback = new Feedback(this);
    clearAttributes();    
  }

  private void clearAttributes() {
    remove(tabbedPane);
    add(unavailableLabel);
    validate();
  }

  /**
   * Loads attributes from datasetConfig when a new datasetConfig is set on
   * the query.
   * @see org.ensembl.mart.lib.QueryChangeListener#datasetConfigChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.config.DatasetConfig, org.ensembl.mart.lib.config.DatasetConfig)
   */
  public void datasetConfigChanged(
    Query query,
    DatasetConfig oldDatasetConfig,
    DatasetConfig newDatasetConfig) {

    if (newDatasetConfig == null) {
      clearAttributes();
    } else {
      remove( unavailableLabel );
      tabbedPane.removeAll();
      AttributePage[] aps = newDatasetConfig.getAttributePages();
      for (int i = 0; i < aps.length; i++)
      {
      	if (skipPage(aps[i])) continue;
        tabbedPane.add(
          new AttributePageWidget(query, aps[i].getDisplayName(), aps[i], tree, newDatasetConfig, manager));
      add(tabbedPane);
      validate();
    }
    }
  }
  
  private boolean skipPage(AttributePage page) {
      boolean skip = tree.skipConfigurationObject(page);
      
      //skip the structure page for now
      if (!skip && page.getInternalName().equalsIgnoreCase("structure"))
          skip = true;
      
      if (!skip && page.getInternalName().equalsIgnoreCase("sequence"))
          skip = true;
      
      if (!skip && page.getInternalName().equalsIgnoreCase("sequences"))
          skip = true;

      /*
      //we only support sequences with pointer attributes
      if (!skip) {
          if (page.containsOnlyPointerAttributes()) {
              //AttributeGroup seqGroup = (AttributeGroup) page.getAttributeGroupByName("sequence");
              
              //skip if this does not contain a sequence group (non ensembl)
              // We hate sequences!
              //if (seqGroup == null)
                  skip = true;
              else {
                  AttributeCollection seqCol = null;
                  
                  AttributeCollection[] cols = seqGroup.getAttributeCollections();
                  for (int i = 0, n = cols.length; i < n; i++) {
                    AttributeCollection collection = cols[i];
                    if (collection.getInternalName().matches("\\w*seq_scope\\w*")) {
                        seqCol = collection;
                        break;
                    }
                }
                  
                  //skip if the sequence group does not contain a page called "seq_scope_type" (non ensembl)
                  if (seqCol == null)
                      skip = true;
                  
                  if (!skip) {
                      //test for presence of sequence dataset
                      AttributeDescription seqDesc = (AttributeDescription) seqCol.getAttributeDescriptions().get(0);
                      String seqDataset = seqDesc.getPointerDataset();
                      if (manager.getRootAdaptor().getNumDatasetConfigsByDataset(seqDataset) < 1) {
                          feedback.info("You must load sequence dataset " 
                                       + seqDataset 
                                       + " with this sequence supporting dataset, skipping sequence page.");
                          skip = true;
                      }
                  }
              }
              
              if (!skip) {
                  //test for ambiguous links
                  AttributeGroup nonSeqGroup = null;
                  List groups = page.getAttributeGroups();
                  for (int i = 0, n = groups.size(); i < n; i++) {
                    AttributeGroup element = (AttributeGroup) groups.get(i);
                    if (!element.getInternalName().equals("sequence")) {
                        nonSeqGroup = element;
                        break;
                    }
                }
                  
                //get the first attribute, and test its dataset to see if it is duplicated
                AttributeDescription firstAtt = (AttributeDescription) nonSeqGroup.getAttributeCollections()[0].getAttributeDescriptions().get(0);
                String dataset = firstAtt.getPointerDataset();
                
                if (dataset!=null && manager.getRootAdaptor().getNumDatasetConfigsByDataset(dataset) > 1) {
                    feedback.info("Dataset " + dataset + " with sequence support has been loaded more than once, skipping sequence page\n");
                    skip = true;
                }   
              }
          }
      }
              */
      return skip;
  }

}
