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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Widget representing an AttributePage.
 */
public class AttributePageWidget extends PageWidget {

  private final Logger logger =
    Logger.getLogger(AttributePageWidget.class.getName());
  private Feedback feedback = null;
  
  private AttributePage page;
  /**
   * @param name
   * @param query
 * @throws ConfigurationException 
   */
  public AttributePageWidget(
    Query query,
    String name,
    AttributePage page,
    QueryTreeView tree,
    DatasetConfig dsv,
    AdaptorManager manager) {

    super(query, name, tree);

    this.page = page;
    feedback = new Feedback(this);
    List attributeGroups = page.getAttributeGroups();
    for (Iterator iter = attributeGroups.iterator(); iter.hasNext();) {
      Object element = iter.next();
   
      if (element instanceof AttributeGroup) {
   
        AttributeGroup group = (AttributeGroup) element;        
        if (tree.skipConfigurationObject(group)) continue;
        
        String groupName = group.getDisplayName();

        AttributeGroupWidget w =
          new AttributeGroupWidget(query, groupName, group, page, tree, dsv, manager);
        tabbedPane.add(groupName, w);
        leafWidgets.addAll(w.getLeafWidgets());
      } else {
        throw new RuntimeException(
          "Unrecognised type in attribute group list: " + element);
      }
    }
  }
  /* (non-Javadoc)
   * @see org.ensembl.mart.explorer.InputPage#attributeAdded(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Attribute)
   */
  public void attributeAdded(Query sourceQuery, int index, Attribute attribute) {
      if (this.isShowing()) {
          ArrayList attsToRemove = new ArrayList();       
          boolean removeSeq = false;
          
          Attribute[] queryAtts = sourceQuery.getAttributes();
          for (int i = 0, n = queryAtts.length; i < n; i++) {
              Attribute thisAtt = queryAtts[i];
              
              if (page.getAttributeDescriptionByFieldNameTableConstraint(thisAtt.getField(), thisAtt.getTableConstraint()) == null) {
                  attsToRemove.add(thisAtt);
              }
          }
          
          if (sourceQuery.getSequenceDescription() != null) {
              if (!page.getInternalName().equals("sequences"))
                  removeSeq = true;
          }
          
          if (attsToRemove.size() > 0 || removeSeq) {
              feedback.info("Removing attributes from pages not compatible with " + page.getDisplayName());
              
              for (int i = 0, n = attsToRemove.size(); i < n; i++) {
                  Attribute attToRemove = (Attribute) attsToRemove.get(i);
                  sourceQuery.removeAttribute(attToRemove);
              }
              
              if (removeSeq)
                  sourceQuery.setSequenceDescription(null);
          }
      }
  }
}
