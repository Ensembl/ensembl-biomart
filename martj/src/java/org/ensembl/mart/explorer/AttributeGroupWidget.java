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

import java.awt.Container;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;

import javax.swing.Box;
import javax.swing.JScrollPane;

import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.AttributeCollection;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Widget representing an AttibuteGroup.
 */
public class AttributeGroupWidget extends GroupWidget {

	private final static Logger logger = Logger
			.getLogger(AttributeGroupWidget.class.getName());

	private int lastWidth;

	private AttributeGroup group;

	private AttributePage page;

	/**
	 * @param query
	 * @param name
	 */
	public AttributeGroupWidget(Query query, String name, AttributeGroup group,
			AttributePage page, QueryTreeView tree, DatasetConfig dsv,
			AdaptorManager manager) {

		super(name, query, tree);

		this.group = group;
		this.page = page;

		Box panel = Box.createVerticalBox();
		leafWidgets = addCollections(panel, group.getAttributeCollections(),
				dsv, manager);
		panel.add(Box.createVerticalGlue());

		add(new JScrollPane(panel));

	}

	/**
	 * @param collections
	 */
	private List addCollections(Container container,
			AttributeCollection[] collections, DatasetConfig dsv,
			AdaptorManager manager) {

		List widgets = new ArrayList();
		
		
		
		for (int i = 0; i < collections.length; i++) {

			if (tree.skipConfigurationObject(collections[i]))
				continue;

			if (group.getInternalName().equals("sequence")) {
				// We don't support sequences any more.
				/*
				if (collections[i].getInternalName().matches(
						"\\w*seq_scope\\w*")) {
					SequenceGroupWidget w = new SequenceGroupWidget(
							collections[i].getDisplayName(), collections[i]
									.getInternalName(), query, tree, dsv,
							manager);
					widgets.add(w);
					container.add(w);
				} else
				*/
					continue;
			} else {
				AttributeCollection collection = collections[i];
				InputPage[] attributes = getAttributeWidgets(collection,
						manager, dsv);
				widgets.addAll(Arrays.asList(attributes));
				GridPanel p = new GridPanel(attributes, 2, 200, 35, collection
						.getDisplayName());
				container.add(p);
			}
		}
		return widgets;
	}

	/**
	 * Converts collection.UIAttributeDescriptions into InputPages.
	 * 
	 * @param collection
	 * @return array of AttributeDescriptionWidgets, one for each
	 *         AttributeDescription in the collection.
	 */
private InputPage[] getAttributeWidgets(AttributeCollection collection, AdaptorManager manager, DatasetConfig dsv) 
{

    List attributeDescriptions = collection.getAttributeDescriptions();
    List pages = new ArrayList();

    for (Iterator iter = attributeDescriptions.iterator(); iter.hasNext();) 
    {
      Object element = iter.next();
      
      if (element instanceof AttributeDescription) 
      {

        AttributeDescription a = (AttributeDescription) element;
        if (tree.skipConfigurationObject(a)) continue;

        if (a.getPointerDataset()!=null && !"".equals(a.getPointerDataset())) 
        {
            String dname = a.getPointerDataset();
            String aname = a.getPointerAttribute();

        		try {
                    DatasetConfig ds = (dname.equals(dsv.getDataset()))?dsv:null;
                    if (ds==null) continue; // We don't like pointer attributes.
                    AttributeDescription a2 = ds.getAttributeDescriptionByInternalName(aname);
                    a.setDisplayName(a2.getDisplayName());
                    a.setInternalName(a2.getInternalName());
                    a.setField(a2.getField());
                    a.setTableConstraint(a2.getTableConstraint());
                    a.setKey(a2.getKey());
        		} catch (RuntimeException e) {
        			e.printStackTrace();
                	continue; // This is not a resolvable placeholder. Skip to the next.
        		}
        }
        
        AttributeDescriptionWidget w = new AttributeDescriptionWidget(query, a, tree);
        pages.add(w);
      }
      else {

        logger.severe("Unsupported attribute description: " +  element.getClass().getName() + element);
      }
    }

    return (InputPage[]) pages.toArray(new InputPage[pages.size()]);

  }
}
