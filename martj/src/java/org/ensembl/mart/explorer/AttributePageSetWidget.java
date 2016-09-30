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

import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Holds all the attribute pages.
 */
public class AttributePageSetWidget extends PageSetWidget {

  /**
	 * @param query
	 */
	public AttributePageSetWidget(Query query, DatasetConfig dataset, QueryTreeView tree, AdaptorManager manager) {
		
    super(query, "Attributes", tree);
    
		AttributePage[] attributePages = dataset.getAttributePages();
		for (int i = 0, n = attributePages.length; i < n; i++) {
			AttributePage page = attributePages[i];
      String name = page.getDisplayName();
      AttributePageWidget p = new AttributePageWidget(query, name, page, tree, dataset, manager);
			tabbedPane.add( name, p );
      leafWidgets.addAll( p.getLeafWidgets() );
		}
    resetTabColors(); 
    
    
    
	}

}
