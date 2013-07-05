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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.FilterPage;

/**
 * Holds all filter pages.
 */
public class FilterPageSetWidget extends PageSetWidget {

	private List filterDescriptionWidgets;

  final static Set TYPES = new HashSet();
  final static Set UNSUPPORTED_TYPES = new HashSet();

	/**
	 * @param query
	 */
	public FilterPageSetWidget(Query query, DatasetConfig dataset, QueryTreeView tree) {
		super(query, "Filters", tree);

		filterDescriptionWidgets = new ArrayList();

		FilterPage[] filterPages = dataset.getFilterPages();
		for (int i = 0, n = filterPages.length; i < n; i++) {
			FilterPage page = filterPages[i];
            if (tree.skipConfigurationObject(page)) continue;
			String name = page.getDisplayName();
			FilterPageWidget p = new FilterPageWidget(query, name, page, tree);
			tabbedPane.add(name, p);
			filterDescriptionWidgets.addAll(p.getLeafWidgets());
      leafWidgets.addAll( p.getLeafWidgets() );
		}
		resetTabColors();

		
		System.out.println("TYPES: " + TYPES);
    System.out.println("UNSUPPORTED_TYPES: " + UNSUPPORTED_TYPES);
	}

	/**
	 * 
	 * @return all filterDescriptionWidgets contained in sub pages.
	 */
	public List getFilterDescriptionWidgets() {
		return filterDescriptionWidgets;
	}
}