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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.Box;
import javax.swing.JScrollPane;

import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.FilterCollection;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterGroup;

/**
 * @author craig
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class FilterGroupWidget extends PageWidget {

	private Logger logger = Logger.getLogger(FilterGroupWidget.class.getName());

	private FilterGroup group;

	private Map internalNameToLeafWidget = new HashMap();

	/**
	 * @param name
	 * @param query
	 */
	public FilterGroupWidget(Query query, String name, FilterGroup group, QueryTreeView tree) {
		super(query, name, tree);

		this.group = group;

		Box panel = Box.createVerticalBox();
		leafWidgets = addCollections(panel, group.getFilterCollections());
		panel.add(Box.createVerticalGlue());

		add(new JScrollPane(panel));

	}

    private boolean skipCollection(FilterCollection collection) {
        boolean skip = false;
        
        if (tree.skipConfigurationObject(collection))
            skip = true;
        
        if (!skip && collection.containsOnlyPointerFilters())
            skip = true;
        
        if (!skip && collection.containsOnlyFilterListFilterUploadFilters())
            skip = true;
        
        return skip;
    }
	/**
	 * @param panel
	 * @param collections
	 * @return
	 */
	private List addCollections(Box panel, FilterCollection[] collections) {
		List widgets = new ArrayList();

		for (int i = 0; i < collections.length; i++) {

			FilterCollection collection = collections[i];


            if (skipCollection(collection)) continue;
            
			if (collection.getFilterDescriptions().size() > 0) {
				InputPage[] attributes = getFilterWidgets(collection);
				widgets.addAll(Arrays.asList(attributes));
				GridPanel p = new GridPanel(attributes, 1, 400, 35, collection.getDisplayName());
				panel.add(p);
				panel.add(Box.createVerticalStrut(Constants.GAP_BETWEEN_COMPONENTS_IN_WIDGET));
			}

		}
		return widgets;
	}

	/**
	 * @param collection
	 * @return
	 */
	private InputPage[] getFilterWidgets(FilterCollection collection) {
		List filterDescriptions = collection.getFilterDescriptions();
		List pages = new ArrayList();

		for (Iterator iter = filterDescriptions.iterator(); iter.hasNext();) {
			Object element = iter.next();

			if (element instanceof FilterDescription) {
                
				FilterDescription a = (FilterDescription) element;

                if (tree.skipConfigurationObject(a)) continue;
                
				//FilterWidget w = new FilterWidget(query, a);
				FilterWidget w = createFilterWidget(query, a);
				if (w != null)
					pages.add(w);
			} else {
				logger.severe("Unrecognised filter: " + element.getClass().getName() + element);
			}

		}

		return (InputPage[]) pages.toArray(new InputPage[pages.size()]);
	}

	private FilterWidget createFilterWidget(Query query, FilterDescription filterDescription) {

		String type = filterDescription.getType();
		FilterWidget w = null;

		if ("text".equals(type)) {

			w = new TextFilterWidget(this, query, filterDescription, tree);

		} else if ("list".equals(type)) {

			w = new ListFilterWidget(this, query, filterDescription, tree);

		} else if ("tree".equals(type)) {

			w = new TreeFilterWidget(this, query, filterDescription, tree);

		} else if ("boolean".equals(type) || "boolean_num".equals(type) || "boolean_list".equals(type)) {

			w = new BooleanFilterWidget(this, query, filterDescription, tree);

		} else if ("text_entry_basic_filter".equals(type) || "drop_down_basic_filter".equals(type)) {

			w = new ListFilterWidget(this, query, filterDescription, tree);

		} else if ("id_list".equals(type)) {

			w = new IDListFilterWidget(this, query, filterDescription, tree);

		}

		if (w != null) {

			internalNameToLeafWidget.put(filterDescription.getInternalName(), w);
			FilterPageSetWidget.TYPES.add(type);

		} else {
			FilterPageSetWidget.UNSUPPORTED_TYPES.add(type);

            if (logger.isLoggable(Level.INFO))
			  logger.info("Unsupported filter: " + filterDescription.getClass().getName() + ", " + filterDescription);

		}

		return w;
	}

	/**
	 * @param string
	 * @return
	 */
	public FilterWidget getFilterWidget(String internalName) {
		return (FilterWidget) internalNameToLeafWidget.get(internalName);
	}

}
