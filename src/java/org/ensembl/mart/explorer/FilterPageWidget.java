package org.ensembl.mart.explorer;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.FilterGroup;
import org.ensembl.mart.lib.config.FilterPage;

/**
 * This is a GUI representation of a
 * the FilterPage it is created from.
 * It contains a FilterGroupWidget corresponding
 * to each of the elements filterPage.getFilterGroups().
 */
public class FilterPageWidget extends PageWidget {

	private final static Logger logger = Logger.getLogger(FilterPageWidget.class.getName());

	/**
	 * @param query model
	 * @param name name of this page
	 * @param filterPage source object this instance represents
	 */
	public FilterPageWidget(Query query, String name, FilterPage filterPage, QueryTreeView tree) {
		super(query, name, tree);

		List filterGroups = filterPage.getFilterGroups();
		for (Iterator iter = filterGroups.iterator(); iter.hasNext();) {
			Object element = iter.next();
			if (element instanceof FilterGroup) {
				FilterGroup group = (FilterGroup) element;
                if (tree.skipConfigurationObject(group)) continue;
                if (group.containsOnlyPointerFilters()) continue;
                
				String groupName = group.getDisplayName();

				FilterGroupWidget w = new FilterGroupWidget(query, groupName, group, tree);

				if (w.getLeafWidgets().size() > 0) {
					tabbedPane.add(groupName, w);
					leafWidgets.addAll(w.getLeafWidgets());
				}
			}
			//else if ( element instanceof DSFilterGroup ) {
			// TODO handle DSAttributeGroup
			//logger.warning( "TODO: handle DSAttributeGroup: " + element.getClass().getName() );
			// create filterPage
			// add pag as tab
			//}
			else {
				throw new RuntimeException("Unrecognised type in filter group list: " + element);
			}

		}
	}

}
