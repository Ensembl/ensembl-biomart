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

import java.awt.Component;
import java.awt.Point;
import java.awt.Rectangle;
import java.util.logging.Logger;

import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.lib.config.PushAction;
import org.ensembl.mart.util.StringUtil;

/**
 * Base class for FilterWidgets. 
 */
public abstract class FilterWidget
  extends InputPage
  implements TreeSelectionListener {

  private final static Logger logger =
    Logger.getLogger(FilterWidget.class.getName());

  protected String fieldName;

  protected String tableConstraint;

  protected String key;
  
  protected String qualifier;

  protected FilterGroupWidget filterGroupWidget;

  protected FilterDescription filterDescription;

  protected Filter filter = null;

  /**
   * @param query
   * @param name
   */
  public FilterWidget(
    FilterGroupWidget filterGroupWidget,
    Query query,
    FilterDescription filterDescription,
    QueryTreeView tree) {

    super(query, filterDescription.getDisplayName());
    if (tree != null)
      tree.addTreeSelectionListener(this);
    this.filterDescription = filterDescription;
    this.filterGroupWidget = filterGroupWidget;
    this.fieldName = filterDescription.getFieldFromContext();
    this.tableConstraint = filterDescription.getTableConstraintFromContext();
    this.key = filterDescription.getKeyFromContext();
    this.qualifier = filterDescription.getQualifierFromContext();
  }

  /**
   * @return
   */
  public FilterDescription getFilterDescription() {
    return filterDescription;
  }

  public abstract void setOptions(Option[] options);

  protected OptionWrapper emptySelection = new OptionWrapper(null);

  /**
   * Holds an Option and returns option.getDisplayName() from
   * toString(). This class is used to add Options to the
   * combo box.
   */
  protected class OptionWrapper {
    protected Option option;

    protected OptionWrapper(Option option) {
      this.option = option;
    }

    public String toString() {
      return (option == null) ? "No Filter" : option.getDisplayName();
    }
  }

  protected PushOptionsHandler[] pushOptionHandlers;

  /**
     * Removes all options from the push targets.
     */
  protected void unassignPushOptions() {

    int n = (pushOptionHandlers == null) ? 0 : pushOptionHandlers.length;
    for (int i = 0; i < n; i++)
      pushOptionHandlers[i].remove();

  }

  /**
     * @param pushs
     */
  protected void assignPushOptions(PushAction[] optionPushes) {

//  disabling pushactions for now
    return;
    
//    pushOptionHandlers = new PushOptionsHandler[optionPushes.length];
//
//    for (int i = 0; i < optionPushes.length; i++) {
//      pushOptionHandlers[i] =
//        new PushOptionsHandler(optionPushes[i], filterGroupWidget);
//      pushOptionHandlers[i].push();
//    }
  }

  /**
   * @return true if otherfilter has the same fieldName
   */
  protected boolean equivalentFilter(Object otherFilter) {
    if (otherFilter == null || !(otherFilter instanceof Filter))
      return false;

    Filter of = (Filter) otherFilter;
    return
    //
    fieldName != null
    && !"".equals(fieldName)
    && of.getField() != null
    && of.getField().equals(fieldName)
    //
    && tableConstraint != null
    && !"".equals(tableConstraint)
    && of.getTableConstraint() != null
    && of.getTableConstraint().equals(tableConstraint)
    //
    && key != null
    && !"".equals(key)
    && of.getKey() != null
    && of.getKey().equals(key)
    //
    && qualifier != null
    && !"".equals(qualifier)
    && of.getQualifier() != null
    && of.getQualifier().equals(qualifier);
  }

  protected abstract void setFilter(Filter filter);

  /**
   * BasicFilter containing an InputPage, this page is used by the QueryEditor
   * when it detects the filter has been added or removed from the query.
   */
  static class InputPageAwareBasicFilter
    extends BasicFilter
    implements InputPageAware {

    private InputPage inputPage;
    public InputPageAwareBasicFilter(
      String field,
      String qualifier,
      String value,
      InputPage inputPage) {
      this(field, null, null, qualifier, value, inputPage);
    }

    public InputPageAwareBasicFilter(
      String field,
      String tableConstraint,
      String key,
      String qualifier,
      String value,
      InputPage inputPage) {
      super(field, tableConstraint, key, qualifier, value);
      this.inputPage = inputPage;
    }

    public InputPageAwareBasicFilter(Option option, InputPage inputPage) {
      super(
        option.getFieldFromContext(),
        option.getTableConstraintFromContext(),
        option.getKeyFromContext(),
        option.getQualifierFromContext(),
        option.getValueFromContext());
      this.inputPage = inputPage;
    }

    public InputPage getInputPage() {
      return inputPage;
    }
  }

  /** 
   * Responds to the addition of a relevant filter to the query by 
   * updates the state of widget to reflect the filter.
   * @see org.ensembl.mart.lib.QueryChangeListener#filterAdded(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Filter)
   */
  public void filterAdded(Query sourceQuery, int index, Filter filter) {
    if (equivalentFilter(filter))
      setFilter(filter);
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.lib.QueryChangeListener#filterChanged(org.ensembl.mart.lib.Query, org.ensembl.mart.lib.Filter, org.ensembl.mart.lib.Filter)
   */
  public void filterChanged(
    Query sourceQuery,
    Filter oldFilter,
    Filter newFilter) {
  }

  /**
   * Responds to the removal of a relevant filters from the query by 
   * updates the state of widget to reflect the filter.
   * @see org.ensembl.mart.lib.QueryChangeListener#filterRemoved(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Filter)
   */
  public void filterRemoved(Query sourceQuery, int index, Filter filter) {
    if (equivalentFilter(filter))
      setFilter(null);
  }

  /**
   * Callback method called when an item in the tree is selected.
   * Brings this widget to the front if the selecte node corresponds to this widget this
   * TODO get scrolling to a selected attribute working properly
   * @see javax.swing.event.TreeSelectionListener#valueChanged(javax.swing.event.TreeSelectionEvent)
   */
  public void valueChanged(TreeSelectionEvent e) {

    if (filter != null) {

      if (e.getNewLeadSelectionPath() != null
        && e.getNewLeadSelectionPath().getLastPathComponent() != null) {

        DefaultMutableTreeNode node =
          (DefaultMutableTreeNode) e
            .getNewLeadSelectionPath()
            .getLastPathComponent();

        if (node != null) {

          TreeNodeData tnd = (TreeNodeData) node.getUserObject();
          Filter f = tnd.getFilter();
          if (f != null && f == filter) {

            for (Component p, c = this; c != null; c = p) {
              p = c.getParent();
              if (p instanceof JTabbedPane)
                 ((JTabbedPane) p).setSelectedComponent(c);
              else if (p instanceof JScrollPane) {
                // not sure if this is being used
                Point pt = c.getLocation();
                Rectangle r = new Rectangle(pt);
                ((JScrollPane) p).scrollRectToVisible(r);
              }

            }
          }
        }
      }
    }

  }

  protected JLabel createLabel() {
    String label = filterDescription.getDisplayName();
    if (label == null)
      label = "";
    else
      label =
        StringUtil.wrapLinesAsHTML(
          label,
          Constants.LABEL_WIDTH_IN_CHARS,
          false);
    return new JLabel(label);
  }

}
