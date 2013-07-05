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

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JRadioButton;

import org.ensembl.mart.guiutils.QuickFrame;
import org.ensembl.mart.lib.BooleanFilter;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterGroup;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.lib.config.QueryFilterSettings;
import org.ensembl.mart.util.LoggingUtil;

/**
 * A boolean filter widget has a description, an optional list, and three radio buttons;
 * "require", "ignore", "irrelevant". 
 */
public class BooleanFilterWidget
  extends FilterWidget
  implements ActionListener {

  private String excludeFilterType;
  private String requireFilterType;

  private Box panel = Box.createHorizontalBox();

  private JRadioButton require = new JRadioButton("require");
  private JRadioButton exclude = new JRadioButton("exclude");
  private JRadioButton irrelevant = new JRadioButton("irrelevant");
  private JComboBox list = null;
  private ActionListener listSelectionListener = null;

  private Object lastSelectedComponent = null;
  private Object lastSelectedListItem = null;

  /**
   * @param query
   * @param filterDescription
   */
  public BooleanFilterWidget(
    FilterGroupWidget filterGroupWidget,
    Query query,
    FilterDescription fd,
    QueryTreeView tree) {

    super(filterGroupWidget, query, fd, tree);

    if ("boolean".equals(fd.getType())) {

      requireFilterType = BooleanFilter.isNotNULL;
      excludeFilterType = BooleanFilter.isNULL;

    } else if ("boolean_num".equals(fd.getType())) {

      requireFilterType = BooleanFilter.isNotNULL_NUM;
      excludeFilterType = BooleanFilter.isNULL_NUM;

    } else if ("boolean_list".equals(fd.getType())) {

      requireFilterType = BooleanFilter.isNotNULL;
      excludeFilterType = BooleanFilter.isNULL;

      list = new JComboBox();

      Dimension s =
        new Dimension(
          Constants.LIST_MAX_PIXEL_WIDTH,
          Constants.LIST_MAX_PIXEL_HEIGHT);
      list.setMaximumSize(s);
      //    we need to set preferred because maximum is ignored by Boxcontainer (at least on JVM1.4@linux)
      list.setPreferredSize(s);
      list.setMinimumSize(s);

      setOptions(fd.getOptions());
    } else
      new RuntimeException(
        "BooleanFilterWidget does not support filter description: "
          + fd
          + " becasue unrecognised type:"
          + fd.getType());

    irrelevant.setSelected(true);
    lastSelectedComponent = irrelevant;

    ButtonGroup group = new ButtonGroup();
    group.add(require);
    group.add(exclude);
    group.add(irrelevant);

    require.addActionListener(this);
    exclude.addActionListener(this);
    irrelevant.addActionListener(this);

    if (list != null) {
      Box left = Box.createVerticalBox();
      left.add(createLabel());
      left.add(list);
      panel.add(left);
      //panel.add(list);
    } else {
      panel.add(createLabel());
    }

    panel.add(Box.createHorizontalGlue());
    panel.add(require);
    panel.add(exclude);
    panel.add(irrelevant);

    add(panel);

  }

  /**
   * Responds to user selecting a button or selecting an item in the list.
   * 
   * Adds and removes filters to/from query.
   * 
   * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
   */
  public void actionPerformed(ActionEvent evt) {

    Object src = evt.getSource();

    if (src == list) {

      Object item = list.getSelectedItem();
      if (item == lastSelectedListItem)
        return;
      lastSelectedListItem = item;

    } else if (src == lastSelectedComponent)
      return;

    lastSelectedComponent = src;

    // stop listening to the query otherwise we will have changes
    // we make reflected back to us.
    query.removeQueryChangeListener(this);

    Filter newFilter = null;

    if (src == require || require.isSelected())
      newFilter = createFilter(requireFilterType);
    else if (src == exclude || exclude.isSelected())
      newFilter = createFilter(excludeFilterType);
    else if (src == irrelevant || irrelevant.isSelected())
      newFilter = null;

    if (newFilter == null) {
      if (filter != null)
        query.removeFilter(filter);
    } else {
      if (filter != null)
        query.replaceFilter(filter, newFilter);
      else
        query.addFilter(newFilter);
    }

    filter = newFilter;

    query.addQueryChangeListener(this);

  }

  private Filter createFilter(String filterType) {

    QueryFilterSettings settings = filterDescription;
    if (list != null) {

      settings = ((OptionToStringWrapper) list.getSelectedItem()).option;
      // The fieldName will change if this is a list 
      fieldName = settings.getFieldFromContext();
      tableConstraint = settings.getTableConstraint();
      key = settings.getKey();

    }

    return new BooleanFilter(
      settings.getFieldFromContext(),
      settings.getTableConstraintFromContext(),
      settings.getKeyFromContext(),
      filterType);
  }

  public void setOptions(Option[] options) {

    if (list == null)
      return;

    list.removeActionListener(this);
    list.removeAllItems();

    // add items
    for (int i = 0; i < options.length; i++) {
      Option o = options[i];
      if (o.isSelectable())
        list.addItem(new OptionToStringWrapper(this, o));
    }

    list.addActionListener(this);

    list.validate();

  }

  /**
   * Selects the button and item in list (if necessary) when filter changed.
   * This is a callback method called when a filter with the same fieldName 
   * as this widget is added or
   * removed to/from the query. It is called from 
   * filterAdded(...) and filterRemoved(...) in FilterWidget base class.
   *  
   */
  public void setFilter(Filter filter) {

    if (filter == null) {

      irrelevant.setSelected(true);

    } else {

      if (list != null) {

        int index = indexOfListItemMatchingFilter(filter);
        if (index > -1) {
          list.removeActionListener(this);
          list.setSelectedIndex(index);
          list.addActionListener(this);
        }
      }

      if (filter.getQualifier().equals(requireFilterType))
        require.setSelected(true);

      else if (filter.getQualifier().equals(excludeFilterType))
        exclude.setSelected(true);
    }
  }

  /**
   * Test program; a simple GUI using test data.
   * @param args
   * @throws org.ensembl.mart.lib.config.ConfigurationException
   */
  public static void main(String[] args)
    throws org.ensembl.mart.lib.config.ConfigurationException {

    // switch on logging for test purposes.
    LoggingUtil.setAllRootHandlerLevelsToFinest();
    Logger.getLogger(Query.class.getName()).setLevel(Level.FINE);
/*
    Query q = new Query();
    FilterGroup fg = new FilterGroup();
    FilterGroupWidget fgw = new FilterGroupWidget(q, "fgw", fg, null);
    FilterDescription fd =
      new FilterDescription(
        "someInternalName",
        "someField",
        "boolean",
        "someQualifier",
        "someLegalQualifiers",
        "test boolean",
        "someTableConstraint",
        "someKey",
        "someDescription",
        "",
        "","","","","","","");
    BooleanFilterWidget bfw = new BooleanFilterWidget(fgw, q, fd, null);

    FilterDescription fd2 =
      new FilterDescription(
        "someInternalName",
        "someField_num",
        "boolean_num",
        "someQualifier",
        "someLegalQualifiers",
        "test boolean_num ",
        "someTableConstraint",
        "someKey",
        "someDescription",
        "",
        "","","","","","","");
    BooleanFilterWidget bfw2 = new BooleanFilterWidget(fgw, q, fd2, null);

    FilterDescription fd3 =
      new FilterDescription(
        "someInternalName",
        "someField_list",
        "boolean_list",
        "someQualifier",
        "someLegalQualifiers",
        "test boolean_list Onfd3 ",
        "someTableConstraint Onfd3",
        "someKey Onfd3",
        "someDescription",
        "",
        "","","","","","","");
    Option o = new Option("fred_id", "true");
    o.setParent(fd3);
    o.setDisplayName("Fred");
    o.setField("fred_field");
    fd3.addOption(o);
    Option o2 = new Option("barney_id", "true");
    o2.setParent(fd3);
    o2.setDisplayName("Barney");
    o2.setField("barney");
    fd3.addOption(o2);
    BooleanFilterWidget bfw3 = new BooleanFilterWidget(fgw, q, fd3, null);

    Box p = Box.createVerticalBox();
    p.add(bfw);
    p.add(bfw2);
    p.add(bfw3);

    new QuickFrame("BooleanFilterWidget test", p);
*/
  }

  protected boolean equivalentFilter(Object otherFilter) {

    if (super.equivalentFilter(otherFilter))
      return true;

    if (otherFilter == null || !(otherFilter instanceof Filter))
      return false;

    if (list != null) {

      if (indexOfListItemMatchingFilter((Filter) otherFilter) > -1)
        return true;

    }

    return false;

  }

  /**
   * 
   * @param otherFilter
   * @return -1 if filter not relat
   */
  private int indexOfListItemMatchingFilter(Filter filter) {
    // check that otherFilter does not correspond to one of the list items.
    final int n = list.getItemCount();
    for (int i = 0; i < n; i++) {

      OptionToStringWrapper op = (OptionToStringWrapper) list.getItemAt(i);
      Option o = op.option;
      String f = filter.getField();
      String tc = filter.getTableConstraint();
      String k = filter.getKey();

      if (f != null
        && tc != null
        && k != null
        && !"".equals(f)
        && !"".equals(k)
        && !"".equals(tc)
        && f.equals(o.getFieldFromContext())
        && tc.equals(o.getTableConstraintFromContext())
        && k.equals(o.getKeyFromContext()))
        return i;
    }

    return -1;
  }

}
