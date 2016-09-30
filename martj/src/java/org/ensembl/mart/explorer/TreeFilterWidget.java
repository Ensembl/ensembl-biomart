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
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.Box;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JTextField;

import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterGroup;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.util.LoggingUtil;

/**
 * Represents a set of user options as a tree.
 * Component consists of a label, text area and button. 
 * 
 */
public class TreeFilterWidget extends FilterWidget {
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
        "text",
        "someQualifier",
        "someLegalQualifiers",
        "someDisplayName",
        "someTableConstraint",
        "someKey",
        "someDescription",
        "",
        "","","","","","","");

    TreeFilterWidget tfw = new TreeFilterWidget(fgw, q, fd, null);

    JFrame f = new JFrame("Tree Filter - test");
    f.getContentPane().add(tfw);
    f.pack();
    f.setVisible(true);
*/
  }

  private Feedback feedback = new Feedback(this);

  private HashSet allOptions;

  private JMenuItem nullItem;

  private Option nullOption;

  private Option lastSelectedOption;

  private Logger logger = Logger.getLogger(TreeFilterWidget.class.getName());

  /** represent a "tree" of options. */
  private JMenuBar treeMenu = new JMenuBar();
  private JMenu treeTopOptions = null;
  private JLabel label = null;
  private JTextField currentSelectedText = new JTextField(30);
  private JButton button = new JButton("change");
  private String propertyName;
  private Map valueToOption = new HashMap();
  private Option option = null;
  /**
   * 
   * @param query
   * @param filterDescription
   */
  public TreeFilterWidget(
    FilterGroupWidget filterGroupWidget,
    Query query,
    FilterDescription filterDescription,
  QueryTreeView tree) {
    super(filterGroupWidget, query, filterDescription, tree);

    try {
      nullOption = new Option("No Filter", "true");
    } catch (ConfigurationException e) {
      // shouldn't happen
      e.printStackTrace();
    }
    nullItem = new JMenuItem(nullOption.getInternalName());
    nullItem.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent event) {
        setOption(nullOption);
      }
    });

    // default property name.
    this.propertyName = filterDescription.getInternalName();

    label = new JLabel(filterDescription.getDisplayName());
    currentSelectedText.setEditable(false);
    currentSelectedText.setMaximumSize(new Dimension(400, 27));

    button.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent event) {
        showTree();
      }
    });

    // make the menu appear beneath the row of components 
    // containing the label, textField and button when displayed.
    treeMenu.setMaximumSize(new Dimension(0, 100));
    setOptions(filterDescription.getOptions());

    Box box = Box.createHorizontalBox();
    box.add(treeMenu);
    box.add(label);
    box.add(Box.createHorizontalStrut(5));
    box.add(button);
    box.add(Box.createHorizontalStrut(5));
    box.add(currentSelectedText);

    setLayout(new BorderLayout());
    add(box, BorderLayout.NORTH);

    option = nullOption;
    lastSelectedOption = option;

  }

  /**
   * Adds menu items and submenus to menu based on contents of _options_. A submenu is added 
   * when an option contains
   * sub options. If an option has no sub options it is added as a leaf node. This method calls
   * itself recursively to build up the menu tree.
   * @param menu menu to add options to
   * @param options options to be added, method does nothing if null.
   * @param prefix prepended to option.getDisplayName() to create internal name for menu item 
   * created for each option.
   */
  private void addOptions(JMenu menu, Option[] options, String prefix) {

    for (int i = 0; options != null && i < options.length; i++) {

      final Option option = options[i];
      if (option.getHidden() != null && option.getHidden().equals("true")) continue;
      if (option.getAttribute("hideDisplay") != null && option.getAttribute("hideDisplay").equals("true")) continue;
      
      String displayName = option.getDisplayName();
      String qualifiedName = prefix + " " + displayName;

      if (option.getOptions().length == 0) {

        // add menu item
        JMenuItem item = new JMenuItem(displayName);
        item.setName(qualifiedName);
        menu.add(item);
        item.addActionListener(new ActionListener() {
          public void actionPerformed(ActionEvent event) {
            setOption(option);
          }
        });

        valueToOption.put(option.getValue(), option);

      } else {

        // Add sub menu
        JMenu subMenu = new JMenu(displayName);
        menu.add(subMenu);
        addOptions(subMenu, option.getOptions(), qualifiedName);

      }
    }

  }

  /**
   * Sets the currentlySelectedText and node label based on
   * the option. If option is null these values are cleared.
   * @param option
   */
  private void updateDisplay(Option option) {
    String name = "";
    if (option != null && option != nullOption)
      name = option.getDisplayName();
    currentSelectedText.setText(name);
    setNodeLabel(fieldName, name);
  }

  /**
   * 
   * @return selected option if one is selected, otherwise null.
   */
  public Option getOption() {
    return option;
  }

  /**
   * Default value is filterDescription.getInternalName().
   * @return propertyName included in PropertyChangeEvents.
   */
  public String getPropertyName() {
    return propertyName;
  }

  /**
   * Set the propertyName to some specific value.
   * @param string
   */
  public void setPropertyName(String string) {
    propertyName = string;
  }

  /* (non-Javadoc)
   * @see org.ensembl.mart.explorer.FilterWidget#setOptions(org.ensembl.mart.lib.config.Option[])
   */
  public void setOptions(Option[] options) {

    if ( filter!=null ) query.removeFilter( filter );

    // reset the maps so we can can find things later
    valueToOption.clear();

    treeMenu.removeAll();

    treeTopOptions = new JMenu();
    treeMenu.add(treeTopOptions);

    //  add the nullItem to the top of the list, user selects this to clear
    // choice.
    treeTopOptions.add(nullItem);
    valueToOption.put(nullOption.getValue(), nullOption);

    addOptions(treeTopOptions, options, "");

    allOptions = new HashSet(valueToOption.values());
  }

  public void showTree() {
    treeTopOptions.doClick();
  }

  /**
   * Sets filter and also causes the appropriate item in the tree to be selected and any relevant
   * PushOption to be assigned. If filter is null then "No Filter" is selected
   * and and PushOption are unassigned.
   * @see org.ensembl.mart.explorer.FilterWidget#setFilter(org.ensembl.mart.lib.Filter)
   */
  protected void setFilter(Filter filter) {
    
    this.filter = filter;

    if (filter == null) {

      updateDisplay(null);
      unassignPushOptions();

    } else {

      Option option = (Option) valueToOption.get(filter.getValue());
      updateDisplay(option);
      assignPushOptions(option.getPushActions());

    }
  }

  /**
   * Callback method called in response to user selecting an item.
   * Updates query by removing any relevant query and adding a new one, also
   * updates other widgets with push options.
   * @param option should be one of the options currently available to this filter.
   * @throws IllegalArgumentException if option unavailable in filter.
   */
  public void setOption(Option option) {

    if (!allOptions.contains(option))
      throw new IllegalArgumentException(
        "Option is unailable in filter: " + option);

    if (option == lastSelectedOption)
      return;

    updateDisplay(option);

    this.option = option;

    unassignPushOptions();

    if (filter != null)
      query.removeFilter(filter);
    filter = null;

    if (option != nullOption) {

      assignPushOptions(option.getPushActions());

      String tmp = option.getValueFromContext();
      String value = (tmp != null && !"".equals(tmp)) ? tmp : null;

      if (value != null) {

        // need to reset FilterWidget.fieldName because it is used by 
        // FilterWidget.equivalentFilter(...) during callbacks
        fieldName = option.getFieldFromContext();
        if ( fieldName == null) {
          String s =
            "Can't add filter because of configuration problem in DatsetConfig.";
          String s2 =
            s
              + "option= " + option
              + " filterDescription="
              + filterDescription;

          logger.warning(s2);
          feedback.warning(s);

          // tidy up after disovering problem. Force the selected item
          // to be removed.
          lastSelectedOption = option;
          setOption(nullOption);

        } else {
          //String handler = option.getHandlerFromContext();
          filter =
            new BasicFilter( fieldName, option.getTableConstraintFromContext(), option.getKeyFromContext(), "=", value);
          query.addFilter(filter);
        }
      }
    }

    lastSelectedOption = option;

  }


}
