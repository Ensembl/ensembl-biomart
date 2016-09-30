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

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JFrame;
import javax.swing.JTextField;

import org.ensembl.mart.lib.BasicFilter;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterGroup;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.util.LoggingUtil;

/**
 * Widget with a label and text entry area which adds/removes
 * a corresponding <code>Filter</code> object from the query. Entering text folled by
 * <code>return</code> causes a filter to be added or changed. Clearing the text
 * and pressing <code>return</code> removes the filter.
 */
public class TextFilterWidget extends FilterWidget implements ActionListener {

  private final static Logger logger =
    Logger.getLogger(TextFilterWidget.class.getName());

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

    TextFilterWidget tfw = new TextFilterWidget(fgw, q, fd, null);

    JFrame f = new JFrame("Text Filter - test");
    f.getContentPane().add(tfw);
    f.pack();
    f.setVisible(true);
*/
  }

  private JTextField textField;
  /**
     * BooleanFilter that has contains a tree node.
     */
  private class InputPageAwareBasicFilter
    extends BasicFilter
    implements InputPageAware {
    private InputPage inputPage;

    public InputPageAwareBasicFilter(
      String field,
      String qualifier,
      String value,
      InputPage inputPage) {

      super(field, qualifier, value);
      this.inputPage = inputPage;

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

    public InputPage getInputPage() {
      return inputPage;
    }
  }

  /**
   * @param query
   * @param filterDescription
   */
  public TextFilterWidget(
    FilterGroupWidget filterGroupWidget,
    Query query,
    FilterDescription filterDescription,
    QueryTreeView tree) {

    super(filterGroupWidget, query, filterDescription, tree);
    String type = filterDescription.getType();
    if (!"text".equals(type))
      throw new IllegalArgumentException(
        "FilterDescription.type is not text:" + type);

    setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
    textField = new JTextField(5);
    textField.addActionListener(this); // listen for user entered changes
    add(createLabel());
    add(textField);
    add(Box.createHorizontalGlue());
  }

  /**
   * Update query when user presses enter in this text widget. If this filter currently
   * has a filter which is in the query that is removed. If the text field is not empty then
   * a new filter is added to query using this value.
   */
  public void actionPerformed(ActionEvent e) {

    String value = textField.getText();

    // Do nothing if value hasn't changed
    if ( filter != null && filter.getValue().equals( value ) ) 
      return;

    if (filter != null ) 
      query.removeFilter(filter);
    filter = null;

    // remove filter
    if (value != null && !"".equals(value)) {

      filter =
        new BasicFilter(
          filterDescription.getField(),
          filterDescription.getTableConstraint(),
		  filterDescription.getKey(),          
          filterDescription.getLegalQualifiers(),
          value);
      query.addFilter(filter);
    }
  }

  /**
   * Callback method used to update this widget if a relevant filter is added.
   */
  public void setFilter(Filter filter) {

    this.filter = filter;
    if (filter == null)
      textField.setText("");
    else
      textField.setText(filter.getValue());
  }

  /**
   * Does nothing.
   * @see org.ensembl.mart.explorer.FilterWidget#setOptions(org.ensembl.mart.lib.config.Option[])
   */
  public void setOptions(Option[] options) {
  }

}
