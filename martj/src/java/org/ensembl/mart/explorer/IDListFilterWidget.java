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

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.AbstractButton;
import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JRadioButton;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.border.LineBorder;
import javax.swing.text.JTextComponent;

import org.ensembl.mart.guiutils.QuickFrame;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.IDListFilter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterGroup;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.util.LoggingUtil;

/**
 * An ID list filter offers the user with a mechanism for filtering by IDs. The user can specify
 * a list oif IDs using verious sources and specify the type of the IDs.
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 *
 */
public class IDListFilterWidget
  extends FilterWidget
  implements ActionListener {

  /**
   * "Listens" to changes in component and performs button.doClick() in response. 
   * coClick() is perfomred if "enter" is pressed in the component or the text changed between 
   * the component gaining and losing focus.
   * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
   */
  private class ModificationListener
    extends KeyAdapter
    implements FocusListener {

    private AbstractButton button;

    private JTextComponent component;

    private int textHashCode = -1;

    /**
     * Adds self to component as both key and focus listener.
     * @param component
     * @param button
     */
    public ModificationListener(JTextComponent component, AbstractButton button) {
      this.component = component;
      this.button = button;

      component.addKeyListener(this);
      component.addFocusListener(this);
    }

    /**
     * doClick() if "enter" pressed.
     */
    public void keyReleased(KeyEvent e) {
      if (e.getKeyCode() == KeyEvent.VK_ENTER){
        textHashCode = component.getText().hashCode();
        button.doClick();
      }
        
    }

    public void focusGained(FocusEvent e) {
      textHashCode = component.getText().hashCode();
    }

    /**
     * doClick() if text changed between gaining and losing focus.
     */
    public void focusLost(FocusEvent e) {
      int tmp = component.getText().hashCode();
      if (tmp != textHashCode) {
        textHashCode = tmp;
        button.doClick();
      }
    }
  }

  protected int lastIDStringHashCode;

  private JComboBox list = new JComboBox();

  private JTextArea idString = new JTextArea(10, 10);
  private JTextField file = new JTextField(20);
  private JTextField url = new JTextField(20);
  private JButton chooseFileButton = new JButton("Choose");
  private JFileChooser fileChooser = new JFileChooser();

  private JRadioButton idStringRadioButton =
    new JRadioButton("IDs (type or paste)");
  private JRadioButton fileRadioButton =
    new JRadioButton("File containing IDs");
  private JRadioButton urlRadioButton = new JRadioButton("URL containing IDs");
  private JRadioButton noneButton = new JRadioButton("None");

  private Feedback feedback = new Feedback(this);

  /**
   * @param filterGroupWidget
   * @param query
   * @param filterDescription
   * @param tree
   */
  public IDListFilterWidget(
    FilterGroupWidget filterGroupWidget,
    Query query,
    FilterDescription filterDescription,
    QueryTreeView tree) {
    super(filterGroupWidget, query, filterDescription, tree);

    file.setEditable(false);

    ButtonGroup bg = new ButtonGroup();
    bg.add(idStringRadioButton);
    bg.add(fileRadioButton);
    bg.add(urlRadioButton);
    bg.add(noneButton);
    noneButton.setSelected(true);

    idStringRadioButton.addActionListener(this);
    fileRadioButton.addActionListener(this);
    urlRadioButton.addActionListener(this);
    noneButton.addActionListener(this);

    new ModificationListener(idString, idStringRadioButton);
    new ModificationListener(url, urlRadioButton);

    Box b = Box.createVerticalBox();
    b.setBorder(new LineBorder(Color.BLACK));

    b.add(
      createRow(createLabel(), (JComponent) Box.createHorizontalGlue(), null));
    b.add(list);

    b.add(Box.createVerticalStrut(Constants.GAP_BETWEEN_COMPONENTS_IN_WIDGET));
    b.add(createRow(idStringRadioButton, idString, null));

    b.add(Box.createVerticalStrut(Constants.GAP_BETWEEN_COMPONENTS_IN_WIDGET));
    b.add(createRow(fileRadioButton, chooseFileButton, file));

    b.add(Box.createVerticalStrut(Constants.GAP_BETWEEN_COMPONENTS_IN_WIDGET));
    b.add(createRow(urlRadioButton, url, null));

    b.add(Box.createVerticalStrut(Constants.GAP_BETWEEN_COMPONENTS_IN_WIDGET));
    b.add(createRow(noneButton, (JComponent) Box.createHorizontalGlue(), null));

    setOptions(filterDescription.getOptions());

    add(b);

    final IDListFilterWidget parent = this;
    chooseFileButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent event) {
        if (fileChooser.showOpenDialog(parent)
          == JFileChooser.APPROVE_OPTION) {
          file.setText(fileChooser.getSelectedFile().getAbsolutePath());
          fileRadioButton.doClick();
        }
      }

    });
  }

  private JComponent createRow(JComponent a, JComponent b, JComponent c) {
    Box p = Box.createHorizontalBox();
    if (a != null)
      p.add(a);
    if (b != null)
      p.add(b);
    if (c != null)
      p.add(c);

    return p;
  }

  public void setOptions(Option[] options) {

    if (list == null)
      return;

    list.removeActionListener(this);
    list.removeAllItems();

    // add items
    for (int i = 0; i < options.length; i++) {
      Option o = options[i];
      if (o.getHidden() != null && o.getHidden().equals("true")) continue;
      if (o.getAttribute("hideDisplay") != null && o.getAttribute("hideDisplay").equals("true")) continue;
      
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
   * removed to/from the query.
   */
  protected void setFilter(Filter filter) {

    if (filter == null) {

      noneButton.setSelected(true);

    } else {

      IDListFilter f = (IDListFilter) filter;

      String[] ids = null;
      URL u = null;
      File fl = null;

      if ((ids = f.getIdentifiers()) != null && ids.length != 0) {

        idStringRadioButton.setSelected(true);

        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < ids.length; i++)
          buf.append(ids[i]).append('\n');

        idString.setText(buf.toString());

      } else if ((u = f.getUrl()) != null) {

        urlRadioButton.setSelected(true);
        url.setText(u.toExternalForm());

      } else if ((fl = f.getFile()) != null) {

        fileRadioButton.setSelected(true);
        file.setText(fl.getName());
        fileChooser.setSelectedFile(fl);

      }

    }

    this.filter = filter;

  }

  /**
   * Updates query in response to a user action. Removes old filter if necessary, adds new one if necessary
   * , or replaces old with new if necessary.
   */
  public void actionPerformed(ActionEvent e) {

    Filter newFilter = createFilter();

    query.removeQueryChangeListener(this);

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

  /**
   * Creates a filter based on the current state of the widget. If "none" is selected or
   * the field associated with the radio button is empty/invalid then no filter is returned.
   * @return filter if current state relates to one, otherwise null.
   */
  private Filter createFilter() {

    Option o = ((OptionToStringWrapper) list.getSelectedItem()).option;
    String f = o.getFieldFromContext();
    String tc = o.getTableConstraintFromContext();
    String k = o.getKeyFromContext();

    if (idStringRadioButton.isSelected() && idString.getText().length() != 0)
      return new IDListFilter(
        f,
        tc,
        k,
        idString.getText().split("(\\s+|\\s*,\\s*)"));

    else if (urlRadioButton.isSelected() && url.getText().length() != 0)
      try {
        return new IDListFilter(f, tc, k, new URL(url.getText()));
      } catch (MalformedURLException e) {
        feedback.warning("There is a problem with the URL: " + url.getText());
        noneButton.doClick();
      } else if (fileRadioButton.isSelected() && file.getText().length() != 0)
      return new IDListFilter(f, tc, k, new File(file.getText()));

    return null;
  }

  /**
   * Unit test for this class.
   * @param args
   * @throws Exception
   */

  public static void main(String[] args) throws Exception {

    // enable logging messages
    LoggingUtil.setAllRootHandlerLevelsToFinest();
    Logger.getLogger(Query.class.getName()).setLevel(Level.FINEST);
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
        "id_list test",
        "someTableConstraint",
        "someKey",
        "someDescription",
        "",
        "","","","","","","");

    Option o = new Option("fred_id", "true");
    o.setParent(fd);
    o.setDisplayName("Fred");
    o.setField("fred_field");
    fd.addOption(o);
    Option o2 = new Option("barney_id", "true");
    o2.setParent(fd);
    o2.setDisplayName("Barney");
    o2.setField("barney");
    fd.addOption(o2);

    new QuickFrame(
      IDListFilterWidget.class.getName(),
      new IDListFilterWidget(null, q, fd, null));
  */
  }

  protected boolean equivalentFilter(Object otherFilter) {

    if (super.equivalentFilter(otherFilter))
      return true;

    if (otherFilter == null || !(otherFilter instanceof Filter))
      return false;

    if (indexOfListItemMatchingFilter((Filter) otherFilter) > -1)
      return true;

    return false;

  }

  /**
   * 
   * @param otherFilter
   * @return -1 if filter not relat
   */
  private int indexOfListItemMatchingFilter(Filter filter) {

    int index = -1;
    final int n = list.getItemCount();
    for (int i = 0; index == -1 && i < n; i++) {

      OptionToStringWrapper op = (OptionToStringWrapper) list.getItemAt(i);
      Option o = op.option;
      String f = filter.getField();
      String tc = filter.getTableConstraint();
      String k = filter.getKey();

      if (f != null
        && tc != null
        && k != null
        && !"".equals(f)
        && !"".equals(tc)
        && !"".equals(k)
        && f.equals(o.getFieldFromContext())
        && tc.equals(o.getTableConstraintFromContext())
        && k.equals(o.getKeyFromContext()))
        index = i;
    }

    return index;
  }

}
