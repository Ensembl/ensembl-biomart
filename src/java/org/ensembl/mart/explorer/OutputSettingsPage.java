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
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;

import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.JRadioButton;

import org.ensembl.mart.lib.FormatSpec;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.SequenceDescription;

/**
 * Input panel where user can set the result's format and destination.
 */
public class OutputSettingsPage extends InputPage implements ActionListener {

  private JRadioButton tabulated;
  private JRadioButton fasta;
  private JRadioButton tab;
  private JRadioButton comma;

  private PropertyChangeSupport changeSupport = new PropertyChangeSupport(this);

  /**
   *
   */
  public OutputSettingsPage(Query query) {
    super(query, "Output");

    ButtonGroup group = new ButtonGroup();
    tabulated = new JRadioButton("Tabulated Format");
    tabulated.setSelected(true);
    tabulated.addActionListener(this);
    group.add(tabulated);

    fasta = new JRadioButton("FASTA Format");
    fasta.setEnabled(query.getSequenceDescription()!=null);
    fasta.addActionListener(this);
    group.add(fasta);

    group = new ButtonGroup();

    tab = new JRadioButton("tabs");
    tab.addActionListener(this);
    tab.setSelected(true);
    group.add(tab);

    comma = new JRadioButton("comma");
    comma.addActionListener(this);
    group.add(comma);

    Dimension d = new Dimension(500, 35);
    Box tabulatedOptions = Box.createHorizontalBox();
    tabulatedOptions.setPreferredSize(d);
    tabulatedOptions.setMaximumSize(d);
    tabulatedOptions.add(Box.createHorizontalStrut(50));
    tabulatedOptions.add(tabulated);
    tabulatedOptions.add(tab);
    tabulatedOptions.add(comma);
    tabulatedOptions.add(Box.createHorizontalGlue());

    Box fastaOptions = Box.createHorizontalBox();
    fastaOptions.setPreferredSize(d);
    fastaOptions.setMaximumSize(d);
    fastaOptions.add(Box.createHorizontalStrut(50));
    fastaOptions.add(fasta);
    fastaOptions.add(Box.createHorizontalGlue());

    Box v = Box.createVerticalBox();
    v.add(tabulatedOptions);
    v.add(fastaOptions);
    v.add(Box.createVerticalGlue());
    add(v);

    dependencies();

  }

  /* (non-Javadoc)
   * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
   */
  public void actionPerformed(ActionEvent e) {
    dependencies();
    String description = "fasta";
    if (tabulated.isSelected()) {
      if (comma.isSelected())
        description = "comma separated";
      else
        description = "tab separated";
    }
    setNodeLabel(getName(), description);
    changeSupport.firePropertyChange(
      "output",
      "olddummyvalue",
      "newdummyvalue");
  }

  public void dependencies() {
    tab.setEnabled(tabulated.isSelected());
    comma.setEnabled(tabulated.isSelected());
  }

  /* (non-Javadoc)
   * @see javax.swing.JComponent#addPropertyChangeListener(java.beans.PropertyChangeListener)
   */
  public synchronized void addPropertyChangeListener(PropertyChangeListener listener) {
    changeSupport.addPropertyChangeListener(listener);
  }

  /* (non-Javadoc)
   * @see javax.swing.JComponent#addPropertyChangeListener(java.lang.String, java.beans.PropertyChangeListener)
   */
  public synchronized void addPropertyChangeListener(
    String propertyName,
    PropertyChangeListener listener) {
    changeSupport.addPropertyChangeListener(propertyName, listener);
  }

  /* (non-Javadoc)
   * @see javax.swing.JComponent#getPropertyChangeListeners()
   */
  public synchronized PropertyChangeListener[] getPropertyChangeListeners() {
    return changeSupport.getPropertyChangeListeners();
  }

  /* (non-Javadoc)
   * @see javax.swing.JComponent#getPropertyChangeListeners(java.lang.String)
   */
  public synchronized PropertyChangeListener[] getPropertyChangeListeners(String propertyName) {
    return changeSupport.getPropertyChangeListeners(propertyName);
  }

  /* (non-Javadoc)
   * @see javax.swing.JComponent#removePropertyChangeListener(java.beans.PropertyChangeListener)
   */
  public synchronized void removePropertyChangeListener(PropertyChangeListener listener) {
    changeSupport.removePropertyChangeListener(listener);
  }

  /* (non-Javadoc)
   * @see javax.swing.JComponent#removePropertyChangeListener(java.lang.String, java.beans.PropertyChangeListener)
   */
  public synchronized void removePropertyChangeListener(
    String propertyName,
    PropertyChangeListener listener) {
    changeSupport.removePropertyChangeListener(propertyName, listener);
  }

  /**
   * @return
   */
  public FormatSpec getFormat() {
    if (fasta.isSelected())
      return FormatSpec.FASTAFORMAT;
    else if (tab.isSelected())
      return FormatSpec.TABSEPARATEDFORMAT;
    else
      return new FormatSpec(FormatSpec.TABULATED, ",");
  }

  public void sequenceDescriptionChanged(
    Query sourceQuery,
    SequenceDescription oldSequenceDescription,
    SequenceDescription newSequenceDescription) {

    fasta.setEnabled(newSequenceDescription != null);
    if (!fasta.isEnabled()) tabulated.doClick();
  }

}
