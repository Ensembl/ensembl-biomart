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

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.border.TitledBorder;

/**
 * Lays out components in a grid.
 */
public class GridPanel extends Box {

  private Dimension componentSize;
  private JComponent[] components;
  private int nColumns;
  private int lastWidth;

  public GridPanel(
    JComponent[] components,
    int nColumns,
    int colWidth,
    int rowHeight,
    String title) {

    super(BoxLayout.Y_AXIS);

    this.components = components;
    this.nColumns = nColumns;
    componentSize = new Dimension(colWidth, rowHeight);

    setBorder(new TitledBorder(title));

    addComponents(components);
  }

  private void addComponents(JComponent[] components) {

    Box row = null;

    for (int i = 0; i < components.length; i++) {

      if (row == null) {
        row = Box.createHorizontalBox();
        add(row);
      }

      JComponent c = components[i];
      setComponentSize(c);
      row.add(c);

      if ((i + 1) % nColumns == 0)
        row = null;
    }

    int nPadingCells = components.length % nColumns;
    for (int i = 0; i < nPadingCells; ++i) {
      JComponent c = new JLabel();
      setComponentSize(c);
      row.add(c);
    }

  }

  private void setComponentSize(JComponent c) {

    Dimension d = componentSize;

    int ph = c.getPreferredSize().height;
    if (ph > d.height) {
      d = new Dimension(componentSize);

      // note: box layout's calculation of the preferred height is incorrect in some cases.
      // We add 35 (a hack) to ensure the component is tall enough
      // to show all elements. It is possible that some components will 
      // still not have enough room to be displayed.
      d.height = ph+35;
    }

    c.setPreferredSize(d);
    c.setMinimumSize(d);
    c.setMaximumSize(d);
  }

}
