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

package org.ensembl.mart.editor;

import java.awt.Dimension;
import java.awt.Insets;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.dnd.Autoscroll;
import java.awt.Color;

import javax.swing.JInternalFrame;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.DefaultTableCellRenderer;


import org.ensembl.mart.lib.config.DatasetConfig;

/**
 * Class DatasetConfigAttributesTable extending JTable.
 *
 * <p>This class is written for the attributes table to implement auscroll
 * </p>
 *
 * @author <a href="mailto:katerina@ebi.ac.uk">Katerina Tzouvara</a>
 * //@see org.ensembl.mart.config.DatasetConfig
 */

public class DatasetConfigAttributesTable extends JTable implements Autoscroll{

     public static final Insets defaultScrollInsets = new Insets(8, 8, 8, 8);
     protected Insets scrollInsets = defaultScrollInsets;
     protected DatasetConfigTreeModel treemodel = null;
     protected JInternalFrame frame;
     protected DatasetConfig dsConfig = null;

     public DatasetConfigAttributesTable(DatasetConfig dsConfig, JInternalFrame frame) {
        //super(new DatasetConfigAttributeTableModel());
        super(null);
        this.dsConfig = dsConfig;
        this.frame = frame;

     }

	public TableCellRenderer getCellRenderer(int row, int column) {
		
		DatasetConfigAttributeTableModel model = (DatasetConfigAttributeTableModel) this.getModel();
		int[] requiredFields = model.getRequiredFields();
				
				
		for (int i = 0; i < requiredFields.length; i++){
			if (row == requiredFields[i]){
				DefaultTableCellRenderer newCellR = new DefaultTableCellRenderer();
				newCellR.setForeground(Color.red);	
				return newCellR;
			}
		}
		return getDefaultRenderer(getColumnClass(column));	
	}


     // Autoscrolling support
    public void setScrollInsets(Insets insets) {
        this.scrollInsets = insets;
    }

    public Insets getScrollInsets() {
        return scrollInsets;
    }

    // Implementation of Autoscroll interface
    public Insets getAutoscrollInsets() {
        Rectangle r = getVisibleRect();
        Dimension size = getSize();
        Insets i = new Insets(r.y + scrollInsets.top, r.x + scrollInsets.left,
                size.height - r.y - r.height + scrollInsets.bottom,
                size.width - r.x - r.width + scrollInsets.right);
        return i;
    }

    public void autoscroll(Point location) {
        JScrollPane scroller =
                (JScrollPane) SwingUtilities.getAncestorOfClass(JScrollPane.class, this);
        if (scroller != null) {
            JScrollBar hBar = scroller.getHorizontalScrollBar();
            JScrollBar vBar = scroller.getVerticalScrollBar();
            Rectangle r = getVisibleRect();
            if (location.x <= r.x + scrollInsets.left) {
                // Need to scroll left
                hBar.setValue(hBar.getValue() - hBar.getUnitIncrement(-1));
            }
            if (location.y <= r.y + scrollInsets.top) {
                // Need to scroll up
                vBar.setValue(vBar.getValue() - vBar.getUnitIncrement(-1));
            }
            if (location.x >= r.x + r.width - scrollInsets.right) {
                // Need to scroll right
                hBar.setValue(hBar.getValue() + hBar.getUnitIncrement(1));
            }
            if (location.y >= r.y + r.height - scrollInsets.bottom) {
                // Need to scroll down
                vBar.setValue(vBar.getValue() + vBar.getUnitIncrement(1));
            }
        }

    }
}
