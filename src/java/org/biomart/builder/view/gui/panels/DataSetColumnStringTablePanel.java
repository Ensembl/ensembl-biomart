/*
 Copyright (C) 2006 EBI
 
 This library is free software; you can redistribute it and/or
 modify it under the terms of the GNU Lesser General Public
 License as published by the Free Software Foundation; either
 version 2.1 of the License, or (at your option) any later version.
 
 This library is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the itmplied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 Lesser General Public License for more details.
 
 You should have received a copy of the GNU Lesser General Public
 License along with this library; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.biomart.builder.view.gui.panels;

import java.awt.Component;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JTable;
import javax.swing.ListCellRenderer;
import javax.swing.table.TableCellRenderer;

import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.view.gui.panels.TwoColumnTablePanel.ColumnStringTablePanel;

/**
 * A two-column table model which displays dataset columns as a drop-down on the
 * left, and strings on the right.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.9 $, $Date: 2007-12-17 10:09:38 $, modified by 
 * 			$Author: rh4 $
 * @since 0.6
 */
public abstract class DataSetColumnStringTablePanel extends
		ColumnStringTablePanel {
	private final DataSetColumn dontIncludeThis;

	/**
	 * Constructs a new two-column table showing the given values and allowing
	 * the user to choose from the given collection of {@link DataSetColumn}
	 * objects.
	 * 
	 * @param values
	 *            the initial values to show in the table, or <tt>null</tt> if
	 *            none.
	 * @param columns
	 *            the range of columns to allow as options.
	 * @param dontIncludeThis
	 *            the column to exclude from the range shown, e.g. to prevent
	 *            recursion. Can be <tt>null</tt> in which case no columns are
	 *            excluded.
	 */
	public DataSetColumnStringTablePanel(final Map values,
			final Collection columns, final DataSetColumn dontIncludeThis) {
		super(values, columns);
		// Same as ColumnString except slightly modified to show
		// the modifiedName instead of the plain name.
		this.getFirstColumnEditor().setRenderer(new ListCellRenderer() {
			public Component getListCellRendererComponent(final JList list,
					final Object value, final int index,
					final boolean isSelected, final boolean cellHasFocus) {
				final DataSetColumn col = (DataSetColumn) value;
				final JLabel label = new JLabel();
				if (col != null)
					label.setText(col.getModifiedName());
				label.setOpaque(true);
				label.setFont(list.getFont());
				if (isSelected) {
					label.setBackground(list.getSelectionBackground());
					label.setForeground(list.getSelectionForeground());
				} else {
					label.setBackground(list.getBackground());
					label.setForeground(list.getForeground());
				}
				return label;
			}
		});
		this.dontIncludeThis = dontIncludeThis;
	}

	public Collection getSortedColumns(final Collection columns) {
		final Map sortedCols = new TreeMap();
		// Sorts on the modified names, and excludes the column
		// specified in the constructor if required. Also
		// excludes optimiser columns.
		for (final Iterator i = columns.iterator(); i.hasNext();) {
			final DataSetColumn col = (DataSetColumn) i.next();
			if (this.dontIncludeThis == null
					|| !col.equals(this.dontIncludeThis))
				sortedCols.put(col.getModifiedName(), col);
		}
		return sortedCols.values();
	}

	public Class getFirstColumnType() {
		return DataSetColumn.class;
	}

	public TableCellRenderer getFirstColumnRenderer() {
		return new TableCellRenderer() {
			public Component getTableCellRendererComponent(final JTable table,
					final Object value, final boolean isSelected,
					final boolean hasFocus, final int row, final int column) {
				final DataSetColumn col = (DataSetColumn) value;
				final JLabel label = new JLabel();
				// As for ColumnString but uses the modified name.
				if (col != null)
					label.setText(col.getModifiedName());
				label.setOpaque(true);
				label.setFont(table.getFont());
				if (isSelected) {
					label.setBackground(table.getSelectionBackground());
					label.setForeground(table.getSelectionForeground());
				} else {
					label.setBackground(table.getBackground());
					label.setForeground(table.getForeground());
				}
				return label;
			}
		};
	}

	public Map getValues() {
		final Map values = new HashMap();
		// As for ColumnString but uses the modified name as the key
		// instead of the column object itself.
		for (final Iterator i = super.getValues().entrySet().iterator(); i
				.hasNext();) {
			final Map.Entry entry = (Map.Entry) i.next();
			final DataSetColumn dsCol = (DataSetColumn) entry.getKey();
			values.put(dsCol.getName(), entry.getValue());
		}
		return values;
	}
}