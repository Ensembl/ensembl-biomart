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

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;

import org.biomart.builder.model.Column;

/**
 * This panel represents a two-column table which can contain the entries of a
 * map. The keys go in the left column and the values in the right. It includes
 * methods to update the contents of the table and to obtain the current
 * contents, including stripping out rows with blank keys.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.3 $, $Date: 2007-10-03 10:41:02 $, modified by 
 * 			$Author: rh4 $
 * @since 0.6
 */
public abstract class TwoColumnTablePanel extends JPanel {
	private static final long serialVersionUID = 1;

	private TwoColumnTableModel tableModel;

	private JButton insert;

	private JButton remove;

	/**
	 * Creates a two-columned table that displays an initial set of values. It
	 * optionally restricts input on either column to two further sets of
	 * values.
	 * 
	 * @param values
	 *            initial values to display in the two columns of the table.
	 * @param firstColValues
	 *            the values to restrict entry in the first (left) column with.
	 *            If <tt>null</tt> then no restriction is made.
	 * @param secondColValues
	 *            the values to restrict entry in the second (right) column
	 *            with. If <tt>null</tt> then no restriction is made.
	 */
	public TwoColumnTablePanel(final Map values,
			final Collection firstColValues, final Collection secondColValues) {
		// Create the basic panel.
		super();

		// Create the layout to display the rest of the panel.
		this.setLayout(new GridBagLayout());

		// Create constraints for labels that are not in the last row.
		final GridBagConstraints labelConstraints = new GridBagConstraints();
		labelConstraints.gridwidth = GridBagConstraints.RELATIVE;
		labelConstraints.fill = GridBagConstraints.HORIZONTAL;
		labelConstraints.anchor = GridBagConstraints.LINE_END;
		labelConstraints.insets = new Insets(0, 2, 0, 0);
		// Create constraints for fields that are not in the last row.
		final GridBagConstraints fieldConstraints = new GridBagConstraints();
		fieldConstraints.gridwidth = GridBagConstraints.REMAINDER;
		fieldConstraints.fill = GridBagConstraints.NONE;
		fieldConstraints.anchor = GridBagConstraints.LINE_START;
		fieldConstraints.insets = new Insets(0, 1, 0, 2);
		// Create constraints for labels that are in the last row.
		final GridBagConstraints labelLastRowConstraints = (GridBagConstraints) labelConstraints
				.clone();
		labelLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;
		// Create constraints for fields that are in the last row.
		final GridBagConstraints fieldLastRowConstraints = (GridBagConstraints) fieldConstraints
				.clone();
		fieldLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;

		// Set up the data model.
		this.tableModel = new TwoColumnTableModel(values, this
				.getFirstColumnHeader(), this.getSecondColumnHeader(), this
				.getFirstColumnType(), this.getSecondColumnType());
		final JTable table = new JTable(this.tableModel);
		table.setGridColor(Color.LIGHT_GRAY); // Mac OSX.
		// First column.
		final JComboBox firstEd = this
				.getFirstColumnEditor(firstColValues == null ? Collections.EMPTY_SET
						: firstColValues);
		if (firstEd != null) {
			table.getColumnModel().getColumn(0).setCellEditor(
					new DefaultCellEditor(firstEd));
			table.getColumnModel().getColumn(0).setPreferredWidth(
					firstEd.getPreferredSize().width);
		} else
			table.getColumnModel().getColumn(0).setPreferredWidth(
					Math.max(250, table.getTableHeader().getDefaultRenderer()
							.getTableCellRendererComponent(
									null,
									table.getColumnModel().getColumn(0)
											.getHeaderValue(), false, false, 0,
									0).getPreferredSize().width));
		final TableCellRenderer firstRend = this.getFirstColumnRenderer();
		if (firstRend != null)
			table.getColumnModel().getColumn(0).setCellRenderer(firstRend);
		// Second column.
		final JComboBox secondEd = this
				.getSecondColumnEditor(secondColValues == null ? Collections.EMPTY_SET
						: secondColValues);
		if (secondEd != null) {
			table.getColumnModel().getColumn(1).setCellEditor(
					new DefaultCellEditor(secondEd));
			table.getColumnModel().getColumn(1).setPreferredWidth(
					secondEd.getPreferredSize().width);
		} else
			table.getColumnModel().getColumn(1).setPreferredWidth(
					Math.max(250, table.getTableHeader().getDefaultRenderer()
							.getTableCellRendererComponent(
									null,
									table.getColumnModel().getColumn(1)
											.getHeaderValue(), false, false, 0,
									0).getPreferredSize().width));
		final TableCellRenderer secondRend = this.getSecondColumnRenderer();
		if (secondRend != null)
			table.getColumnModel().getColumn(1).setCellRenderer(secondRend);
		// Buttons.
		table
				.setPreferredScrollableViewportSize(new Dimension(table
						.getColumnModel().getColumn(0).getPreferredWidth()
						+ table.getColumnModel().getColumn(1)
								.getPreferredWidth(), 150));
		if (this.getInsertButtonText() != null) {
			this.insert = new JButton(this.getInsertButtonText());
			this.insert.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					TwoColumnTablePanel.this.tableModel.insertRow(
							TwoColumnTablePanel.this.tableModel.getRowCount(),
							new Object[] {
									TwoColumnTablePanel.this
											.getNewRowFirstColumn(),
									TwoColumnTablePanel.this
											.getNewRowSecondColumn() });
				}
			});
		}
		if (this.getRemoveButtonText() != null) {
			this.remove = new JButton(this.getRemoveButtonText());
			this.remove.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					final int rows[] = table.getSelectedRows();
					// Reverse order, so we don't end up with changing
					// indices along the way.
					for (int i = rows.length - 1; i >= 0; i--)
						TwoColumnTablePanel.this.tableModel.removeRow(rows[i]);
				}
			});
		}

		// Display the table and buttons as two parts of a single panel.
		final JPanel tableField = new JPanel();
		tableField.add(new JScrollPane(table));
		if (this.insert != null || this.remove != null) {
			final JPanel field = new JPanel();
			if (this.insert != null)
				field.add(this.insert);
			if (this.remove != null)
				field.add(this.remove);
			this.add(tableField, fieldConstraints);
			this.add(field, fieldLastRowConstraints);
		}
		// If cannot insert/remove then also cannot change.
		else {
			this.add(tableField, fieldLastRowConstraints);
			table.setEnabled(false);
		}
	}

	/**
	 * Retrieve the text to display on the 'insert row' button. Return
	 * <tt>null</tt> if this button is to be disabled.
	 * 
	 * @return the text to display on the 'insert row' button.
	 */
	public String getInsertButtonText() {
		return null;
	}

	/**
	 * Retrieve the text to display on the 'remove row' button. Return
	 * <tt>null</tt> if this button is to be disabled.
	 * 
	 * @return the text to display on the 'remove row' button.
	 */
	public String getRemoveButtonText() {
		return null;
	}

	/**
	 * Retrieve the header text for the first column.
	 * 
	 * @return the header text for the first column.
	 */
	public abstract String getFirstColumnHeader();

	/**
	 * Retrieve the header text for the second column.
	 * 
	 * @return the header text for the second column.
	 */
	public abstract String getSecondColumnHeader();

	/**
	 * Retrieve the data type for the first column.
	 * 
	 * @return the data type for the first column.
	 */
	public abstract Class getFirstColumnType();

	/**
	 * Retrieve the data type for the second column.
	 * 
	 * @return the data type for the second column.
	 */
	public abstract Class getSecondColumnType();

	/**
	 * Retrieve the value to populate the first column for every new row
	 * created.
	 * 
	 * @return the value to use.
	 */
	public abstract Object getNewRowFirstColumn();

	/**
	 * Retrieve the value to populate the second column for every new row
	 * created.
	 * 
	 * @return the value to use.
	 */
	public abstract Object getNewRowSecondColumn();

	/**
	 * Retrieve the editor to use to edit values in the first column.
	 * 
	 * @param values
	 *            the values that can be chosen from. This will never be
	 *            <tt>null</tt> but may be empty.
	 * @return the editor to use for this column. Return <tt>null</tt> if the
	 *         default editor should be used.
	 */
	public abstract JComboBox getFirstColumnEditor(final Collection values);

	/**
	 * Retrieve the editor to use to edit values in the second column.
	 * 
	 * @param values
	 *            the values that can be chosen from. This will never be
	 *            <tt>null</tt> but may be empty.
	 * @return the editor to use for this column. Return <tt>null</tt> if the
	 *         default editor should be used.
	 */
	public abstract JComboBox getSecondColumnEditor(final Collection values);

	/**
	 * Retrieve the renderer to use for the first column.
	 * 
	 * @return the renderer to use. Return <tt>null</tt> if the default
	 *         renderer should be used.
	 */
	public abstract TableCellRenderer getFirstColumnRenderer();

	/**
	 * Retrieve the renderer to use for the second column.
	 * 
	 * @return the renderer to use. Return <tt>null</tt> if the default
	 *         renderer should be used.
	 */
	public abstract TableCellRenderer getSecondColumnRenderer();

	/**
	 * Replace the contents of the displayed table with the given set of values.
	 * Keys of the map will appear in the left (first) column and values in the
	 * second (right) column. All existing contents will be removed before these
	 * new contents are inserted.
	 * 
	 * @param values
	 *            the map of values to display.
	 */
	public void setValues(final Map values) {
		this.tableModel.setValues(values);
	}

	/**
	 * Retrieve the currently displayed set of values, with any that have empty
	 * left columns removed first.
	 * 
	 * @return the set of current values. Keys of the map are the left (first)
	 *         column and values of the map are the right (second) column.
	 */
	public Map getValues() {
		return this.tableModel.getValues();
	}

	/**
	 * This internal class represents the data model upon which the two column
	 * table is based.
	 */
	private static class TwoColumnTableModel extends DefaultTableModel {
		private final Class[] colClasses;

		private static final long serialVersionUID = 1;

		/**
		 * Construct a model of data from the given information.
		 * 
		 * @param values
		 *            the initial values to display. If <tt>null</tt>, no
		 *            initial values are displayed. Keys of the map go in the
		 *            left column (first), values in the right (second).
		 * @param firstColHeader
		 *            the header to give the first column.
		 * @param secondColHeader
		 *            the header to give the second column.
		 * @param firstColType
		 *            the type of data displayed in the first column.
		 * @param secondColType
		 *            the type of data displayed in the second column.
		 */
		public TwoColumnTableModel(final Map values,
				final String firstColHeader, final String secondColHeader,
				final Class firstColType, final Class secondColType) {
			super(new Object[] { firstColHeader, secondColHeader }, 0);
			this.colClasses = new Class[] { firstColType, secondColType };
			this.setValues(values);
		}

		/**
		 * Overwrite (clear) the existing contents of the table and replace with
		 * values from the given map.
		 * 
		 * @param values
		 *            the new values to use. If <tt>null</tt> then all
		 *            existing data is dropped and no new data inserted. Keys of
		 *            the map go in the left column, values in the right.
		 */
		public void setValues(final Map values) {
			while (this.getRowCount() > 0)
				this.removeRow(0);
			if (values != null)
				for (final Iterator i = values.entrySet().iterator(); i
						.hasNext();) {
					final Map.Entry entry = (Map.Entry) i.next();
					this.insertRow(this.getRowCount(), new Object[] {
							entry.getKey(), entry.getValue() });
				}
		}

		/**
		 * Obtain the currently displayed set of values. Any which have empty or
		 * all-whitespace strings in the left (first) column are not included in
		 * the results.
		 * 
		 * @return a map containing the results. The keys of the map are the
		 *         entries in the first (left) column and the values are those
		 *         in the second (right) column.
		 */
		public Map getValues() {
			final HashMap aliases = new HashMap();
			for (int i = 0; i < this.getRowCount(); i++) {
				final Object alias = this.getValueAt(i, 0);
				final Object expr = this.getValueAt(i, 1);
				if (alias != null && alias.toString().trim().length() > 0)
					aliases.put(alias,
							expr != null ? expr.toString().length() == 0 ? null
									: expr : null);
			}
			return aliases;
		}

		public Class getColumnClass(final int column) {
			return this.colClasses[column];
		}
	}

	/**
	 * This is a simple two-column base table which allows any string in both
	 * columns.
	 */
	public abstract static class StringStringTablePanel extends
			TwoColumnTablePanel {

		/**
		 * Constructs a two-column table which displays simple strings in both
		 * columns.
		 * 
		 * @param values
		 *            the values to display initially, or <tt>null</tt> if
		 *            none.
		 * @param firstColValues
		 *            the values to restrict the first column to, or
		 *            <tt>null</tt> if none required.
		 * @param secondColValues
		 *            the values to restrict the second column to, or
		 *            <tt>null</tt> if none required.
		 */
		protected StringStringTablePanel(final Map values,
				final Collection firstColValues,
				final Collection secondColValues) {
			super(values, firstColValues, secondColValues);
		}

		/**
		 * Constructs a two-column table which displays simple strings in both
		 * columns.
		 * 
		 * @param values
		 *            the values to display initially, or <tt>null</tt> if
		 *            none.
		 */
		public StringStringTablePanel(final Map values) {
			this(values, null, null);
		}

		public Class getFirstColumnType() {
			return String.class;
		}

		public Class getSecondColumnType() {
			return String.class;
		}

		public Object getNewRowFirstColumn() {
			return "";
		}

		public Object getNewRowSecondColumn() {
			return "";
		}

		public JComboBox getFirstColumnEditor(final Collection values) {
			return null;
		}

		public JComboBox getSecondColumnEditor(final Collection values) {
			return null;
		}

		public TableCellRenderer getFirstColumnRenderer() {
			return null;
		}

		public TableCellRenderer getSecondColumnRenderer() {
			return null;
		}
	}

	/**
	 * This class displays a database column lookup in the first column and a
	 * simple string in the second column.
	 */
	public abstract static class ColumnStringTablePanel extends
			StringStringTablePanel {
		private JComboBox editor;

		/**
		 * Constructs a table with a drop-down for database columns in the first
		 * column and allows simple string entry in the second column.
		 * 
		 * @param values
		 *            the initial values to display, or <tt>null</tt> if none.
		 * @param cols
		 *            the columns to show in the drop-down.
		 */
		public ColumnStringTablePanel(final Map values, final Collection cols) {
			super(values, cols, null);
		}

		/**
		 * Given a bunch of columns, sort them into a user-pleasing order. This
		 * implementation uses the {@link Column#getName()} method to get a name
		 * for each one then uses {@link String#compareTo(Object)} method to
		 * sort them.
		 * 
		 * @param columns
		 *            the columns to sort.
		 * @return the sorted columns.
		 */
		protected Collection getSortedColumns(final Collection columns) {
			final List cols = new ArrayList(columns);
			Collections.sort(cols);
			return cols;
		}

		/**
		 * Gets a reference to the editor used to display the drop-down in the
		 * first column.
		 * 
		 * @return the editor used.
		 */
		protected JComboBox getFirstColumnEditor() {
			return this.editor;
		}

		public Class getFirstColumnType() {
			return Column.class;
		}

		public Object getNewRowFirstColumn() {
			return this.editor.getItemAt(0);
		}

		public JComboBox getFirstColumnEditor(final Collection values) {
			if (this.editor == null) {
				// Create and store the editor for future reference.
				this.editor = new JComboBox();
				for (final Iterator i = this.getSortedColumns(values)
						.iterator(); i.hasNext();)
					this.editor.addItem(i.next());
			}
			return this.editor;
		}
	}
}
