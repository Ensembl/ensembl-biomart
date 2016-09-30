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

package org.biomart.builder.view.gui.dialogs;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;

import org.biomart.builder.model.Column;
import org.biomart.builder.model.TransformationUnit;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.DataSet.DataSetColumn.WrappedColumn;
import org.biomart.builder.model.TransformationUnit.SelectFromTable;
import org.biomart.common.resources.Resources;

/**
 * This dialog asks users what kind of invisible dataset suggestion the user
 * wants to do. It does this by giving them a list of tables from which columns
 * in the specified dataset table have been derived, then allowing them to
 * select one or more columns from a single table on this list.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.18 $, $Date: 2007-08-21 15:19:54 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class SuggestInvisibleDataSetDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private JButton cancel;

	private JList columns;

	private JButton execute;

	private JComboBox tables;

	/**
	 * Creates (but does not open) a dialog requesting details of invisible
	 * dataset suggestion.
	 * 
	 * @param table
	 *            the main dataset table to source columns from to show in the
	 *            list.
	 */
	public SuggestInvisibleDataSetDialog(final DataSetTable table) {
		// Creates the basic dialog.
		super();
		this.setTitle(Resources.get("suggestInvisibleDataSetDialogTitle"));
		this.setModal(true);

		// Create the content pane to store the create dialog panel.
		final JPanel content = new JPanel(new GridBagLayout());
		this.setContentPane(content);

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

		// Create a drop-down list of underlying tables.
		this.tables = new JComboBox();
		for (final Iterator i = table.getTransformationUnits().iterator(); i
				.hasNext();) {
			final TransformationUnit tu = (TransformationUnit) i.next();
			if (tu instanceof SelectFromTable
					&& !(((SelectFromTable) tu).getTable() instanceof DataSetTable))
				this.tables.addItem(((SelectFromTable) tu).getTable());
		}

		// Start with an empty available columns list.
		this.columns = new JList();
		this.columns.setVisibleRowCount(10); // Arbitrary.
		// Set the list to 50-characters wide. Longer than this and it will
		// show a horizontal scrollbar.
		this.columns
				.setPrototypeCellValue("01234567890123456789012345678901234567890123456789");
		this.columns
				.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

		// When underlying table selected, update the
		// available columns list to suit.
		this.tables.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (SuggestInvisibleDataSetDialog.this.tables.getSelectedItem() != null) {
					final List availableColumns = new ArrayList();
					for (final Iterator i = table.getColumns().values()
							.iterator(); i.hasNext();) {
						final DataSetColumn col = (DataSetColumn) i.next();
						if (col instanceof WrappedColumn
								&& ((WrappedColumn) col)
										.getWrappedColumn()
										.getTable()
										.equals(
												SuggestInvisibleDataSetDialog.this.tables
														.getSelectedItem()))
							availableColumns.add(((WrappedColumn) col)
									.getWrappedColumn());
					}
					SuggestInvisibleDataSetDialog.this.columns
							.setListData(availableColumns
									.toArray(new Column[0]));
				}
			}
		});

		// Select the default table.
		this.tables.setSelectedIndex(0);

		// Create the buttons.
		this.cancel = new JButton(Resources.get("cancelButton"));
		this.execute = new JButton(Resources.get("suggestButton"));

		// Add the table name.
		JLabel label = new JLabel(Resources.get("suggestDSTableLabel"));
		content.add(label, labelConstraints);
		JPanel field = new JPanel();
		field.add(this.tables);
		content.add(field, fieldConstraints);

		// Add the list of columns.
		label = new JLabel(Resources.get("suggestDSColumnsLabel"));
		content.add(label, labelConstraints);
		field = new JPanel();
		field.add(new JScrollPane(this.columns));
		content.add(field, fieldConstraints);

		// Add the buttons to the dialog.
		label = new JLabel();
		content.add(label, labelLastRowConstraints);
		field = new JPanel();
		field.add(this.cancel);
		field.add(this.execute);
		content.add(field, fieldLastRowConstraints);

		// Intercept the cancel button and use it to close this
		// dialog without making any changes.
		this.cancel.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SuggestInvisibleDataSetDialog.this.columns.clearSelection();
				SuggestInvisibleDataSetDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button and use it to create
		// the appropriate partition type, then close the dialog.
		this.execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (SuggestInvisibleDataSetDialog.this.validateFields())
					SuggestInvisibleDataSetDialog.this.setVisible(false);
			}
		});

		// Make the execute button the default button.
		this.getRootPane().setDefaultButton(this.execute);

		// Set the size of the dialog.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	private boolean validateFields() {
		// A placeholder to hold the validation messages, if any.
		final List messages = new ArrayList();

		// We must have a selected column!
		if (this.columns.getSelectedValues().length == 0)
			messages.add(Resources.get("suggestDSColumnsEmpty"));

		// If there any messages, display them.
		if (!messages.isEmpty())
			JOptionPane.showMessageDialog(null,
					messages.toArray(new String[0]), Resources
							.get("validationTitle"),
					JOptionPane.INFORMATION_MESSAGE);

		// Validation succeeds if there are no messages.
		return messages.isEmpty();
	}

	/**
	 * Return the set of columns the user selected.
	 * 
	 * @return the set of columns the user selected.
	 */
	public Collection getSelectedColumns() {
		return Arrays.asList(this.columns.getSelectedValues());
	}
}
