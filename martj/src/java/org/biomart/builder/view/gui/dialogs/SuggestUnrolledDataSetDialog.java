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
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.biomart.builder.model.Column;
import org.biomart.builder.model.Table;
import org.biomart.common.resources.Resources;

/**
 * This dialog box allows the user to define an unrolled dataset.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.6 $, $Date: 2007-10-17 14:00:31 $, modified by 
 * 			$Author: rh4 $
 * @since 0.7
 */
public class SuggestUnrolledDataSetDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private final JComboBox nTable;

	private final JComboBox nrTable;

	private final JComboBox nIDColumn;

	private final JComboBox nrParentIDColumn;

	private final JComboBox nrChildIDColumn;

	private final JComboBox nNamingColumn;

	private final JCheckBox reversed;

	private boolean cancelled = true;

	/**
	 * Pop up a suggest unrolled dataset dialog.
	 * 
	 * @param nTable
	 *            the parent table we are initially working with.
	 */
	public SuggestUnrolledDataSetDialog(final Table nTable) {
		// Create the basic dialog centred on the main mart builder window.
		super();
		this.setTitle(Resources.get("suggestUnrolledDataSetDialogTitle"));
		this.setModal(true);

		// Create the content pane for the dialog, ie. the bit that will hold
		// all the various questions and answers.
		final JPanel content = new JPanel(new GridBagLayout());
		this.setContentPane(content);

		// Create some constraints for labels, except those on the last row
		// of the dialog.
		final GridBagConstraints labelConstraints = new GridBagConstraints();
		labelConstraints.gridwidth = GridBagConstraints.RELATIVE;
		labelConstraints.fill = GridBagConstraints.HORIZONTAL;
		labelConstraints.anchor = GridBagConstraints.LINE_END;
		labelConstraints.insets = new Insets(0, 2, 0, 0);
		// Create some constraints for fields, except those on the last row
		// of the dialog.
		final GridBagConstraints fieldConstraints = new GridBagConstraints();
		fieldConstraints.gridwidth = GridBagConstraints.REMAINDER;
		fieldConstraints.fill = GridBagConstraints.NONE;
		fieldConstraints.anchor = GridBagConstraints.LINE_START;
		fieldConstraints.insets = new Insets(0, 1, 0, 2);
		// Create some constraints for labels on the last row of the dialog.
		final GridBagConstraints labelLastRowConstraints = (GridBagConstraints) labelConstraints
				.clone();
		labelLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;
		// Create some constraints for fields on the last row of the dialog.
		final GridBagConstraints fieldLastRowConstraints = (GridBagConstraints) fieldConstraints
				.clone();
		fieldLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;

		// Build Insert drop downs.
		this.nTable = new JComboBox(new TreeSet(nTable.getSchema().getTables()
				.values()).toArray());
		this.nIDColumn = new JComboBox();
		this.nNamingColumn = new JComboBox();
		this.nrTable = new JComboBox();
		this.nrParentIDColumn = new JComboBox();
		this.nrChildIDColumn = new JComboBox();
		this.reversed = new JCheckBox(Resources.get("reversedLabel"));
		this.reversed.setSelected(true);
		// Add listener to update parent and child columns.
		this.nrTable.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SuggestUnrolledDataSetDialog.this.nrParentIDColumn
						.removeAllItems();
				SuggestUnrolledDataSetDialog.this.nrChildIDColumn
						.removeAllItems();
				final Table nrTable = (Table) SuggestUnrolledDataSetDialog.this.nrTable
						.getSelectedItem();
				if (nrTable != null) {
					for (final Iterator i = new TreeSet(nrTable.getColumns()
							.values()).iterator(); i.hasNext();) {
						final Column col = (Column) i.next();
						SuggestUnrolledDataSetDialog.this.nrParentIDColumn
								.addItem(col);
						SuggestUnrolledDataSetDialog.this.nrChildIDColumn
								.addItem(col);
					}
					SuggestUnrolledDataSetDialog.this.nrParentIDColumn
							.setSelectedIndex(0);
					SuggestUnrolledDataSetDialog.this.nrChildIDColumn
							.setSelectedIndex(0);
				} else {
					SuggestUnrolledDataSetDialog.this.nrParentIDColumn
							.setSelectedIndex(-1);
					SuggestUnrolledDataSetDialog.this.nrChildIDColumn
							.setSelectedIndex(-1);
				}
			}
		});
		// Add listener to update n and nr tables.
		this.nTable.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SuggestUnrolledDataSetDialog.this.nIDColumn.removeAllItems();
				SuggestUnrolledDataSetDialog.this.nNamingColumn
						.removeAllItems();
				SuggestUnrolledDataSetDialog.this.nrTable.removeAllItems();
				final Table nTable = (Table) SuggestUnrolledDataSetDialog.this.nTable
						.getSelectedItem();
				if (nTable != null) {
					for (final Iterator i = new TreeSet(nTable.getColumns()
							.values()).iterator(); i.hasNext();) {
						final Column col = (Column) i.next();
						SuggestUnrolledDataSetDialog.this.nIDColumn
								.addItem(col);
						SuggestUnrolledDataSetDialog.this.nNamingColumn
								.addItem(col);
					}
					for (final Iterator i = new TreeSet(nTable.getSchema()
							.getTables().values()).iterator(); i.hasNext();) {
						final Table cand = (Table) i.next();
						if (!cand.equals(nTable))
							SuggestUnrolledDataSetDialog.this.nrTable
									.addItem(cand);
					}
					SuggestUnrolledDataSetDialog.this.nIDColumn
							.setSelectedIndex(0);
					SuggestUnrolledDataSetDialog.this.nNamingColumn
							.setSelectedIndex(0);
					SuggestUnrolledDataSetDialog.this.nrTable
							.setSelectedIndex(0);
				} else {
					SuggestUnrolledDataSetDialog.this.nIDColumn
							.setSelectedIndex(-1);
					SuggestUnrolledDataSetDialog.this.nNamingColumn
							.setSelectedIndex(-1);
					SuggestUnrolledDataSetDialog.this.nrTable
							.setSelectedIndex(-1);
				}
			}
		});

		JLabel label = new JLabel(Resources.get("nTableLabel"));
		content.add(label, labelConstraints);
		content.add(this.nTable, fieldConstraints);
		label = new JLabel(Resources.get("nIDColumnLabel"));
		content.add(label, labelConstraints);
		content.add(this.nIDColumn, fieldConstraints);
		label = new JLabel(Resources.get("nNamingColumnLabel"));
		content.add(label, labelConstraints);
		content.add(this.nNamingColumn, fieldConstraints);
		label = new JLabel(Resources.get("nrTableLabel"));
		content.add(label, labelConstraints);
		content.add(this.nrTable, fieldConstraints);
		label = new JLabel(Resources.get("nrParentIDColumnLabel"));
		content.add(label, labelConstraints);
		content.add(this.nrParentIDColumn, fieldConstraints);
		label = new JLabel(Resources.get("nrChildIDColumnLabel"));
		content.add(label, labelConstraints);
		content.add(this.nrChildIDColumn, fieldConstraints);
		content.add(new JLabel(), labelConstraints);
		content.add(this.reversed, fieldConstraints);

		// Add the buttons.
		final JButton cancel = new JButton(Resources.get("cancelButton"));
		final JButton execute = new JButton(Resources.get("suggestButton"));
		label = new JLabel();
		content.add(label, labelLastRowConstraints);
		final JPanel field = new JPanel();
		field.add(cancel);
		field.add(execute);
		content.add(field, fieldLastRowConstraints);

		// Intercept the cancel button, which closes the dialog
		// without taking any action.
		cancel.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SuggestUnrolledDataSetDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button, which causes the
		// schema to be created as a temporary schema object. If
		// successful, the dialog closes.
		execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (SuggestUnrolledDataSetDialog.this.validateFields()) {
					SuggestUnrolledDataSetDialog.this.cancelled = false;
					SuggestUnrolledDataSetDialog.this.setVisible(false);
				}
			}
		});

		// Select first available values.
		this.nTable.setSelectedItem(nTable);

		// Make the execute button the default button.
		this.getRootPane().setDefaultButton(execute);

		// Pack and resize the window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	/**
	 * Were we canceled?
	 * 
	 * @return <tt>true</tt> if we were.
	 */
	public boolean isCancelled() {
		return this.cancelled;
	}

	/**
	 * 
	 * @return the chosen item.
	 */
	public Table getNTable() {
		return (Table) this.nTable.getSelectedItem();
	}

	/**
	 * 
	 * @return the status of the reversed checkbox.
	 */
	public boolean isReversed() {
		return this.reversed.isSelected();
	}

	/**
	 * 
	 * @return the chosen item.
	 */
	public Table getNRTable() {
		return (Table) this.nrTable.getSelectedItem();
	}

	/**
	 * 
	 * @return the chosen item.
	 */
	public Column getNIDColumn() {
		return (Column) this.nIDColumn.getSelectedItem();
	}

	/**
	 * 
	 * @return the chosen item.
	 */
	public Column getNRParentIDColumn() {
		return (Column) this.nrParentIDColumn.getSelectedItem();
	}

	/**
	 * 
	 * @return the chosen item.
	 */
	public Column getNRChildIDColumn() {
		return (Column) this.nrChildIDColumn.getSelectedItem();
	}

	/**
	 * 
	 * @return the chosen item.
	 */
	public Column getNNamingColumn() {
		return (Column) this.nNamingColumn.getSelectedItem();
	}

	private boolean validateFields() {
		// Make a list to hold messages.
		final List messages = new ArrayList();

		// We don't like missing drop-downs.
		if (this.nTable.getSelectedIndex() == -1)
			messages
					.add(Resources.get("fieldIsEmpty", Resources.get("nTable")));
		if (this.nrTable.getSelectedIndex() == -1)
			messages.add(Resources
					.get("fieldIsEmpty", Resources.get("nrTable")));
		if (this.nIDColumn.getSelectedIndex() == -1)
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("nIDColumn")));
		if (this.nrParentIDColumn.getSelectedIndex() == -1)
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("nrParentIDColumn")));
		if (this.nrChildIDColumn.getSelectedIndex() == -1)
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("nrChildIDColumn")));
		if (this.nNamingColumn.getSelectedIndex() == -1)
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("nNamingColumn")));

		// We don't like same-as relations.
		if (this.nrParentIDColumn.getSelectedIndex() == this.nrChildIDColumn
				.getSelectedIndex())
			messages.add(Resources.get("childParentColumnSame"));

		// If we have any messages, show them.
		if (!messages.isEmpty())
			JOptionPane.showMessageDialog(null,
					messages.toArray(new String[0]), Resources
							.get("validationTitle"),
					JOptionPane.INFORMATION_MESSAGE);

		// If there were no messages, then validated OK.
		return messages.isEmpty();
	}
}
