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

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import javax.swing.Box;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.SwingConstants;
import javax.swing.border.EmptyBorder;
import javax.swing.plaf.basic.BasicArrowButton;

import org.biomart.builder.model.Column;
import org.biomart.builder.model.Table;
import org.biomart.common.resources.Resources;

/**
 * A dialog which lists all the columns in a key, and all the columns in the
 * table which are available to put in that key. It can then allow the user to
 * move those columns around, thus editing the key.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.11 $, $Date: 2007-09-10 12:29:36 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class KeyDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private DefaultListModel selectedColumns;

	private DefaultListModel tableColumns;

	/**
	 * Pop up a dialog to define or edit a key.
	 * 
	 * @param table
	 *            the table the key belongs to.
	 * @param title
	 *            the title to give the dialog.
	 * @param action
	 *            the text to put on the OK button.
	 * @param columns
	 *            the columns to preselect as part of the key.
	 */
	public KeyDialog(final Table table, final String title,
			final String action, final Column[] columns) {
		// Create the base dialog.
		super();
		this.setTitle(title);
		this.setModal(true);

		// The list of table columns is populated with the names of columns.
		this.tableColumns = new DefaultListModel();
		for (final Iterator i = new TreeSet(table.getColumns().values())
				.iterator(); i.hasNext();)
			this.tableColumns.addElement(i.next());

		// The list of selected columns is populated with the columns from
		// the existing key. These are also removed from the list of table
		// columns, to prevent duplication.
		this.selectedColumns = new DefaultListModel();
		if (columns != null)
			for (int i = 0; i < columns.length; i++) {
				this.tableColumns.removeElement(columns[i]);
				this.selectedColumns.addElement(columns[i]);
			}

		// The close and execute buttons.
		final JButton close = new JButton(Resources.get("closeButton"));
		final JButton execute = new JButton(action);

		// Create the table column list, and the buttons
		// to move columns to/from the selected column list.
		final JList tabColList = new JList(this.tableColumns);
		final JButton insertButton = new BasicArrowButton(SwingConstants.EAST);
		final JButton removeButton = new BasicArrowButton(SwingConstants.WEST);

		// Create the key column list, and the buttons to
		// move columns to/from the table columns list.
		final JList keyColList = new JList(this.selectedColumns);
		final JButton upButton = new BasicArrowButton(SwingConstants.NORTH);
		final JButton downButton = new BasicArrowButton(SwingConstants.SOUTH);

		// Put the two halves of the dialog side-by-side in a horizontal box.
		final Box content = Box.createHorizontalBox();
		this.setContentPane(content);

		// Left-hand side goes the table columns that are unused.
		final JPanel leftPanel = new JPanel(new BorderLayout());
		// Label at the top.
		leftPanel.add(new JLabel(Resources.get("columnsAvailableLabel")),
				BorderLayout.PAGE_START);
		// Table columns list in the middle.
		leftPanel.add(new JScrollPane(tabColList), BorderLayout.CENTER);
		leftPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		// Buttons down the right-hand-side, vertically.
		final Box leftButtonPanel = Box.createVerticalBox();
		leftButtonPanel.add(insertButton);
		leftButtonPanel.add(removeButton);
		leftButtonPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		leftPanel.add(leftButtonPanel, BorderLayout.LINE_END);
		content.add(leftPanel);

		// Right-hand side goes the key columns that are used.
		final JPanel rightPanel = new JPanel(new BorderLayout());
		// Label at the top.
		rightPanel.add(new JLabel(Resources.get("keyColumnsLabel")),
				BorderLayout.PAGE_START);
		// Key columns in the middle.
		rightPanel.add(new JScrollPane(keyColList), BorderLayout.CENTER);
		rightPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		// Buttons down the right-hand-side, vertically.
		final Box rightButtonPanel = Box.createVerticalBox();
		rightButtonPanel.add(upButton);
		rightButtonPanel.add(downButton);
		rightButtonPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		rightPanel.add(rightButtonPanel, BorderLayout.LINE_END);
		// Close/Execute buttons at the bottom.
		final Box actionButtons = Box.createHorizontalBox();
		actionButtons.add(close);
		actionButtons.add(execute);
		actionButtons.setBorder(new EmptyBorder(2, 2, 2, 2));
		rightPanel.add(actionButtons, BorderLayout.PAGE_END);
		content.add(rightPanel);

		// Intercept the insert/remove buttons
		insertButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object selected = tabColList.getSelectedValue();
				if (selected != null) {
					// Move a column from table to key.
					KeyDialog.this.selectedColumns.addElement(selected);
					KeyDialog.this.tableColumns.removeElement(selected);
				}
			}
		});
		removeButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object selected = keyColList.getSelectedValue();
				if (selected != null) {
					// Move a column from key to table.
					KeyDialog.this.tableColumns.addElement(selected);
					KeyDialog.this.selectedColumns.removeElement(selected);
				}
			}
		});

		// Intercept the up/down buttons
		upButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object selected = keyColList.getSelectedValue();
				if (selected != null) {
					final int currIndex = KeyDialog.this.selectedColumns
							.indexOf(selected);
					if (currIndex > 0) {
						// Swap the selected item with the one above it.
						final Object swap = KeyDialog.this.selectedColumns
								.get(currIndex - 1);
						KeyDialog.this.selectedColumns.setElementAt(selected,
								currIndex - 1);
						KeyDialog.this.selectedColumns.setElementAt(swap,
								currIndex);
						// Select the selected item again, as it will
						// have moved.
						keyColList.setSelectedIndex(currIndex - 1);
					}
				}
			}
		});
		downButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object selected = keyColList.getSelectedValue();
				if (selected != null) {
					final int currIndex = KeyDialog.this.selectedColumns
							.indexOf(selected);
					if (currIndex < KeyDialog.this.selectedColumns.size() - 1) {
						// Swap the selected item with the one below it.
						final Object swap = KeyDialog.this.selectedColumns
								.get(currIndex + 1);
						KeyDialog.this.selectedColumns.setElementAt(selected,
								currIndex + 1);
						KeyDialog.this.selectedColumns.setElementAt(swap,
								currIndex);
						// Select the selected item again, as it will
						// have moved.
						keyColList.setSelectedIndex(currIndex + 1);
					}
				}
			}
		});

		// Intercept the close button, which closes the dialog
		// without taking any action.
		close.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				KeyDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button, which validates the fields
		// then closes the dialog.
		execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (KeyDialog.this.validateFields())
					KeyDialog.this.setVisible(false);
			}
		});

		// Make execute the default button.
		this.getRootPane().setDefaultButton(execute);

		// Set size of window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	private boolean validateFields() {
		// List of messages to display, if any are necessary.
		final List messages = new ArrayList();

		// Must have at least one column selected.
		if (this.selectedColumns.isEmpty())
			messages.add(Resources.get("keyColumnsEmpty"));

		// Any messages to display? Show them.
		if (!messages.isEmpty())
			JOptionPane.showMessageDialog(null,
					messages.toArray(new String[0]), Resources
							.get("validationTitle"),
					JOptionPane.INFORMATION_MESSAGE);

		// Validation succeeds if there are no messages.
		return messages.isEmpty();
	}

	/**
	 * Get the columns the user chose.
	 * 
	 * @return the columns the user selected, in order.
	 */
	public Column[] getSelectedColumns() {
		// For some reason, can't cast Object[] to Column[].
		final Object[] objs = this.selectedColumns.toArray();
		final Column[] cols = new Column[objs.length];
		for (int i = 0; i < objs.length; i++)
			cols[i] = (Column) objs[i];
		return cols;
	}
}
