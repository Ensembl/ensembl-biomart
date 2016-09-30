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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.biomart.builder.model.Column;
import org.biomart.builder.model.Table;
import org.biomart.builder.model.TransformationUnit;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.TransformationUnit.JoinTable;
import org.biomart.builder.model.TransformationUnit.SelectFromTable;
import org.biomart.common.resources.Resources;

/**
 * This dialog box allows the user to automate the loopback+compound process.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.1 $, $Date: 2008-02-01 16:35:43 $, modified by $Author:
 *          rh4 $
 * @since 0.7
 */
public class LoopbackWizardDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private final JComboBox loopbackTable;

	private final JComboBox diffColumn;

	private boolean cancelled = true;

	/**
	 * Pop up a dialog to automate the loopback+compound process.
	 * 
	 * @param dsTable
	 *            the dataset table we are working with.
	 */
	public LoopbackWizardDialog(final DataSetTable dsTable) {
		// Create the basic dialog centred on the main mart builder window.
		super();
		this.setTitle(Resources.get("loopbackWizardDialogTitle"));
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

		// Make map of table to TU.
		final Map tableToTU = new TreeMap();
		final Map tuToCols = new HashMap();
		final Set seenTables = new HashSet();
		for (final Iterator i = dsTable.getTransformationUnits().iterator(); i
				.hasNext();) {
			// If tables are used twice, the first use is the one that stays.
			final TransformationUnit tu = (TransformationUnit) i.next();
			if (tu instanceof JoinTable) {
				final JoinTable jt = (JoinTable) tu;
				if (jt.getSchemaRelation().isOneToMany()
						&& !seenTables.contains(jt.getTable())) {
					// Identify previous unit.
					TransformationUnit ptu = jt.getPreviousUnit();
					// Get all columns from that unit.
					final Set cols = new TreeSet();
					if (ptu instanceof JoinTable)
						cols.addAll(((JoinTable) ptu).getTable().getColumns()
								.values());
					else if (ptu instanceof SelectFromTable)
						cols.addAll(((SelectFromTable) ptu).getTable()
								.getColumns().values());
					for (final Iterator j = cols.iterator(); j.hasNext();) {
						final Column col = (Column) j.next();
						if (col instanceof DataSetColumn)
							j.remove();
					}
					if (cols.size() > 0) {
						tuToCols.put(jt, cols);
						tableToTU.put(jt.getTable(), jt);
					}
				}
				seenTables.add(jt.getTable());
			}
		}

		// Build Insert drop downs.
		this.loopbackTable = new JComboBox(tableToTU.keySet().toArray());
		this.diffColumn = new JComboBox();
		// Add listener to update parent and child columns.
		this.loopbackTable.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				LoopbackWizardDialog.this.diffColumn.removeAllItems();
				final Table table = (Table) LoopbackWizardDialog.this.loopbackTable
						.getSelectedItem();
				if (table != null) {
					// Identify previous unit.
					TransformationUnit tu = (TransformationUnit) tableToTU
							.get(table);
					// Get all new columns from that unit.
					for (final Iterator i = ((Collection) tuToCols.get(tu))
							.iterator(); i.hasNext();) {
						final Column col = (Column) i.next();
						LoopbackWizardDialog.this.diffColumn.addItem(col);
					}
					LoopbackWizardDialog.this.diffColumn.setSelectedIndex(0);
				} else {
					LoopbackWizardDialog.this.diffColumn.setSelectedIndex(-1);
				}
			}
		});

		JLabel label = new JLabel(Resources.get("loopbackTableLabel"));
		content.add(label, labelConstraints);
		content.add(this.loopbackTable, fieldConstraints);
		label = new JLabel(Resources.get("diffColumnLabel"));
		content.add(label, labelConstraints);
		content.add(this.diffColumn, fieldConstraints);

		// Add the buttons.
		final JButton cancel = new JButton(Resources.get("cancelButton"));
		final JButton execute = new JButton(Resources.get("loopbackButton"));
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
				LoopbackWizardDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button, which causes the
		// schema to be created as a temporary schema object. If
		// successful, the dialog closes.
		execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (LoopbackWizardDialog.this.validateFields()) {
					LoopbackWizardDialog.this.cancelled = false;
					LoopbackWizardDialog.this.setVisible(false);
				}
			}
		});

		// Default selection.
		if (this.loopbackTable.getItemCount() > 0)
			this.loopbackTable.setSelectedIndex(0);

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
	public Column getDiffColumn() {
		return (Column) this.diffColumn.getSelectedItem();
	}

	/**
	 * 
	 * @return the chosen item.
	 */
	public Table getLoopbackTable() {
		return (Table) this.loopbackTable.getSelectedItem();
	}

	private boolean validateFields() {
		// Make a list to hold messages.
		final List messages = new ArrayList();

		// We don't like missing drop-downs.
		if (this.loopbackTable.getSelectedIndex() == -1)
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("loopbackTable")));
		if (this.diffColumn.getSelectedIndex() == -1)
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("diffColumn")));

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
