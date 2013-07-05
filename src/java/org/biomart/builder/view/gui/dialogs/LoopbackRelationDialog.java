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
import java.util.Iterator;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.biomart.builder.model.Column;
import org.biomart.common.resources.Resources;

/**
 * A dialog which allows the user to specify how a relation can be traversed in
 * both directions.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.3 $, $Date: 2007-10-03 10:41:01 $, modified by 
 * 			$Author: rh4 $
 * @since 0.7
 */
public class LoopbackRelationDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private JComboBox column;

	private JCheckBox loopback;

	/**
	 * Pop up a dialog to define the loopbackness of a relation.
	 * 
	 * @param isLoopback
	 *            is it already looped back?
	 * @param loopbackDiffColumn
	 *            the existing loopback diff column, if any.
	 * @param columnOptions
	 *            the columns the user can choose from.
	 */
	public LoopbackRelationDialog(final boolean isLoopback,
			final Column loopbackDiffColumn, final Collection columnOptions) {
		// Create the base dialog.
		super();
		this.setTitle(Resources.get("loopbackRelationDialogTitle"));
		this.setModal(true);

		final Object colSelect = loopbackDiffColumn == null ? Resources
				.get("loopbackRelationNoDiff") : (Object) loopbackDiffColumn;

		// Create the layout manager for this panel.
		final JPanel content = new JPanel();
		content.setLayout(new GridBagLayout());
		this.setContentPane(content);

		// Create constraints for fields that are not in the last row.
		final GridBagConstraints fieldConstraints = new GridBagConstraints();
		fieldConstraints.gridwidth = GridBagConstraints.REMAINDER;
		fieldConstraints.fill = GridBagConstraints.NONE;
		fieldConstraints.anchor = GridBagConstraints.LINE_START;
		fieldConstraints.insets = new Insets(0, 2, 0, 0);
		// Create constraints for fields that are in the last row.
		final GridBagConstraints fieldLastRowConstraints = (GridBagConstraints) fieldConstraints
				.clone();
		fieldLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;

		// Set up the arity spinner field.
		this.loopback = new JCheckBox(Resources
				.get("loopbackRelationEnableLabel"));
		this.loopback.setSelected(isLoopback);

		// Set up the combo box of columns.
		this.column = new JComboBox();
		this.column.addItem(Resources.get("loopbackRelationNoDiff"));
		for (final Iterator i = columnOptions.iterator(); i.hasNext();)
			this.column.addItem(i.next());
		this.column.setEnabled(isLoopback);
		this.column.setSelectedItem(colSelect);

		// The close and execute buttons.
		final JButton close = new JButton(Resources.get("closeButton"));
		final JButton execute = new JButton(Resources.get("updateButton"));

		// Input fields.
		JPanel field = new JPanel();
		field.add(this.loopback);
		content.add(field, fieldConstraints);

		// Parallel button.
		field = new JPanel();
		field.add(new JLabel(Resources.get("loopbackRelationDiffLabel")));
		field.add(this.column);
		content.add(field, fieldConstraints);

		// Close/Execute buttons at the bottom.
		field = new JPanel();
		field.add(close);
		field.add(execute);
		content.add(field, fieldLastRowConstraints);

		// Intercept the checkbox.
		this.loopback.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				LoopbackRelationDialog.this.column
						.setEnabled(LoopbackRelationDialog.this.loopback
								.isSelected());
				if (!LoopbackRelationDialog.this.loopback.isSelected())
					LoopbackRelationDialog.this.column.setSelectedItem(null);
			}
		});

		// Intercept the close button, which closes the dialog
		// without taking any action.
		close.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				// Reset to default value.
				LoopbackRelationDialog.this.loopback.setSelected(isLoopback);
				LoopbackRelationDialog.this.column.setSelectedItem(colSelect);
				LoopbackRelationDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button, which validates the fields
		// then closes the dialog.
		execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (LoopbackRelationDialog.this.validateFields())
					LoopbackRelationDialog.this.setVisible(false);
			}
		});

		// Make execute the default button.
		this.getRootPane().setDefaultButton(execute);

		// Set size of window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	/**
	 * Get the column the user selected.
	 * 
	 * @return the selected column.
	 */
	public Column getLoopbackDiffColumn() {
		return this.column.getSelectedItem().equals(
				Resources.get("loopbackRelationNoDiff")) ? null
				: (Column) this.column.getSelectedItem();
	}

	/**
	 * If the user ticked the loopback relation box, this will return
	 * <tt>true</tt>.
	 * 
	 * @return <tt>true</tt> if the user ticked the loopback box.
	 */
	public boolean isLoopback() {
		return this.loopback.isSelected();
	}

	private boolean validateFields() {
		// List of messages to display, if any are necessary.
		final List messages = new ArrayList();

		// Nothing to do here.

		// Any messages to display? Show them.
		if (!messages.isEmpty())
			JOptionPane.showMessageDialog(null,
					messages.toArray(new String[0]), Resources
							.get("validationTitle"),
					JOptionPane.INFORMATION_MESSAGE);

		// Validation succeeds if there are no messages.
		return messages.isEmpty();
	}
}
