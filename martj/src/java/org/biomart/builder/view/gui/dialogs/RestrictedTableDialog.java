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
import java.util.List;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import org.biomart.builder.model.Table;
import org.biomart.builder.model.Table.RestrictedTableDefinition;
import org.biomart.builder.view.gui.panels.TwoColumnTablePanel;
import org.biomart.builder.view.gui.panels.TwoColumnTablePanel.ColumnStringTablePanel;
import org.biomart.common.resources.Resources;

/**
 * This dialog asks users to create or modify a restriction over a particular
 * table for this dataset only.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.30 $, $Date: 2007-12-21 12:47:55 $, modified by
 *          $Author: rh4 $
 * @since 0.6
 */
public class RestrictedTableDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private JButton cancel;

	private boolean cancelled;

	private TwoColumnTablePanel columnAliasModel;

	private JButton execute;

	private JTextArea expression;

	/**
	 * Creates (but does not open) a dialog requesting details of a restricted
	 * table.
	 * 
	 * @param table
	 *            the table to restrict.
	 * @param template
	 *            the restriction to use as a template, if any.
	 */
	public RestrictedTableDialog(final Table table,
			final RestrictedTableDefinition template) {
		// Creates the basic dialog.
		super();
		this.setTitle(template == null ? Resources
				.get("addTblRestrictDialogTitle") : Resources
				.get("modifyTblRestrictDialogTitle"));
		this.setModal(true);

		// Remembers the dataset tabset this dialog is referring to.
		this.cancelled = true;

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

		// Create the fields that will contain the user's table choices.
		this.expression = new JTextArea(10, 40); // Arbitrary size.

		// Work out what column/relation pairs are available to us.
		final List colsAvailable = new ArrayList(table.getColumns().values());

		// Table aliases.
		this.columnAliasModel = new ColumnStringTablePanel(
				template == null ? null : template.getAliases(), colsAvailable) {
			private static final long serialVersionUID = 1L;

			private int alias = 1;

			public String getInsertButtonText() {
				return Resources.get("insertAliasButton");
			}

			public String getRemoveButtonText() {
				return Resources.get("removeAliasButton");
			}

			public String getFirstColumnHeader() {
				return Resources.get("columnAliasTableColHeader");
			}

			public String getSecondColumnHeader() {
				return Resources.get("columnAliasTableAliasHeader");
			}

			public Object getNewRowSecondColumn() {
				return Resources.get("defaultAlias") + this.alias++;
			}
		};

		// Create the buttons.
		this.cancel = new JButton(Resources.get("cancelButton"));
		this.execute = template == null ? new JButton(Resources
				.get("addButton")) : new JButton(Resources.get("modifyButton"));

		// Add the aliases.
		JLabel label = new JLabel(Resources.get("columnAliasLabel"));
		content.add(label, labelConstraints);
		JPanel field = new JPanel();
		field.add(this.columnAliasModel);
		content.add(field, fieldConstraints);

		// Add the expression option.
		label = new JLabel(Resources.get("expressionLabel"));
		content.add(label, labelConstraints);
		field = new JPanel();
		field.add(new JScrollPane(this.expression));
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
				RestrictedTableDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button and use it to create
		// the appropriate partition type, then close the dialog.
		this.execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (RestrictedTableDialog.this.validateFields()) {
					RestrictedTableDialog.this.cancelled = false;
					RestrictedTableDialog.this.setVisible(false);
				}
			}
		});

		// Make the execute button the default button.
		this.getRootPane().setDefaultButton(this.execute);

		// Set some nice defaults.
		if (template != null) 
			this.expression.setText(template.getExpression());
		// Aliases were already copied in the JTable constructor above.

		// Set the size of the dialog.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	private boolean isEmpty(final String string) {
		// Strings are empty if they are null or all whitespace.
		return string == null || string.trim().length() == 0;
	}

	private boolean validateFields() {
		// A placeholder to hold the validation messages, if any.
		final List messages = new ArrayList();

		// We must have an expression!
		if (this.isEmpty(this.expression.getText()))
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("expression")));

		// Validate other fields.
		if (this.columnAliasModel.getValues().isEmpty())
			messages.add(Resources.get("columnAliasMissing"));

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
	 * Return <tt>true</tt> if the user cancelled the box.
	 * 
	 * @return <tt>true</tt> if the box was cancelled.
	 */
	public boolean getCancelled() {
		return this.cancelled;
	}

	/**
	 * Return the column aliases the user selected.
	 * 
	 * @return the aliases.
	 */
	public Map getColumnAliases() {
		return this.columnAliasModel.getValues();
	}

	/**
	 * Return the expression the user selected.
	 * 
	 * @return the expression.
	 */
	public String getExpression() {
		return this.expression.getText().trim();
	}
}
