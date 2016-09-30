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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;

import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Table;
import org.biomart.common.resources.Resources;

/**
 * This dialog asks users what kind of dataset suggestion they want to do. It
 * does this by presenting a list of tables in all available schemas and asking
 * the user to select one or more of them for inclusion.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.18 $, $Date: 2007-08-21 15:19:53 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class SuggestDataSetDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private JButton cancel;

	private JButton execute;

	private JList tables;

	/**
	 * Creates (but does not open) a dialog requesting details of dataset
	 * suggestion.
	 * 
	 * @param schemas
	 *            the schemas to include when listing tables for the user to
	 *            choose.
	 * @param initialTable
	 *            the initial table to select in the list of tables.
	 */
	public SuggestDataSetDialog(final Collection schemas,
			final Table initialTable) {
		// Creates the basic dialog.
		super();
		this.setTitle(Resources.get("suggestDataSetDialogTitle"));
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

		final List availableTables = new ArrayList();
		for (final Iterator i = schemas.iterator(); i.hasNext();)
			for (final Iterator j = ((Schema) i.next()).getTables().values()
					.iterator(); j.hasNext();)
				availableTables.add(j.next());
		// Sort the list and make a component to display it.
		Collections.sort(availableTables);
		this.tables = new JList(availableTables.toArray(new Table[0]));
		this.tables.setVisibleRowCount(10); // Arbitrary.
		// Set the list to 50-characters wide. Longer than this and it will
		// show a horizontal scrollbar.
		this.tables
				.setPrototypeCellValue("01234567890123456789012345678901234567890123456789");
		this.tables
				.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);

		// Create the buttons.
		this.cancel = new JButton(Resources.get("cancelButton"));
		this.execute = new JButton(Resources.get("suggestButton"));

		// Add the list of tables.
		JLabel label = new JLabel(Resources.get("suggestDSTablesLabel"));
		content.add(label, labelConstraints);
		JPanel field = new JPanel();
		field.add(new JScrollPane(this.tables));
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
				SuggestDataSetDialog.this.tables.clearSelection();
				SuggestDataSetDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button and use it to create
		// the appropriate partition type, then close the dialog.
		this.execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (SuggestDataSetDialog.this.validateFields())
					SuggestDataSetDialog.this.setVisible(false);
			}
		});

		// Make the execute button the default button.
		this.getRootPane().setDefaultButton(this.execute);

		// Set the default selected table.
		if (initialTable != null)
			this.tables.setSelectedValue(initialTable, true);

		// Set the size of the dialog.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	private boolean validateFields() {
		// A placeholder to hold the validation messages, if any.
		final List messages = new ArrayList();

		// We must have a selected table!
		if (this.tables.getSelectedValues().length == 0)
			messages.add(Resources.get("suggestDSTablesEmpty"));

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
	 * Return the set of tables the user selected.
	 * 
	 * @return the set of tables the user selected.
	 */
	public Collection getSelectedTables() {
		return Arrays.asList(this.tables.getSelectedValues());
	}
}
