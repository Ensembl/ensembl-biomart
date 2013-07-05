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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFormattedTextField;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.biomart.builder.model.Mart;
import org.biomart.common.resources.Resources;

/**
 * This dialog asks users to give a host and port to connect to a remote host
 * with.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.5 $, $Date: 2007-10-03 10:41:01 $, modified by 
 * 			$Author: rh4 $
 * @since 0.6
 */
public class MartRunnerConnectionDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private JTextField runDDLHost;

	private JTextField runDDLPort;

	private String host = null;

	private String port = null;

	/**
	 * Creates (but does not open) a dialog requesting details of a remote host.
	 * Uses the defaults from the mart if supplied.
	 * 
	 * @param mart
	 *            the mart to get the default settings from.
	 */
	public MartRunnerConnectionDialog(final Mart mart) {
		// Creates the basic dialog.
		super();
		this.setTitle(Resources.get("monitorConnectDialogTitle"));
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

		// Create the host/port fields.
		this.runDDLHost = new JTextField(20);
		this.runDDLHost.setText(mart == null ? null : mart.getOutputHost());
		this.runDDLPort = new JFormattedTextField(new DecimalFormat("0"));
		this.runDDLPort.setColumns(5);
		this.runDDLPort.setText(mart == null ? null : mart.getOutputPort());

		// Add the output host/port etc..
		JLabel label = new JLabel(Resources.get("runDDLHostLabel"));
		content.add(label, labelConstraints);
		JPanel field = new JPanel();
		field.add(this.runDDLHost);
		content.add(field, fieldConstraints);
		label = new JLabel(Resources.get("runDDLPortLabel"));
		content.add(label, labelConstraints);
		field = new JPanel();
		field.add(this.runDDLPort);
		content.add(field, fieldConstraints);

		// The close and execute buttons.
		final JButton cancel = new JButton(Resources.get("cancelButton"));
		final JButton execute = new JButton(Resources.get("monitorButton"));

		// Intercept the close button, which closes the dialog
		// without taking any action.
		cancel.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				MartRunnerConnectionDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button, which validates the fields
		// then creates the DDL and closes the dialog.
		execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (MartRunnerConnectionDialog.this.validateFields()) {
					MartRunnerConnectionDialog.this.host = MartRunnerConnectionDialog.this.runDDLHost
							.getText().trim();
					MartRunnerConnectionDialog.this.port = MartRunnerConnectionDialog.this.runDDLPort
							.getText().trim();
					MartRunnerConnectionDialog.this.setVisible(false);
				}
			}
		});

		// Add the buttons.
		label = new JLabel();
		content.add(label, labelLastRowConstraints);
		field = new JPanel();
		field.add(cancel);
		field.add(execute);
		content.add(field, fieldLastRowConstraints);

		// Make execute the default button.
		this.getRootPane().setDefaultButton(execute);

		// Set size of window.
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
		if (this.isEmpty(this.runDDLHost.getText()))
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("runDDLHost")));
		if (this.isEmpty(this.runDDLPort.getText()))
			messages.add(Resources.get("fieldIsEmpty", Resources
					.get("runDDLPort")));

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
	 * Return the host the user selected.
	 * 
	 * @return the host.
	 */
	public String getHost() {
		return this.host;
	}

	/**
	 * Return the port the user selected.
	 * 
	 * @return the port.
	 */
	public String getPort() {
		return this.port;
	}
}
