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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;

import org.biomart.builder.model.Mart;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Schema.JDBCSchema;
import org.biomart.builder.view.gui.panels.SchemaConnectionPanel;
import org.biomart.builder.view.gui.panels.SchemaConnectionPanel.JDBCSchemaConnectionPanel;
import org.biomart.common.exceptions.BioMartError;
import org.biomart.common.resources.Resources;
import org.biomart.common.resources.Settings;
import org.biomart.common.view.gui.dialogs.StackTrace;
import org.biomart.common.view.gui.dialogs.TransactionalDialog;

/**
 * This dialog box allows the user to define or modify a schema, by giving it a
 * name, choosing a type, then displaying the appropriate
 * {@link SchemaConnectionPanel} according to the type chosen. The connection
 * panel then is given the job of actually creating or modifying the schema,
 * before the result is returned to the caller.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.13 $, $Date: 2007-11-27 10:47:01 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class SchemaConnectionDialog extends TransactionalDialog {
	private static final long serialVersionUID = 1;

	/**
	 * Pop up a dialog asking the user for details for a new schema, then create
	 * and return that schema.
	 * 
	 * @param mart
	 *            the mart to create the schema in.
	 * 
	 * @return the newly created schema, or null if it was cancelled.
	 */
	public static Schema createSchema(final Mart mart) {
		final SchemaConnectionDialog dialog = new SchemaConnectionDialog(mart,
				Resources.get("newSchemaDialogTitle"), Resources
						.get("addButton"), null);
		dialog.setLocationRelativeTo(null);
		dialog.setVisible(true);
		final Schema sch = dialog.schema;
		dialog.dispose();
		return sch;
	}

	/**
	 * Pop up a dialog asking the user to modify details for a schema, then
	 * modify that schema. Returns whether it was successful or not.
	 * 
	 * @param schema
	 *            the schema to modify.
	 * @return <tt>true</tt> if modification was successful, <tt>false</tt>
	 *         if not.
	 */
	public static boolean modifySchema(final Schema schema) {
		final SchemaConnectionDialog dialog = new SchemaConnectionDialog(schema
				.getMart(), Resources.get("modifySchemaDialogTitle"), Resources
				.get("modifyButton"), schema);
		dialog.setLocationRelativeTo(null);
		dialog.setVisible(true);
		final boolean result;
		if (dialog.schema != null && dialog.schema instanceof JDBCSchema)
			result = ((JDBCSchemaConnectionPanel) dialog.connectionPanel)
					.copySettingsToExistingSchema(schema) != null;
		else
			result = false;
		dialog.dispose();
		return result;
	}

	private JButton cancel;

	private SchemaConnectionPanel connectionPanel;

	private JButton execute;

	private JComboBox name;

	private Schema schema;

	private JButton test;

	private JComboBox type;

	private SchemaConnectionDialog(final Mart mart, final String title,
			final String executeButtonText, final Schema template) {
		// Create the basic dialog centred on the main mart builder window.
		super();
		this.setTitle(title);
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

		// Create the input fields for the type, and the
		// holder for the connection panel details.
		this.type = new JComboBox(new String[] { Resources.get("jdbcSchema") });
		final JPanel connectionPanelHolder = new JPanel();
		this.type.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				// JDBC specific stuff.
				if (SchemaConnectionDialog.this.type.getSelectedItem().equals(
						Resources.get("jdbcSchema")))
					if (!(SchemaConnectionDialog.this.connectionPanel instanceof JDBCSchemaConnectionPanel)) {
						connectionPanelHolder.removeAll();
						SchemaConnectionDialog.this.connectionPanel = new JDBCSchemaConnectionPanel(
								mart);
						connectionPanelHolder
								.add(SchemaConnectionDialog.this.connectionPanel);
						SchemaConnectionDialog.this.pack();
					}
				// General stuff for all schema types, including populating
				// the name combo-box with all historical schema objects
				// of the same class as the currently selected type.
				// We have to reverse this otherwise the last-accessed (ie.
				// last-added) item gets moved to the top of the list and
				// the whole list is reversed simply by displaying it.
				SchemaConnectionDialog.this.name.removeAllItems();
				final List names = new ArrayList(
						Settings
								.getHistoryNamesForClass(SchemaConnectionDialog.this.connectionPanel
										.getSchemaClass()));
				Collections.reverse(names);
				for (final Iterator i = names.iterator(); i.hasNext();)
					SchemaConnectionDialog.this.name.addItem(i.next());
				SchemaConnectionDialog.this.name.setSelectedItem(null);
			}
		});

		// Build a combo box that allows the user to change the name
		// of a schema, or select one from history to copy settings from.
		this.name = new JComboBox();
		this.name.setEditable(true);
		this.name.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				// Identify which schema to copy settings from.
				final Object obj = SchemaConnectionDialog.this.name
						.getSelectedItem();

				// Load the schema settings from our history.
				final Properties historyProps = Settings.getHistoryProperties(
						SchemaConnectionDialog.this.connectionPanel
								.getSchemaClass(), (String) obj);

				// Copy the settings, if we found any that matched.
				if (historyProps != null)
					SchemaConnectionDialog.this.connectionPanel
							.copySettingsFromProperties(historyProps);
			}
		});

		// Make a default selection for the connection panel holder. Use JDBC
		// as it is the most obvious choice. We have to do something here else
		// the box won't size properly without one.
		this.type.setSelectedItem(Resources.get("jdbcSchema"));

		// Create buttons in dialog.
		this.test = new JButton(Resources.get("testButton"));
		this.cancel = new JButton(Resources.get("cancelButton"));
		this.execute = new JButton(executeButtonText);

		// Add the name label and name field.
		JLabel label = new JLabel(Resources.get("nameLabel"));
		content.add(label, labelConstraints);

		// In the name field, also include the type label and field, to save
		// space.
		JPanel field = new JPanel();
		field.add(this.name);
		label = new JLabel(Resources.get("typeLabel"));
		field.add(label);
		field.add(this.type);
		content.add(field, fieldConstraints);

		// Add the connection panel holder.
		content.add(connectionPanelHolder, fieldConstraints);

		// Add the buttons.
		label = new JLabel();
		content.add(label, labelLastRowConstraints);
		field = new JPanel();
		field.add(this.test);
		field.add(this.cancel);
		field.add(this.execute);
		content.add(field, fieldLastRowConstraints);

		// Intercept the cancel button, which closes the dialog
		// without taking any action.
		this.cancel.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SchemaConnectionDialog.this.schema = null;
				SchemaConnectionDialog.this.setVisible(false);
			}
		});

		// Intercept the test button, which causes the schema
		// details as currently entered to be used to create
		// a temporary schema object, which is then tested.
		this.test.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Schema testSchema = SchemaConnectionDialog.this
						.createSchemaFromSettings();
				if (testSchema != null)
					SchemaConnectionDialog.this.requestTestSchema(testSchema);
			}
		});

		// Intercept the execute button, which causes the
		// schema to be created as a temporary schema object. If
		// successful, the dialog closes.
		this.execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SchemaConnectionDialog.this.schema = SchemaConnectionDialog.this
						.createSchemaFromSettings();
				if (SchemaConnectionDialog.this.schema != null) {
					// Assume we've failed.
					boolean passedTest = false;

					try {
						// Attempt to pass the test.
						passedTest = SchemaConnectionDialog.this.schema.test();
					} catch (final Throwable t) {
						// If we get an exception, we failed the test, and
						// should
						// tell the user why.
						passedTest = false;
						StackTrace.showStackTrace(t);
					}

					// Tell the user if we passed or failed.
					if (passedTest) {
						SchemaConnectionDialog.this.schema.storeInHistory();
						SchemaConnectionDialog.this.setVisible(false);
					} else
						JOptionPane.showMessageDialog(null, Resources
								.get("schemaTestFailed"), Resources
								.get("testTitle"), JOptionPane.ERROR_MESSAGE);
				}
			}
		});

		// Make the execute button the default button.
		this.getRootPane().setDefaultButton(this.execute);

		// Reset the fields to their default values.
		this.copySettingsFromSchema(template);

		// Pack and resize the window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	private void requestTestSchema(final Schema schema) {
		// Assume we've failed.
		boolean passedTest = false;

		try {
			// Attempt to pass the test.
			passedTest = schema.test();
		} catch (final Throwable t) {
			// If we get an exception, we failed the test, and should
			// tell the user why.
			passedTest = false;
			StackTrace.showStackTrace(t);
		}

		// Tell the user if we passed or failed.
		if (passedTest)
			JOptionPane.showMessageDialog(null, Resources
					.get("schemaTestPassed"), Resources.get("testTitle"),
					JOptionPane.INFORMATION_MESSAGE);
		else
			JOptionPane.showMessageDialog(null, Resources
					.get("schemaTestFailed"), Resources.get("testTitle"),
					JOptionPane.ERROR_MESSAGE);
	}

	private Schema createSchemaFromSettings() {
		// Refuse to create a temporary schema object if we can't validate it.
		if (!this.validateFields())
			return null;

		try {
			// Look up the type and use the appropriate schema type to
			// actually create the object.
			final String type = (String) this.type.getSelectedItem();
			if (type.equals(Resources.get("jdbcSchema")))
				return ((JDBCSchemaConnectionPanel) this.connectionPanel)
						.createSchemaFromSettings((String) this.name
								.getSelectedItem());

			// What kind of type is it then??
			else
				throw new BioMartError();
		} catch (final Throwable t) {
			StackTrace.showStackTrace(t);
		}

		// If we got here, something went wrong with creation, so we
		// have nothing to return.
		return null;
	}

	private boolean isEmpty(final String string) {
		// Return true if the string is null or empty.
		return string == null || string.trim().length() == 0;
	}

	private void copySettingsFromSchema(final Schema template) {
		// If we are modifying something, use it to fill the details
		// in the dialog.
		if (template != null) {
			// Select the schema type.
			if (template instanceof JDBCSchema)
				this.type.setSelectedItem(Resources.get("jdbcSchema"));

			// Unknown schema type!
			else
				throw new BioMartError();
			this.type.setEnabled(false); // Gray out as we can't change this.

			// Set the name.
			this.name.setSelectedItem(template.getName());
			this.name.setEnabled(false); // Gray out as we can't change this.
		}

		// Otherwise, use some sensible defaults.
		else {
			this.type.setSelectedIndex(0);
			this.name.setSelectedItem(null);
		}

		// Update the connection panel.
		this.connectionPanel.copySettingsFromSchema(template);
	}

	private boolean validateFields() {
		// Make a list to hold messages.
		final List messages = new ArrayList();

		// We don't like missing names.
		if (this.isEmpty(this.name.getSelectedItem().toString()))
			messages.add(Resources.get("fieldIsEmpty", Resources.get("name")));

		// We don't like missing types either.
		if (this.type.getSelectedIndex() == -1)
			messages.add(Resources.get("fieldIsEmpty", Resources.get("type")));

		// If we have any messages, show them.
		if (!messages.isEmpty())
			JOptionPane.showMessageDialog(null,
					messages.toArray(new String[0]), Resources
							.get("validationTitle"),
					JOptionPane.INFORMATION_MESSAGE);

		// If there were no messages, then validated OK if the connection
		// panel also validated OK.
		return messages.isEmpty() && this.connectionPanel.validateFields(true);
	}
}
