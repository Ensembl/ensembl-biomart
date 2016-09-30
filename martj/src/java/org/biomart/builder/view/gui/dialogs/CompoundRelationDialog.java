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
import java.util.List;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.biomart.builder.model.Relation.CompoundRelationDefinition;
import org.biomart.common.resources.Resources;

/**
 * A dialog which allows the user to specify how many times a relation will be
 * followed by the transformation process.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.14 $, $Date: 2007-08-21 15:19:54 $, modified by
 *          $Author: rh4 $
 * @since 0.6
 */
public class CompoundRelationDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private SpinnerNumberModel arity;

	private JCheckBox parallel;

	/**
	 * Pop up a dialog to define the arity of a relation.
	 * 
	 * @param startvalue
	 *            the initial preselected arity.
	 * @param title
	 *            the title to give the dialog.
	 * @param label
	 *            the title to give the arity selector.
	 * @param forceParallel
	 *            <tt>true</tt> if parallelism is not optional.
	 * @param partitionOptions
	 *            a list (possibly empty) of options available for attaching a
	 *            partition table to this compound relation. Each option is a
	 *            fully qualified partition column name.
	 */
	public CompoundRelationDialog(final CompoundRelationDefinition startvalue,
			final String title, final String label,
			final boolean forceParallel, final Collection partitionOptions) {
		// Create the base dialog.
		super();
		this.setTitle(title);
		this.setModal(true);

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
		this.arity = new SpinnerNumberModel(startvalue.getN(), 1,
				Integer.MAX_VALUE, 1);
		final JSpinner spinner = new JSpinner(this.arity);
		this.parallel = new JCheckBox(Resources.get("parallelLabel"));

		// Set up the check box to turn it on and off.
		final JCheckBox checkbox = new JCheckBox();
		if (startvalue.getN() > 1)
			checkbox.setSelected(true);
		this.parallel.setSelected(startvalue.isParallel());
		this.parallel.setEnabled(!forceParallel && startvalue.getN() > 1);

		// The close and execute buttons.
		final JButton close = new JButton(Resources.get("closeButton"));
		final JButton execute = new JButton(Resources.get("updateButton"));

		// Input fields.
		JPanel field = new JPanel();
		field.add(checkbox);
		field.add(new JLabel(label));
		field.add(spinner);
		field.add(new JLabel(Resources.get("compoundRelationSpinnerLabel")));
		content.add(field, fieldConstraints);

		// Parallel button.
		field = new JPanel();
		field.add(this.parallel);
		content.add(field, fieldConstraints);

		// Close/Execute buttons at the bottom.
		field = new JPanel();
		field.add(close);
		field.add(execute);
		content.add(field, fieldLastRowConstraints);

		// Intercept the spinner.
		spinner.addChangeListener(new ChangeListener() {
			public void stateChanged(final ChangeEvent e) {
				if (CompoundRelationDialog.this.getArity() <= 1) {
					checkbox.setSelected(false);
					CompoundRelationDialog.this.parallel.setSelected(false);
					CompoundRelationDialog.this.parallel.setEnabled(false);
				} else {
					checkbox.setSelected(true);
					if (forceParallel)
						CompoundRelationDialog.this.parallel.setSelected(true);
					CompoundRelationDialog.this.parallel
							.setEnabled(!forceParallel);
				}
			}
		});
		// Intercept the checkbox.
		checkbox.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (!checkbox.isSelected())
					CompoundRelationDialog.this.arity.setValue(new Integer(1));
			}
		});

		// Intercept the close button, which closes the dialog
		// without taking any action.
		close.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				// Reset to default value.
				CompoundRelationDialog.this.arity.setValue(new Integer(
						startvalue.getN()));
				CompoundRelationDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button, which validates the fields
		// then closes the dialog.
		execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (CompoundRelationDialog.this.validateFields())
					CompoundRelationDialog.this.setVisible(false);
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
	 * Get the arity the user selected.
	 * 
	 * @return the selected arity.
	 */
	public int getArity() {
		return this.arity.getNumber().intValue();
	}

	/**
	 * If the user ticked the parallel relation box, indicating that this
	 * relation should be treated as a fork point, this will return
	 * <tt>true</tt>.
	 * 
	 * @return <tt>true</tt> if the user ticked the parallel box.
	 */
	public boolean getParallel() {
		return this.parallel.isSelected();
	}

	private boolean validateFields() {
		// List of messages to display, if any are necessary.
		final List messages = new ArrayList();

		// Must enter something in the arity box.
		if (this.arity.getNumber() == null)
			messages.add(Resources.get("fieldIsEmpty", Resources.get("arity")));

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
