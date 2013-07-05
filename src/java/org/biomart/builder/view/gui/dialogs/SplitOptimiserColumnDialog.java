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

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.ListCellRenderer;

import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.SplitOptimiserColumnDef;
import org.biomart.common.resources.Resources;

/**
 * A dialog which allows the user to split an optimiser column and optionally
 * specify a column to provide values for it.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.6 $, $Date: 2008-03-12 14:22:38 $, modified by $Author:
 *          rh4 $
 * @since 0.7
 */
public class SplitOptimiserColumnDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private JComboBox column;

	private JTextField separator;

	private JTextField size;

	private JCheckBox split;

	private JCheckBox prefix;

	private JCheckBox suffix;

	/**
	 * Pop up a dialog to define the input.
	 * 
	 * @param isSplit
	 *            is it already split?
	 * @param split
	 *            the existing split def, if any.
	 * @param columnOptions
	 *            the columns the user can choose from.
	 */
	public SplitOptimiserColumnDialog(final boolean isSplit,
			final SplitOptimiserColumnDef split, final Map columnOptions) {
		// Create the base dialog.
		super();
		this.setTitle(Resources.get("splitOptimiserColumnDialogTitle"));
		this.setModal(true);

		final Object colSelect = split == null ? Resources
				.get("splitOptimiserNoContentCol") : columnOptions.get(split
				.getContentCol());

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
		this.split = new JCheckBox(Resources.get("splitOptimiserEnableLabel"));
		this.split.setSelected(isSplit);

		// Set up the suffix/prefix.
		this.prefix = new JCheckBox(Resources.get("splitOptimiserPrefixLabel"));
		this.prefix.setSelected(split == null || split.isPrefix());
		this.suffix = new JCheckBox(Resources.get("splitOptimiserSuffixLabel"));
		this.suffix.setSelected(split == null || split.isSuffix());

		// Set up the separator.
		this.separator = new JTextField(3);
		if (split != null)
			this.separator.setText(split.getSeparator());
		this.size = new JTextField(4);
		this.size.setText(split == null ? "255" : "" + split.getSize());

		// Set up the combo box of columns.
		// Same as ColumnString except slightly modified to show
		// the modifiedName instead of the plain name.
		this.column = new JComboBox();
		this.column.addItem(Resources.get("splitOptimiserNoContentCol"));
		for (final Iterator i = columnOptions.values().iterator(); i.hasNext();)
			this.column.addItem(i.next());
		this.column.setEnabled(isSplit);
		this.column.setSelectedItem(colSelect);
		this.column.setRenderer(new ListCellRenderer() {
			public Component getListCellRendererComponent(final JList list,
					final Object value, final int index,
					final boolean isSelected, final boolean cellHasFocus) {
				String textToDisplay = null;
				if (value instanceof String)
					textToDisplay = (String) value;
				else if (value instanceof DataSetColumn)
					textToDisplay = ((DataSetColumn) value).getModifiedName();
				final JLabel label = new JLabel();
				if (textToDisplay != null)
					label.setText(textToDisplay);
				label.setOpaque(true);
				label.setFont(list.getFont());
				if (isSelected) {
					label.setBackground(list.getSelectionBackground());
					label.setForeground(list.getSelectionForeground());
				} else {
					label.setBackground(list.getBackground());
					label.setForeground(list.getForeground());
				}
				return label;
			}
		});

		// The close and execute buttons.
		final JButton close = new JButton(Resources.get("closeButton"));
		final JButton execute = new JButton(Resources.get("updateButton"));

		// Input fields.
		JPanel field = new JPanel();
		field.add(this.split);
		content.add(field, fieldConstraints);

		// Parallel button.
		field = new JPanel();
		field.add(new JLabel(Resources.get("splitOptimiserContentLabel")));
		field.add(this.column);
		content.add(field, fieldConstraints);

		// Separator field.
		field = new JPanel();
		field.add(new JLabel(Resources.get("splitOptimiserSeparatorLabel")));
		field.add(this.separator);
		content.add(field, fieldConstraints);

		// Prefix button.
		field = new JPanel();
		field.add(this.prefix);
		content.add(field, fieldConstraints);

		// Suffix button.
		field = new JPanel();
		field.add(this.suffix);
		content.add(field, fieldConstraints);

		// Separator field.
		field = new JPanel();
		field.add(new JLabel(Resources.get("splitOptimiserSizeLabel")));
		field.add(this.size);
		content.add(field, fieldConstraints);

		// Close/Execute buttons at the bottom.
		field = new JPanel();
		field.add(close);
		field.add(execute);
		content.add(field, fieldLastRowConstraints);

		// Intercept the checkbox.
		this.split.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SplitOptimiserColumnDialog.this.column
						.setEnabled(SplitOptimiserColumnDialog.this.split
								.isSelected());
				SplitOptimiserColumnDialog.this.separator
						.setEnabled(SplitOptimiserColumnDialog.this.split
								.isSelected());
				SplitOptimiserColumnDialog.this.size
						.setEnabled(SplitOptimiserColumnDialog.this.split
								.isSelected());
				SplitOptimiserColumnDialog.this.prefix
						.setEnabled(SplitOptimiserColumnDialog.this.split
								.isSelected());
				SplitOptimiserColumnDialog.this.suffix
						.setEnabled(SplitOptimiserColumnDialog.this.split
								.isSelected());
				if (!SplitOptimiserColumnDialog.this.split.isSelected()) {
					SplitOptimiserColumnDialog.this.column
							.setSelectedItem(null);
					SplitOptimiserColumnDialog.this.separator.setText(null);
					SplitOptimiserColumnDialog.this.size.setText(null);
					SplitOptimiserColumnDialog.this.prefix.setSelected(true);
					SplitOptimiserColumnDialog.this.suffix.setSelected(true);
				}
			}
		});

		// Intercept the close button, which closes the dialog
		// without taking any action.
		close.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				// Reset to default value.
				SplitOptimiserColumnDialog.this.split.setSelected(isSplit);
				SplitOptimiserColumnDialog.this.separator
						.setText(split == null ? null : split.getSeparator());
				SplitOptimiserColumnDialog.this.size
						.setText(split == null ? null : "" + split.getSize());
				SplitOptimiserColumnDialog.this.column
						.setSelectedItem(colSelect);
				SplitOptimiserColumnDialog.this.prefix
						.setSelected(split == null || split.isPrefix());
				SplitOptimiserColumnDialog.this.suffix
						.setSelected(split == null || split.isSuffix());
				SplitOptimiserColumnDialog.this.setVisible(false);
			}
		});

		// Intercept the execute button, which validates the fields
		// then closes the dialog.
		execute.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (SplitOptimiserColumnDialog.this.validateFields())
					SplitOptimiserColumnDialog.this.setVisible(false);
			}
		});

		// Make execute the default button.
		this.getRootPane().setDefaultButton(execute);

		// Set size of window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	private String getContentCol() {
		return this.column.getSelectedItem().equals(
				Resources.get("splitOptimiserNoContentCol")) ? null
				: ((DataSetColumn) this.column.getSelectedItem()).getName();
	}

	private String getSeparator() {
		return this.separator.getText();
	}

	/**
	 * Obtain the defined definition.
	 * 
	 * @return the definition.
	 */
	public SplitOptimiserColumnDef getSplitOptimiserColumnDef() {
		if (!this.isSplit())
			return null;
		final SplitOptimiserColumnDef def = new SplitOptimiserColumnDef(this
				.getContentCol(), this.getSeparator());
		def.setPrefix(this.prefix.isSelected());
		def.setSuffix(this.suffix.isSelected());
		int newSize = 255;
		try {
			newSize = Integer.parseInt(this.size.getText());
		} catch (final NumberFormatException ne) {
			newSize = 255;
		}
		def.setSize(newSize);
		return def;
	}

	/**
	 * If the user ticked the loopback relation box, this will return
	 * <tt>true</tt>.
	 * 
	 * @return <tt>true</tt> if the user ticked the loopback box.
	 */
	public boolean isSplit() {
		return this.split.isSelected();
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
