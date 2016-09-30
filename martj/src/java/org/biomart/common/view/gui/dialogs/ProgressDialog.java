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

package org.biomart.common.view.gui.dialogs;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JPanel;
import javax.swing.JProgressBar;

import org.biomart.common.resources.Resources;

/**
 * A dialog which shows progress. Similar to ProgressMonitor in the Swing API,
 * but this one actually works.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.5 $, $Date: 2007-10-31 10:32:56 $, modified by 
 * 			$Author: rh4 $
 * @since 0.7
 */
public class ProgressDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private final JProgressBar progress;

	private boolean canceled = false;

	/**
	 * Create a new progress dialog.
	 * 
	 * @param parent
	 *            the parent dialog to centre over.
	 * @param min
	 *            the minimum value of the progress bar.
	 * @param max
	 *            the maximum value of the progress bar.
	 * @param showCancel
	 *            <tt>true</tt> if a cancel button should be shown.
	 */
	public ProgressDialog(final JComponent parent, final int min,
			final int max, final boolean showCancel) {
		// Create the base dialog.
		super();
		this.setModal(false);
		this.setUndecorated(true);
		this.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);

		// Create the content pane for the dialog, ie. the bit that will hold
		// all the various questions and answers.
		final JPanel content = new JPanel(new GridBagLayout());
		this.setContentPane(content);

		// Create some constraints for fields, except those on the last row
		// of the dialog.
		final GridBagConstraints fieldConstraints = new GridBagConstraints();
		fieldConstraints.gridwidth = GridBagConstraints.REMAINDER;
		fieldConstraints.fill = GridBagConstraints.NONE;
		fieldConstraints.anchor = GridBagConstraints.LINE_START;
		fieldConstraints.insets = new Insets(10, 10, 10, 10);
		// Create some constraints for fields on the last row of the dialog.
		final GridBagConstraints fieldLastRowConstraints = (GridBagConstraints) fieldConstraints
				.clone();
		fieldLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;

		// Progress bar.
		this.progress = new JProgressBar(min, max);
		this.progress.setOrientation(JProgressBar.HORIZONTAL);
		this.progress.setBorderPainted(true);
		this.progress.setString(null);
		this.progress.setIndeterminate(true);
		this.progress.setValue(0);
		this.progress.setStringPainted(false);
		content.add(this.progress, showCancel ? fieldConstraints
				: fieldLastRowConstraints);

		// Cancel button.
		if (showCancel) {
			final JButton cancel = new JButton(Resources.get("cancelButton"));
			cancel.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent evt) {
					ProgressDialog.this.canceled = true;
					cancel.setEnabled(false);
				}
			});
			this.addWindowListener(new WindowAdapter() {
				public void windowClosing(final WindowEvent evt) {
					ProgressDialog.this.canceled = true;
				}
			});
			content.add(cancel, fieldLastRowConstraints);
		}

		// Set size of window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(parent);
	}

	/**
	 * Is the box canceled by the user?
	 * 
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isCanceled() {
		return this.canceled;
	}

	/**
	 * Update the progress.
	 * 
	 * @param progress
	 *            the new progress value.
	 */
	public void setProgress(final int progress) {
		if (this.progress.isIndeterminate())
			this.progress.setIndeterminate(false);
		this.progress.setValue(progress);
	}
}
