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

import javax.swing.ImageIcon;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.biomart.common.resources.Resources;
import org.biomart.common.view.gui.OpenBrowser.OpenBrowserLabel;

/**
 * A dialog which shows the about info.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.9 $, $Date: 2007-10-03 10:41:02 $, modified by 
 * 			$Author: rh4 $
 * @since 0.6
 */
public class AboutDialog extends JDialog {
	private static final long serialVersionUID = 1;

	private AboutDialog() {
		// Create the base dialog.
		super();
		this.setTitle(Resources.get("aboutTitle", Resources.get("GUITitle",
				Resources.BIOMART_VERSION)));
		this.setModal(false);

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
		// Create some constraints for logos, except those on the last row
		// of the dialog.
		final GridBagConstraints logoConstraints = new GridBagConstraints();
		logoConstraints.gridwidth = GridBagConstraints.REMAINDER;
		logoConstraints.fill = GridBagConstraints.HORIZONTAL;
		logoConstraints.anchor = GridBagConstraints.LINE_START;
		logoConstraints.insets = new Insets(20, 20, 20, 20);
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

		// Logo
		JComponent item = new JLabel(new ImageIcon(Resources
				.getResourceAsURL("biomart-logo.gif")));
		content.add(item, logoConstraints);

		// Title: Resources.get("GUITitle")
		item = new JLabel(Resources.get("aboutAppTitle"));
		content.add(item, labelConstraints);
		item = new JLabel(Resources.get("plainGUITitle"));
		content.add(item, fieldConstraints);

		// Version: Resources.BIOMART_VERSION
		item = new JLabel(Resources.get("aboutVersion"));
		content.add(item, labelConstraints);
		item = new JLabel(Resources.BIOMART_VERSION);
		content.add(item, fieldConstraints);

		// Website: http://www.biomart.org/
		item = new JLabel(Resources.get("aboutWebsite"));
		content.add(item, labelConstraints);
		item = new OpenBrowserLabel(Resources.get("aboutWebsiteAddress"));
		content.add(item, fieldConstraints);

		// Contact: mart-dev@ebi.ac.uk
		item = new JLabel(Resources.get("aboutContact"));
		content.add(item, labelLastRowConstraints);
		item = new OpenBrowserLabel(Resources.get("aboutContactAddress"),
				"mailto:" + Resources.get("aboutContactAddress"));
		content.add(item, fieldLastRowConstraints);

		// Set size of window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	/**
	 * Shows a window displaying details about this app.
	 */
	public static void displayAbout() {
		// Create and show a window frame.
		(new AboutDialog()).setVisible(true);
	}
}
