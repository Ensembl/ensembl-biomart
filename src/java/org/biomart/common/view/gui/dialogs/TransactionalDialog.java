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

import java.awt.Dialog;
import java.awt.Frame;
import java.awt.GraphicsConfiguration;
import java.awt.HeadlessException;

import javax.swing.JDialog;

import org.biomart.common.utils.Transaction;

/**
 * A dialog which starts a transaction when it opens, and ends it when it shuts.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.3 $, $Date: 2007-10-03 10:41:02 $, modified by 
 * 			$Author: rh4 $
 * @since 0.7
 */
public class TransactionalDialog extends JDialog {
	private static final long serialVersionUID = 1L;

	public void setVisible(final boolean visible) {
		this.setVisible(visible, false);
	}
	
	/**
	 * @see #setVisible(boolean)
	 * @param visible
	 * @see #setVisible(boolean)
	 * @param allowVisModChange
	 *            <tt>true</tt> if this dialog should reset visible changes.
	 */
	public void setVisible(final boolean visible,
			final boolean allowVisModChange) {
		if (visible)
			Transaction.start(allowVisModChange);
		super.setVisible(visible);
		if (!visible)
			Transaction.end();
	}

	/**
	 * @see JDialog#JDialog()
	 */
	public TransactionalDialog() throws HeadlessException {
		super();
	}

	/**
	 * @see JDialog#JDialog(Dialog, boolean)
	 */
	public TransactionalDialog(final Dialog owner, final boolean modal)
			throws HeadlessException {
		super(owner, modal);
	}

	/**
	 * @see JDialog#JDialog(Dialog, String, boolean, GraphicsConfiguration)
	 */
	public TransactionalDialog(final Dialog owner, final String title,
			final boolean modal, final GraphicsConfiguration gc)
			throws HeadlessException {
		super(owner, title, modal, gc);
	}

	/**
	 * @see JDialog#JDialog(Dialog, String, boolean)
	 */
	public TransactionalDialog(final Dialog owner, final String title,
			final boolean modal) throws HeadlessException {
		super(owner, title, modal);
	}

	/**
	 * @see JDialog#JDialog(Dialog, String)
	 */
	public TransactionalDialog(final Dialog owner, final String title)
			throws HeadlessException {
		super(owner, title);
	}

	/**
	 * @see JDialog#JDialog(Dialog)
	 */
	public TransactionalDialog(final Dialog owner) throws HeadlessException {
		super(owner);
	}

	/**
	 * @see JDialog#JDialog(Frame, boolean)
	 */
	public TransactionalDialog(final Frame owner, final boolean modal)
			throws HeadlessException {
		super(owner, modal);
	}

	/**
	 * @see JDialog#JDialog(Frame, String, boolean, GraphicsConfiguration)
	 */
	public TransactionalDialog(final Frame owner, final String title,
			final boolean modal, final GraphicsConfiguration gc) {
		super(owner, title, modal, gc);
	}

	/**
	 * @see JDialog#JDialog(Frame, String, boolean)
	 */
	public TransactionalDialog(final Frame owner, final String title,
			final boolean modal) throws HeadlessException {
		super(owner, title, modal);
	}

	/**
	 * @see JDialog#JDialog(Frame, String)
	 */
	public TransactionalDialog(final Frame owner, final String title)
			throws HeadlessException {
		super(owner, title);
	}

	/**
	 * @see JDialog#JDialog(Frame)
	 */
	public TransactionalDialog(final Frame owner) throws HeadlessException {
		super(owner);
	}

}
