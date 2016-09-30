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

package org.biomart.builder.view.gui.diagrams.contexts;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Collection;

import javax.swing.JComponent;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;

import org.biomart.builder.model.DataSet;
import org.biomart.builder.view.gui.MartTabSet.MartTab;
import org.biomart.builder.view.gui.diagrams.components.DataSetComponent;
import org.biomart.common.resources.Resources;

/**
 * Provides the context menus and colour schemes to use when viewing the all
 * datasets tab.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.33 $, $Date: 2007-11-28 14:38:15 $, modified by
 *          $Author: rh4 $
 * @since 0.6
 */
public class AllDataSetsContext implements DiagramContext {
	private MartTab martTab;

	/**
	 * Creates a new context which will pass any menu actions onto the given
	 * mart tab.
	 * 
	 * @param martTab
	 *            the mart tab which will receive any menu actions the user
	 *            selects.
	 */
	public AllDataSetsContext(final MartTab martTab) {
		this.martTab = martTab;
	}

	/**
	 * Obtain the mart tab to pass menu events onto.
	 * 
	 * @return the mart tab this context is attached to.
	 */
	protected MartTab getMartTab() {
		return this.martTab;
	}

	public void customiseAppearance(final JComponent component,
			final Object object) {
		if (object instanceof DataSet) {
			final DataSet ds = (DataSet) object;
			final DataSetComponent dscomp = (DataSetComponent) component;

			// Set the background colour.
			if (ds.isPartitionTable())
				dscomp.setBackground(DataSetComponent.PARTITION_BACKGROUND);
			else if (this.isMasked(ds))
				dscomp.setBackground(DataSetComponent.MASKED_BACKGROUND);
			else if (ds.isInvisible())
				dscomp.setBackground(DataSetComponent.INVISIBLE_BACKGROUND);
			else
				dscomp.setBackground(DataSetComponent.VISIBLE_BACKGROUND);

			// Update dotted line (partitioned).
			dscomp.setRestricted(((DataSet) object)
					.getPartitionTableApplication() != null);

			dscomp.setRenameable(true);
			dscomp.setSelectable(true);
		}
	}

	public boolean isMasked(final Object object) {

		final String schemaPrefix = this.getMartTab()
				.getPartitionViewSelection();

		if (object instanceof DataSet) {
			final DataSet ds = (DataSet) object;
			if (ds.isMasked() || ds.getMainTable() == null
					|| !ds.getMainTable().existsForPartition(schemaPrefix))
				return true;
		}

		return false;
	}

	public void populateMultiContextMenu(final JPopupMenu contextMenu,
			final Collection selectedItems, final Class clazz) {
		// Nothing else to do.
	}

	public void populateContextMenu(final JPopupMenu contextMenu,
			final Object object) {

		if (object instanceof DataSet) {
			if (contextMenu.getComponentCount() > 0)
				contextMenu.addSeparator();

			final DataSet ds = (DataSet) object;
			// Accept/Reject changes - only enabled if dataset table
			// is visible modified.
			final JMenuItem accept = new JMenuItem(Resources
					.get("acceptChangesTitle"));
			accept
					.setMnemonic(Resources.get("acceptChangesMnemonic").charAt(
							0));
			accept.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent evt) {
					AllDataSetsContext.this.getMartTab().getDataSetTabSet()
							.requestAcceptAll(ds, null);
				}
			});
			accept.setEnabled(ds.isVisibleModified());
			contextMenu.add(accept);

			final JMenuItem reject = new JMenuItem(Resources
					.get("rejectChangesTitle"));
			reject
					.setMnemonic(Resources.get("rejectChangesMnemonic").charAt(
							0));
			reject.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent evt) {
					AllDataSetsContext.this.getMartTab().getDataSetTabSet()
							.requestRejectAll(ds, null);
				}
			});
			reject.setEnabled(ds.isVisibleModified());
			contextMenu.add(reject);

			contextMenu.addSeparator();

			final JMenuItem replicate = new JMenuItem(Resources
					.get("replicateDataSetTitle"));
			replicate.setMnemonic(Resources.get("replicateDataSetMnemonic")
					.charAt(0));
			replicate.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent evt) {
					AllDataSetsContext.this.getMartTab().getDataSetTabSet()
							.requestReplicateDataSet(ds);
				}
			});
			contextMenu.add(replicate);

		}
	}
}
