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

package org.biomart.builder.view.gui.diagrams;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Iterator;

import org.biomart.builder.model.DataSet;
import org.biomart.builder.view.gui.MartTabSet.MartTab;
import org.biomart.builder.view.gui.diagrams.SchemaLayoutManager.SchemaLayoutConstraint;
import org.biomart.builder.view.gui.diagrams.components.DataSetComponent;

/**
 * This diagram draws a {@link DataSetComponent} for each dataset in a mart.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.20 $, $Date: 2007-10-31 10:32:56 $, modified by
 *          $Author: rh4 $
 * @since 0.6
 */
public class AllDataSetsDiagram extends Diagram {
	private static final long serialVersionUID = 1;

	private final PropertyChangeListener listener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			AllDataSetsDiagram.this.needsRecalc = true;
		}
	};

	private final PropertyChangeListener repaintListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			AllDataSetsDiagram.this.needsRepaint = true;
		}
	};

	/**
	 * The constructor creates the diagram and associates it with a given mart
	 * tab.
	 * 
	 * @param martTab
	 *            the mart tab to associate with this diagram. It will be used
	 *            to work out who receives all user menu events, etc.
	 */
	public AllDataSetsDiagram(final MartTab martTab) {
		super(new SchemaLayoutManager(), martTab);

		// Calculate the diagram.
		this.recalculateDiagram();

		// Listener to know when to recalculate entire diagram,
		// based on mart dataset entries.
		// If any change, whole diagram needs redoing from scratch,
		// and new listeners need setting up.
		martTab.getMart().getDataSets()
				.addPropertyChangeListener(this.listener);

		// Listen to when hide masked gets changed.
		martTab.getMart().addPropertyChangeListener("hideMaskedDataSets",
				this.repaintListener);

		this.setHideMasked(martTab.getMart().isHideMaskedDataSets());
	}

	protected void hideMaskedChanged(final boolean newHideMasked) {
		this.getMartTab().getMart().setHideMaskedDataSets(newHideMasked);
	}

	public void doRecalculateDiagram() {
		// Add a DataSetComponent for each dataset.
		for (final Iterator i = this.getMartTab().getMart().getDataSets()
				.values().iterator(); i.hasNext();) {
			final DataSet ds = (DataSet) i.next();
			final DataSetComponent dsComponent = new DataSetComponent(ds, this);
			this.add(dsComponent, new SchemaLayoutConstraint(0),
					Diagram.TABLE_LAYER);
		}
	}
}
