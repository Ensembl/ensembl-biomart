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

import java.util.Collection;
import java.util.Iterator;

import javax.swing.JComponent;
import javax.swing.JPopupMenu;

import org.biomart.builder.model.Column;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Table;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.view.gui.MartTabSet.MartTab;
import org.biomart.builder.view.gui.diagrams.ExplainTransformationDiagram.FakeSchema;
import org.biomart.builder.view.gui.diagrams.ExplainTransformationDiagram.FakeTable;
import org.biomart.builder.view.gui.diagrams.ExplainTransformationDiagram.RealisedRelation;
import org.biomart.builder.view.gui.diagrams.ExplainTransformationDiagram.RealisedTable;
import org.biomart.builder.view.gui.diagrams.components.TableComponent;

/**
 * This context is basically the same as {@link TransformationContext}, except
 * it only provides context menus and adaptations for {@link DataSetColumn}
 * instances.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.16 $, $Date: 2007-10-31 10:32:56 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class TransformationContext extends DataSetContext {

	/**
	 * Creates a new context that will adapt objects according to the settings
	 * in the specified dataset.
	 * 
	 * @param martTab
	 *            the mart tab this context appears in.
	 * @param dataset
	 *            the dataset this context will use for customising menus and
	 *            colours.
	 */
	public TransformationContext(final MartTab martTab, final DataSet dataset) {
		super(martTab, dataset);
	}

	public void customiseAppearance(final JComponent component,
			final Object object) {
		// Don't process fake schemas.
		if (object instanceof FakeSchema)
			return;

		// Normal background on fake tables.
		else if (object instanceof FakeTable)
			component.setBackground(TableComponent.BACKGROUND_COLOUR);

		// Convert tables to real tables then process.
		else if (object instanceof RealisedTable) {
			final Table actualTbl = ((RealisedTable) object).getTable();
			final ExplainContext explCon = ((RealisedTable) object)
					.getExplainContext();

			boolean allMasked = true;
			for (final Iterator i = ((RealisedTable) object).getColumns()
					.values().iterator(); allMasked && i.hasNext();)
				allMasked &= ((DataSetColumn) i.next()).isColumnMasked();

			// Call the ExplainContext method for this table.
			if (allMasked)
				component.setBackground(TableComponent.MASKED_COLOUR);
			else
				explCon.customiseAppearance(component, actualTbl);
		}

		// Convert relations to real relations then process.
		else if (object instanceof RealisedRelation) {
			final Relation actualRel = ((RealisedRelation) object)
					.getRelation();
			final int actualRelIt = ((RealisedRelation) object)
					.getRelationIteration();
			final ExplainContext explCon = ((RealisedRelation) object)
					.getExplainContext();
			// Call the ExplainContext method for this relation.
			explCon.customiseRelationAppearance(component, actualRel,
					actualRelIt);
		}

		// Just process everything else.
		else
			super.customiseAppearance(component, object);
	}

	public boolean isMasked(final Object object) {

		final String schemaPrefix = this.getMartTab()
				.getPartitionViewSelection();

		// Is it a column?
		if (object instanceof DataSetColumn) {
			final DataSetColumn dsCol = (DataSetColumn) object;
			if (dsCol.isColumnMasked()
					|| !dsCol.existsForPartition(schemaPrefix))
				return true;
		}

		return false;
	}

	public void populateContextMenu(final JPopupMenu contextMenu,
			final Object object) {
		// Don't process fake tables.
		if (object instanceof FakeTable || object instanceof FakeSchema)
			return;

		// Convert tables to real tables then process.
		else if (object instanceof RealisedTable) {
			final Table actualTbl = ((RealisedTable) object).getTable();
			final ExplainContext explCon = ((RealisedTable) object)
					.getExplainContext();
			// Call the ExplainContext method for this table.
			explCon.populateContextMenu(contextMenu, actualTbl);
		}

		// Convert relations to real relations then process.
		else if (object instanceof RealisedRelation) {
			final Relation actualRel = ((RealisedRelation) object)
					.getRelation();
			final int actualRelIt = ((RealisedRelation) object)
					.getRelationIteration();
			final ExplainContext explCon = ((RealisedRelation) object)
					.getExplainContext();
			// Call the ExplainContext method for this relation.
			explCon.populateRelationContextMenu(contextMenu, actualRel,
					actualRelIt);
		}

		// Just process everything else.
		else
			super.populateContextMenu(contextMenu, object);
	}

	public void populateMultiContextMenu(final JPopupMenu contextMenu,
			final Collection selectedItems, final Class clazz) {

		// Don't process anything except columns.
		if (Column.class.isAssignableFrom(clazz))
			super.populateMultiContextMenu(contextMenu, selectedItems, clazz);
	}

}
