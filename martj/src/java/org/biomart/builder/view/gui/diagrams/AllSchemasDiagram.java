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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Schema;
import org.biomart.builder.view.gui.MartTabSet.MartTab;
import org.biomart.builder.view.gui.diagrams.SchemaLayoutManager.SchemaLayoutConstraint;
import org.biomart.builder.view.gui.diagrams.components.RelationComponent;
import org.biomart.builder.view.gui.diagrams.components.SchemaComponent;

/**
 * This diagram draws a {@link SchemaComponent} for each schema in the mart. If
 * any of them have external relations to other schemas, then a
 * {@link RelationComponent} is drawn between them, and implicitly this causes
 * the table that the relation links from to be added inside the appropriate
 * schema component.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.25 $, $Date: 2007-10-31 10:32:56 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class AllSchemasDiagram extends Diagram {
	private static final long serialVersionUID = 1;

	private final PropertyChangeListener listener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			AllSchemasDiagram.this.needsRecalc = true;
		}
	};

	private final PropertyChangeListener repaintListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			AllSchemasDiagram.this.needsRepaint = true;
		}
	};

	/**
	 * The constructor creates the diagram and associates it with a given mart
	 * tab.
	 * 
	 * @param martTab
	 *            the mart tab to associate with this schema. It will be used to
	 *            work out who receives all user menu events, etc.
	 */
	public AllSchemasDiagram(final MartTab martTab) {
		super(new SchemaLayoutManager(), martTab);

		// Calculate the diagram.
		this.recalculateDiagram();

		// Listener to know when to recalculate entire diagram,
		// based on tables in schema, and keys+relations on those
		// tables (presence/absence only).
		// If any change, whole diagram needs redoing from scratch,
		// and new listeners need setting up.
		martTab.getMart().getSchemas().addPropertyChangeListener(this.listener);

		// Listen to when hide masked gets changed.
		martTab.getMart().addPropertyChangeListener("hideMaskedSchemas",
				this.repaintListener);

		this.setHideMasked(martTab.getMart().isHideMaskedSchemas());
	}

	protected void hideMaskedChanged(final boolean newHideMasked) {
		this.getMartTab().getMart().setHideMaskedSchemas(newHideMasked);
	}

	public void doRecalculateDiagram() {
		// Add a SchemaComponent for each schema.
		final Set usedRels = new HashSet();
		for (final Iterator i = this.getMartTab().getMart().getSchemas()
				.values().iterator(); i.hasNext();) {
			final Schema schema = (Schema) i.next();
			final SchemaComponent schemaComponent = new SchemaComponent(schema,
					this);
			// Count and remember relations.
			int indent = 0;
			final Collection extRels = new HashSet();
			for (final Iterator j = schema.getRelations().iterator(); j
					.hasNext();) {
				final Relation rel = (Relation) j.next();
				if (rel.isExternal() && !usedRels.contains(rel)) {
					extRels.add(rel);
					this.add(new RelationComponent(rel, this),
							new SchemaLayoutConstraint(indent++),
							Diagram.RELATION_LAYER);
					usedRels.add(rel);
				}
			}
			this.add(schemaComponent,
					new SchemaLayoutConstraint(extRels.size()),
					Diagram.TABLE_LAYER);
			// Update ourselves when relations are added or removed.
			schema.getRelations().addPropertyChangeListener(this.listener);
		}
	}
}
