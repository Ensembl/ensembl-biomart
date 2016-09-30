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

import java.awt.LayoutManager;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Table;
import org.biomart.builder.view.gui.MartTabSet.MartTab;
import org.biomart.builder.view.gui.diagrams.SchemaLayoutManager.SchemaLayoutConstraint;
import org.biomart.builder.view.gui.diagrams.components.RelationComponent;
import org.biomart.builder.view.gui.diagrams.components.TableComponent;

/**
 * Displays the contents of a schema within a diagram object. It adds a series
 * of {@link TableComponent} and {@link RelationComponent} objects when the
 * diagram is recalculated, and treats the schema object it represents as the
 * basic background object of the diagram.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.26 $, $Date: 2007-10-31 10:32:56 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class SchemaDiagram extends Diagram {
	private static final long serialVersionUID = 1;

	private Schema schema;

	private final PropertyChangeListener listener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			SchemaDiagram.this.needsRecalc = true;
		}
	};

	private final PropertyChangeListener repaintListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			SchemaDiagram.this.needsRepaint = true;
		}
	};

	/**
	 * Creates a new diagram that displays the tables and relations inside a
	 * specific schema.
	 * 
	 * @param layout
	 *            the layout manager to use to display the diagram.
	 * @param martTab
	 *            the tab within which this diagram appears.
	 * @param schema
	 *            the schema to draw in this diagram.
	 */
	public SchemaDiagram(final LayoutManager layout, final MartTab martTab,
			final Schema schema) {
		// Call the general diagram constructor first.
		super(layout, martTab);

		// Remember the schema, then lay it out.
		this.schema = schema;
		this.recalculateDiagram();

		// Listener to know when to recalculate entire diagram,
		// based on tables in schema, and keys+relations on those
		// tables (presence/absence only).
		// If any change, whole diagram needs redoing from scratch,
		// and new listeners need setting up.
		schema.getTables().addPropertyChangeListener(this.listener);
		schema.getRelations().addPropertyChangeListener(this.listener);

		// Listen to when hide masked gets changed or gets renamed.
		schema.addPropertyChangeListener("hideMasked", this.repaintListener);
		schema.addPropertyChangeListener("name", this.listener);

		this.setHideMasked(schema.isHideMasked());
	}

	/**
	 * Creates a new diagram that displays the tables and relations inside a
	 * specific schema. Uses a default layout specified by {@link Diagram}.
	 * 
	 * @param martTab
	 *            the tab within which this diagram appears.
	 * @param schema
	 *            the schema to draw in this diagram.
	 */
	public SchemaDiagram(final MartTab martTab, final Schema schema) {
		// Call the general diagram constructor first.
		this(new SchemaLayoutManager(), martTab, schema);
	}

	protected void hideMaskedChanged(final boolean newHideMasked) {
		this.getSchema().setHideMasked(newHideMasked);
	}

	public void doRecalculateDiagram() {
		// Add a TableComponent for each table in the schema.
		final Set usedRels = new HashSet();
		for (final Iterator i = this.getSchema().getTables().values()
				.iterator(); i.hasNext();) {
			final Table t = (Table) i.next();
			final Collection tRels = new HashSet();
			int indent = 0;
			for (final Iterator j = t.getRelations().iterator(); j.hasNext();) {
				final Relation rel = (Relation) j.next();
				if (!rel.isExternal() && !usedRels.contains(rel)) {
					tRels.add(rel);
					this.add(new RelationComponent(rel, this),
							new SchemaLayoutConstraint(indent++),
							Diagram.RELATION_LAYER);
					usedRels.add(rel);
				}
			}
			this.add(new TableComponent(t, this), new SchemaLayoutConstraint(
					tRels.size()), Diagram.TABLE_LAYER);
		}
	}

	/**
	 * Returns the schema that this diagram represents.
	 * 
	 * @return the schema this diagram represents.
	 */
	public Schema getSchema() {
		return this.schema;
	}
}
