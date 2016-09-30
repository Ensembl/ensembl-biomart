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

package org.biomart.builder.view.gui.diagrams.components;

import java.awt.Color;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Iterator;

import javax.swing.ImageIcon;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.JTextField;

import org.biomart.builder.model.Key;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Table;
import org.biomart.builder.view.gui.diagrams.Diagram;
import org.biomart.common.resources.Resources;

/**
 * A diagram component that represents a schema. It usually only has a label in
 * it, but if the schema has any external relations, then the tables with those
 * relations will appear in full using {@link TableComponent}s.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.35 $, $Date: 2008-02-19 13:27:29 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class SchemaComponent extends BoxShapedComponent {
	private static final long serialVersionUID = 1;

	private static final Color TRANSPARENT_COLOR = new Color(0, 0, 0, 0);

	/**
	 * Normal background colour.
	 */
	public static final Color BACKGROUND_COLOUR = Color.YELLOW;

	/**
	 * Background for masked schemas.
	 */
	public static Color MASKED_BACKGROUND = Color.LIGHT_GRAY;

	private static final Font BOLD_FONT = Font.decode("SansSerif-BOLD-10");

	private GridBagConstraints constraints;

	private GridBagLayout layout;

	private final PropertyChangeListener repaintListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent e) {
			SchemaComponent.this.needsRepaint = true;
		}
	};

	private final PropertyChangeListener recalcListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent e) {
			SchemaComponent.this.needsRecalc = true;
		}
	};

	/**
	 * Constructs a schema diagram component in the given diagram that displays
	 * details of a particular schema.
	 * 
	 * @param schema
	 *            the schema to display details of.
	 * @param diagram
	 *            the diagram to display the details in.
	 */
	public SchemaComponent(final Schema schema, final Diagram diagram) {
		super(schema, diagram);

		// Schema components are set out in a vertical list.
		this.layout = new GridBagLayout();
		this.setLayout(this.layout);

		// Constraints for each part of the schema component.
		this.constraints = new GridBagConstraints();
		this.constraints.gridwidth = GridBagConstraints.REMAINDER;
		this.constraints.fill = GridBagConstraints.HORIZONTAL;
		this.constraints.anchor = GridBagConstraints.CENTER;
		this.constraints.insets = new Insets(5, 5, 5, 5);

		// Set the background colour.
		this.setBackground(SchemaComponent.BACKGROUND_COLOUR);

		// Calculate the components and add them to the list.
		this.recalculateDiagramComponent();

		// Repaint events.
		schema
				.addPropertyChangeListener("directModified",
						this.repaintListener);

		// Recalc events.
		schema.addPropertyChangeListener("name", this.recalcListener);
		schema.getRelations().addPropertyChangeListener(this.recalcListener);
	}

	private Schema getSchema() {
		return (Schema) this.getObject();
	}

	protected void processMouseEvent(final MouseEvent evt) {
		boolean eventProcessed = false;
		// Is it a right-click?
		if (evt.getButton() == MouseEvent.BUTTON1 && evt.getClickCount() >= 2) {
			final int index = SchemaComponent.this.getDiagram().getMartTab()
					.getSchemaTabSet().indexOfTab(
							SchemaComponent.this.getSchema().getName());
			SchemaComponent.this.getDiagram().getMartTab().getSchemaTabSet()
					.setSelectedIndex(index);
			// Mark as handled.
			eventProcessed = true;
		}
		// Pass it on up if we're not interested.
		if (!eventProcessed)
			super.processMouseEvent(evt);
	}

	public JPopupMenu getContextMenu() {
		// First of all, work out what would have been shown by default.
		final JPopupMenu contextMenu = super.getContextMenu();

		// Add the 'show tables' option, which opens the tab representing
		// this schema.
		final JMenuItem showTables = new JMenuItem(Resources
				.get("showTablesTitle"));
		showTables.setMnemonic(Resources.get("showTablesMnemonic").charAt(0));
		showTables.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				final int index = SchemaComponent.this.getDiagram()
						.getMartTab().getSchemaTabSet().indexOfTab(
								SchemaComponent.this.getSchema().getName());
				SchemaComponent.this.getDiagram().getMartTab()
						.getSchemaTabSet().setSelectedIndex(index);
			}
		});
		contextMenu.add(showTables);

		// Separator
		contextMenu.addSeparator();

		// Update menu option.
		final JMenuItem update = new JMenuItem(Resources
				.get("updateSchemaTitle"), new ImageIcon(Resources
				.getResourceAsURL("refresh.gif")));
		update.setMnemonic(Resources.get("updateSchemaMnemonic").charAt(0));
		update.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				SchemaComponent.this.getDiagram().getMartTab()
						.getSchemaTabSet().requestModifySchema(
								SchemaComponent.this.getSchema());
			}
		});
		contextMenu.add(update);

		// Add a separator.
		contextMenu.addSeparator();

		// Add an option to rename this dataset.
		final JMenuItem rename = new JMenuItem(Resources
				.get("renameSchemaTitle"));
		rename.setMnemonic(Resources.get("renameSchemaMnemonic").charAt(0));
		rename.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				SchemaComponent.this.getDiagram().getMartTab()
						.getSchemaTabSet().requestRenameSchema(
								SchemaComponent.this.getSchema());
			}
		});
		contextMenu.add(rename);

		// Option to remove the dataset from the mart.
		final JMenuItem remove = new JMenuItem(Resources
				.get("removeSchemaTitle"), new ImageIcon(Resources
				.getResourceAsURL("cut.gif")));
		remove.setMnemonic(Resources.get("removeSchemaMnemonic").charAt(0));
		remove.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				SchemaComponent.this.getDiagram().getMartTab()
						.getSchemaTabSet().requestRemoveSchema(
								SchemaComponent.this.getSchema());
			}
		});
		contextMenu.add(remove);

		// Separator
		contextMenu.addSeparator();

		// Masked menu option.
		final JMenuItem masked = new JCheckBoxMenuItem(Resources
				.get("maskedSchemaTitle"));
		masked.setMnemonic(Resources.get("maskedSchemaMnemonic").charAt(0));
		masked.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				SchemaComponent.this.getDiagram().getMartTab()
						.getSchemaTabSet().requestMaskSchema(
								SchemaComponent.this.getSchema(),
								masked.isSelected());
			}
		});
		masked.setSelected(this.getSchema().isMasked());
		contextMenu.add(masked);

		// Return it. Will be further adapted by a listener elsewhere.
		return contextMenu;
	}

	protected void doRecalculateDiagramComponent() {
		// Clear subcomponents.
		this.getSubComponents().clear();

		// Add the label for the schema name,
		final JTextField name = new JTextField() {
			private static final long serialVersionUID = 1L;

			private Color opaqueBackground;

			// work around transparency issue in OS X 10.5
			public void setOpaque(boolean opaque) {
				if (opaque != isOpaque()) {
					if (opaque) {
						super.setBackground(opaqueBackground);
					} else if (opaqueBackground != null) {
						opaqueBackground = getBackground();
						super.setBackground(TRANSPARENT_COLOR);
					}
				}
				super.setOpaque(opaque);
			}

			// work around transparency issue in OS X 10.5
			public void setBackground(Color color) {
				if (isOpaque()) {
					super.setBackground(color);
				} else {
					opaqueBackground = color;
				}
			}
		};
		name.setFont(SchemaComponent.BOLD_FONT);
		this.setRenameTextField(name);
		this.layout.setConstraints(name, this.constraints);
		this.add(name);

		// Now add any tables with external relations. Loop through the
		// external keys to identify the tables to do this.
		for (final Iterator i = this.getSchema().getRelations().iterator(); i
				.hasNext();) {
			final Relation rel = (Relation) i.next();

			if (!rel.isExternal())
				continue;

			final Key key = rel.getKeyForSchema(this.getSchema());
			final Table table = key.getTable();

			// Only add the table if it's not already added!
			if (!this.getSubComponents().containsKey(table)) {

				// Create the table component that represents this table.
				final TableComponent tableComponent = new TableComponent(table,
						this.getDiagram());

				// Remember, internally, the subcomponents of this table, as
				// well as the table itself as a subcomponent.
				this.addSubComponent(table, tableComponent);
				this.getSubComponents().putAll(
						tableComponent.getSubComponents());

				// Add the table component to our layout.
				this.layout.setConstraints(tableComponent, this.constraints);
				this.add(tableComponent);
			}
		}
	}

	public void performRename(final String newName) {
		this.getDiagram().getMartTab().getSchemaTabSet().requestRenameSchema(
				this.getSchema(), newName);
	}

	public String getEditableName() {
		return this.getSchema().getName();
	}

	public String getDisplayName() {
		final StringBuffer name = new StringBuffer();
		if (this.getSchema().getPartitionRegex() != null
				&& !this.getSchema().getPartitionRegex().equals("")) {
			name.append(" (");
			name.append(Resources.get("partitionedSchemaMessage"));
			name.append(')');
		}
		name.append(this.getEditableName());
		return name.toString();
	}
}
