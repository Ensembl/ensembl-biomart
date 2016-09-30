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
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.biomart.builder.model.Column;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Key;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Table;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.DataSet.DataSetTableType;
import org.biomart.builder.view.gui.diagrams.Diagram;
import org.biomart.builder.view.gui.diagrams.ExplainTransformationDiagram.FakeTable;
import org.biomart.builder.view.gui.diagrams.ExplainTransformationDiagram.RealisedTable;
import org.biomart.common.resources.Resources;

/**
 * Table components are box-shaped, and represent an individual table. Inside
 * them may appear a number of key or column components, and a button which
 * shows or hides the columns. They have a label indicating their full name, and
 * a secondary label indicating which schema they belong to.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.49 $, $Date: 2008-02-29 11:09:44 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class TableComponent extends BoxShapedComponent {
	private static final long serialVersionUID = 1;

	private static final Color TRANSPARENT_COLOR = new Color(0, 0, 0, 0);

	/**
	 * Background colour for all normal tables.
	 */
	public static final Color BACKGROUND_COLOUR = Color.PINK;

	/**
	 * Background colour for masked tables.
	 */
	public static Color MASKED_COLOUR = Color.LIGHT_GRAY;

	/**
	 * Background colour for unrolled tables.
	 */
	public static Color UNROLLED_COLOUR = Color.CYAN;

	private static final Color NORMAL_COLOUR = Color.BLACK;

	/**
	 * Background colour for all ignored tables.
	 */
	public static Color IGNORE_COLOUR = Color.LIGHT_GRAY;

	private static final Font ITALIC_FONT = Font.decode("SansSerif-ITALIC-10");

	private static final Font BOLD_FONT = Font.decode("SansSerif-BOLD-10");

	private JComponent columnsListPanel;

	private GridBagConstraints constraints;

	private GridBagLayout layout;

	private JButton showHide;

	private boolean hidingMaskedCols = false;

	private final PropertyChangeListener repaintListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent e) {
			TableComponent.this.needsRepaint = true;
		}
	};

	private final PropertyChangeListener recalcListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent e) {
			TableComponent.this.needsRecalc = true;
		}
	};

	/**
	 * This constructor makes a new table component, associated with a
	 * particular table, and remembers that this component appears in a
	 * particular diagram. All operations on the component will be related back
	 * to that diagram where necessary.
	 * 
	 * @param table
	 *            the table we wish to represent in the diagram.
	 * @param diagram
	 *            the diagram we wish to make the table appear in.
	 */
	public TableComponent(final Table table, final Diagram diagram) {
		super(table, diagram);

		// Table components are set out in a vertical list.
		this.layout = new GridBagLayout();
		this.setLayout(this.layout);

		// Constraints for each component within the table component.
		this.constraints = new GridBagConstraints();
		this.constraints.gridwidth = GridBagConstraints.REMAINDER;
		this.constraints.fill = GridBagConstraints.HORIZONTAL;
		this.constraints.anchor = GridBagConstraints.CENTER;
		this.constraints.insets = new Insets(0, 2, 0, 2);

		// Set the background colour.
		this.setForeground(TableComponent.NORMAL_COLOUR);

		// Draw our contents.
		this.recalculateDiagramComponent();

		// Repaint events.
		table.addPropertyChangeListener("directModified", this.repaintListener);
		// Listen to all relations on this table and repaint when needed.
		// We don't need to monitor relations themselves as the entire
		// diagram gets recalculated if they change.
		for (final Iterator i = table.getRelations().iterator(); i.hasNext();) {
			final Relation rel = (Relation) i.next();
			rel.addPropertyChangeListener("directModified",
					this.repaintListener);
		}
		// If this is a dataset table, listen to the partition table
		// conversion and merge/unrolled signals and repaint on that too.
		if (table instanceof DataSetTable) {
			final DataSetTable dsTbl = (DataSetTable) table;
			if (dsTbl.getType().equals(DataSetTableType.MAIN)) {
				dsTbl.getDataSet().addPropertyChangeListener(
						"partitionTableApplication", this.repaintListener);
				dsTbl.getDataSet().addPropertyChangeListener("partitionTable",
						this.repaintListener);
			} else {
				dsTbl.addPropertyChangeListener("partitionTableApplication",
						this.repaintListener);
				dsTbl.getFocusRelation().addPropertyChangeListener(
						"mergeRelation", this.repaintListener);
				dsTbl.getFocusRelation().addPropertyChangeListener(
						"unrolledRelation", this.repaintListener);
			}
		}

		// Recalc events.
		table.addPropertyChangeListener("tableRename", this.recalcListener);
		table.getKeys().addPropertyChangeListener(this.recalcListener);
		table.getColumns().addPropertyChangeListener(this.recalcListener);
	}

	/**
	 * Gets the table this component is representing.
	 * 
	 * @return the table we represent.
	 */
	public Table getTable() {
		return (Table) this.getObject();
	}

	/**
	 * Is this table component hiding masked columns currently?
	 * 
	 * @return <tt>true</tt> if it is.
	 */
	public boolean isHidingMaskedCols() {
		return this.hidingMaskedCols;
	}

	protected void doRecalculateDiagramComponent() {
		// Clear subcomponents.
		this.getSubComponents().clear();

		// Add the table name label.
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
		name.setFont(TableComponent.BOLD_FONT);
		this.setRenameTextField(name);
		this.add(name, this.constraints);

		// Add the schema name label.
		final JLabel label = new JLabel(this.getTable().getSchema().getName());
		label.setFont(TableComponent.ITALIC_FONT);
		this.add(label, this.constraints);

		// Start masked?
		if (TableComponent.this.getTable() instanceof DataSetTable)
			this.hidingMaskedCols = ((DataSetTable) TableComponent.this
					.getTable()).isTableHideMasked();

		// Add a key component as a sub-component of this table,
		// for each of the foreign keys in the table.
		for (final Iterator i = this.getTable().getKeys().iterator(); i
				.hasNext();) {
			final Key key = (Key) i.next();
			final KeyComponent keyComponent = new KeyComponent(key, this
					.getDiagram());

			// Add it as a sub-component (internal representation only).
			this.addSubComponent(key, keyComponent);
			this.getSubComponents().putAll(keyComponent.getSubComponents());

			for (int j = 0; j < key.getColumns().length; j++)
				key.getColumns()[j].addPropertyChangeListener("columnRename",
						this.recalcListener);

			// Physically add it to the table component layout.
			this.add(keyComponent, this.constraints);
		}

		// Now the columns, as a vertical list in their own panel.
		this.columnsListPanel = new JPanel();
		this.columnsListPanel.setLayout(new BoxLayout(this.columnsListPanel,
				BoxLayout.Y_AXIS));

		// Do a bit of sorting to make them alphabetical first.
		final Map sortedColMap = new TreeMap();
		for (final Iterator i = this.getTable().getColumns().values()
				.iterator(); i.hasNext();) {
			final Column col = (Column) i.next();
			sortedColMap.put(
					col instanceof DataSetColumn ? ((DataSetColumn) col)
							.getModifiedName() : col.getName(), col);
		}

		// If dataset table...
		if (this.getTable() instanceof DataSetTable
				|| this.getTable() instanceof RealisedTable
				|| this.getTable() instanceof FakeTable) {
			final JCheckBox hideMaskedButton = new JCheckBox(Resources
					.get("hideMaskedTitle"), this.hidingMaskedCols);
			hideMaskedButton.setFont(TableComponent.BOLD_FONT);
			this.columnsListPanel.add(hideMaskedButton);
			hideMaskedButton.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					TableComponent.this.hidingMaskedCols = hideMaskedButton
							.isSelected();
					if (TableComponent.this.getTable() instanceof DataSetTable)
						TableComponent.this.getDiagram().getMartTab()
								.getDataSetTabSet().requestTableHideMasked(
										(DataSet) TableComponent.this
												.getTable().getSchema(),
										(DataSetTable) TableComponent.this
												.getTable(),
										TableComponent.this.hidingMaskedCols);
					// Recalculate the diagram.
					for (final Iterator i = TableComponent.this
							.getSubComponents().values().iterator(); i
							.hasNext();) {
						final DiagramComponent comp = (DiagramComponent) i
								.next();
						if (comp instanceof ColumnComponent)
							comp.repaintDiagramComponent();
					}
				}
			});
		}

		// Show/hide the columns panel with a button.
		this.showHide = new JButton(Resources.get("showColumnsButton"));
		this.showHide.setFont(TableComponent.BOLD_FONT);
		this.add(this.showHide, this.constraints);
		this.showHide.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				if (TableComponent.this.getState().equals(Boolean.TRUE))
					TableComponent.this.setState(Boolean.FALSE);
				else
					TableComponent.this.setState(Boolean.TRUE);
			}
		});
		this.showHide.setEnabled(sortedColMap.size() > 0);
		this.add(this.columnsListPanel, this.constraints);
		this.columnsListPanel.setVisible(false);

		// Add columns to the list one by one, as column sub-components.
		for (final Iterator i = sortedColMap.values().iterator(); i.hasNext();) {
			final Column col = (Column) i.next();
			final ColumnComponent colComponent = new ColumnComponent(col, this
					.getDiagram());

			// Add it as a sub-component (internal representation only).
			this.addSubComponent(col, colComponent);
			this.getSubComponents().putAll(colComponent.getSubComponents());

			// Physically add it to the list of columns.
			this.columnsListPanel.add(colComponent);
		}

		// Recalculate the diagram if masking as the recalc can sometimes
		// lose it.
		if (this.hidingMaskedCols)
			for (final Iterator i = TableComponent.this.getSubComponents()
					.values().iterator(); i.hasNext();) {
				final DiagramComponent comp = (DiagramComponent) i.next();
				if (comp instanceof ColumnComponent)
					comp.repaintDiagramComponent();
			}

		// Set our initial display state as false, which means columns are
		// hidden.
		this.setState(Boolean.FALSE);
	}

	public void performRename(final String newName) {
		this.getDiagram().getMartTab().getDataSetTabSet()
				.requestRenameDataSetTable((DataSetTable) this.getTable(),
						newName);
	}

	public String getEditableName() {
		return this.getTable() instanceof DataSetTable ? ((DataSetTable) this
				.getTable()).getModifiedName() : this.getTable().getName();
	}

	public String getDisplayName() {
		return this.getEditableName();
	}

	public void setState(final Object state) {
		// For us, state is TRUE if we want the columns panel visible.
		if (state != null && state.equals(Boolean.TRUE)) {
			if (this.getState() != null
					&& this.getState().equals(Boolean.FALSE))
				this.columnsListPanel.setVisible(true);
			this.showHide.setText(Resources.get("hideColumnsButton"));
		} else {
			if (this.getState() != null && this.getState().equals(Boolean.TRUE))
				this.columnsListPanel.setVisible(false);
			this.showHide.setText(Resources.get("showColumnsButton"));
		}

		// Delegate upwards, so that the state is remembered for later.
		super.setState(state);
	}
}
