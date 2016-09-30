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

import java.awt.AWTEvent;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.Stroke;
import java.awt.event.MouseEvent;
import java.awt.geom.Rectangle2D;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Collections;

import javax.swing.JComponent;
import javax.swing.JPopupMenu;

import org.biomart.builder.model.Relation;
import org.biomart.builder.view.gui.diagrams.Diagram;
import org.biomart.builder.view.gui.diagrams.contexts.DiagramContext;
import org.biomart.common.utils.BeanMap;
import org.biomart.common.utils.Transaction;
import org.biomart.common.utils.Transaction.TransactionEvent;
import org.biomart.common.utils.Transaction.TransactionListener;

/**
 * This component represents a relation between two keys, in the form of a line.
 * The path of the line is defined by one of the layout managers provided with
 * MartBuilder.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.37 $, $Date: 2007-12-20 18:43:46 $, modified by
 *          $Author: arek $
 * @since 0.5
 */
public class RelationComponent extends JComponent implements DiagramComponent,
		TransactionListener {

	/**
	 * Subclasses use this if the component needs repainting.
	 */
	protected boolean needsRepaint = false;

	/**
	 * Subclasses use this if the component needs recalculating.
	 */
	protected boolean needsRecalc = false;

	private boolean changed = false;

	private static final float RELATION_DASHSIZE = 6.0f; // 72 = 1 inch

	private static final float RELATION_DOTSIZE = 2.0f; // 72 = 1 inch

	private static final float RELATION_LINEWIDTH = 1.0f; // 72 = 1 inch

	private static final float RELATION_MITRE_TRIM = 10.0f; // 72 = 1 inch

	private static final Stroke ONE_MANY = new BasicStroke(
			RelationComponent.RELATION_LINEWIDTH, BasicStroke.CAP_ROUND,
			BasicStroke.JOIN_ROUND, RelationComponent.RELATION_MITRE_TRIM);

	private static final Stroke ONE_ONE = new BasicStroke(
			RelationComponent.RELATION_LINEWIDTH * 2.0f, BasicStroke.CAP_ROUND,
			BasicStroke.JOIN_ROUND, RelationComponent.RELATION_MITRE_TRIM);

	private static final Stroke ONE_MANY_DOTTED = new BasicStroke(
			RelationComponent.RELATION_LINEWIDTH, BasicStroke.CAP_ROUND,
			BasicStroke.JOIN_ROUND, RelationComponent.RELATION_MITRE_TRIM,
			new float[] { RelationComponent.RELATION_DOTSIZE,
					RelationComponent.RELATION_DOTSIZE }, 0);

	private static final Stroke ONE_ONE_DOTTED = new BasicStroke(
			RelationComponent.RELATION_LINEWIDTH * 2.0f, BasicStroke.CAP_ROUND,
			BasicStroke.JOIN_ROUND, RelationComponent.RELATION_MITRE_TRIM,
			new float[] { RelationComponent.RELATION_DOTSIZE,
					RelationComponent.RELATION_DOTSIZE }, 0);

	private static final Stroke ONE_MANY_DASHED = new BasicStroke(
			RelationComponent.RELATION_LINEWIDTH, BasicStroke.CAP_ROUND,
			BasicStroke.JOIN_ROUND, RelationComponent.RELATION_MITRE_TRIM,
			new float[] { RelationComponent.RELATION_DASHSIZE,
					RelationComponent.RELATION_DASHSIZE }, 0);

	private static final Stroke ONE_ONE_DASHED = new BasicStroke(
			RelationComponent.RELATION_LINEWIDTH * 2.0f, BasicStroke.CAP_ROUND,
			BasicStroke.JOIN_ROUND, RelationComponent.RELATION_MITRE_TRIM,
			new float[] { RelationComponent.RELATION_DASHSIZE,
					RelationComponent.RELATION_DASHSIZE }, 0);

	private static final Stroke ONE_MANY_DOTTED_DASHED = new BasicStroke(
			RelationComponent.RELATION_LINEWIDTH, BasicStroke.CAP_ROUND,
			BasicStroke.JOIN_ROUND, RelationComponent.RELATION_MITRE_TRIM,
			new float[] { RelationComponent.RELATION_DASHSIZE,
					RelationComponent.RELATION_DOTSIZE,
					RelationComponent.RELATION_DOTSIZE,
					RelationComponent.RELATION_DOTSIZE }, 0);

	private static final Stroke ONE_ONE_DOTTED_DASHED = new BasicStroke(
			RelationComponent.RELATION_LINEWIDTH * 2.0f, BasicStroke.CAP_ROUND,
			BasicStroke.JOIN_ROUND, RelationComponent.RELATION_MITRE_TRIM,
			new float[] { RelationComponent.RELATION_DASHSIZE,
					RelationComponent.RELATION_DOTSIZE,
					RelationComponent.RELATION_DOTSIZE,
					RelationComponent.RELATION_DOTSIZE }, 0);

	private static final Stroke OUTLINE = new BasicStroke();

	private static final long serialVersionUID = 1;

	/**
	 * Constant referring to modified relation colour.
	 */
	public static Color MODIFIED_COLOUR = Color.BLUE;

	/**
	 * Constant referring to handmade relation colour.
	 */
	public static Color HANDMADE_COLOUR = Color.GREEN;

	/**
	 * Constant referring to incorrect relation colour.
	 */
	public static Color INCORRECT_COLOUR = Color.RED;

	/**
	 * Constant referring to masked relation colour.
	 */
	public static Color MASKED_COLOUR = Color.RED;

	/**
	 * Constant referring to normal relation colour.
	 */
	public static Color NORMAL_COLOUR = Color.DARK_GRAY;

	/**
	 * Constant referring to subclassed relation colour.
	 */
	public static Color SUBCLASS_COLOUR = Color.RED;

	/**
	 * Constant referring to unrolled relation colour.
	 */
	public static Color UNROLLED_COLOUR = Color.CYAN;

	private boolean restricted = false;

	private boolean compounded = false;

	private boolean loopback = false;

	private Diagram diagram;

	private Shape lineShape;

	private Shape outline;

	private Relation object;

	private RenderingHints renderHints;

	private Object state;

	private Stroke stroke;

	private final PropertyChangeListener repaintListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent e) {
			RelationComponent.this.needsRepaint = true;
		}
	};

	/**
	 * The constructor constructs a component around a given relation, and
	 * associates the component with the given diagram.
	 * 
	 * @param relation
	 *            the relation to show in the component.
	 * @param diagram
	 *            the diagram to show this component in.
	 */
	public RelationComponent(final Relation relation, final Diagram diagram) {
		super();

		// Remember settings.
		this.object = relation;
		this.diagram = diagram;

		// Turn on the mouse.
		this.enableEvents(AWTEvent.MOUSE_EVENT_MASK);
		this.setDoubleBuffered(true); // Stop flicker.

		// Make sure we're transparent.
		this.setOpaque(false);

		// Set-up rendering hints.
		this.renderHints = new RenderingHints(RenderingHints.KEY_ANTIALIASING,
				RenderingHints.VALUE_ANTIALIAS_ON);
		this.renderHints.put(RenderingHints.KEY_RENDERING,
				RenderingHints.VALUE_RENDER_QUALITY);

		// Draw our contents, as we don't have any child classes that
		// do this for us unfortunately.
		this.recalculateDiagramComponent();

		Transaction.addTransactionListener(this);

		// Repaint events.
		relation.addPropertyChangeListener("directModified",
				this.repaintListener);
		relation.getFirstKey().addPropertyChangeListener("status",
				this.repaintListener);
		relation.getFirstKey().getTable().addPropertyChangeListener("masked",
				this.repaintListener);
		relation.getFirstKey().getTable().addPropertyChangeListener(
				"dimensionMasked", this.repaintListener);
		relation.getSecondKey().addPropertyChangeListener("status",
				this.repaintListener);
		relation.getSecondKey().getTable().addPropertyChangeListener("masked",
				this.repaintListener);
		relation.getSecondKey().getTable().addPropertyChangeListener(
				"dimensionMasked", this.repaintListener);

		this.changed = this.getObject().isVisibleModified();
	}

	public void setDirectModified(final boolean modified) {
		// Ignore, for now.
	}

	public boolean isDirectModified() {
		return false;
	}

	public void setVisibleModified(final boolean modified) {
		// Ignore, for now.
	}

	public boolean isVisibleModified() {
		return false;
	}

	public void transactionResetDirectModified() {
		// Ignore, for now.
	}

	public void transactionResetVisibleModified() {
		// Ignore, for now.
	}

	public void transactionStarted(final TransactionEvent evt) {
		// Ignore, for now.
	}

	public void transactionEnded(final TransactionEvent evt) {
		final boolean visMod = this.getObject().isVisibleModified()
				&& this.getDiagram().getMartTab().getPartitionViewSelection() == null;
		this.needsRepaint |= this.changed ^ visMod;
		this.changed = visMod;
		if (this.needsRecalc)
			this.recalculateDiagramComponent();
		else if (this.needsRepaint)
			this.repaintDiagramComponent();
		this.needsRecalc = false;
		this.needsRepaint = false;
	}

	protected void paintComponent(final Graphics g) {
		final Graphics2D g2d = (Graphics2D) g;
		g2d.setRenderingHints(this.renderHints);
		if (this.changed) {
			g2d.setColor(DiagramComponent.GLOW_COLOUR);
			g2d.setStroke(new BasicStroke(DiagramComponent.GLOW_WIDTH));
			g2d.draw(this.outline);
		}
		g2d.setColor(this.getForeground());
		g2d.setStroke(this.stroke);
		g2d.draw(this.loopback ? this.outline : this.lineShape);
	}

	protected void processMouseEvent(final MouseEvent evt) {
		boolean eventProcessed = false;

		if (evt.getButton() != 0)
			this.getDiagram().deselectAll();

		// Is it a right-click?
		if (evt.isPopupTrigger()) {
			// Build the basic menu.
			final JPopupMenu contextMenu = this.getContextMenu();

			// Customise it using the diagram context.
			if (this.getDiagram().getDiagramContext() != null)
				this.getDiagram().getDiagramContext().populateContextMenu(
						contextMenu, this.getObject());

			// Display.
			if (contextMenu.getComponentCount() > 0)
				contextMenu.show(this, evt.getX(), evt.getY());

			// We have successfully handled the event.
			eventProcessed = true;
		}

		// Pass the event on up if we're not interested.
		if (!eventProcessed)
			super.processMouseEvent(evt);
	}

	public boolean contains(final int x, final int y) {
		// Clicks are on us if they are within a certain distance
		// of the outline shape.
		return this.outline != null
				&& this.outline.intersects(new Rectangle2D.Double(x
						- RelationComponent.RELATION_LINEWIDTH * 2, y
						- RelationComponent.RELATION_LINEWIDTH * 2,
						RelationComponent.RELATION_LINEWIDTH * 4,
						RelationComponent.RELATION_LINEWIDTH * 4));
	}

	public JPopupMenu getContextMenu() {
		final JPopupMenu contextMenu = new JPopupMenu();
		// No additional entries for us yet.
		// Return it.
		return contextMenu;
	}

	public JPopupMenu getMultiContextMenu() {
		final JPopupMenu contextMenu = new JPopupMenu();
		// No additional entries for us yet.
		// Return it.
		return contextMenu;
	}

	public Diagram getDiagram() {
		return this.diagram;
	}

	/**
	 * Returns the diagram component representing the first key of this
	 * relation.
	 * 
	 * @return the diagram component for the first key.
	 */
	public KeyComponent getFirstKeyComponent() {
		return (KeyComponent) this.diagram.getDiagramComponent(((Relation)this.getObject())
				.getFirstKey());
	}

	public TransactionListener getObject() {
		return this.object;
	}

	/**
	 * Returns the diagram component representing the second key of this
	 * relation.
	 * 
	 * @return the diagram component for the second key.
	 */
	public KeyComponent getSecondKeyComponent() {
		return (KeyComponent) this.diagram.getDiagramComponent(((Relation)this.getObject())
				.getSecondKey());
	}

	public Object getState() {
		return this.state;
	}

	public BeanMap getSubComponents() {
		// We have no sub-components.
		return new BeanMap(Collections.EMPTY_MAP);
	}

	public void recalculateDiagramComponent() {
		// Nothing to do here.
	}

	public void repaintDiagramComponent() {
		this.updateAppearance();
		this.repaint();
	}

	/**
	 * Sets the shape for us to display the outline of. This will usually be a
	 * line, however it's up to the layout manager entirely.
	 * 
	 * @param shape
	 *            the shape this relation should take on screen.
	 */
	public void setLineShape(final Shape shape) {
		// Only change if the shape has changed.
		if (this.lineShape != shape || this.lineShape != null
				&& !this.lineShape.equals(shape)) {
			this.lineShape = shape;
			// Update the outline of the relation shape accordingly.
			if (this.lineShape != null)
				this.outline = RelationComponent.OUTLINE
						.createStrokedShape(this.lineShape);
		}
		// Update our appearance.
		this.updateAppearance();
	}

	public void setState(final Object state) {
		this.state = state;
	}

	/**
	 * If this is set to <tt>true</tt> then the component will appear with a
	 * dashed outline. Otherwise, it appears with a solid outline.
	 * 
	 * @param restricted
	 *            <tt>true</tt> if the component is to appear with a dashed
	 *            outline. The default is <tt>false</tt>.
	 */
	public void setRestricted(final boolean restricted) {
		this.restricted = restricted;
	}

	/**
	 * If this is set to <tt>true</tt> then the component will appear with a
	 * dotted outline. Otherwise, it appears with a solid outline.
	 * 
	 * @param compounded
	 *            <tt>true</tt> if the component is to appear with a dotted
	 *            outline. The default is <tt>false</tt>.
	 */
	public void setCompounded(final boolean compounded) {
		this.compounded = compounded;
	}

	/**
	 * If this is set to <tt>true</tt> then the component will appear with a
	 * double outline. Otherwise, it appears with a single outline.
	 * 
	 * @param loopback
	 *            <tt>true</tt> if the component is to appear with a double
	 *            outline. The default is <tt>false</tt>.
	 */
	public void setLoopback(final boolean loopback) {
		this.loopback = loopback;
	}

	public void updateAppearance() {
		// Use the context to alter us first.
		final DiagramContext mod = this.getDiagram().getDiagramContext();
		if (mod != null)
			if (this.getDiagram().isHideMasked()
					&& mod.isMasked(this.getObject())) {
				this.setVisible(false);
				return;
			} else
				mod.customiseAppearance(this, this.getObject());
		// Work out what style to draw the relation line.
		final Stroke oldStroke = this.stroke;
		if (((Relation)this.getObject()).isOneToOne())
			this.stroke = this.restricted ? this.compounded ? RelationComponent.ONE_ONE_DOTTED_DASHED
					: RelationComponent.ONE_ONE_DASHED
					: this.compounded ? RelationComponent.ONE_ONE_DOTTED
							: RelationComponent.ONE_ONE;
		else
			this.stroke = this.restricted ? this.compounded ? RelationComponent.ONE_MANY_DOTTED_DASHED
					: RelationComponent.ONE_MANY_DASHED
					: this.compounded ? RelationComponent.ONE_MANY_DOTTED
							: RelationComponent.ONE_MANY;
		this.setVisible(true);
		// Force repaint of area if stroke changed.
		if (oldStroke != this.stroke) {
			this.revalidate();
			this.repaint(this.getBounds());
		}
		if (this.getObject() != null)
			this.setToolTipText(this.getObject().toString());
	}
}
