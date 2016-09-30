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

import javax.swing.JPopupMenu;

import org.biomart.builder.view.gui.diagrams.Diagram;
import org.biomart.builder.view.gui.diagrams.contexts.DiagramContext;
import org.biomart.common.utils.BeanMap;
import org.biomart.common.utils.Transaction.TransactionListener;

/**
 * An element that can be drawn on a diagram. It can provide a context menu for
 * itself, the diagram it belongs to, its current state (and allow its state to
 * be set), and a map of any components it may contain inside that are of
 * interest to the diagram (for instance keys for relations).
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.19 $, $Date: 2007-10-31 10:32:56 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public interface DiagramComponent {

	/**
	 * The colour used to highlight changes.
	 */
	public static final Color GLOW_COLOUR = new Color(0.0f, 0.5f, 0.0f);

	/**
	 * The width of the line used to highlight changes.
	 */
	public static final int GLOW_WIDTH = 2;

	/**
	 * Construct a context menu for the model object.
	 * 
	 * @return the popup menu.
	 */
	public JPopupMenu getContextMenu();

	/**
	 * Construct a context menu for the model object to be used when multiple
	 * items are selected at once.
	 * 
	 * @return the popup menu.
	 */
	public JPopupMenu getMultiContextMenu();

	/**
	 * Retrieves the diagram this component belongs to.
	 * 
	 * @return the diagram.
	 */
	public Diagram getDiagram();

	/**
	 * Retrieves the model object this component is a representation of.
	 * 
	 * @return the model object.
	 */
	public TransactionListener getObject();

	/**
	 * The current state of the component is returned by this. States are
	 * arbitrary and can be null. States can be set by using
	 * {@link #setState(Object)}
	 * 
	 * @return the current state.
	 */
	public Object getState();

	/**
	 * Returns a map of inner components inside the diagram. The keys are
	 * database object references, and the values are the diagram components
	 * representing them inside the current diagram component. This is useful
	 * for instance when wanting to obtain key components for a database
	 * component without knowing which diagram component it may be inside.
	 * 
	 * @return the map of inner components.
	 */
	public BeanMap getSubComponents();

	/**
	 * This method is called when the component needs to rethink its contents
	 * and layout.
	 */
	public void recalculateDiagramComponent();

	/**
	 * This method is called when the component needs to repaint its contents.
	 */
	public void repaintDiagramComponent();

	/**
	 * Sets the current state of the component. See {@link #getState()}.
	 * 
	 * @param state
	 *            the new state for the component.
	 */
	public void setState(Object state);

	/**
	 * Updates the appearance of this component, usually by setting colours.
	 * This may often be handled by delegating calls to a {@link DiagramContext}.
	 * It does _not_ repaint the object.
	 */
	public void updateAppearance();
}
