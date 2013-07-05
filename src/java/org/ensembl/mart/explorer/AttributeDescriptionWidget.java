/*
	Copyright (C) 2003 EBI, GRL

	This library is free software; you can redistribute it and/or
	modify it under the terms of the GNU Lesser General Public
	License as published by the Free Software Foundation; either
	version 2.1 of the License, or (at your option) any later version.

	This library is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
	Lesser General Public License for more details.

	You should have received a copy of the GNU Lesser General Public
	License along with this library; if not, write to the Free Software
	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.ensembl.mart.explorer;

import java.awt.Component;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.logging.Logger;

import javax.swing.JCheckBox;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.FieldAttribute;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.config.AttributeDescription;

/**
 * @author craig
 *
 * To change the template for this generated type comment go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class AttributeDescriptionWidget
	extends InputPage
	implements TreeSelectionListener {

	private final static Logger logger =
		Logger.getLogger(AttributeDescriptionWidget.class.getName());
	private AttributeDescription attributeDescription;
	private Query query;
	private Attribute attribute;
	private JCheckBox button;
    
    private Feedback feedback = new Feedback(this);

	/**
	 * BooleanFilter containing an InputPage, this page is used by the QueryEditor
	 * when it detects the filter has been added or removed from the query.
	 */
	private class InputPageAwareAttribute
		extends FieldAttribute
		implements InputPageAware {

		private InputPage inputPage;

		public InputPageAwareAttribute(
			String field,
			String tableConstraint,
			String key,
			InputPage inputPage) {
			super(field, tableConstraint,key);
			this.inputPage = inputPage;
		}

		public InputPage getInputPage() {
			return inputPage;
		}
	}

	/**
	 * @param query
	 * @param name
	 */
	public AttributeDescriptionWidget(
		final Query query,
		AttributeDescription attributeDescription,
		QueryTreeView tree) {

		super(query, attributeDescription.getDisplayName(), tree);
        
		if (tree != null)
			tree.addTreeSelectionListener(this);
		this.attributeDescription = attributeDescription;
		this.query = query;

		attribute =
			new InputPageAwareAttribute(
				attributeDescription.getField(),
				attributeDescription.getTableConstraint(),
		        attributeDescription.getKey(),
				this);
		setField(attribute);

		button = new JCheckBox(attributeDescription.getDisplayName());
		button.setToolTipText(attributeDescription.getDescription());
		button.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent event) {

				doClick();

			}
		});

		query.addQueryChangeListener(this);

		add(button);
	}

	/**
	 * 
	 */
	private void doClick() {
		if (button.isSelected())
          query.addAttribute(attribute);
        else
		  query.removeAttribute(attribute);
	}
    
    public Attribute getAttribute() {
		return attribute;
	}

	/** 
	 * If the attribute added corresponds to this widget then show it is
	 * selected.
	 * @see org.ensembl.mart.lib.QueryChangeListener#attributeAdded(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Attribute)
	 */
	public void attributeAdded(
		Query sourceQuery,
		int index,
		Attribute attribute) {

		if (this.attribute.sameFieldTableConstraint(attribute))
			button.setSelected(true);
	}

	/**
	 * If removed attribute corresponds to this widget then show 
	 * it is not selected.
	 * @see org.ensembl.mart.lib.QueryChangeListener#attributeRemoved(org.ensembl.mart.lib.Query, int, org.ensembl.mart.lib.Attribute)
	 */
	public void attributeRemoved(
		Query sourceQuery,
		int index,
		Attribute attribute) {
		if (this.attribute.sameFieldTableConstraint(attribute))
			button.setSelected(false);
	}

	/**
	 * Callback method called when an item in the tree is selected.
	 * Brings this widget to the front if the selecte node corresponds to this widget this
	 * TODO get scrolling to a selected attribute working properly
	 * @see javax.swing.event.TreeSelectionListener#valueChanged(javax.swing.event.TreeSelectionEvent)
	 */
	public void valueChanged(TreeSelectionEvent e) {

		if (button.isSelected()) {

			if (e.getNewLeadSelectionPath() != null
				&& e.getNewLeadSelectionPath().getLastPathComponent() != null) {

				DefaultMutableTreeNode node =
					(DefaultMutableTreeNode) e
						.getNewLeadSelectionPath()
						.getLastPathComponent();

				if (node != null) {

					TreeNodeData tnd = (TreeNodeData) node.getUserObject();
					Attribute a = tnd.getAttribute();
					if (a != null && a == attribute) {
						for (Component p, c = this; c != null; c = p) {
							p = c.getParent();
							if (p instanceof JTabbedPane)
								 ((JTabbedPane) p).setSelectedComponent(c);
							else if (p instanceof JScrollPane) {
								// not sure if this is being used
								Point pt = c.getLocation();
								Rectangle r = new Rectangle(pt);
								((JScrollPane) p).scrollRectToVisible(r);
							}

						}

					}
				}
			}
		}

	}

}
