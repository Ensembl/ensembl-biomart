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

package org.biomart.builder.view.gui;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.swing.ImageIcon;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.table.DefaultTableModel;

import org.biomart.builder.model.Column;
import org.biomart.builder.model.ComponentStatus;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Key;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.Table;
import org.biomart.builder.model.Key.ForeignKey;
import org.biomart.builder.model.Key.PrimaryKey;
import org.biomart.builder.model.Relation.Cardinality;
import org.biomart.builder.view.gui.MartTabSet.MartTab;
import org.biomart.builder.view.gui.diagrams.AllSchemasDiagram;
import org.biomart.builder.view.gui.diagrams.Diagram;
import org.biomart.builder.view.gui.diagrams.SchemaDiagram;
import org.biomart.builder.view.gui.diagrams.contexts.DiagramContext;
import org.biomart.builder.view.gui.dialogs.KeyDialog;
import org.biomart.builder.view.gui.dialogs.SchemaConnectionDialog;
import org.biomart.common.resources.Resources;
import org.biomart.common.utils.Transaction;
import org.biomart.common.view.gui.LongProcess;
import org.biomart.common.view.gui.SwingWorker;
import org.biomart.common.view.gui.dialogs.ProgressDialog;
import org.biomart.common.view.gui.dialogs.StackTrace;

/**
 * This tabset has one tab for the diagram which represents all schemas, and one
 * tab each for each schema in the mart. It provides methods for working with a
 * given schema, such as adding or removing them, or grouping them together. It
 * can update itself based on the schemas in the mart on request.
 * <p>
 * Like a diagram, it can have a {@link DiagramContext} associated with it.
 * Whenever this context changes, all {@link Diagram} instances represented in
 * the tabs have the same context applied.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.114 $, $Date: 2008-02-21 09:35:26 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class SchemaTabSet extends JTabbedPane {
	private static final long serialVersionUID = 1;

	private AllSchemasDiagram allSchemasDiagram;

	private DiagramContext diagramContext;

	private MartTab martTab;

	// Schema hashcodes change, so we must use a double-list.
	private final Map schemaToDiagram = new HashMap();

	// Make a listener which knows how to handle masking and
	// renaming.
	private final PropertyChangeListener updateListener = new PropertyChangeListener() {
		public void propertyChange(PropertyChangeEvent evt) {
			final Schema sch = (Schema) evt.getSource();
			if (evt.getPropertyName().equals("name")) {
				// Rename in diagram set.
				SchemaTabSet.this.schemaToDiagram.put(evt.getNewValue(),
						SchemaTabSet.this.schemaToDiagram.remove(evt
								.getOldValue()));
				SchemaTabSet.this.setTitleAt(SchemaTabSet.this
						.indexOfTab((String) evt.getOldValue()), (String) evt
						.getNewValue());
			} else if (evt.getPropertyName().equals("masked")) {
				// For masks, if unmasking, add a tab, otherwise
				// remove the tab.
				final boolean masked = ((Boolean) evt.getNewValue())
						.booleanValue();
				if (masked)
					SchemaTabSet.this.removeSchemaTab(sch.getName(), true);
				else
					SchemaTabSet.this.addSchemaTab(sch, false);
			}
		}
	};

	// A listener for updating our tabs.
	private final PropertyChangeListener tabListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			// Listen to masked schema and rename
			// schema events on each new schema added
			// regardless of tab presence.
			// Mass change. Copy to prevent concurrent mods.
			final Set oldSchs = new HashSet(SchemaTabSet.this.schemaToDiagram
					.keySet());
			for (final Iterator i = SchemaTabSet.this.martTab.getMart()
					.getSchemas().values().iterator(); i.hasNext();) {
				final Schema sch = (Schema) i.next();
				if (!oldSchs.remove(sch.getName())) {
					// Single-add.
					if (!sch.isMasked())
						SchemaTabSet.this.addSchemaTab(sch, true);
					sch.addPropertyChangeListener("masked",
							SchemaTabSet.this.updateListener);
					sch.addPropertyChangeListener("name",
							SchemaTabSet.this.updateListener);
				}
			}
			for (final Iterator i = oldSchs.iterator(); i.hasNext();)
				SchemaTabSet.this.removeSchemaTab((String) i.next(), true);
		}
	};

	/**
	 * Creates a new set of tabs to represent the schemas in a mart. The mart is
	 * obtained by using methods on the mart tab passed in as a parameter. The
	 * mart tab is the parent tab that this schema tabset will appear inside the
	 * tabs of.
	 * 
	 * @param martTab
	 *            the parent tab this schema tabset will appear inside the tabs
	 *            of.
	 */
	public SchemaTabSet(final MartTab martTab) {
		super();

		// Remember the mart tabset we are shown inside.
		this.martTab = martTab;

		// Add the all-schemas overview tab. This tab displays a diagram
		// in which all schemas appear, linked where necessary by external
		// relations. This diagram could be quite large, so it is held inside
		// a scrollpane.
		this.allSchemasDiagram = new AllSchemasDiagram(this.martTab);
		final JScrollPane scroller = new JScrollPane(this.allSchemasDiagram);
		scroller.getViewport().setBackground(
				this.allSchemasDiagram.getBackground());
		scroller.getHorizontalScrollBar().addAdjustmentListener(
				this.allSchemasDiagram);
		scroller.getVerticalScrollBar().addAdjustmentListener(
				this.allSchemasDiagram);
		this.addTab(Resources.get("multiSchemaOverviewTab"), scroller);

		// Populate the map to hold the relation between schemas and the
		// diagrams representing them.
		for (final Iterator i = martTab.getMart().getSchemas().values()
				.iterator(); i.hasNext();) {
			final Schema sch = (Schema) i.next();
			// Don't add schemas which are initially masked.
			if (!sch.isMasked())
				this.addSchemaTab(sch, false);
			sch.addPropertyChangeListener("masked", this.updateListener);
			sch.addPropertyChangeListener("name", this.updateListener);
		}

		// Listen to add/remove/mass change schema events.
		martTab.getMart().getSchemas().addPropertyChangeListener(
				this.tabListener);
	}

	/**
	 * Works out which schema tab is selected, and return it.
	 * 
	 * @return the currently selected schema, or <tt>null</tt> if none is
	 *         selected.
	 */
	public Schema getSelectedSchema() {
		if (this.getSelectedIndex() <= 0 || !this.isShowing())
			return null;
		final SchemaDiagram selectedDiagram = (SchemaDiagram) ((JScrollPane) this
				.getSelectedComponent()).getViewport().getView();
		return selectedDiagram.getSchema();
	}

	private synchronized void addSchemaTab(final Schema schema,
			final boolean selectNewSchema) {
		// Create the diagram to represent this schema.
		final SchemaDiagram schemaDiagram = new SchemaDiagram(this.martTab,
				schema);

		// Create a scroller to contain the diagram.
		final JScrollPane scroller = new JScrollPane(schemaDiagram);
		scroller.getViewport().setBackground(schemaDiagram.getBackground());
		scroller.getHorizontalScrollBar().addAdjustmentListener(schemaDiagram);
		scroller.getVerticalScrollBar().addAdjustmentListener(schemaDiagram);

		// Add a tab containing the scroller, with the same name as the schema.
		this.addTab(schema.getName(), scroller);

		// Remember which diagram the schema is connected with.
		this.schemaToDiagram.put(schema.getName(), schemaDiagram);

		// Set the current context on the diagram to be the same as the
		// current context on this schema tabset.
		schemaDiagram.setDiagramContext(this.getDiagramContext());

		if (selectNewSchema) {
			// Fake a click on the schema tab and on the button
			// that selects the schema editor in the current mart tabset.
			this.setSelectedIndex(this.indexOfTab(schema.getName()));
			this.martTab.selectSchemaEditor();
		}
	}

	private String askUserForSchemaName(final String defaultResponse) {
		// Ask user for a name, giving them the default suggestion.
		String name = (String) JOptionPane.showInputDialog(null, Resources
				.get("requestSchemaName"), Resources.get("questionTitle"),
				JOptionPane.QUESTION_MESSAGE, null, null, defaultResponse);

		// If they didn't select anything, return null.
		if (name == null)
			return null;

		// If they entered an empty string, ie. deleted the default
		// but didn't type anything else, make it as though
		// it had not been deleted.
		else if (name.trim().length() == 0)
			name = defaultResponse;

		// Return the response.
		return name;
	}

	private Key askUserForTargetKey(final Key from) {
		// Given a particular key, work out which other keys, in any schema,
		// this key may be linked to.

		// Start by making a list to contain the candidates.
		final List candidates = new ArrayList();

		// We want all keys that have the same number of columns.
		for (final Iterator i = this.martTab.getMart().getSchemas().values()
				.iterator(); i.hasNext();)
			for (final Iterator j = ((Schema) i.next()).getTables().values()
					.iterator(); j.hasNext();) {
				final Table tbl = (Table) j.next();
				for (final Iterator k = tbl.getKeys().iterator(); k.hasNext();) {
					final Key key = (Key) k.next();
					if (key.getColumns().length == from.getColumns().length
							&& !key.equals(from))
						candidates.add(key);
				}
			}
		// Alphabetize.
		Collections.sort(candidates);

		// Put up a box asking which key to link this key to, based on the
		// list of candidates we just made. Return the key that the user
		// selects, or null if none was selected.
		return (Key) JOptionPane.showInputDialog(null, Resources
				.get("whichKeyToLinkRelationTo"), Resources
				.get("questionTitle"), JOptionPane.QUESTION_MESSAGE, null,
				candidates.toArray(), null);
	}

	private JPopupMenu getSchemaTabContextMenu(final Schema schema) {
		// This menu will appear when a schema tab is right-clicked on
		// (that is, the tab itself, not the contents of the tab).

		// The empty menu to start with.
		final JPopupMenu contextMenu = new JPopupMenu();

		// Update schema.
		final JMenuItem updateSchema = new JMenuItem(Resources
				.get("updateSchemaTitle"), new ImageIcon(Resources
				.getResourceAsURL("refresh.gif")));
		updateSchema.setMnemonic(Resources.get("updateSchemaMnemonic")
				.charAt(0));
		updateSchema.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				SchemaTabSet.this.requestModifySchema(schema);
			}
		});
		contextMenu.add(updateSchema);

		// Add an option to rename this schema tab and associated schema.
		final JMenuItem rename = new JMenuItem(Resources
				.get("renameSchemaTitle"));
		rename.setMnemonic(Resources.get("renameSchemaMnemonic").charAt(0));
		rename.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				SchemaTabSet.this.requestRenameSchema(schema);
			}
		});
		contextMenu.add(rename);

		// Add an option to remove this schema tab, and the
		// associated schema from the mart.
		final JMenuItem close = new JMenuItem(Resources
				.get("removeSchemaTitle"), new ImageIcon(Resources
				.getResourceAsURL("cut.gif")));
		close.setMnemonic(Resources.get("removeSchemaMnemonic").charAt(0));
		close.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				SchemaTabSet.this.requestRemoveSchema(schema);
			}
		});
		contextMenu.add(close);

		// Return the menu.
		return contextMenu;
	}

	private synchronized void removeSchemaTab(final String schemaName,
			final boolean select) {
		// Work out the currently selected tab.
		final int currentTab = this.getSelectedIndex();

		// Work out the tab index for the schema.
		final int tabIndex = this.indexOfTab(schemaName);

		// Remove the tab. Also remove schema mapping from the schema-to-diagram
		// map.
		this.remove(tabIndex);

		this.schemaToDiagram.remove(schemaName);

		if (select)
			// Fake a click on the last tab before this one to ensure
			// at least one tab remains visible and up-to-date.
			this.setSelectedIndex(currentTab == 0 ? 0 : Math.max(tabIndex - 1,
					0));
	}

	protected void processMouseEvent(final MouseEvent evt) {
		boolean eventProcessed = false;

		// Is it a right-click?
		if (evt.isPopupTrigger()) {

			// Where was the click?
			final int selectedIndex = this.indexAtLocation(evt.getX(), evt
					.getY());

			// Was the click on a tab?
			if (selectedIndex >= 0) {

				// Work out which tab was selected and which diagram
				// is displayed in that tab.
				final Component selectedComponent = this
						.getComponentAt(selectedIndex);
				if (selectedComponent instanceof JScrollPane) {
					final Component selectedDiagram = ((JScrollPane) selectedComponent)
							.getViewport().getView();
					if (selectedDiagram instanceof SchemaDiagram) {

						// Set the schema diagram as the currently selected one.
						this.setSelectedIndex(selectedIndex);

						// Work out the schema inside the diagram.
						final Schema schema = ((SchemaDiagram) selectedDiagram)
								.getSchema();

						// Show the context-menu for the tab for this schema.
						this.getSchemaTabContextMenu(schema).show(this,
								evt.getX(), evt.getY());

						// We've handled the event so mark it as processed.
						eventProcessed = true;
					}
				}
			}
		}

		// Pass the event on up if we're not interested.
		if (!eventProcessed)
			super.processMouseEvent(evt);
	}

	/**
	 * Returns the diagram context currently being used by {@link Diagram}s in
	 * this schema tabset.
	 * 
	 * @return the diagram context currently being used.
	 */
	public DiagramContext getDiagramContext() {
		return this.diagramContext;
	}

	/**
	 * Returns the mart tab that this schema tabset lives inside.
	 * 
	 * @return the parent mart tab.
	 */
	public MartTab getMartTab() {
		return this.martTab;
	}

	/**
	 * Asks user to define a new schema, then adds it.
	 */
	public void requestAddSchema() {
		// Pop up a dialog to get the details of the new schema, then
		// obtain a copy of that schema.
		final Schema schema = SchemaConnectionDialog.createSchema(this.martTab
				.getMart());

		// If no schema was defined, ignore the request.
		if (schema == null)
			return;

		// Add the schema to the mart, then synchronise it.
		SchemaTabSet.this.martTab.getMart().getSchemas().put(
				schema.getOriginalName(), schema);

		// Sync it.
		this.requestSynchroniseSchema(schema, false);
	}

	/**
	 * Update a key status.
	 * 
	 * @param key
	 *            the key to update the status of.
	 * @param status
	 *            the new status to give it.
	 */
	public void requestChangeKeyStatus(final Key key,
			final ComponentStatus status) {
		new LongProcess() {
			public void run() {
				Transaction.start(false);
				key.setStatus(status);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Update a relation cardinality.
	 * 
	 * @param relation
	 *            the relation to change cardinality of.
	 * @param cardinality
	 *            the new cardinality to give it.
	 */
	public void requestChangeRelationCardinality(final Relation relation,
			final Cardinality cardinality) {
		new LongProcess() {
			public void run() throws Exception {
				try {
					Transaction.start(true);
					relation.setCardinality(cardinality);
					if (!relation.getStatus().equals(ComponentStatus.HANDMADE))
						relation
								.setStatus(cardinality.equals(relation
										.getOriginalCardinality()) ? ComponentStatus.INFERRED
										: ComponentStatus.MODIFIED);
				} finally {
					Transaction.end();
				}
			}
		}.start();
	}

	/**
	 * Asks the user if they are sure they want to remove all schema partitions.
	 */
	public void requestRemoveAllSchemaPartitions() {
		// Confirm the decision first.
		final int choice = JOptionPane.showConfirmDialog(null, Resources
				.get("confirmUnpartitionAllSchemas"), Resources
				.get("questionTitle"), JOptionPane.YES_NO_OPTION);

		// Refuse to do it if they said no.
		if (choice != JOptionPane.YES_OPTION)
			return;

		new LongProcess() {
			public void run() {
				Transaction.start(true);
				for (final Iterator i = SchemaTabSet.this.martTab.getMart()
						.getSchemas().values().iterator(); i.hasNext();) {
					final Schema sch = (Schema) i.next();
					sch.setPartitionNameExpression(null);
					sch.setPartitionRegex(null);
				}
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Update a relation status.
	 * 
	 * @param relation
	 *            the relation to change the status for.
	 * @param status
	 *            the new status to give it.
	 */
	public void requestChangeRelationStatus(final Relation relation,
			final ComponentStatus status) {
		new LongProcess() {
			public void run() throws Exception {
				try {
					Transaction.start(false);
					relation.setStatus(status);
				} finally {
					Transaction.end();
				}
			}
		}.start();
	}

	/**
	 * Ask the user to define a foreign key on a table, then create it.
	 * 
	 * @param table
	 *            the table to define the key on.
	 */
	public void requestCreateForeignKey(final Table table) {
		// Pop up a dialog to ask which columns to use.
		final KeyDialog dialog = new KeyDialog(table, Resources
				.get("newFKDialogTitle"), Resources.get("addButton"), null);
		dialog.setLocationRelativeTo(null);
		dialog.setVisible(true);
		final Column[] cols = dialog.getSelectedColumns();
		dialog.dispose();

		// If they chose some columns, create the key.
		if (cols.length > 0)
			this.requestCreateForeignKey(table, cols);
	}

	/**
	 * Given a set of columns, create a foreign key on the given table that
	 * contains those columns in the order they appear in the iterator.
	 * 
	 * @param table
	 *            the table to create the key over.
	 * @param columns
	 *            the columns to include the key.
	 */
	public void requestCreateForeignKey(final Table table,
			final Column[] columns) {
		new LongProcess() {
			public void run() {
				Transaction.start(false);
				final ForeignKey fk = new ForeignKey(columns);
				fk.setStatus(ComponentStatus.HANDMADE);
				table.getForeignKeys().add(fk);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Ask the user to define a primary key on a table, then create it.
	 * 
	 * @param table
	 *            the table to define the key on.
	 */
	public void requestCreatePrimaryKey(final Table table) {
		// Pop up a dialog to ask which columns to use.
		final KeyDialog dialog = new KeyDialog(table, Resources
				.get("newPKDialogTitle"), Resources.get("addButton"), null);
		dialog.setLocationRelativeTo(null);
		dialog.setVisible(true);
		final Column[] cols = dialog.getSelectedColumns();
		dialog.dispose();

		// If they chose some columns, create the key.
		if (cols.length > 0)
			this.requestCreatePrimaryKey(table, cols);
	}

	/**
	 * Given a set of columns, create a primary key on the given table that
	 * contains those columns in the order they appear in the iterator. This
	 * will replace any existing primary key on the table.
	 * 
	 * @param table
	 *            the table to create the key over.
	 * @param columns
	 *            the columns to include the key.
	 */
	public void requestCreatePrimaryKey(final Table table,
			final Column[] columns) {
		new LongProcess() {
			public void run() {
				Transaction.start(false);
				final PrimaryKey pk = new PrimaryKey(columns);
				pk.setStatus(ComponentStatus.HANDMADE);
				table.setPrimaryKey(pk);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Given a key, ask the user which other key they want to make a relation to
	 * from this key.
	 * 
	 * @param from
	 *            the key to make a relation from.
	 */
	public void requestCreateRelation(final Key from) {
		// Ask them which key they want to link to.
		final Key to = this.askUserForTargetKey(from);

		// If they selected something, create the relation to it.
		if (to != null)
			this.requestCreateRelation(from, to);
	}

	/**
	 * Given a pair of keys, establish a relation between them.
	 * 
	 * @param from
	 *            one end of the relation.
	 * @param to
	 *            the other end.
	 */
	public void requestCreateRelation(final Key from, final Key to) {
		// Create the relation in the background.
		new LongProcess() {
			public void run() throws Exception {
				try {
					Transaction.start(true);
					final Relation rel = new Relation(
							from,
							to,
							from instanceof PrimaryKey ? (to instanceof PrimaryKey ? Cardinality.ONE
									: Cardinality.MANY_A)
									: (to instanceof PrimaryKey ? Cardinality.MANY_B
											: Cardinality.MANY_A));
					rel.setStatus(ComponentStatus.HANDMADE);
					from.getRelations().add(rel);
					to.getRelations().add(rel);
				} finally {
					Transaction.end();
				}
			}
		}.start();
	}

	/**
	 * Asks that a table be (un)ignored.
	 * 
	 * @param table
	 *            the table to (un)ignore.
	 * @param ignored
	 *            ignore it?
	 */
	public void requestIgnoreTable(final Table table, final boolean ignored) {
		new LongProcess() {
			public void run() {
				Transaction.start(false);
				table.setMasked(ignored);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Asks that a schema be (un)masked.
	 * 
	 * @param s
	 *            the schema we are working with.
	 * @param masked
	 *            mask it?
	 */
	public void requestMaskSchema(final Schema s, final boolean masked) {
		new LongProcess() {
			public void run() {
				Transaction.start(false);
				s.setMasked(masked);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Pop up a dialog describing the key, and ask the user to modify it, before
	 * carrying out the modification.
	 * 
	 * @param key
	 *            the key to edit.
	 */
	public void requestEditKey(final Key key) {
		// Pop up the dialog which describes the key, and obtain the
		// list of columns they selected in response.
		final KeyDialog dialog = new KeyDialog(key.getTable(), Resources
				.get("editKeyDialogTitle"), Resources.get("modifyButton"), key
				.getColumns());
		dialog.setLocationRelativeTo(null);
		dialog.setVisible(true);
		final Column[] cols = dialog.getSelectedColumns();
		dialog.dispose();

		// If they selected any columns, modify the key.
		if (cols.length > 0)
			new LongProcess() {
				public void run() {
					Transaction.start(false);
					key.setColumns(cols);
					key.setStatus(ComponentStatus.HANDMADE);
					Transaction.end();
				}
			}.start();
	}

	/**
	 * Turn keyguessing on for a schema.
	 * 
	 * @param schema
	 *            the schema to turn keyguessing on for.
	 * @param keyGuessing
	 *            <tt>true</tt> to turn it on, not for off.
	 */
	public void requestKeyGuessing(final Schema schema,
			final boolean keyGuessing) {
		// Create a progress monitor.
		final ProgressDialog progressMonitor = new ProgressDialog(this, 0, 100,
				false);
		progressMonitor.setVisible(true);

		// Start the construction in a thread. It does not need to be
		// Swing-thread-safe because it will never access the GUI. All
		// GUI interaction is done through the Timer below.
		final SwingWorker worker = new SwingWorker() {
			public Object construct() {
				Transaction.start(true);
				try {
					schema.setKeyGuessing(keyGuessing);
				} catch (final Throwable t) {
					SwingUtilities.invokeLater(new Runnable() {
						public void run() {
							StackTrace.showStackTrace(t);
						}
					});
				}
				Transaction.end();
				return null;
			}

			public void finished() {
				// Close the progress dialog.
				progressMonitor.setVisible(false);
				progressMonitor.dispose();
				// This is to ensure that any modified flags get cleared.
				((SchemaDiagram) SchemaTabSet.this.schemaToDiagram.get(schema
						.getName())).repaintDiagram();
			}
		};

		// Create a timer thread that will update the progress dialog.
		// We use the Swing Timer to make it Swing-thread-safe. (1000 millis
		// equals 1 second.)
		final Timer timer = new Timer(300, null);
		timer.setInitialDelay(0); // Start immediately upon request.
		timer.setCoalesce(true); // Coalesce delayed events.
		timer.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						final double progress = schema.getProgress();
						// Did the job complete yet?
						if (progress < 100.0 && progressMonitor.isVisible())
							// If not, update the progress report.
							progressMonitor.setProgress((int) progress);
						else {
							// If it completed, close the task and tidy up.
							// Stop the timer.
							timer.stop();
						}
					}
				});
			}
		});

		// Start the timer.
		timer.start();
		worker.start();
	}

	/**
	 * Pops up a dialog with details of the schema, which allows the user to
	 * modify them.
	 * 
	 * @param schema
	 *            the schema to modify.
	 */
	public void requestModifySchema(final Schema schema) {
		if (SchemaConnectionDialog.modifySchema(schema))
			this.requestSynchroniseSchema(schema, true);
	}

	/**
	 * Remove a key.
	 * 
	 * @param key
	 *            the key to remove.
	 */
	public void requestRemoveKey(final Key key) {
		new LongProcess() {
			public void run() {
				Transaction.start(false);
				if (key instanceof PrimaryKey)
					key.getTable().setPrimaryKey(null);
				else
					key.getTable().getForeignKeys().remove(key);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Remove a relation.
	 * 
	 * @param relation
	 *            the relation to remove.
	 */
	public void requestRemoveRelation(final Relation relation) {
		new LongProcess() {
			public void run() {
				Transaction.start(false);
				relation.getFirstKey().getRelations().remove(relation);
				relation.getSecondKey().getRelations().remove(relation);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Confirms with user then removes a schema.
	 * 
	 * @param schema
	 *            the schema to remove.
	 */
	public void requestRemoveSchema(final Schema schema) {
		// Confirm if the user really wants to do it.
		final int choice = JOptionPane.showConfirmDialog(null, Resources
				.get("confirmDelSchema"), Resources.get("questionTitle"),
				JOptionPane.YES_NO_OPTION);

		// If they don't, cancel out.
		if (choice != JOptionPane.YES_OPTION)
			return;

		new LongProcess() {
			public void run() {
				Transaction.start(false);
				SchemaTabSet.this.martTab.getMart().getSchemas().remove(
						schema.getOriginalName());
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Asks user for a new name, then renames a schema.
	 * 
	 * @param schema
	 *            the schema to rename.
	 */
	public void requestRenameSchema(final Schema schema) {
		// Ask for a new name, suggesting the schema's existing name
		// as the default response.
		this.requestRenameSchema(schema, this.askUserForSchemaName(schema
				.getName()));
	}

	/**
	 * Requests that the schema be given the new name, now, without further
	 * prompting
	 * 
	 * @param schema
	 *            the schema to rename.
	 * @param name
	 *            the new name to give it.
	 */
	public void requestRenameSchema(final Schema schema, final String name) {
		// Ask for a new name, suggesting the schema's existing name
		// as the default response.
		final String newName = name == null ? "" : name.trim();

		// If they cancelled or entered the same name, ignore the request.
		if (newName.length() == 0)
			return;

		new LongProcess() {
			public void run() {
				Transaction.start(false);
				schema.setName(newName);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * Shows some rows of the table in a {@link JTable} in a popup dialog.
	 * 
	 * @param table
	 *            the table to show rows from.
	 * @param count
	 *            how many rows to show.
	 */
	public void requestShowRows(final Table table, final int count) {
		new LongProcess() {
			public void run() throws Exception {
				// Get the rows.
				final Collection rows = table.getSchema().getRows(
						martTab.getPartitionViewSelection(), table, count);
				// Convert to a nested vector.
				final Vector data = new Vector();
				for (final Iterator i = rows.iterator(); i.hasNext();)
					data.add(new Vector((List) i.next()));
				// Get the column names.
				final Vector colNames = new Vector(table.getColumns().keySet());
				// Construct a JTable.
				final JTable jtable = new JTable(new DefaultTableModel(data,
						colNames));
				final Dimension size = new Dimension();
				size.width = 0;
				size.height = jtable.getRowHeight() * count;
				for (int i = 0; i < jtable.getColumnCount(); i++)
					size.width += jtable.getColumnModel().getColumn(i)
							.getPreferredWidth();
				size.width = Math.min(size.width, 800); // Arbitrary.
				size.height = Math.min(size.height, 200); // Arbitrary.
				jtable.setPreferredScrollableViewportSize(size);
				// Display them.
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						JOptionPane.showMessageDialog(null, new JScrollPane(
								jtable), Resources.get("showRowsDialogTitle",
								new String[] { "" + count, table.getName() }),
								JOptionPane.INFORMATION_MESSAGE);
					}
				});
			}
		}.start();
	}

	/**
	 * Synchronises all schemas in the mart.
	 */
	public void requestSynchroniseAllSchemas() {
		// Create a progress monitor.
		final ProgressDialog progressMonitor = new ProgressDialog(this, 0, 100,
				false);
		progressMonitor.setVisible(true);

		// Start the construction in a thread. It does not need to be
		// Swing-thread-safe because it will never access the GUI. All
		// GUI interaction is done through the Timer below.
		final List allSchemas = new ArrayList(SchemaTabSet.this.martTab
				.getMart().getSchemas().values());
		final List doneSchemas = new ArrayList();
		final double scale = 1.0 / allSchemas.size();
		final SwingWorker worker = new SwingWorker() {
			public Object construct() {
				Transaction.start(true);
				while (allSchemas.size() > 0)
					try {
						((Schema) allSchemas.get(0)).synchronise();
						doneSchemas.add(allSchemas.remove(0));
					} catch (final Throwable t) {
						SwingUtilities.invokeLater(new Runnable() {
							public void run() {
								StackTrace.showStackTrace(t);
							}
						});
					}
				Transaction.end();
				return null;
			}

			public void finished() {
				// Close the progress dialog.
				progressMonitor.setVisible(false);
				progressMonitor.dispose();
				// This is to ensure that any modified flags get cleared.
				for (final Iterator i = SchemaTabSet.this.schemaToDiagram
						.values().iterator(); i.hasNext();)
					((SchemaDiagram) i.next()).repaintDiagram();
			}

		};
		worker.start();

		// Create a timer thread that will update the progress dialog.
		// We use the Swing Timer to make it Swing-thread-safe. (1000 millis
		// equals 1 second.)
		final Timer timer = new Timer(300, null);
		timer.setInitialDelay(300); // Start immediately upon request.
		timer.setCoalesce(true); // Coalesce delayed events.
		timer.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						final double progress = scale
								* 100.0
								* doneSchemas.size()
								+ (allSchemas.size() == 0 ? 0.0
										: ((Schema) allSchemas.get(0))
												.getProgress()
												* scale);
						// Did the job complete yet?
						if (progress < 100.0 && progressMonitor.isVisible())
							// If not, update the progress report.
							progressMonitor.setProgress((int) progress);
						else {
							// If it completed, close the task and tidy up.
							// Stop the timer.
							timer.stop();
						}
					}
				});
			}
		});

		// Start the timer.
		timer.start();
	}

	/**
	 * Syncs this schema against the database.
	 * 
	 * @param schema
	 *            the schema to synchronise.
	 * @param transactionMod
	 *            <tt>true</tt> if the transaction is allowed to show visible
	 *            modifications.
	 */
	public void requestSynchroniseSchema(final Schema schema,
			final boolean transactionMod) {
		// Create a progress monitor.
		final ProgressDialog progressMonitor = new ProgressDialog(this, 0, 100,
				false);
		progressMonitor.setVisible(true);

		// Start the construction in a thread. It does not need to be
		// Swing-thread-safe because it will never access the GUI. All
		// GUI interaction is done through the Timer below.
		final SwingWorker worker = new SwingWorker() {
			public Object construct() {
				Transaction.start(transactionMod);
				try {
					schema.synchronise();
				} catch (final Throwable t) {
					SwingUtilities.invokeLater(new Runnable() {
						public void run() {
							StackTrace.showStackTrace(t);
						}
					});
				}
				Transaction.end();
				return null;
			}

			public void finished() {
				// Close the progress dialog.
				progressMonitor.setVisible(false);
				progressMonitor.dispose();
				// This is to ensure that any modified flags get cleared.
				((SchemaDiagram) SchemaTabSet.this.schemaToDiagram.get(schema
						.getName())).repaintDiagram();
			}
		};

		// Create a timer thread that will update the progress dialog.
		// We use the Swing Timer to make it Swing-thread-safe. (1000 millis
		// equals 1 second.)
		final Timer timer = new Timer(300, null);
		timer.setInitialDelay(0); // Start immediately upon request.
		timer.setCoalesce(true); // Coalesce delayed events.
		timer.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						final double progress = schema.getProgress();
						// Did the job complete yet?
						if (progress < 100.0 && progressMonitor.isVisible())
							// If not, update the progress report.
							progressMonitor.setProgress((int) progress);
						else {
							// If it completed, close the task and tidy up.
							// Stop the timer.
							timer.stop();
						}
					}
				});
			}
		});

		// Start the timer.
		timer.start();
		worker.start();
	}

	/**
	 * Request that all changes on this schema are accepted.
	 * 
	 * @param sch
	 *            the target schema.
	 */
	public void requestAcceptAll(final Schema sch) {
		new LongProcess() {
			public void run() {
				final List modTbls = new ArrayList();
				Transaction.start(true);
				for (final Iterator i = sch.getTables().values().iterator(); i
						.hasNext();) {
					final Table tbl = (Table) i.next();
					if (tbl.isVisibleModified()) {
						modTbls.add(tbl);
						for (final Iterator k = tbl.getColumns().values().iterator(); k.hasNext(); )
							((Column)k.next()).setVisibleModified(false);
						for (final Iterator k = tbl.getRelations().iterator(); k.hasNext(); )
							((Relation)k.next()).setVisibleModified(false);
					}
				}
				Transaction.end();
				for (final Iterator i = sch.getMart().getDataSets().values()
						.iterator(); i.hasNext();) {
					final DataSet ds = (DataSet) i.next();
					if (!ds.isVisibleModified())
						continue;
					for (final Iterator j = modTbls.iterator(); j.hasNext();) {
						final Table modTbl = (Table)j.next();
						SchemaTabSet.this.getMartTab().getDataSetTabSet()
								.requestAcceptAll(ds, modTbl);
					}
				}
			}
		}.start();
	}

	/**
	 * Request that all changes on this schema are rejected.
	 * 
	 * @param sch
	 *            the target schema.
	 */
	public void requestRejectAll(final Schema sch) {
		new LongProcess() {
			public void run() {
				final List modTbls = new ArrayList();
				Transaction.start(true);
				for (final Iterator i = sch.getTables().values().iterator(); i
						.hasNext();) {
					final Table tbl = (Table) i.next();
					if (tbl.isVisibleModified()) {
						modTbls.add(tbl);
						for (final Iterator k = tbl.getColumns().values().iterator(); k.hasNext(); )
							((Column)k.next()).setVisibleModified(false);
						for (final Iterator k = tbl.getRelations().iterator(); k.hasNext(); )
							((Relation)k.next()).setVisibleModified(false);
					}
				}
				Transaction.end();
				for (final Iterator i = sch.getMart().getDataSets().values()
						.iterator(); i.hasNext();) {
					final DataSet ds = (DataSet) i.next();
					if (!ds.isVisibleModified())
						continue;
					for (final Iterator j = modTbls.iterator(); j.hasNext();) {
						final Table modTbl = (Table)j.next();
						SchemaTabSet.this.getMartTab().getDataSetTabSet()
								.requestRejectAll(ds, modTbl);
					}
				}
			}
		}.start();
	}

	/**
	 * Sets the diagram context to use for all {@link Diagram}s inside this
	 * schema tabset. Once set,
	 * {@link Diagram#setDiagramContext(DiagramContext)} is called on each
	 * diagram in the tabset in turn so that they are all working with the same
	 * context.
	 * 
	 * @param diagramContext
	 *            the context to use for all {@link Diagram}s in this schema
	 *            tabset.
	 */
	public void setDiagramContext(final DiagramContext diagramContext) {
		this.diagramContext = diagramContext;
		this.allSchemasDiagram.setDiagramContext(diagramContext);
		for (final Iterator i = this.schemaToDiagram.values().iterator(); i
				.hasNext();)
			((Diagram) i.next()).setDiagramContext(diagramContext);
	}
}
