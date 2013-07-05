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

package org.ensembl.mart.editor;

import java.awt.Dimension;
import java.awt.Insets;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.datatransfer.ClipboardOwner;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.dnd.Autoscroll;
import java.awt.dnd.DnDConstants;
import java.awt.dnd.DragGestureEvent;
import java.awt.dnd.DragGestureListener;
import java.awt.dnd.DragSource;
import java.awt.dnd.DragSourceDragEvent;
import java.awt.dnd.DragSourceDropEvent;
import java.awt.dnd.DragSourceEvent;
import java.awt.dnd.DragSourceListener;
import java.awt.dnd.DropTarget;
import java.awt.dnd.DropTargetDragEvent;
import java.awt.dnd.DropTargetDropEvent;
import java.awt.dnd.DropTargetEvent;
import java.awt.dnd.DropTargetListener;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPopupMenu;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeExpansionListener;
import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.TableCellEditor;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import org.ensembl.mart.lib.config.AttributeCollection;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributeList;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.BaseConfigurationObject;
import org.ensembl.mart.lib.config.BaseNamedConfigurationObject;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DatabaseDSConfigAdaptor;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.lib.config.DynamicDataset;
import org.ensembl.mart.lib.config.Exportable;
import org.ensembl.mart.lib.config.FilterCollection;
import org.ensembl.mart.lib.config.FilterDescription;
import org.ensembl.mart.lib.config.FilterGroup;
import org.ensembl.mart.lib.config.FilterPage;
import org.ensembl.mart.lib.config.Importable;
import org.ensembl.mart.lib.config.Option;
import org.ensembl.mart.lib.config.PushAction;
import org.ensembl.mart.lib.config.SpecificAttributeContent;
import org.ensembl.mart.lib.config.SpecificFilterContent;
import org.ensembl.mart.lib.config.SpecificOptionContent;
import org.ensembl.mart.lib.config.URLDSConfigAdaptor;

/**
 * Class DatasetConfigTree extends JTree.
 * 
 * <p>
 * This is the main class of the config editor that creates and populates the
 * tree etc
 * </p>
 * 
 * @author <a href="mailto:katerina@ebi.ac.uk">Katerina Tzouvara</a> //@see
 *         org.ensembl.mart.config.DatasetConfig
 */

public class DatasetConfigTree extends JTree implements Autoscroll { // ,
																		// ClipboardOwner
																		// {

	public static final Insets defaultScrollInsets = new Insets(8, 8, 8, 8);
	protected Insets scrollInsets = defaultScrollInsets;
	protected DatasetConfig dsConfig = null;
	protected DatasetConfigTreeNode lastSelectedNode = null;
	protected DatasetConfigTreeNode editingNode = null;
	protected DatasetConfigTreeNode editingNodeParent = null;
	protected DatasetConfigTreeNode rootNode = null;
	protected TreePath clickedPath = null;
	protected DatasetConfigTreeModel treemodel = null;
	protected DatasetConfigTreeWidget frame;
	protected DatasetConfigAttributesTable attrTable = null;
	protected DatasetConfigAttributeTableModel attrTableModel = null;
	// protected Clipboard clipboard;
	protected boolean cut = false;
	protected int editingNodeIndex;
	protected File file = null;

	public DatasetConfigTree(DatasetConfig dsConfig,
			DatasetConfigTreeWidget frame,
			DatasetConfigAttributesTable attrTable) {
		super((TreeModel) null);
		this.dsConfig = dsConfig;
		this.frame = frame;
		this.attrTable = attrTable;
		file = frame.getFileChooserPath();
		addMouseListener(new DatasetConfigTreeMouseListener());
		addTreeSelectionListener(new DatasetConfigTreeSelectionListener());
		// Use horizontal and vertical lines
		putClientProperty("JTree.lineStyle", "Angled");
		setEditable(true);
		// Create the first node
		rootNode = new DatasetConfigTreeNode(dsConfig.getDisplayName());
		rootNode.setUserObject(dsConfig);
		treemodel = new DatasetConfigTreeModel(rootNode, dsConfig);
		setModel(treemodel);
		this.setSelectionInterval(0, 0);
		DatasetConfigTreeDnDListener dndListener = new DatasetConfigTreeDnDListener(
				this);
		// clipboard = new Clipboard("tree_clipboard");

	}

	public DatasetConfig getDatasetConfig() {
		dsConfig = (DatasetConfig) rootNode.getUserObject();
		return dsConfig;
	}

	// Autoscrolling support
	public void setScrollInsets(Insets insets) {
		this.scrollInsets = insets;
	}

	public Insets getScrollInsets() {
		return scrollInsets;
	}

	// Implementation of Autoscroll interface
	public Insets getAutoscrollInsets() {
		Rectangle r = getVisibleRect();
		Dimension size = getSize();
		Insets i = new Insets(r.y + scrollInsets.top, r.x + scrollInsets.left,
				size.height - r.y - r.height + scrollInsets.bottom, size.width
						- r.x - r.width + scrollInsets.right);
		return i;
	}

	public void autoscroll(Point location) {
		JScrollPane scroller = (JScrollPane) SwingUtilities.getAncestorOfClass(
				JScrollPane.class, this);
		if (scroller != null) {
			JScrollBar hBar = scroller.getHorizontalScrollBar();
			JScrollBar vBar = scroller.getVerticalScrollBar();
			Rectangle r = getVisibleRect();
			if (location.x <= r.x + scrollInsets.left) {
				// Need to scroll left
				hBar.setValue(hBar.getValue() - hBar.getUnitIncrement(-1));
			}
			if (location.y <= r.y + scrollInsets.top) {
				// Need to scroll up
				vBar.setValue(vBar.getValue() - vBar.getUnitIncrement(-1));
			}
			if (location.x >= r.x + r.width - scrollInsets.right) {
				// Need to scroll right
				hBar.setValue(hBar.getValue() + hBar.getUnitIncrement(1));
			}
			if (location.y >= r.y + r.height - scrollInsets.bottom) {
				// Need to scroll down
				vBar.setValue(vBar.getValue() + vBar.getUnitIncrement(1));
			}
		}

	}

	// Inner class that handles Tree Expansion Events
	protected class DatasetConfigTreeExpansionHandler implements
			TreeExpansionListener {
		public void treeExpanded(TreeExpansionEvent evt) {
			TreePath path = evt.getPath(); // The expanded path
			JTree tree = (JTree) evt.getSource(); // The tree

			// Get the last component of the path and
			// arrange to have it fully populated.
			DatasetConfigTreeNode node = (DatasetConfigTreeNode) path
					.getLastPathComponent();
			/*
			 * if (node.populateFolders(true)) { ((DefaultTreeModel)
			 * tree.getModel()).nodeStructureChanged(node); }
			 */
		}

		public void treeCollapsed(TreeExpansionEvent evt) {
			// Nothing to do
		}
	}

	// Inner class that handles Tree Expansion Events
	protected class AttrTableModelListener implements TableModelListener {
		public void tableChanged(TableModelEvent evt) {

			// treemodel.reload(attrTableModel.getNode(),(DatasetConfigTreeNode)attrTableModel.getNode().getParent());
			treemodel.reload(attrTableModel.getParentNode());
		}

		public void treeCollapsed(TreeExpansionEvent evt) {
			// Nothing to do
		}
	}

	// Inner class that handles Menu Action Events
	protected class MenuActionListener implements ActionListener {
		public void actionPerformed(ActionEvent e) {
			try {
				if (e.getActionCommand().equals("cut"))
					cut();
				else if (e.getActionCommand().equals("copy"))
					copy();
				else if (e.getActionCommand().equals("paste"))
					paste();
				else if (e.getActionCommand().equals("insert importable")) {
					Importable ad = new Importable();
					ad.setAttribute("internalName", "new");
					insert(ad, "Importable:");
				} else if (e.getActionCommand().equals("edit main table(s)"))
					editMains();
				else if (e.getActionCommand().equals("edit primary key(s)"))
					editKeys();
				else if (e.getActionCommand().equals("insert exportable")) {
					Exportable ad = new Exportable();
					ad.setAttribute("internalName", "new");
					insert(ad, "Exportable:");
				} else if (e.getActionCommand().equals("insert filter page"))
					insert(new FilterPage("new"), "FilterPage:");
				else if (e.getActionCommand().equals("insert attribute page"))
					insert(new AttributePage("new"), "AttributePage:");
				else if (e.getActionCommand().equals("insert filter group"))
					insert(new FilterGroup("new"), "FilterGroup:");
				else if (e.getActionCommand().equals("insert attribute group"))
					insert(new AttributeGroup("new"), "AttributeGroup:");

				else if (e.getActionCommand()
						.equals("insert filter collection"))
					insert(new FilterCollection("new"), "FilterCollection:");
				else if (e.getActionCommand().equals(
						"insert attribute collection"))
					insert(new AttributeCollection("new"),
							"AttributeCollection:");
				else if (e.getActionCommand().equals("insert filter")) {
					FilterDescription fd = new FilterDescription();
					fd.setAttribute("internalName", "new");
					insert(fd, "FilterDescription");
				} else if (e.getActionCommand().equals("insert attribute list")) {
					AttributeList ad = new AttributeList();
					ad.setAttribute("internalName", "new");
					insert(ad, "AttributeList");
				} else if (e.getActionCommand().equals("insert attribute")) {
					AttributeDescription ad = new AttributeDescription();
					ad.setAttribute("internalName", "new");
					insert(ad, "AttributeDescription");
				} else if (e.getActionCommand().equals(
						"insert specific filter content")) {
					SpecificFilterContent dynAtt = new SpecificFilterContent();
					DatasetConfig dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) DatasetConfigTree.this
							.getModel().getRoot()).getUserObject();
					if (dsConfig.getTemplateFlag() != null
							&& dsConfig.getTemplateFlag().equals("1")) {
						String[] datasets = dsConfig.getDynamicDatasetNames();
						FilterDescription fd = (FilterDescription) ((DatasetConfigTreeNode) clickedPath
								.getLastPathComponent()).getUserObject();
						String intName = datasets.length == 0 ? "new"
								: (String) JOptionPane.showInputDialog(null,
										null, "Select dataset:",
										JOptionPane.QUESTION_MESSAGE, null,
										datasets, datasets[0]);
						dynAtt.setAttribute("internalName",
								intName == null ? "new" : intName);
						dynAtt.setAttribute("tableConstraint",
								fd.getTableConstraint());
						dynAtt.setAttribute("field", fd.getField());
						dynAtt.setAttribute("key", fd.getKey());
						insert(dynAtt, "SpecificFilterContent");
					}
				} else if (e.getActionCommand().equals(
						"insert specific option content")) {
					SpecificOptionContent dynAtt = new SpecificOptionContent();
					DatasetConfig dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) DatasetConfigTree.this
							.getModel().getRoot()).getUserObject();
					if (dsConfig.getTemplateFlag() != null
							&& dsConfig.getTemplateFlag().equals("1")) {
						String[] datasets = dsConfig.getDynamicDatasetNames();
						Option fd = (Option) ((DatasetConfigTreeNode) clickedPath
								.getLastPathComponent()).getUserObject();
						String intName = datasets.length == 0 ? "new"
								: (String) JOptionPane.showInputDialog(null,
										null, "Select dataset:",
										JOptionPane.QUESTION_MESSAGE, null,
										datasets, datasets[0]);
						dynAtt.setAttribute("internalName",
								intName == null ? "new" : intName);
						dynAtt.setAttribute("tableConstraint",
								fd.getTableConstraint());
						dynAtt.setAttribute("field", fd.getField());
						dynAtt.setAttribute("key", fd.getKey());
						dynAtt.setAttribute("value", fd.getValue());
						insert(dynAtt, "SpecificOptionContent");
					}
				} else if (e.getActionCommand().equals(
						"insert specific attribute content")) {
					SpecificAttributeContent dynAtt = new SpecificAttributeContent();
					DatasetConfig dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) DatasetConfigTree.this
							.getModel().getRoot()).getUserObject();
					if (dsConfig.getTemplateFlag() != null
							&& dsConfig.getTemplateFlag().equals("1")) {
						String[] datasets = dsConfig.getDynamicDatasetNames();
						AttributeDescription fd = (AttributeDescription) ((DatasetConfigTreeNode) clickedPath
								.getLastPathComponent()).getUserObject();
						String intName = datasets.length == 0 ? "new"
								: (String) JOptionPane.showInputDialog(null,
										null, "Select dataset:",
										JOptionPane.QUESTION_MESSAGE, null,
										datasets, datasets[0]);
						dynAtt.setAttribute("internalName",
								intName == null ? "new" : intName);
						dynAtt.setAttribute("tableConstraint",
								fd.getTableConstraint());
						dynAtt.setAttribute("field", fd.getField());
						dynAtt.setAttribute("key", fd.getKey());
						insert(dynAtt, "SpecificAttributeContent");
					}
				} else if (e.getActionCommand().equals("insert option")) {
					Option option = new Option();
					option.setAttribute("internalName", "new");
					insert(option, "Option");
				} else if (e.getActionCommand().equals("insert push action")) {
					PushAction pa = new PushAction();
					pa.setAttribute("ref", "new");
					insert(pa, "PushAction");
				} else if (e.getActionCommand().equals("automate push action")) {
					addPushAction();
				} else if (e.getActionCommand().equals("make drop down")) {
					makeDropDown();
				} else if (e.getActionCommand().equals("auto specific filters")) {
					autoSpecifics();
				} else if (e.getActionCommand().equals(
						"auto specific dropdowns")) {
					autoSpecificDropDowns();
				} else if (e.getActionCommand().equals(
						"generate auto specific dropdowns for entire template")) {
					allAutoSpecificDropDowns();
				} else if (e.getActionCommand().equals(
						"auto specific push actions")) {
					autoSpecificPushActions();
				} else if (e.getActionCommand().equals("add ontology")) {
					addOntology();
				} else if (e.getActionCommand().equals("delete"))
					delete();
				else if (e.getActionCommand().equals("delete options"))
					deleteOptions();
				else if (e.getActionCommand().equals("save"))
					save();
				else if (e.getActionCommand().equals("save as"))
					save_as();
				else if (e.getActionCommand().equals("hide toggle"))
					makeHidden();
				else if (e.getActionCommand().equals("hideDisplay toggle"))
					makeDisplay();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	// Inner class that handles Tree Selection Events
	protected class DatasetConfigTreeSelectionListener implements
			TreeSelectionListener {
		public void valueChanged(TreeSelectionEvent e) {
			doOnSelection();
		}
	}

	private void doOnSelection() {
		if (attrTable != null)
			if (attrTable.getEditorComponent() != null) {
				TableCellEditor attrTableEditor = attrTable.getCellEditor();
				// attrTableEditor.stopCellEditing();// this was making
				// setValueAt be called twice and breaking the duplication
				// testing
			}
		lastSelectedNode = (DatasetConfigTreeNode) this
				.getLastSelectedPathComponent();

		if (lastSelectedNode == null)
			return;
		BaseConfigurationObject nodeObject = (BaseConfigurationObject) lastSelectedNode
				.getUserObject();
		String nodeObjectClass = nodeObject.getClass().getName();
		String[] data = nodeObject.getXmlAttributeTitles();

		attrTableModel = new DatasetConfigAttributeTableModel(
				(DatasetConfigTreeNode) this.getLastSelectedPathComponent(),
				data, nodeObjectClass);
		attrTableModel.addTableModelListener(new AttrTableModelListener());

		// TESTING
		// DefaultTableCellRenderer tcr = (DefaultTableCellRenderer)
		// attrTable.getCellRenderer(0,0);
		// tcr.setBackground(Color.red);
		// DefaultTableCellRenderer tcr2 = (DefaultTableCellRenderer)
		// attrTable.getCellRenderer(1,1);
		// tcr2.setBackground(Color.white);

		// model.setObject(nodeObject);
		attrTable.setModel(attrTableModel);

	}

	// Inner class that handles Tree Model Events
	protected class DatasetConfigTreeModelListener implements TreeModelListener {
		public void treeNodesChanged(TreeModelEvent e) {
			System.out.println("treeNodesChanged");
		}

		public void treeStructureChanged(TreeModelEvent e) {
			System.out.println("tree structure changed");
		}

		public void treeNodesInserted(TreeModelEvent e) {
			TreePath tPath = e.getTreePath();
			System.out.println("tree nodes inserted");
			// tPath.getPathComponent();
		}

		public void treeNodesRemoved(TreeModelEvent e) {
			System.out.println("tree nodes removed");
		}

	}

	// Inner class that handles Tree Expansion Events
	protected class DatasetConfigTreeDnDListener implements DropTargetListener,
			DragSourceListener, DragGestureListener {
		protected DropTarget dropTarget = null;
		protected DragSource dragSource = null;
		protected DatasetConfigTreeNode selnode = null;
		protected DatasetConfigTreeNode dropnode = null;

		public DatasetConfigTreeDnDListener(DatasetConfigTree tree) {
			dropTarget = new DropTarget(tree, this);
			dragSource = new DragSource();
			dragSource.createDefaultDragGestureRecognizer(tree,
					DnDConstants.ACTION_MOVE, this);
		}

		public void dragEnter(DropTargetDragEvent event) {
			event.acceptDrag(DnDConstants.ACTION_MOVE);
		}

		public void dragExit(DropTargetEvent event) {
		}

		public void dragOver(DropTargetDragEvent event) {
		}

		public void drop(DropTargetDropEvent event) {
			try {
				Transferable transferable = event.getTransferable();

				if (transferable.isDataFlavorSupported(DataFlavor.stringFlavor)) {
					event.acceptDrop(DnDConstants.ACTION_MOVE);
					String s = (String) transferable
							.getTransferData(DataFlavor.stringFlavor);
					Object ob = event.getSource();
					Point droppoint = event.getLocation();
					TreePath droppath = getClosestPathForLocation(droppoint.x,
							droppoint.y);
					dropnode = (DatasetConfigTreeNode) droppath
							.getLastPathComponent();
					event.getDropTargetContext().dropComplete(true);
				} else {
					event.rejectDrop();
				}
			} catch (IOException exception) {
				event.rejectDrop();
			} catch (UnsupportedFlavorException ufException) {
				event.rejectDrop();
			}
		}

		public void dropActionChanged(DropTargetDragEvent event) {
		}

		public void dragGestureRecognized(DragGestureEvent event) {
			selnode = null;
			dropnode = null;
			Object selected = getSelectionPath();
			TreePath treepath = (TreePath) selected;
			selnode = (DatasetConfigTreeNode) treepath.getLastPathComponent();
			if (selected != null) {
				StringSelection text = new StringSelection(selected.toString());
				dragSource.startDrag(event, DragSource.DefaultMoveDrop, text,
						this);
			} else {
			}
		}

		public void dragDropEnd(DragSourceDropEvent event) {
			if (event.getDropSuccess()) {
				try {
					if (dropnode.equals(selnode)) {
						String result = "Error, illegal action, drag==drop, the source is \nthe same as the destination";
						JOptionPane.showMessageDialog(frame, result, "Error",
								JOptionPane.ERROR_MESSAGE);
						return;
					} else {
						String result = new String();
						DatasetConfigTreeNode selnodeParent;
						int selnodeIndex;
						if (selnode.getUserObject().getClass()
								.equals(dropnode.getUserObject().getClass())) {
							selnodeParent = (DatasetConfigTreeNode) selnode
									.getParent();
							selnodeIndex = selnodeParent.getIndex(selnode);
							treemodel.removeNodeFromParent(selnode);

							if (selnode.getUserObject() instanceof org.ensembl.mart.lib.config.FilterDescription) {
								// can convert FD to Option and insert into
								// another FD
								result = treemodel
										.insertNodeInto(
												selnode,
												dropnode,
												DatasetConfigTreeNode
														.getHeterogenousOffset(
																((DatasetConfigTreeNode) dropnode)
																		.getUserObject(),
																((DatasetConfigTreeNode) selnode)
																		.getUserObject()));
							} else
								result = treemodel
										.insertNodeInto(
												selnode,
												(DatasetConfigTreeNode) dropnode
														.getParent(),
												dropnode.getParent().getIndex(
														dropnode) + 1);
						} else {
							selnodeParent = (DatasetConfigTreeNode) selnode
									.getParent();
							selnodeIndex = selnodeParent.getIndex(selnode);
							treemodel.removeNodeFromParent(selnode);
							result = treemodel
									.insertNodeInto(
											selnode,
											dropnode,
											DatasetConfigTreeNode
													.getHeterogenousOffset(
															((DatasetConfigTreeNode) dropnode)
																	.getUserObject(),
															((DatasetConfigTreeNode) selnode)
																	.getUserObject()));
						}
						if (result.startsWith("Error")) {
							JOptionPane.showMessageDialog(frame, result,
									"Error", JOptionPane.ERROR_MESSAGE);
							treemodel.insertNodeInto(selnode, selnodeParent,
									selnodeIndex);
						}
					}
				} catch (IllegalArgumentException iae) {
					iae.printStackTrace();
				}

			}
		}

		public void dragEnter(DragSourceDragEvent event) {
		}

		public void dragExit(DragSourceEvent event) {
		}

		public void dragOver(DragSourceDragEvent event) {
		}

		public void dropActionChanged(DragSourceDragEvent event) {
		}
	}

	protected class DatasetConfigTreeMouseListener implements MouseListener {
		public void mousePressed(MouseEvent e) {
			// if (attrTable != null)
			// if (attrTable.getEditorComponent() != null) {
			// TableCellEditor attrTableEditor = attrTable.getCellEditor();
			// attrTableEditor.stopCellEditing();
			// }
			// need to evaluate here as well as mouseReleased, for cross
			// platform portability
			if (e.isPopupTrigger()) {
				// Create the popup menu.
				loungePopupMenu(e);
			}
		}

		public void mouseReleased(MouseEvent e) {
			if (e.isPopupTrigger()) {
				// Create the popup menu.
				loungePopupMenu(e);
			}
		}

		public void mouseEntered(MouseEvent e) {
		}

		public void mouseExited(MouseEvent e) {
		}

		public void mouseClicked(MouseEvent e) {
			// if (e.isPopupTrigger()) {
			// //Create the popup menu.
			// loungePopupMenu(e);
			// }
		}
	}

	private void loungePopupMenu(MouseEvent e) {
		JPopupMenu popup = new JPopupMenu();
		clickedPath = this.getClosestPathForLocation(e.getX(), e.getY());
		editingNode = (DatasetConfigTreeNode) clickedPath
				.getLastPathComponent();
		setSelectionPath(clickedPath);
		String[] menuItems = null;
		String clickedNodeClass = editingNode.getUserObject().getClass()
				.getName();
		if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.DatasetConfig"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle", "insert filter page",
					"insert attribute page", "insert importable",
					"insert exportable", "edit main table(s)",
					"edit primary key(s)",
					"generate auto specific dropdowns for entire template" };
		else if ((clickedNodeClass)
				.equals("org.ensembl.mart.lib.config.FilterPage"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle", "insert filter group" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.AttributePage"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle",
					"insert attribute group", };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.FilterGroup"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle",
					"insert filter collection" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.AttributeGroup"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle",
					"insert attribute collection" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.FilterCollection"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle", "insert filter" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.AttributeCollection"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle", "insert attribute",
					"insert attribute list" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.FilterDescription"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"delete options", "hide toggle", "hideDisplay toggle",
					"insert option", "make drop down", "add ontology",
					"automate push action", "insert specific filter content",
					"auto specific filters", "auto specific dropdowns",
					"auto specific push actions", };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.PushAction"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle", "insert push action",
					"automate push action" };
		else if (clickedNodeClass.equals("org.ensembl.mart.lib.config.Option"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "insert option", "insert push action",
					"insert specific option content" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.AttributeDescription"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"hide toggle", "hideDisplay toggle",
					"insert specific attribute content" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.AttributeList"))
			menuItems = new String[] { "copy", "cut", "paste", "delete" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.SpecificFilterContent"))
			menuItems = new String[] { "copy", "cut", "paste", "delete",
					"delete options", "hide toggle", "hideDisplay toggle",
					"insert option", "make drop down", "add ontology",
					"automate push action" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.SpecificAttributeContent"))
			menuItems = new String[] { "copy", "cut", "paste", "delete" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.SpecificOptionContent"))
			menuItems = new String[] { "copy", "cut", "paste", "delete" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.Importable"))
			menuItems = new String[] { "copy", "cut", "paste", "delete" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.DynamicDataset"))
			menuItems = new String[] { "copy", "cut", "paste", "delete" };
		else if (clickedNodeClass
				.equals("org.ensembl.mart.lib.config.Exportable"))
			menuItems = new String[] { "copy", "cut", "paste", "delete" };
		else
			menuItems = new String[0];

		for (int i = 0; i < menuItems.length; i++) {
			JMenuItem menuItem = new JMenuItem(menuItems[i]);
			MenuActionListener menuActionListener = new MenuActionListener();
			menuItem.addActionListener(menuActionListener);
			popup.add(menuItem);
		}
		popup.show(e.getComponent(), e.getX(), e.getY());

	}

	public void cut() {
		cut = true;
		editingNode = setEditingNode();

		if (editingNode == null)
			return;
		if (editingNode.getParent() == null)
			return; // Can't remove root node.

		editingNodeParent = (DatasetConfigTreeNode) editingNode.getParent();
		editingNodeIndex = editingNode.getParent().getIndex(editingNode);
		treemodel.removeNodeFromParent(editingNode);

		copy();
	}

	private DatasetConfigTreeNode setEditingNode() {

		TreePath path = getSelectionPath();
		return ((DatasetConfigTreeNode) path.getLastPathComponent());
	}

	public void copy() {

		// TreePath path = getSelectionPath();
		// if (path==null) return;
		// editingNode= ((DatasetConfigTreeNode)path.getLastPathComponent());

		if (!cut)
			editingNode = setEditingNode();

		if (editingNode == null)
			return;
		String editingNodeClass = editingNode.getUserObject().getClass()
				.getName();
		DatasetConfigTreeNode copiedNode = new DatasetConfigTreeNode("");

		if ((editingNodeClass).equals("org.ensembl.mart.lib.config.FilterPage"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new FilterPage((FilterPage) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.AttributePage"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new AttributePage(
							(AttributePage) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.FilterGroup"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new FilterGroup((FilterGroup) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.AttributeGroup"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new AttributeGroup(
							(AttributeGroup) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.FilterCollection"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new FilterCollection(
							(FilterCollection) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.AttributeCollection"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new AttributeCollection(
							(AttributeCollection) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.FilterDescription"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new FilterDescription(
							(FilterDescription) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.AttributeDescription"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new AttributeDescription(
							(AttributeDescription) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.AttributeList"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new AttributeList(
							(AttributeList) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.SpecificAttributeContent"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new SpecificAttributeContent(
							(SpecificAttributeContent) editingNode
									.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.SpecificFilterContent"))
			copiedNode = new DatasetConfigTreeNode(
					editingNode.toString(),
					new SpecificFilterContent(
							(SpecificFilterContent) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.SpecificOptionContent"))
			copiedNode = new DatasetConfigTreeNode(
					editingNode.toString(),
					new SpecificOptionContent(
							(SpecificOptionContent) editingNode.getUserObject()));
		else if (editingNodeClass.equals("org.ensembl.mart.lib.config.Option"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new Option((Option) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.PushAction"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new PushAction((PushAction) editingNode.getUserObject()));

		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.Importable"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new Importable((Importable) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.Exportable"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new Exportable((Exportable) editingNode.getUserObject()));
		else if (editingNodeClass
				.equals("org.ensembl.mart.lib.config.DynamicDataset"))
			copiedNode = new DatasetConfigTreeNode(editingNode.toString(),
					new DynamicDataset(
							(DynamicDataset) editingNode.getUserObject()));

		DatasetConfigTreeNodeSelection ss = new DatasetConfigTreeNodeSelection(
				copiedNode);
		// clipboard.setContents(ss, this);
		// try to set owner as the MartEditor object so can copy and paste
		// between trees
		frame.getEditor().clipboardEditor.setContents(ss,
				(ClipboardOwner) frame.getEditor());
	}

	public void makeHidden() {
		BaseNamedConfigurationObject bc = (BaseNamedConfigurationObject) editingNode
				.getUserObject();
		if (bc.getHidden() == null || !bc.getHidden().equals("true")) {
			bc.setHidden("true");
			Enumeration children = editingNode.breadthFirstEnumeration();
			DatasetConfigTreeNode childNode = null;
			while (children.hasMoreElements()) {
				childNode = (DatasetConfigTreeNode) children.nextElement();
				BaseNamedConfigurationObject ch = (BaseNamedConfigurationObject) childNode
						.getUserObject();
				ch.setHidden("true");
			}
		} else {
			bc.setHidden("false");
			Enumeration children = editingNode.breadthFirstEnumeration();
			DatasetConfigTreeNode childNode = null;
			while (children.hasMoreElements()) {
				childNode = (DatasetConfigTreeNode) children.nextElement();
				BaseNamedConfigurationObject ch = (BaseNamedConfigurationObject) childNode
						.getUserObject();
				ch.setHidden("false");
			}
		}

	}

	public void makeDisplay() {
		BaseNamedConfigurationObject bc = (BaseNamedConfigurationObject) editingNode
				.getUserObject();
		if (bc.getDisplay() == null || !bc.getDisplay().equals("true")) {
			bc.setDisplay("true");
			Enumeration children = editingNode.breadthFirstEnumeration();
			DatasetConfigTreeNode childNode = null;
			while (children.hasMoreElements()) {
				childNode = (DatasetConfigTreeNode) children.nextElement();
				BaseNamedConfigurationObject ch = (BaseNamedConfigurationObject) childNode
						.getUserObject();
				ch.setDisplay("true");
			}
		} else {
			bc.setDisplay("false");
			Enumeration children = editingNode.breadthFirstEnumeration();
			DatasetConfigTreeNode childNode = null;
			while (children.hasMoreElements()) {
				childNode = (DatasetConfigTreeNode) children.nextElement();
				BaseNamedConfigurationObject ch = (BaseNamedConfigurationObject) childNode
						.getUserObject();
				ch.setDisplay("false");
			}
		}

	}

	public DatasetConfigTreeNode getEditingNode() {
		return editingNode;
	}

	public void paste() {
		Transferable t = frame.getEditor().clipboardEditor.getContents(this);
		try {
			DatasetConfigTreeNode selnode = (DatasetConfigTreeNode) t
					.getTransferData(new DataFlavor(
							Class.forName("org.ensembl.mart.editor.DatasetConfigTreeNode"),
							"treeNode"));

			BaseNamedConfigurationObject test = (BaseNamedConfigurationObject) selnode
					.getUserObject();
			System.out.println("PASTING " + test.getInternalName());

			// DatasetConfigTreeNode dropnode = (DatasetConfigTreeNode)
			// clickedPath.getLastPathComponent();
			DatasetConfigTreeNode dropnode = setEditingNode();
			if (dropnode == null)
				return;

			String result = new String();
			int insertIndex = -1;
			if (selnode.getUserObject().getClass()
					.equals(dropnode.getUserObject().getClass())) {
				System.out.println(selnode.getUserObject().getClass());
				if (selnode
						.getUserObject()
						.getClass()
						.getName()
						.equals("org.ensembl.mart.lib.config.FilterDescription")) {
					Option op = new Option(
							(FilterDescription) selnode.getUserObject());
					selnode.setUserObject(op);
					insertIndex = DatasetConfigTreeNode.getHeterogenousOffset(
							dropnode.getUserObject(), selnode.getUserObject());
				} else {
					insertIndex = dropnode.getParent().getIndex(dropnode) + 1;
					dropnode = (DatasetConfigTreeNode) dropnode.getParent();
				}
			} else {
				insertIndex = DatasetConfigTreeNode.getHeterogenousOffset(
						dropnode.getUserObject(), selnode.getUserObject());
			}
			// make sure internalName is unique within its parent group
			DatasetConfigTreeNode childNode = null;
			Enumeration children = dropnode.children();
			while (children.hasMoreElements()) {
				childNode = (DatasetConfigTreeNode) children.nextElement();
				BaseNamedConfigurationObject ch = (BaseNamedConfigurationObject) childNode
						.getUserObject();

				BaseNamedConfigurationObject sel = (BaseNamedConfigurationObject) selnode
						.getUserObject();
				if (sel.getInternalName().equals(ch.getInternalName())) {

					// sel.setInternalName(sel.getInternalName() + "_copy");

					String selnodeName = selnode.getUserObject().getClass()
							.getName();

					BaseNamedConfigurationObject newSel = null;// no copy
																// constructor
																// for abstract
																// class
					if (selnodeName
							.equals("org.ensembl.mart.lib.config.FilterPage"))
						newSel = new FilterPage((FilterPage) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.FilterGroup"))
						newSel = new FilterGroup((FilterGroup) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.FilterCollection"))
						newSel = new FilterCollection((FilterCollection) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.FilterDescription"))
						newSel = new FilterDescription((FilterDescription) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.Importable"))
						newSel = new Importable((Importable) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.DynamicDataset"))
						newSel = new DynamicDataset((DynamicDataset) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.Exportable"))
						newSel = new Exportable((Exportable) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.Option"))
						newSel = new Option((Option) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.PushAction"))
						newSel = new PushAction((PushAction) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.AttributePage"))
						newSel = new AttributePage((AttributePage) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.AttributeGroup"))
						newSel = new AttributeGroup((AttributeGroup) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.AttributeCollection"))
						newSel = new AttributeCollection(
								(AttributeCollection) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.AttributeDescription"))
						newSel = new AttributeDescription(
								(AttributeDescription) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.AttributeList"))
						newSel = new AttributeList((AttributeList) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.SpecificFilterContent"))
						newSel = new SpecificFilterContent(
								(SpecificFilterContent) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.SpecificOptionContent"))
						newSel = new SpecificOptionContent(
								(SpecificOptionContent) sel);
					else if (selnodeName
							.equals("org.ensembl.mart.lib.config.SpecificAttributeContent"))
						newSel = new SpecificAttributeContent(
								(SpecificAttributeContent) sel);
					newSel.setInternalName(sel.getInternalName() + "_copy");
					// need to make sure refers to a different object for
					// multiple pastes
					selnode = new DatasetConfigTreeNode(selnode.name + "_copy",
							newSel);
					DatasetConfigTreeNodeSelection ss = new DatasetConfigTreeNodeSelection(
							selnode);
					frame.getEditor().clipboardEditor.setContents(ss,
							(ClipboardOwner) frame.getEditor());

					break;
				}
			}

			result = treemodel.insertNodeInto(selnode, dropnode, insertIndex);
			if (result.startsWith("Error")) {
				JOptionPane.showMessageDialog(frame, result, "Error",
						JOptionPane.ERROR_MESSAGE);
				if (cut)
					treemodel.insertNodeInto(selnode, editingNodeParent,
							editingNodeIndex);
			}
			cut = false;

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public DatasetConfigTreeNode insert(DatasetConfigTreeNode parentNode,
			BaseConfigurationObject obj, String name)
			throws ConfigurationException {

		DatasetConfigTreeNode newNode = new DatasetConfigTreeNode(name
				+ "newNode", obj);

		String result = treemodel.insertNodeInto(
				newNode,
				parentNode,
				DatasetConfigTreeNode.getHeterogenousOffset(
						parentNode.getUserObject(), newNode.getUserObject()));

		if (result.startsWith("Error")) {
			JOptionPane.showMessageDialog(frame, result, "Error",
					JOptionPane.ERROR_MESSAGE);
			return null;
		}
		return newNode;
	}

	public DatasetConfigTreeNode insert(BaseConfigurationObject obj, String name)
			throws ConfigurationException {

		return this.insert(
				(DatasetConfigTreeNode) clickedPath.getLastPathComponent(),
				obj, name);
	}

	public void autoSpecifics() throws ConfigurationException, SQLException {
		DatasetConfig dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) DatasetConfigTree.this
				.getModel().getRoot()).getUserObject();
		if (dsConfig.getTemplateFlag() != null
				&& dsConfig.getTemplateFlag().equals("1")) {
			String[] datasets = dsConfig.getDynamicDatasetNames();
			FilterDescription fd = (FilterDescription) ((DatasetConfigTreeNode) clickedPath
					.getLastPathComponent()).getUserObject();
			for (int i = 0; i < datasets.length; i++) {
				SpecificFilterContent dynAtt = new SpecificFilterContent();
				dynAtt.setAttribute("internalName", datasets[i]);
				dynAtt.setAttribute("tableConstraint", fd.getTableConstraint());
				dynAtt.setAttribute("field", fd.getField());
				dynAtt.setAttribute("key", fd.getKey());
				insert(dynAtt, "SpecificFilterContent");
			}
		}
	}

	public void autoSpecificDropDowns() throws ConfigurationException,
			SQLException {
		DatasetConfig dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) DatasetConfigTree.this
				.getModel().getRoot()).getUserObject();
		if (dsConfig.getTemplateFlag() != null
				&& dsConfig.getTemplateFlag().equals("1")) {
			autoSpecificDropDowns(dsConfig,
					(DatasetConfigTreeNode) clickedPath.getLastPathComponent());
		}
	}

	public void allAutoSpecificDropDowns() throws ConfigurationException,
			SQLException {

		DatasetConfigTreeNode root = (DatasetConfigTreeNode) this.getModel()
				.getRoot();
		DatasetConfig config = (DatasetConfig) (root.getUserObject());
		autoSpecificDropdownForNode(config, root);
	}

	private void autoSpecificDropdownForNode(DatasetConfig config,
			DatasetConfigTreeNode node) throws ConfigurationException,
			SQLException {
		for (int i = 0; i < node.getChildCount(); i++) {
			autoSpecificDropdownForNode(config,
					(DatasetConfigTreeNode) node.getChildAt(i));
		}
		if (FilterDescription.class.isAssignableFrom(node.getUserObject()
				.getClass())) {
			FilterDescription fd = (FilterDescription) node.getUserObject();
			if (fd.getSpecificFilterContent("replaceMe") != null) {
				System.out.println("Generating dropdowns for filter "
						+ fd.getInternalName());
				autoSpecificDropDowns(config, node);
			}
		}
	}

	private void autoSpecificDropDowns(DatasetConfig dsConfig,
			DatasetConfigTreeNode fnode) throws ConfigurationException,
			SQLException {
		FilterDescription fd = (FilterDescription) fnode.getUserObject();
		String[] datasets = dsConfig.getDynamicDatasetNames();
		for (int i = 0; i < datasets.length; i++) {
			SpecificFilterContent dynAtt = fd
					.getSpecificFilterContent(datasets[i]);
			if (dynAtt == null) {
				dynAtt = new SpecificFilterContent();
				dynAtt.setAttribute("internalName", datasets[i]);
				dynAtt.setAttribute("tableConstraint", fd.getTableConstraint());
				dynAtt.setAttribute("field", fd.getField());
				dynAtt.setAttribute("key", fd.getKey());
			}
			DatasetConfigTreeNode node = insert(fnode, dynAtt,
					"SpecificFilterContent");
			FilterDescription fd1 = (FilterDescription) node.getUserObject();
			this.doDropDown(dsConfig, node, fd1);
			if (node.getChildCount() == 0) {
				System.err.println("No options found for specific content "
						+ datasets[i] + " for so not inserting");
				treemodel.removeNodeFromParent(node);
			}
		}
	}

	public void autoSpecificPushActions() throws ConfigurationException,
			SQLException {

		// Work out what filter to link to.
		String filter2 = JOptionPane
				.showInputDialog("Select second Filter Description (internal name):");
		String orderSQL = JOptionPane
				.showInputDialog("Optional column name to order menu by:");

		DatasetConfig dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) DatasetConfigTree.this
				.getModel().getRoot()).getUserObject();
		if (dsConfig.getTemplateFlag() != null
				&& dsConfig.getTemplateFlag().equals("1")) {
			String[] datasets = dsConfig.getDynamicDatasetNames();
			FilterDescription fd = (FilterDescription) ((DatasetConfigTreeNode) clickedPath
					.getLastPathComponent()).getUserObject();
			for (int i = 0; i < datasets.length; i++) {
				SpecificFilterContent dynAtt = new SpecificFilterContent();
				dynAtt.setAttribute("internalName", datasets[i]);
				dynAtt.setAttribute("tableConstraint", fd.getTableConstraint());
				dynAtt.setAttribute("field", fd.getField());
				dynAtt.setAttribute("key", fd.getKey());
				DatasetConfigTreeNode node = insert(dynAtt,
						"SpecificFilterContent");
				FilterDescription fd1 = (FilterDescription) node
						.getUserObject();
				this.doDropDown(dsConfig, node, fd1);
				// Work out what dataset to select in the second filter.
				DatasetConfig ourConf = dsConfig;

				if (dsConfig.getTemplateFlag() != null
						&& dsConfig.getTemplateFlag().equals("1")) {
					String dataset = ((SpecificFilterContent) node
							.getUserObject()).getInternalName();
					String[] ids = MartEditor.getDatabaseDatasetConfigUtils()
							.getAllDatasetIDsForDataset(MartEditor.getUser(),
									dataset);
					String id;
					if (ids.length > 1) {
						id = (String) JOptionPane.showInputDialog(null, null,
								"Select second dataset id for " + datasets[i]
										+ ":", JOptionPane.QUESTION_MESSAGE,
								null, ids, ids[0]);
					} else if (ids.length == 1) {
						id = ids[0];
					} else {
						id = "";
					}
					ourConf = MartEditor.getDatabaseDatasetConfigUtils()
							.getDatasetConfigByDatasetID(
									MartEditor.getUser(),
									dataset,
									id,
									MartEditor.getDatabaseDatasetConfigUtils()
											.getSchema()[0]);
				}
				this.doPushAction(node, dsConfig, ourConf, filter2, orderSQL);
			}
		}
	}

	public void addPushAction() throws ConfigurationException, SQLException {
		try {
			// set FilterDescription fd1 = to current node
			DatasetConfigTreeNode node = (DatasetConfigTreeNode) clickedPath
					.getLastPathComponent();

			// Work out what dataset to select in the second filter.
			DatasetConfig ourConf = dsConfig;

			if (dsConfig.getTemplateFlag() != null
					&& dsConfig.getTemplateFlag().equals("1")) {
				String dataset;
				if (node.getUserObject() instanceof SpecificFilterContent)
					dataset = ((SpecificFilterContent) node.getUserObject())
							.getInternalName();
				else {
					String[] datasets = dsConfig.getDynamicDatasetNames();
					if (datasets.length > 1) {
						dataset = (String) JOptionPane.showInputDialog(null,
								null, "Select second dataset name:",
								JOptionPane.QUESTION_MESSAGE, null, datasets,
								datasets[0]);
					} else {
						dataset = datasets[0];
					}
				}
				String[] ids = MartEditor.getDatabaseDatasetConfigUtils()
						.getAllDatasetIDsForDataset(MartEditor.getUser(),
								dataset);
				String id;
				if (ids.length > 1) {
					id = (String) JOptionPane.showInputDialog(null, null,
							"Select second dataset id:",
							JOptionPane.QUESTION_MESSAGE, null, ids, ids[0]);
				} else if (ids.length == 1) {
					id = ids[0];
				} else {
					id = "";
				}
				ourConf = MartEditor.getDatabaseDatasetConfigUtils()
						.getDatasetConfigByDatasetID(
								MartEditor.getUser(),
								dataset,
								id,
								MartEditor.getDatabaseDatasetConfigUtils()
										.getSchema()[0]);
			}

			// Work out what filter to link to.
			String filter2 = JOptionPane
					.showInputDialog("Select second Filter Description (internal name):");
			String orderSQL = JOptionPane
					.showInputDialog("Optional column name to order menu by:");

			this.doPushAction(node, dsConfig, ourConf, filter2, orderSQL);
		} catch (Exception e) {
			System.out.println("PROBLEM ADDING PUSH ACTION");
			e.printStackTrace();
		}
	}

	public void makeDropDown() throws ConfigurationException, SQLException {
		try {
			DatasetConfigTreeNode node = (DatasetConfigTreeNode) clickedPath
					.getLastPathComponent();
			FilterDescription fd1 = (FilterDescription) node.getUserObject();
			this.doDropDown(dsConfig, node, fd1);
		} catch (Exception e) {
			System.out.println("PROBLEM MAKING DROP DOWN");
			e.printStackTrace();
		}
	}

	private void doDropDown(DatasetConfig dsConfig, DatasetConfigTreeNode node,
			FilterDescription fd1) throws ConfigurationException, SQLException {
		try {

			// dsConfig = (DatasetConfig) ((DatasetConfigTreeNode)
			// this.getModel().getRoot()).getUserObject();

			String field = fd1.getField();
			String tableName = fd1.getTableConstraint();
			String joinKey = fd1.getKey();
			fd1.setType("list");
			fd1.setDisplayType("list");
			fd1.setStyle("menu");
			fd1.setQualifier("=");
			fd1.setLegalQualifiers("=");
			String colForDisplay = "";
			if (fd1.getColForDisplay() != null) {
				colForDisplay = fd1.getColForDisplay();
			}
			System.out.println("COL FOR DISPLAY:" + colForDisplay);

			DatasetConfig ourConf = dsConfig;
			if (dsConfig.getTemplateFlag() != null
					&& dsConfig.getTemplateFlag().equals("1")) {
				String dataset;
				if (fd1 instanceof SpecificFilterContent)
					dataset = fd1.getInternalName();
				else {
					String[] datasets = dsConfig.getDynamicDatasetNames();
					if (datasets.length > 1) {
						dataset = (String) JOptionPane.showInputDialog(null,
								null, "Select dataset name:",
								JOptionPane.QUESTION_MESSAGE, null, datasets,
								datasets[0]);
					} else {
						dataset = datasets[0];
					}
				}
				String[] ids = MartEditor.getDatabaseDatasetConfigUtils()
						.getAllDatasetIDsForDataset(MartEditor.getUser(),
								dataset);
				String id;
				if (ids.length > 1) {
					id = (String) JOptionPane.showInputDialog(null, null,
							"Select dataset id:", JOptionPane.QUESTION_MESSAGE,
							null, ids, ids[0]);
				} else if (ids.length == 1) {
					id = ids[0];
				} else {
					id = "";
				}
				ourConf = MartEditor.getDatabaseDatasetConfigUtils()
						.getDatasetConfigByDatasetID(
								MartEditor.getUser(),
								dataset,
								id,
								MartEditor.getDatabaseDatasetConfigUtils()
										.getSchema()[0]);
			}

			if (!tableName.endsWith("main")
					&& "1".equals(dsConfig.getTemplateFlag())) {
				tableName = ourConf.getDataset() + "__" + tableName;
			}

			if (ourConf.getAdaptor() == null)
				ourConf.setAdaptor(new DatabaseDSConfigAdaptor(MartEditor
						.getDetailedDataSource(), MartEditor.getUser(),
						MartEditor.getMartUser(), true, false, true, true));

			Option[] options = MartEditor.getDatabaseDatasetConfigUtils()
					.getOptions(field, tableName, joinKey, ourConf,
							ourConf.getDataset(), colForDisplay);

			if (options.length > 200) {
				System.err.println(
								"Many options have been found ("
										+ options.length
										+ " of them). This may affect the performance of MartEditor and MartView.");
			}

			for (int k = options.length - 1; k > -1; k--) {

				insert(node, options[k], "Option");
			}
		} catch (Exception e) {
			System.out.println("PROBLEM MAKING DROP DOWN");
			e.printStackTrace();
		}
	}

	private void doPushAction(DatasetConfigTreeNode node,
			DatasetConfig dsConfig, DatasetConfig ourConf, String filter2,
			String orderSQL) throws ConfigurationException, SQLException {
		try {
			// String filter2 =
			// JOptionPane.showInputDialog("Filter Description to set (TableName:ColName):");
			// String[] filterTokens = filter2.split(":");
			// FilterDescription fd2 =
			// dsConfig.getFilterDescriptionByFieldNameTableConstraint(filterTokens[1],filterTokens[0]);
			dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) this.getModel()
					.getRoot()).getUserObject();
			FilterDescription fd2 = dsConfig
					.getFilterDescriptionByInternalName(filter2);

			fd2.setType("drop_down_basic_filter");

			String pushField = fd2.getField();
			String pushColForDisplay = fd2.getColForDisplay();
			String pushInternalName = fd2.getInternalName();// used for ref name
															// in PushAction
			if (pushInternalName.matches("\\w+\\.\\w+"))
				pushInternalName = pushInternalName.split("\\.")[0] + "__"
						+ pushInternalName.split("\\.")[1];
			String pushTableName = fd2.getTableConstraint();

			// can add push actions to existing push actions so need to know the
			// class of the node
			// String className = node.getUserObject().getClass().getName();
			String field;
			Option[] options;

			if (node.getUserObject() instanceof FilterDescription) {
				FilterDescription fd1 = (FilterDescription) node
						.getUserObject();
				field = fd1.getField();
				// if (!fd1.getTableConstraint().equals(pushTableName))
				// field = "olook_" + field;
				options = fd1.getOptions();

				// if (fd1.getOtherFilters() != null){// refers to a placeholder
				if (fd2.getPointerFilter() != null
						&& !fd2.getPointerFilter().equals("")) {// placeholder)

					// DatasetConfig otherDataset =
					// MartEditor.getDatabaseDatasetConfigUtils().getDatasetConfigByDatasetID(null,
					// newFD.getPointerDataset(),"",MartEditor.getDatabaseDatasetConfigUtils().getSchema()[0]);;

					String otherDatasetFilter1 = null;
					DatasetConfig otherDataset = null;
					FilterDescription newFD = new FilterDescription();
					dsConfig.getDynamicDataset(ourConf.getDataset())
							.resolveText(newFD, fd1);

					String[] otherFilters = newFD.getOtherFilters().split(";");
					String pointerFilter = fd2.getPointerFilter();
					fd2 = null;
					for (int p = 0; p < otherFilters.length; p++) {
						String otherDS = otherFilters[p].split("\\.")[0];
						String otherF = otherFilters[p].split("\\.")[1];
						otherDataset = MartEditor
								.getDatabaseDatasetConfigUtils()
								.getDatasetConfigByDatasetID(
										null,
										otherDS,
										"",
										MartEditor
												.getDatabaseDatasetConfigUtils()
												.getSchema()[1]);
						MartEditor
								.getDatasetConfigXMLUtils()
								.loadDatasetConfigWithDocument(
										otherDataset,
										MartEditor
												.getDatabaseDatasetConfigUtils()
												.getDatasetConfigDocumentByDatasetID(
														null,
														otherDS,
														otherDataset
																.getDatasetID(),
														MartEditor
																.getDatabaseDatasetConfigUtils()
																.getSchema()[1]));
						if (otherDataset
								.containsFilterDescription(pointerFilter))
							fd2 = otherDataset
									.getFilterDescriptionByInternalName(pointerFilter);
						if (fd2 != null) {
							otherDatasetFilter1 = otherF;
							break;
						}
					}
					field = otherDataset.getFilterDescriptionByInternalName(
							otherDatasetFilter1).getField();

					fd2.setType("drop_down_basic_filter");
					pushField = fd2.getField();
					pushColForDisplay = fd2.getColForDisplay();

					pushTableName = fd2.getTableConstraint();

					if (pushTableName != null && pushTableName.equals("main")) {
						String[] mains = otherDataset.getStarBases();
						pushTableName = mains[0];
					}
					ourConf = otherDataset;

					// field =
					// otherDataset.getFilterDescriptionByInternalName(fd2.getPointerFilter()).getField();

				}
			} else {
				PushAction pa1 = (PushAction) node.getUserObject();
				String intName = pa1.getInternalName();
				field = intName.split("_push")[0];
				options = pa1.getOptions();

				if (fd2.getPointerFilter() != null
						&& !fd2.getPointerFilter().equals("")) {// placeholder)
					String otherDatasetFilter1 = null;
					DatasetConfig otherDataset = null;
					FilterDescription referredFilter = dsConfig
							.getFilterDescriptionByInternalName(pa1.getRef());
					if (referredFilter.getOtherFilters() == null) {
						JOptionPane.showMessageDialog(null, pa1.getRef()
								+ " filter needs otherFilters set first",
								"ERROR", 0);
						return;
					}

					String[] otherFilters = referredFilter.getOtherFilters()
							.split(";");
					fd2 = null;
					for (int p = 0; p < otherFilters.length; p++) {
						String otherDS = otherFilters[p].split("\\.")[0];
						String otherF = otherFilters[p].split("\\.")[1];
						otherDataset = MartEditor
								.getDatabaseDatasetConfigUtils()
								.getDatasetConfigByDatasetID(
										null,
										otherDS,
										"",
										MartEditor
												.getDatabaseDatasetConfigUtils()
												.getSchema()[1]);
						MartEditor
								.getDatasetConfigXMLUtils()
								.loadDatasetConfigWithDocument(
										otherDataset,
										MartEditor
												.getDatabaseDatasetConfigUtils()
												.getDatasetConfigDocumentByDatasetID(
														null,
														otherDS,
														otherDataset
																.getDatasetID(),
														MartEditor
																.getDatabaseDatasetConfigUtils()
																.getSchema()[1]));
						if (otherDataset.containsFilterDescription(otherF))
							fd2 = otherDataset
									.getFilterDescriptionByInternalName(otherF);
						if (fd2 != null) {
							otherDatasetFilter1 = otherF;
							break;
						}
					}
					fd2.setType("drop_down_basic_filter");
					pushField = fd2.getField();
					// pushInternalName = fd2.getInternalName();// keep original
					// full name instead
					pushTableName = fd2.getTableConstraint();
					pushColForDisplay = fd2.getColForDisplay();

					if (pushTableName.equals("main")) {
						String[] mains = dsConfig.getStarBases();
						pushTableName = mains[0];
					}
					field = otherDataset.getFilterDescriptionByInternalName(
							otherDatasetFilter1).getField();
				}
			}

			if (!pushTableName.endsWith("main")
					&& dsConfig.getTemplateFlag() != null
					&& dsConfig.getTemplateFlag().equals("1")) {
				pushTableName = ourConf.getDataset() + "__" + pushTableName;
			}

			String joinKey = fd2.getKey();

			for (int i = 0; i < options.length; i++) {

				Option op = options[i];
				String opName = op.getValue();
				// String opName = op.getDisplayName();// incase displayName
				// comes from another co
				PushAction pa = new PushAction(pushInternalName + "_push_"
						+ opName, null, null, pushInternalName, orderSQL);
				pa.addOptions(MartEditor.getDatabaseDatasetConfigUtils()
						.getLookupOptions(
								pushField,
								pushTableName,
								dsConfig,
								joinKey,
								ourConf.getDataset(),
								field,
								opName,
								orderSQL,
								MartEditor.getDatabaseDatasetConfigUtils()
										.getSchema()[1], pushColForDisplay));

				if (pa.getOptions().length > 200) {
					System.err.println(
									"Many options have been found ("
											+ pa.getOptions().length
											+ " of them). This may affect the performance of MartEditor and MartView.");
				}

				if (pa.getOptions().length > 0) {
					Enumeration children = node.children();
					DatasetConfigTreeNode childNode = null;
					while (children.hasMoreElements()) {
						childNode = (DatasetConfigTreeNode) children
								.nextElement();
						if (op.equals(childNode.getUserObject()))
							break;
					}
					DatasetConfigTreeNode newNode = new DatasetConfigTreeNode(
							"PushAction:newNode", pa);
					String result = treemodel.insertNodeInto(newNode,
							childNode, DatasetConfigTreeNode
									.getHeterogenousOffset(
											childNode.getUserObject(),
											newNode.getUserObject()));
					if (result.startsWith("Error")) {
						JOptionPane.showMessageDialog(frame, result, "Error",
								JOptionPane.ERROR_MESSAGE);
						return;
					}

				}
			}
		} catch (Exception e) {
			System.out.println("PROBLEM ADDING PUSH ACTION");
			e.printStackTrace();
		}
	}

	public void addOntology() throws ConfigurationException, SQLException {
		try {
			DatasetConfigTreeNode node = (DatasetConfigTreeNode) clickedPath
					.getLastPathComponent();
			FilterDescription fd1 = (FilterDescription) node.getUserObject();
			dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) this.getModel()
					.getRoot()).getUserObject();
			Box ontologySettings = new Box(BoxLayout.Y_AXIS);
			ontologySettings.add(Box.createRigidArea(new Dimension(600, 1)));

			Box box2 = new Box(BoxLayout.X_AXIS);
			JLabel label2 = new JLabel("Parent child table");
			JTextField childTableField = new JTextField();
			box2.add(label2);
			box2.add(childTableField);

			Box box3 = new Box(BoxLayout.X_AXIS);
			JLabel label3 = new JLabel("Parent ID column");
			JTextField parentIdColField = new JTextField();
			box3.add(label3);
			box3.add(parentIdColField);

			Box box4 = new Box(BoxLayout.X_AXIS);
			JLabel label4 = new JLabel("Child ID column");
			JTextField childIdColField = new JTextField();
			box4.add(label4);
			box4.add(childIdColField);

			Box box5 = new Box(BoxLayout.X_AXIS);
			JLabel label5 = new JLabel("Text term column");
			JTextField childTermColField = new JTextField();
			box5.add(label5);
			box5.add(childTermColField);

			ontologySettings.add(box2);
			ontologySettings.add(box3);
			ontologySettings.add(box4);
			ontologySettings.add(box5);

			String[] standardOptions = new String[] { "OK", "Cancel" };
			int option2 = JOptionPane.showOptionDialog(null, ontologySettings,
					"Ontology Settings", JOptionPane.DEFAULT_OPTION,
					JOptionPane.PLAIN_MESSAGE, null, standardOptions, null);

			String childTable = childTableField.getText();
			String childIdCol = childIdColField.getText();
			String childTermCol = childTermColField.getText();
			String parentIdCol = parentIdColField.getText();

			// String ontologyTable =
			// JOptionPane.showInputDialog(this,"Ontology table name:","hsapiens_gene_ensembl_evoc_ontology__evoc_ontology__main");
			// String ontologyName =
			// JOptionPane.showInputDialog(this,"Ontology name:","Anatomical System");
			fd1.setType("list");
			fd1.setQualifier("=");
			fd1.setLegalQualifiers("=");

			Option[] options = MartEditor.getDatabaseDatasetConfigUtils()
					.getOntologyOptions(childTermCol, childIdCol, childTable,
							parentIdCol);
			for (int k = options.length - 1; k > -1; k--) {
				insert(options[k], "Option");
			}
		} catch (Exception e) {
			System.out.println("PROBLEM ADDING ONTOLOGY");
		}
	}

	public void editMains() {
		String[] mains = dsConfig.getStarBases();
		String mainString = "";
		String comma = "";

		for (int i = 0; i < mains.length; i++) {
			String main = mains[i];
			dsConfig.removeMainTable(main);
			mainString = mainString + comma + main;
			comma = ",";
		}

		String newMain = JOptionPane.showInputDialog("", mainString);

		String[] newMains = newMain.split(",");
		if (!newMain.equals(""))
			dsConfig.addMainTables(newMains);
	}

	public void editKeys() {
		String[] mains = dsConfig.getPrimaryKeys();
		String mainString = "";
		String comma = "";

		for (int i = 0; i < mains.length; i++) {
			String main = mains[i];
			dsConfig.removePrimaryKey(main);
			mainString = mainString + comma + main;
			comma = ",";
		}
		String newMain = JOptionPane.showInputDialog("", mainString);

		String[] newMains = newMain.split(",");
		if (!newMain.equals(""))
			dsConfig.addPrimaryKeys(newMains);
	}

	public void delete() {
		DatasetConfigTreeNode node = setEditingNode();
		if (node == null)
			return;
		if (node.getParent() == null)
			return; // Can't remove root node.
		// DatasetConfigTreeNode node = (DatasetConfigTreeNode)
		// clickedPath.getLastPathComponent();
		treemodel.removeNodeFromParent(node);
	}

	public void deleteOptions() {
		DatasetConfigTreeNode node = setEditingNode();
		if (node == null)
			return;

		Enumeration en = node.children();
		DatasetConfigTreeNode[] childNodes = new DatasetConfigTreeNode[node
				.getChildCount()];
		int i = 0;
		while (en.hasMoreElements()) {
			DatasetConfigTreeNode childNode = (DatasetConfigTreeNode) en
					.nextElement();
			childNodes[i] = childNode;
			i++;
		}
		// have to cycle thro again as removeNodeFromParent alters the
		// enumeration
		for (int j = 0; j < childNodes.length; j++) {
			DatasetConfigTreeNode childNode = childNodes[j];
			treemodel.removeNodeFromParent(childNode);
		}

	}

	public void save_as() {
		dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) this.getModel()
				.getRoot()).getUserObject();

		JFileChooser fc;
		if (frame.getFileChooserPath() != null) {
			fc = new JFileChooser(frame.getFileChooserPath());
			fc.setDragEnabled(true);
			fc.setSelectedFile(frame.getFileChooserPath());
			fc.setDialogTitle("Save as");
		} else
			fc = new JFileChooser();
		XMLFileFilter filter = new XMLFileFilter();
		fc.addChoosableFileFilter(filter);
		int returnVal = fc.showSaveDialog(frame.getContentPane());
		if (returnVal == JFileChooser.APPROVE_OPTION) {
			try {
				URLDSConfigAdaptor.StoreDatasetConfig(dsConfig,
						fc.getSelectedFile());
				frame.setFileChooserPath(fc.getSelectedFile());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void save() {
		dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) this.getModel()
				.getRoot()).getUserObject();
		try {
			if (frame.getFileChooserPath() != null)
				URLDSConfigAdaptor.StoreDatasetConfig(dsConfig,
						frame.getFileChooserPath());

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public boolean export() throws ConfigurationException {
		dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) this.getModel()
				.getRoot()).getUserObject();
		if (dsConfig.getTemplateFlag() != null) {
			JOptionPane
					.showMessageDialog(
							null,
							"This is a template config rather than standard dataset config",
							"", JOptionPane.ERROR_MESSAGE);
			return false;
		}
		if (MartEditor.getDatabaseDatasetConfigUtils()
				.naiveExportWouldOverrideExistingConfig(MartEditor.getUser(),
						dsConfig.getDatasetID(), dsConfig.getDisplayName(),
						dsConfig.getDataset(), dsConfig.getType(),
						dsConfig.getVersion())) {
			if (JOptionPane
					.showConfirmDialog(null,
							"This action will override the existing config for this dataset. Are you sure?") != JOptionPane.YES_OPTION)
				return false;
		}

		MartEditor.getDatabaseDatasetConfigUtils().storeDatasetConfiguration(
				MartEditor.getUser(),
				dsConfig.getInternalName(),
				dsConfig.getDisplayName(),
				dsConfig.getDataset(),
				dsConfig.getDescription(),
				MartEditor.getDatasetConfigXMLUtils()
						.getDocumentForDatasetConfig(dsConfig), true,
				dsConfig.getType(), dsConfig.getVisible(),
				dsConfig.getVersion(), dsConfig.getDatasetID(),
				dsConfig.getMartUsers(), dsConfig.getInterfaces(), dsConfig);
		return true;
	}

	public void exportTemplate() throws ConfigurationException {
		dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) this.getModel()
				.getRoot()).getUserObject();
		if (dsConfig.getTemplateFlag() == null) {
			JOptionPane.showMessageDialog(null,
					"This is not a template config", "",
					JOptionPane.ERROR_MESSAGE);
			return;
		}
		if (!MartEditor.getDatabaseDatasetConfigUtils().uniqueCheckConfig(
				dsConfig))
			return;
		// MartEditor.getDatabaseDatasetConfigUtils().storeTemplateXML(dsConfig,dsConfig.getTemplate());
		// update config to template
		MartEditor.getDatabaseDatasetConfigUtils().updateConfigsToTemplate(
				dsConfig.getTemplate(), dsConfig);
	}

	public void validateTemplate() throws ConfigurationException {
		dsConfig = (DatasetConfig) ((DatasetConfigTreeNode) this.getModel()
				.getRoot()).getUserObject();
		if (dsConfig.getTemplateFlag() == null) {
			JOptionPane.showMessageDialog(null,
					"This is not a template config", "",
					JOptionPane.ERROR_MESSAGE);
			return;
		}

		List problems = new ArrayList();

		List toCheck = new ArrayList();
		toCheck.add(dsConfig);

		for (int i = 0; i < toCheck.size(); i++) {
			BaseNamedConfigurationObject obj = (BaseNamedConfigurationObject) toCheck
					.get(i);
			// Spaces in internal names.
			if (obj.getInternalName().indexOf(' ') >= 0)
				problems.add("Space found in internal name: '"
						+ obj.getInternalName() + "'");
			if (obj.getInternalName().indexOf('\'') >= 0)
				problems.add("Single quote found in internal name: '"
						+ obj.getInternalName() + "'");
			if (obj.getInternalName().indexOf('"') >= 0)
				problems.add("Double quote found in internal name: '"
						+ obj.getInternalName() + "'");

			/*
			 * CANNOT DO THIS AS CANNOT TELL WHICH FIELDS ARE GENUINELY REQUIRED
			 * IN EACH OBJECT.
			 */
			/*
			 * // Missing values in required (red) fields. int[] required =
			 * obj.getRequiredFields(); String[] keys =
			 * obj.getXmlAttributeTitles(); for (int j = 0; j < required.length;
			 * j++) { String key = keys[required[j]]; String value =
			 * obj.getAttribute(key); if (value==null || value.equals(""))
			 * problems
			 * .add("Missing value for "+key+" in "+obj.getInternalName()); }
			 */

			// Append objects to list for checking.
			if (obj instanceof DatasetConfig) {
				// toCheck.addAll(((DatasetConfig)obj).getDynamicDatasetContents());
				toCheck.addAll(Arrays.asList(((DatasetConfig) obj)
						.getAttributePages()));
				toCheck.addAll(Arrays.asList(((DatasetConfig) obj)
						.getFilterPages()));
				toCheck.addAll(Arrays.asList(((DatasetConfig) obj)
						.getExportables()));
				toCheck.addAll(Arrays.asList(((DatasetConfig) obj)
						.getImportables()));
			} else if (obj instanceof AttributePage) {
				toCheck.addAll(((AttributePage) obj).getAttributeGroups());
			} else if (obj instanceof AttributeGroup) {
				toCheck.addAll(Arrays.asList(((AttributeGroup) obj)
						.getAttributeCollections()));
			} else if (obj instanceof AttributeCollection) {
				toCheck.addAll(((AttributeCollection) obj)
						.getAttributeDescriptions());
				toCheck.addAll(((AttributeCollection) obj).getAttributeLists());
			} else if (obj instanceof AttributeDescription) {
				// toCheck.addAll(((AttributeDescription)obj).getDynamicAttributeContents());
			} else if (obj instanceof FilterPage) {
				toCheck.addAll(((FilterPage) obj).getFilterGroups());
			} else if (obj instanceof FilterGroup) {
				toCheck.addAll(Arrays.asList(((FilterGroup) obj)
						.getFilterCollections()));
			} else if (obj instanceof FilterCollection) {
				toCheck.addAll(((FilterCollection) obj).getFilterDescriptions());
			} else if (obj instanceof FilterDescription) {
				// toCheck.addAll(((FilterDescription)obj).getDynamicFilterContents());
				toCheck.addAll(Arrays.asList(((FilterDescription) obj)
						.getOptions()));
			} else if (obj instanceof Option) {
				Option opt = (Option) obj;
				// toCheck.addAll(((FilterDescription)obj).getDynamicFilterContents());

				// if (!opt.getInternalName().equals(opt.getValue()))
				// problems.add("Option "+opt.getInternalName()+" does not have value field equal to internal name");

				toCheck.addAll(Arrays.asList(opt.getPushActions()));
				toCheck.addAll(Arrays.asList(opt.getOptions()));
			} else if (obj instanceof PushAction) {
				// toCheck.addAll(((FilterDescription)obj).getDynamicFilterContents());
				toCheck.addAll(Arrays.asList(((PushAction) obj).getOptions()));
			} else if (obj instanceof AttributeList) {
				AttributeList attr = (AttributeList) obj;

				// Non-existent attributes.
				if (attr.getAttributes() != null) {
					String[] attrs = attr.getAttributes().split(",");
					for (int j = 0; j < attrs.length; j++) {
						if (!dsConfig.containsAttributeDescription(attrs[j]))
							problems.add("AttributeList "
									+ attr.getInternalName()
									+ " refers to non-existent attribute "
									+ attrs[j]);
					}
				}
			} else if (obj instanceof Exportable) {
				Exportable exp = (Exportable) obj;

				// Non-existent attributes.
				if (exp.getAttributes() != null) {
					String[] attrs = exp.getAttributes().split(",");
					for (int j = 0; j < attrs.length; j++) {
						if (!dsConfig.containsAttributeDescription(attrs[j]))
							problems.add("Exportable " + exp.getInternalName()
									+ " refers to non-existent attribute "
									+ attrs[j]);
					}
				}

				// Recurse.
				// toCheck.addAll(exp.getDynamicExportableContents());
			} else if (obj instanceof Importable) {
				Importable imp = (Importable) obj;

				// Non-existent filters.
				if (imp.getFilters() != null) {
					String[] attrs = imp.getFilters().split(",");
					for (int j = 0; j < attrs.length; j++) {
						if (!dsConfig.containsFilterDescription(attrs[j]))
							problems.add("Importable " + imp.getInternalName()
									+ " refers to non-existent filter "
									+ attrs[j]);
					}
				}

				// Recurse.
				// toCheck.addAll(imp.getDynamicImportableContents());
			}
		}

		if (!problems.isEmpty()) {
			StringBuffer message = new StringBuffer();
			for (int i = 0; i < problems.size(); i++)
				message.append(problems.get(i).toString() + '\n');
			JOptionPane.showMessageDialog(null, message.toString());
		} else {
			JOptionPane.showMessageDialog(null, "Validated OK");
		}
	}

	// public void lostOwnership(Clipboard c, Transferable t) {

	// }
}
