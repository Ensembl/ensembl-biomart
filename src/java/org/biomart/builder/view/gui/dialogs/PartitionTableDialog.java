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

package org.biomart.builder.view.gui.dialogs;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.swing.Box;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListCellRenderer;
import javax.swing.SwingConstants;
import javax.swing.WindowConstants;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.event.TableColumnModelEvent;
import javax.swing.event.TableColumnModelListener;
import javax.swing.plaf.basic.BasicArrowButton;
import javax.swing.table.DefaultTableModel;

import org.biomart.builder.exceptions.PartitionException;
import org.biomart.builder.model.Column;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Mart;
import org.biomart.builder.model.PartitionTable;
import org.biomart.builder.model.Relation;
import org.biomart.builder.model.TransformationUnit;
import org.biomart.builder.model.DataSet.DataSetColumn;
import org.biomart.builder.model.DataSet.DataSetTable;
import org.biomart.builder.model.DataSet.DataSetTableType;
import org.biomart.builder.model.DataSet.DataSetColumn.InheritedColumn;
import org.biomart.builder.model.DataSet.DataSetColumn.WrappedColumn;
import org.biomart.builder.model.PartitionTable.PartitionColumn;
import org.biomart.builder.model.PartitionTable.PartitionRow;
import org.biomart.builder.model.PartitionTable.PartitionTableApplication;
import org.biomart.builder.model.PartitionTable.PartitionTableApplication.PartitionAppliedRow;
import org.biomart.builder.model.TransformationUnit.JoinTable;
import org.biomart.builder.view.gui.MartTabSet.MartTab;
import org.biomart.common.exceptions.BioMartError;
import org.biomart.common.resources.Resources;
import org.biomart.common.view.gui.LongProcess;
import org.biomart.common.view.gui.dialogs.StackTrace;
import org.biomart.common.view.gui.dialogs.TransactionalDialog;

/**
 * A dialog which allows the user to turn a dataset into a partition table and
 * modify the way it behaves as such.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.33 $, $Date: 2008-04-11 16:30:31 $, modified by
 *          $Author: syed $
 * @since 0.7
 */
public class PartitionTableDialog extends TransactionalDialog {
	private static final long serialVersionUID = 1;

	/**
	 * How many rows to show in the preview panel.
	 */
	public static int PREVIEW_ROWS = 10;

	private DefaultListModel availableColumns;

	private DefaultListModel selectedColumns;

	private DefaultTableModel previewData;

	private JCheckBox partition;

	private JTextField previewRowCount;

	private JComboBox appliedList;

	private WizardPanel wizardPanel;

	/**
	 * Pop up a dialog to define or edit a dataset partition table data.
	 * 
	 * @param dataset
	 *            the dataset.
	 * @param martTab
	 *            the mart tab the dataset lives in.
	 */
	public PartitionTableDialog(final MartTab martTab, final DataSet dataset) {
		this(martTab, dataset, null);
	}

	private PartitionTableDialog(final MartTab martTab, final DataSet dataset,
			final Object preselect) {
		// Create the base dialog.
		super();
		this.setTitle(Resources.get("partitionTableDialogTitle", dataset
				.getName()));
		this.setModal(true);
		this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

		// Create the content pane to store the create dialog panel.
		final JPanel content = new JPanel(new GridBagLayout());
		this.setContentPane(content);

		// Create constraints for labels that are not in the last row.
		final GridBagConstraints labelConstraints = new GridBagConstraints();
		labelConstraints.gridwidth = GridBagConstraints.RELATIVE;
		labelConstraints.fill = GridBagConstraints.HORIZONTAL;
		labelConstraints.anchor = GridBagConstraints.LINE_END;
		labelConstraints.insets = new Insets(0, 2, 0, 0);
		// Create constraints for fields that are not in the last row.
		final GridBagConstraints fieldConstraints = new GridBagConstraints();
		fieldConstraints.gridwidth = GridBagConstraints.REMAINDER;
		fieldConstraints.fill = GridBagConstraints.NONE;
		fieldConstraints.anchor = GridBagConstraints.LINE_START;
		fieldConstraints.insets = new Insets(0, 1, 0, 2);
		// Create constraints for labels that are in the last row.
		final GridBagConstraints labelLastRowConstraints = (GridBagConstraints) labelConstraints
				.clone();
		labelLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;
		// Create constraints for fields that are in the last row.
		final GridBagConstraints fieldLastRowConstraints = (GridBagConstraints) fieldConstraints
				.clone();
		fieldLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;

		// Stuff that needs to be available early.
		final JPanel wizardHolder = new JPanel();
		wizardHolder.setBorder(new EtchedBorder(EtchedBorder.LOWERED));
		this.appliedList = new JComboBox();
		// On select from list on left, update edit panel with edit wizard
		// for that application. Then pack().
		this.appliedList.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object sel = PartitionTableDialog.this.appliedList
						.getSelectedItem();
				wizardHolder.removeAll();
				PartitionTableDialog.this.wizardPanel = null;
				if (sel != null)
					if (sel instanceof DataSet)
						PartitionTableDialog.this.wizardPanel = new WizardPanel(
								((DataSet) sel).getPartitionTableApplication(),
								((DataSet) sel).getMainTable());
					else if (sel instanceof DataSetTable)
						PartitionTableDialog.this.wizardPanel = new WizardPanel(
								((DataSetTable) sel)
										.getPartitionTableApplication(),
								(DataSetTable) sel);
				if (PartitionTableDialog.this.wizardPanel != null)
					wizardHolder.add(PartitionTableDialog.this.wizardPanel);
				PartitionTableDialog.this.pack();
			}
		});

		// Show/hide pane.
		final JPanel showHide = new JPanel(new GridBagLayout());
		showHide.setVisible(false);

		// First line is a checkbox. If unchecked, partitioning is off
		// on this table. If checked, then it is on. This shows/hides
		// the remaining two panels.
		this.partition = new JCheckBox(Resources.get("partitionTableCheckbox"));
		content.add(this.partition, fieldConstraints);
		content.add(showHide, fieldLastRowConstraints);

		// Put the two halves of the dialog side-by-side in a horizontal box.
		final Box colPanel = Box.createHorizontalBox();
		showHide.add(new JLabel(Resources.get("partitionTableColumnLabel")),
				labelConstraints);
		showHide.add(colPanel, fieldConstraints);

		// Create the available column list, and the buttons
		// to move columns to/from the selected column list.
		this.availableColumns = new DefaultListModel();
		final JList tabColList = new JList(this.availableColumns);
		final JButton insertButton = new BasicArrowButton(SwingConstants.EAST);
		final JButton removeButton = new BasicArrowButton(SwingConstants.WEST);

		// Create the selected column list, and the buttons to
		// move columns to/from the table columns list.
		this.selectedColumns = new DefaultListModel();
		final JList keyColList = new JList(this.selectedColumns);
		final JButton upButton = new BasicArrowButton(SwingConstants.NORTH);
		final JButton downButton = new BasicArrowButton(SwingConstants.SOUTH);

		// Create the regex fields.
		final JTextField matchRegex = new JTextField(20);
		final JTextField replaceRegex = new JTextField(20);
		final JButton regexUpdateButton = new JButton(Resources
				.get("updateButton"));
		final JButton regexResetButton = new JButton(Resources
				.get("resetButton"));

		// Left-hand side goes the table columns that are unused.
		final JPanel leftPanel = new JPanel(new BorderLayout());
		// Label at the top.
		leftPanel.add(new JLabel(Resources.get("columnsAvailableLabel")),
				BorderLayout.PAGE_START);
		// Table columns list in the middle.
		leftPanel.add(new JScrollPane(tabColList), BorderLayout.CENTER);
		leftPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		// Buttons down the right-hand-side, vertically.
		final Box leftButtonPanel = Box.createVerticalBox();
		leftButtonPanel.add(insertButton);
		leftButtonPanel.add(removeButton);
		leftButtonPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		leftPanel.add(leftButtonPanel, BorderLayout.LINE_END);
		colPanel.add(leftPanel);

		// Right-hand side goes the key columns that are used.
		final JPanel rightPanel = new JPanel(new BorderLayout());
		// Label at the top.
		rightPanel.add(new JLabel(Resources.get("selectedColumnsLabel")),
				BorderLayout.PAGE_START);
		// Key columns in the middle.
		rightPanel.add(new JScrollPane(keyColList), BorderLayout.CENTER);
		rightPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		// Buttons down the right-hand-side, vertically.
		final Box rightButtonPanel = Box.createVerticalBox();
		rightButtonPanel.add(upButton);
		rightButtonPanel.add(downButton);
		rightButtonPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		rightPanel.add(rightButtonPanel, BorderLayout.LINE_END);
		colPanel.add(rightPanel);

		// Far side lists regex info.
		final JPanel regexPanel = new JPanel(new BorderLayout());
		// Label at the top.
		regexPanel.add(new JLabel(Resources.get("regexColumnsLabel")),
				BorderLayout.PAGE_START);
		regexPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		// Regex fields in their own sub panel.
		final JPanel subRegexPanel = new JPanel(new GridBagLayout());
		subRegexPanel.add(new JLabel(Resources
				.get("regexColumnMatchRegexLabel")), labelConstraints);
		subRegexPanel.add(matchRegex, fieldConstraints);
		subRegexPanel.add(new JLabel(Resources
				.get("regexColumnReplaceRegexLabel")), labelLastRowConstraints);
		subRegexPanel.add(replaceRegex, fieldLastRowConstraints);
		regexPanel.add(subRegexPanel, BorderLayout.CENTER);
		// Buttons at bottom.
		final JPanel regexButtonPanel = new JPanel();
		regexButtonPanel.add(regexResetButton);
		regexButtonPanel.add(regexUpdateButton);
		regexButtonPanel.setBorder(new EmptyBorder(2, 2, 2, 2));
		regexPanel.add(regexButtonPanel, BorderLayout.PAGE_END);
		colPanel.add(regexPanel);

		// On select of column in third column, update regex data,
		// or disable regex fields.
		keyColList.getSelectionModel().addListSelectionListener(
				new ListSelectionListener() {
					public void valueChanged(final ListSelectionEvent e) {
						// Ignore multiple events.
						if (e.getValueIsAdjusting())
							return;
						// Check if selection valid (single selection only).
						final boolean valid = keyColList.getSelectedValue() != null
								&& !keyColList.getSelectedValue().equals(
										PartitionTable.DIV_COLUMN);
						matchRegex.setEnabled(valid);
						replaceRegex.setEnabled(valid);
						regexResetButton.setEnabled(valid);
						regexUpdateButton.setEnabled(valid);
						// If selection valid, update details by clicking on
						// reset button.
						if (valid)
							regexResetButton.doClick();
						else {
							// If selection invalid, clear fields.
							matchRegex.setText(null);
							replaceRegex.setText(null);
						}
					}
				});
		// Default is all disabled.
		matchRegex.setEnabled(false);
		replaceRegex.setEnabled(false);
		regexResetButton.setEnabled(false);
		regexUpdateButton.setEnabled(false);

		// Intercept the insert/remove buttons
		insertButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object selected = tabColList.getSelectedValue();
				if (selected != null) {
					// Move a column from table to key.
					PartitionTableDialog.this.selectedColumns
							.addElement(selected);
					try {
						dataset.asPartitionTable().setSelectedColumnNames(
								PartitionTableDialog.this
										.getNewSelectedColumns());
					} catch (final PartitionException pe) {
						StackTrace.showStackTrace(pe);
					}
					PartitionTableDialog.this.updateAvailableColumns(dataset);
					PartitionTableDialog.this.updatePreviewPanel(martTab,
							dataset);
				}
			}
		});
		removeButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object selected = keyColList.getSelectedValue();
				if (selected != null) {
					// Move a column from key to table.
					PartitionTableDialog.this.selectedColumns
							.removeElement(selected);
					try {
						dataset.asPartitionTable().setSelectedColumnNames(
								PartitionTableDialog.this
										.getNewSelectedColumns());
					} catch (final PartitionException pe) {
						StackTrace.showStackTrace(pe);
					}
					PartitionTableDialog.this.updateAvailableColumns(dataset);
					PartitionTableDialog.this.updatePreviewPanel(martTab,
							dataset);
				}
			}
		});

		// Intercept the up/down buttons
		upButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object selected = keyColList.getSelectedValue();
				if (selected != null) {
					final int currIndex = keyColList.getSelectedIndex();
					if (currIndex > 0) {
						// Swap the selected item with the one above it.
						final Object swap = PartitionTableDialog.this.selectedColumns
								.get(currIndex - 1);
						PartitionTableDialog.this.selectedColumns.setElementAt(
								selected, currIndex - 1);
						PartitionTableDialog.this.selectedColumns.setElementAt(
								swap, currIndex);
						// Select the selected item again, as it will
						// have moved.
						keyColList.setSelectedIndex(currIndex - 1);
						PartitionTableDialog.this.updatePreviewPanel(martTab,
								dataset);
					}
				}
			}
		});
		downButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object selected = keyColList.getSelectedValue();
				if (selected != null) {
					final int currIndex = keyColList.getSelectedIndex();
					if (currIndex < PartitionTableDialog.this.selectedColumns
							.size() - 1) {
						// Swap the selected item with the one below it.
						final Object swap = PartitionTableDialog.this.selectedColumns
								.get(currIndex + 1);
						PartitionTableDialog.this.selectedColumns.setElementAt(
								selected, currIndex + 1);
						PartitionTableDialog.this.selectedColumns.setElementAt(
								swap, currIndex);
						// Select the selected item again, as it will
						// have moved.
						keyColList.setSelectedIndex(currIndex + 1);
						PartitionTableDialog.this.updatePreviewPanel(martTab,
								dataset);
					}
				}
			}
		});

		// Regex update/reset buttons.
		regexUpdateButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final PartitionColumn col = (PartitionColumn) dataset
						.asPartitionTable().getColumns().get(
								(String) keyColList.getSelectedValue());
				col.setRegexMatch(matchRegex.getText());
				col.setRegexReplace(replaceRegex.getText());
				// Regex update button also updates preview.
				PartitionTableDialog.this.updatePreviewPanel(martTab, dataset);
			}
		});
		regexResetButton.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final PartitionColumn col = (PartitionColumn) dataset
						.asPartitionTable().getColumns().get(
								(String) keyColList.getSelectedValue());
				matchRegex.setText(col.getRegexMatch());
				replaceRegex.setText(col.getRegexReplace());
			}
		});

		// Next area is preview row count.
		final JPanel previewCountPanel = new JPanel();
		this.previewRowCount = new JTextField(5);
		this.previewRowCount.setText("" + PartitionTableDialog.PREVIEW_ROWS);
		showHide.add(new JLabel(Resources.get("previewRowCountLabel")),
				labelConstraints);
		previewCountPanel.add(this.previewRowCount);
		final JButton previewUpdate = new JButton(Resources.get("updateButton"));
		previewCountPanel.add(previewUpdate);
		showHide.add(previewCountPanel, fieldConstraints);
		previewUpdate.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				PartitionTableDialog.this.updatePreviewPanel(martTab, dataset);
			}
		});

		// Second area (bottom) is preview panel. Populated on opening,
		// and on each change.
		this.previewData = new DefaultTableModel();
		final JTable previewTable = new JTable(this.previewData);
		previewTable.setGridColor(Color.LIGHT_GRAY); // Mac OSX.
		previewTable.setEnabled(false);
		previewTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		previewTable.setPreferredScrollableViewportSize(colPanel
				.getPreferredSize());
		showHide.add(new JLabel(Resources.get("previewLabel")),
				labelConstraints);
		showHide.add(new JScrollPane(previewTable), fieldConstraints);

		// If the user rearranges the columns in the preview data,
		// mirror in the selected columns.
		previewTable.getColumnModel().addColumnModelListener(
				new TableColumnModelListener() {

					public void columnAdded(final TableColumnModelEvent e) {
						// Don't care.
					}

					public void columnMarginChanged(final ChangeEvent e) {
						// Don't care.
					}

					public void columnMoved(final TableColumnModelEvent e) {
						if (e.getFromIndex() == e.getToIndex())
							return;
						PartitionTableDialog.this.selectedColumns.clear();
						for (int i = 0; i < previewTable.getColumnCount(); i++)
							PartitionTableDialog.this.selectedColumns
									.addElement(previewTable.getColumnName(i));
						try {
							dataset.asPartitionTable().setSelectedColumnNames(
									PartitionTableDialog.this
											.getNewSelectedColumns());
						} catch (final PartitionException pe) {
							StackTrace.showStackTrace(pe);
						}
					}

					public void columnRemoved(final TableColumnModelEvent e) {
						// Don't care.
					}

					public void columnSelectionChanged(
							final ListSelectionEvent e) {
						// Don't care.
					}
				});

		// Intercept the partition checkbox to update the available cols
		// list.
		this.partition.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				showHide.setVisible(PartitionTableDialog.this.partition
						.isSelected());
				try {
					dataset
							.setPartitionTable(PartitionTableDialog.this.partition
									.isSelected());
					if (PartitionTableDialog.this.partition.isSelected()) {
						// Update available/selected cols and preview.
						PartitionTableDialog.this
								.updateSelectedColumns(dataset);
						PartitionTableDialog.this
								.updateAvailableColumns(dataset);
						PartitionTableDialog.this.updatePreviewPanel(martTab,
								dataset);
						PartitionTableDialog.this.appliedList.removeAllItems();
					}
				} catch (final PartitionException pe) {
					StackTrace.showStackTrace(pe);
					try {
						dataset.setPartitionTable(false);
					} catch (final PartitionException pe2) {
						StackTrace.showStackTrace(pe2);
					} finally {
						PartitionTableDialog.this.setVisible(false);
					}
				} finally {
					PartitionTableDialog.this.pack();
				}
			}
		});
		// Default view of data.
		if (dataset.isPartitionTable()) {
			this.partition.setSelected(true);
			showHide.setVisible(true);
			this.updateSelectedColumns(dataset);
			this.updateAvailableColumns(dataset);
			this.updatePreviewPanel(martTab, dataset);
			for (final Iterator i = dataset.asPartitionTable()
					.getAllApplications().entrySet().iterator(); i.hasNext();) {
				final Map.Entry entry = (Map.Entry) i.next();
				final DataSet ds = (DataSet) entry.getKey();
				for (final Iterator j = ((Map) entry.getValue()).keySet()
						.iterator(); j.hasNext();) {
					final String dimName = (String) j.next();
					if (dimName.equals(PartitionTable.NO_DIMENSION))
						this.appliedList.addItem(ds);
					else
						this.appliedList.addItem(ds.getTables().get(dimName));
				}
			}
			if (this.appliedList.getItemCount() > 0)
				this.appliedList.setSelectedIndex(0);
		}

		// Last section is the partition-applied panel.
		showHide.add(new JLabel(Resources.get("partitionAppliedLabel")),
				labelLastRowConstraints);
		final JPanel appliedPanel = new JPanel();
		final JPanel listHolder = new JPanel(new BorderLayout());
		appliedPanel.add(listHolder);
		appliedPanel.add(wizardHolder);
		// On the left we have a drop-down of places it is applied at.
		listHolder.add(this.appliedList, BorderLayout.CENTER);
		// Below the list are add/remove buttons for selected item.
		final JPanel listButtons = new JPanel();
		final JButton addAppl = new JButton(Resources.get("addButton"));
		final JButton removeAppl = new JButton(Resources.get("removeButton"));
		listButtons.add(removeAppl);
		listButtons.add(addAppl);
		listHolder.add(listButtons, BorderLayout.PAGE_END);
		// Add button calculates list of valid datasets/dimensions,
		// prompts user to choose one, then adds it to the list and selects it.
		addAppl.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				// Lookup valid dataset/dimension options.
				final List choices = new ArrayList(dataset.getMart()
						.getDataSets().values());
				// Remove those that are partition tables themselves.
				for (final Iterator i = choices.iterator(); i.hasNext();)
					if (((DataSet) i.next()).isPartitionTable())
						i.remove();
				// Remove all invisible and masked datasets.
				for (final Iterator i = choices.iterator(); i.hasNext();) {
					final DataSet ds = (DataSet) i.next();
					if (ds.isMasked() || ds.isInvisible())
						i.remove();
				}
				// Only remove those applied to NO_DIMENSION.
				for (final Iterator i = dataset.asPartitionTable()
						.getAllApplications().entrySet().iterator(); i
						.hasNext();) {
					final Map.Entry entry = (Map.Entry) i.next();
					final DataSet ds = (DataSet) entry.getKey();
					final Map map = (Map) entry.getValue();
					if (map.containsKey(PartitionTable.NO_DIMENSION))
						choices.remove(ds);
				}
				// Add all dimensions from remaining dses then remove
				// used dimensions and parent dses.
				for (final Iterator i = new ArrayList(choices).iterator(); i
						.hasNext();) {
					final DataSet ds = (DataSet) i.next();
					// Add all dimension tables.
					for (final Iterator j = ds.getTables().values().iterator(); j
							.hasNext();) {
						final DataSetTable dst = (DataSetTable) j.next();
						if (dst.getType().equals(DataSetTableType.DIMENSION))
							choices.add(dst);
					}
					final Map dims = (Map) dataset.asPartitionTable()
							.getAllApplications().get(ds);
					if (dims != null) {
						choices.remove(ds);
						for (final Iterator j = dims.keySet().iterator(); j
								.hasNext();)
							choices.remove(ds.getTables().get(j.next()));
					}
				}
				// Prompt user to choose one.
				final Object sel = JOptionPane.showInputDialog(
						PartitionTableDialog.this, Resources
								.get("partitionSelectApplyTarget"), Resources
								.get("questionTitle"),
						JOptionPane.QUESTION_MESSAGE, null, choices.toArray(),
						null);
				// Create a default PartitionTableApplication and add it.
				if (sel == null)
					return;
				else if (sel instanceof DataSet)
					dataset.asPartitionTable().applyTo(
							(DataSet) sel,
							PartitionTable.NO_DIMENSION,
							PartitionTableApplication.createDefault(dataset
									.asPartitionTable(), (DataSet) sel));
				else if (sel instanceof DataSetTable)
					dataset.asPartitionTable().applyTo(
							(DataSet) ((DataSetTable) sel).getSchema(),
							((DataSetTable) sel).getName(),
							PartitionTableApplication.createDefault(dataset
									.asPartitionTable(), ((DataSetTable) sel)
									.getDataSet(), ((DataSetTable) sel)
									.getName()));
				else
					throw new BioMartError(); // Eh?
				// Add the selected item to the list and select it.
				PartitionTableDialog.this.appliedList.addItem(sel);
				PartitionTableDialog.this.appliedList.setSelectedItem(sel);
			}
		});
		// Remove button removes current item from list.
		removeAppl.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent e) {
				final Object sel = PartitionTableDialog.this.appliedList
						.getSelectedItem();
				if (sel == null)
					return;
				wizardHolder.removeAll();
				if (sel instanceof DataSet)
					dataset.asPartitionTable().removeFrom((DataSet) sel,
							PartitionTable.NO_DIMENSION);
				else if (sel instanceof DataSetTable)
					dataset.asPartitionTable().removeFrom(
							((DataSetTable) sel).getDataSet(),
							((DataSetTable) sel).getName());
				PartitionTableDialog.this.appliedList.removeItem(sel);
			}
		});
		showHide.add(appliedPanel, fieldLastRowConstraints);

		// Select an applied item.
		if (preselect != null)
			this.appliedList.setSelectedItem(preselect);

		// Set size of window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	private void updateAvailableColumns(final DataSet ds) {
		this.availableColumns.clear();
		if (ds.isPartitionTable())
			for (final Iterator i = ds.asPartitionTable()
					.getAvailableColumnNames().iterator(); i.hasNext();)
				this.availableColumns.addElement(i.next());
		// Only allow div if not already used.
		this.availableColumns.addElement(PartitionTable.DIV_COLUMN);
		for (final Iterator i = this.getNewSelectedColumns().iterator(); i
				.hasNext();)
			this.availableColumns.removeElement(i.next());
	}

	private void updateSelectedColumns(final DataSet ds) {
		this.selectedColumns.clear();
		if (ds.isPartitionTable()) {
			for (final Iterator i = ds.asPartitionTable()
					.getSelectedColumnNames().iterator(); i.hasNext();)
				this.selectedColumns.addElement(i.next());
			this.appliedList.setSelectedIndex(-1);
		}
	}

	private List getNewSelectedColumns() {
		return Arrays.asList(this.selectedColumns.toArray());
	}

	private void updatePreviewPanel(final MartTab martTab, final DataSet ds) {
		new LongProcess() {
			public void run() throws Exception {
				// Update ds with new ones.
				final List selectedCols = PartitionTableDialog.this
						.getNewSelectedColumns();
				if (!ds.asPartitionTable().getSelectedColumnNames().equals(
						selectedCols)) {
					ds.asPartitionTable().setSelectedColumnNames(selectedCols);
					for (final Iterator i = ds.asPartitionTable()
							.getAllApplications().values().iterator(); i
							.hasNext();)
						for (final Iterator j = ((Map) i.next()).values()
								.iterator(); j.hasNext();)
							((PartitionTableApplication) ((WeakReference) j
									.next()).get()).syncCounts();
				}
				// Update preview data column headers.
				PartitionTableDialog.this.previewData
						.setColumnIdentifiers(selectedCols.toArray());
				// Clear preview data model.
				while (PartitionTableDialog.this.previewData.getRowCount() > 0)
					PartitionTableDialog.this.previewData.removeRow(0);
				// No new cols? Don't go any further.
				final List trueSelectedCols = new ArrayList();
				for (final Iterator i = selectedCols.iterator(); i.hasNext();) {
					final String col = (String) i.next();
					if (!col.equals(PartitionTable.DIV_COLUMN))
						trueSelectedCols.add(col);
				}
				if (trueSelectedCols.size() < 1)
					return;
				// Get the rows.
				try {
					ds
							.asPartitionTable()
							.prepareRows(
									martTab.getPartitionViewSelection(),
									Integer
											.parseInt(PartitionTableDialog.this.previewRowCount
													.getText()));
				} catch (final NumberFormatException nfe) {
					ds.asPartitionTable().prepareRows(null,
							PartitionTableDialog.PREVIEW_ROWS);
				}
				while (ds.asPartitionTable().nudgeRow()) {
					final PartitionRow row = ds.asPartitionTable().currentRow();
					final List rowData = new ArrayList();
					for (final Iterator i = selectedCols.iterator(); i
							.hasNext();) {
						final String col = (String) i.next();
						if (col.equals(PartitionTable.DIV_COLUMN))
							rowData.add("->");
						else
							rowData.add(((PartitionColumn) row
									.getPartitionTable().getColumns().get(col))
									.getValueForRow(row));
					}
					// Add an entry to the data model using list.toArray();
					PartitionTableDialog.this.previewData.addRow(rowData
							.toArray());
				}
				// Re-select the current applied item in order
				// to update available columns.
				if (PartitionTableDialog.this.wizardPanel != null) {
					PartitionTableDialog.this.wizardPanel.recalculate();
					PartitionTableDialog.this.pack();
				}
			}
		}.start();
	}

	private static class WizardPanel extends JPanel {
		private static final long serialVersionUID = 1L;

		private final List ptLevels = new ArrayList();

		private final List dsLevels = new ArrayList();

		private final List nameLevels = new ArrayList();

		private final DataSetTable dsTable;

		private final PartitionTableApplication pta;

		private Map dsRelMap = new HashMap();

		private WizardPanel(final PartitionTableApplication pta,
				final DataSetTable dsTable) {
			super(new GridBagLayout());

			this.pta = pta;
			this.dsTable = dsTable;

			this.recalculate();
		}

		private void recalculate() {
			this.removeAll();
			this.ptLevels.clear();
			this.dsLevels.clear();
			this.nameLevels.clear();
			this.dsRelMap.clear();
			// Convert column list to include blank.
			// Make drop-down display modified names, with
			// prefix stripped if necessary. But, it selects real
			// names (with stripped prefixes) in background.
			final Map dsColMap = new TreeMap();
			for (final Iterator j = this.dsTable.getTransformationUnits()
					.iterator(); j.hasNext();) {
				final TransformationUnit tu = (TransformationUnit) j.next();
				final Relation rel = tu instanceof JoinTable ? ((JoinTable) tu)
						.getSchemaRelation() : null;
				for (final Iterator i = tu.getNewColumnNameMap().values()
						.iterator(); i.hasNext();) {
					final DataSetColumn dsCol = (DataSetColumn) i.next();
					if (dsCol instanceof WrappedColumn
							|| dsCol instanceof InheritedColumn) {
						String root = dsCol.getName();
						if (root.indexOf("__") >= 0)
							root = root.substring(root.lastIndexOf("__") + 2);
						String display = dsCol.getModifiedName();
						if (display.indexOf("__") >= 0)
							display = display.substring(display
									.lastIndexOf("__") + 2);
						if (!dsColMap.containsKey(root)) {
							dsColMap.put(root, display);
							this.dsRelMap.put(root, rel);
						}
					}
				}
			}
			final List dsColList = new ArrayList(dsColMap.keySet());
			dsColList.add(0, null);

			// Create constraints for labels that are not in the last row.
			final GridBagConstraints labelConstraints = new GridBagConstraints();
			labelConstraints.gridwidth = GridBagConstraints.RELATIVE;
			labelConstraints.fill = GridBagConstraints.HORIZONTAL;
			labelConstraints.anchor = GridBagConstraints.LINE_END;
			labelConstraints.insets = new Insets(0, 2, 0, 0);
			// Create constraints for fields that are not in the last row.
			final GridBagConstraints fieldConstraints = new GridBagConstraints();
			fieldConstraints.gridwidth = GridBagConstraints.REMAINDER;
			fieldConstraints.fill = GridBagConstraints.NONE;
			fieldConstraints.anchor = GridBagConstraints.LINE_START;
			fieldConstraints.insets = new Insets(0, 1, 0, 2);
			// Create constraints for labels that are in the last row.
			final GridBagConstraints labelLastRowConstraints = (GridBagConstraints) labelConstraints
					.clone();
			labelLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;
			// Create constraints for fields that are in the last row.
			final GridBagConstraints fieldLastRowConstraints = (GridBagConstraints) fieldConstraints
					.clone();
			fieldLastRowConstraints.gridheight = GridBagConstraints.REMAINDER;

			// On partition table change listener.
			// Load partition table.
			final PartitionTable pt = this.pta.getPartitionTable();
			// Identify column names.
			final Iterator colNames = pt.getSelectedColumnNames().iterator();
			// Reinsert one pair of entries per remaining subdivision.
			int currLevel = 0;
			for (List currLevelNames = this.getNextSubdivision(colNames); !currLevelNames
					.isEmpty(); currLevelNames = this
					.getNextSubdivision(colNames)) {
				// Add combos to apply-levels.
				final JComboBox ptCombo = new JComboBox(currLevelNames
						.toArray());
				this.ptLevels.add(ptCombo);
				final JComboBox dsCombo = new JComboBox(dsColList.toArray());
				// Remove null option from first row.
				if (currLevel == 0)
					dsCombo.removeItemAt(0);
				dsCombo.setRenderer(new ListCellRenderer() {
					public Component getListCellRendererComponent(
							final JList list, final Object value,
							final int index, final boolean isSelected,
							final boolean cellHasFocus) {
						final String key = (String) value;
						final JLabel label = new JLabel();
						if (key != null)
							label.setText((String) dsColMap.get(key));
						else
							label.setText(Resources
									.get("partitionDSUnselected"));
						label.setOpaque(true);
						label.setFont(list.getFont());
						if (isSelected) {
							label.setBackground(list.getSelectionBackground());
							label.setForeground(list.getSelectionForeground());
						} else {
							label.setBackground(list.getBackground());
							label.setForeground(list.getForeground());
						}
						return label;
					}
				});
				this.dsLevels.add(dsCombo);
				final JComboBox nameCombo = new JComboBox(currLevelNames
						.toArray());
				this.nameLevels.add(nameCombo);
				final JPanel subpanel = new JPanel(new GridBagLayout());
				subpanel.add(new JLabel(Resources.get("wizardPTColLabel")),
						labelConstraints);
				subpanel.add(ptCombo, fieldConstraints);
				subpanel.add(new JLabel(Resources.get("wizardDSColLabel")),
						labelConstraints);
				subpanel.add(dsCombo, fieldConstraints);
				subpanel.add(new JLabel(currLevel == 0 ? Resources
						.get("wizardTableNameColLabel") : Resources
						.get("wizardColumnNameColLabel")),
						labelLastRowConstraints);
				subpanel.add(nameCombo, fieldLastRowConstraints);
				this.add(subpanel, fieldConstraints);
				// Disable subsequent combos if this one is
				// set to null.
				dsCombo.addActionListener(new ActionListener() {
					public void actionPerformed(final ActionEvent e) {
						for (int i = WizardPanel.this.dsLevels.indexOf(dsCombo) + 1; i < WizardPanel.this.dsLevels
								.size(); i++) {
							final JComboBox currName = (JComboBox) WizardPanel.this.nameLevels
									.get(i);
							final JComboBox currPT = (JComboBox) WizardPanel.this.ptLevels
									.get(i);
							final JComboBox currDS = (JComboBox) WizardPanel.this.dsLevels
									.get(i);
							final JComboBox prevDS = (JComboBox) WizardPanel.this.dsLevels
									.get(i - 1);
							if (prevDS.getSelectedItem() == null) {
								currDS.setSelectedItem(null);
								currName.setSelectedItem(null);
							}
							currDS.setEnabled(prevDS.getSelectedItem() != null);
							currPT.setEnabled(prevDS.getSelectedItem() != null);
							currName
									.setEnabled(prevDS.getSelectedItem() != null);
						}
					}
				});
				// Restrict to first box only at first.
				// Take into account existing settings from application.
				final PartitionAppliedRow selected = currLevel < this.pta
						.getPartitionAppliedRows().size() ? (PartitionAppliedRow) this.pta
						.getPartitionAppliedRows().get(currLevel)
						: null;
				if (selected != null) {
					ptCombo.setSelectedItem(selected.getPartitionCol());
					dsCombo.setSelectedItem(selected.getRootDataSetCol());
					nameCombo.setSelectedItem(selected.getNamePartitionCol());
				} else {
					dsCombo.setSelectedItem(null);
					nameCombo.setSelectedItem(null);
				}
				currLevel++;

				// Update model on change.
				final ActionListener al = new ActionListener() {
					public void actionPerformed(final ActionEvent e) {
						WizardPanel.this.updateModel();
					}
				};
				ptCombo.addActionListener(al);
				dsCombo.addActionListener(al);
				nameCombo.addActionListener(al);
			}
		}

		private void updateModel() {
			if (WizardPanel.this.validateFields()) {
				// How many rows? If less, remove extra ones.
				final int rowCount = WizardPanel.this.ptLevels.size();
				while (rowCount < this.pta.getPartitionAppliedRows().size())
					this.pta.getPartitionAppliedRows().remove(
							this.pta.getPartitionAppliedRows().size() - 1);
				// Update or add existing rows.
				for (int i = 0; i < WizardPanel.this.ptLevels.size()
						&& ((JComboBox) WizardPanel.this.dsLevels.get(i))
								.getSelectedItem() != null; i++) {
					final String ptCol = (String) ((JComboBox) WizardPanel.this.ptLevels
							.get(i)).getSelectedItem();
					final String dsCol = (String) ((JComboBox) WizardPanel.this.dsLevels
							.get(i)).getSelectedItem();
					final String nameCol = (String) ((JComboBox) WizardPanel.this.nameLevels
							.get(i)).getSelectedItem();
					if (i >= this.pta.getPartitionAppliedRows().size())
						this.pta.getPartitionAppliedRows().add(
								new PartitionAppliedRow(ptCol, dsCol, nameCol,
										(Relation) this.dsRelMap.get(dsCol)));
					else {
						final PartitionAppliedRow ptar = (PartitionAppliedRow) this.pta
								.getPartitionAppliedRows().get(i);
						ptar.setPartitionCol(ptCol);
						ptar.setRootDataSetCol(dsCol);
						ptar.setNamePartitionCol(nameCol);
						ptar.setRelation((Relation) this.dsRelMap.get(dsCol));
					}
				}
			}
		}

		private boolean validateFields() {
			// List of messages to display, if any are necessary.
			final List messages = new ArrayList();

			// Must have at least one column selected.
			if (((JComboBox) this.dsLevels.get(0)).getSelectedItem() == null)
				messages.add(Resources.get("wizardMissingFirstMapping"));

			// Any messages to display? Show them.
			if (!messages.isEmpty())
				JOptionPane.showMessageDialog(null, messages
						.toArray(new String[0]), Resources
						.get("validationTitle"),
						JOptionPane.INFORMATION_MESSAGE);

			// Validation succeeds if there are no messages.
			return messages.isEmpty();
		}

		private List getNextSubdivision(final Iterator cols) {
			if (!cols.hasNext())
				return Collections.EMPTY_LIST;
			final List nextcols = new ArrayList();
			while (cols.hasNext()) {
				final String colname = (String) cols.next();
				if (!colname.equals(PartitionTable.DIV_COLUMN))
					nextcols.add(colname);
				else
					break;
			}
			return nextcols;
		}
	}

	/**
	 * Open a wizard which asks user which partition table they want to apply to
	 * the dimension, or which dataset they want to use to make such a partition
	 * table. It then applies a default application to the dimension and opens
	 * the editor with that application selected.
	 * 
	 * @param martTab
	 *            the mart tab the dataset lives in.
	 * @param dimension
	 *            the dimension we want to run the wizard on.
	 * @throws PartitionException
	 *             if the table is invalid.
	 */
	public static void showForDimension(final MartTab martTab,
			final DataSetTable dimension) throws PartitionException {
		final Mart mart = martTab.getMart();
		// Does it already have a partition table? Select that one.
		PartitionTableApplication pta = dimension
				.getPartitionTableApplication();
		if (pta == null) {
			// If not, select an existing one.
			final List options = new ArrayList(mart.getPartitionTables());
			options.add(0, Resources.get("createNewPartitionTable"));
			Object ptSelected = JOptionPane.showInputDialog(null, Resources
					.get("wizardSelectPartitionTable"), Resources
					.get("questionTitle"), JOptionPane.QUESTION_MESSAGE, null,
					options.toArray(), null);
			WrappedColumn autoCol = null;
			Column sourceCol = null;
			PartitionTable pt = null;
			if (ptSelected == null)
				return;
			else if (ptSelected
					.equals(Resources.get("createNewPartitionTable"))) {
				// New dialog asking for a column to use from
				// the currently selected table.
				final Map newOptions = new TreeMap();
				for (final Iterator i = dimension.getColumns().values()
						.iterator(); i.hasNext();) {
					final DataSetColumn dsCol = (DataSetColumn) i.next();
					if (dsCol instanceof WrappedColumn)
						newOptions.put(dsCol.getModifiedName(), dsCol);
				}
				final String colName = (String) JOptionPane.showInputDialog(
						null, Resources.get("wizardCreatePartitionTable"),
						Resources.get("questionTitle"),
						JOptionPane.QUESTION_MESSAGE, null, newOptions.keySet()
								.toArray(), null);
				if (colName == null)
					return;
				// Magically build a dataset and new partition table
				// based on the selected column.
				autoCol = (WrappedColumn) newOptions.get(colName);
				sourceCol = autoCol.getWrappedColumn();
				try {
					final Collection candidates = mart
							.suggestDataSets(Collections
									.singletonList(sourceCol.getTable()));
					final DataSet ds = (DataSet) candidates.iterator().next();
					ds.setPartitionTable(true);
					pt = ds.asPartitionTable();
					// Find ds col for source col and select
					// ds col's name - NOT source col name as may be
					// different (e.g. in _key case).
					DataSetColumn realSourceCol = null;
					for (final Iterator i = ds.getMainTable().getColumns()
							.values().iterator(); i.hasNext()
							&& realSourceCol == null;) {
						final DataSetColumn cand = (DataSetColumn) i.next();
						if (cand instanceof WrappedColumn
								&& ((WrappedColumn) cand).getWrappedColumn()
										.equals(sourceCol))
							realSourceCol = cand;
					}
					if (realSourceCol == null)
						throw new BioMartError(); // Should never happen.
					ds.asPartitionTable().setSelectedColumnNames(
							Collections.singletonList(realSourceCol
									.getModifiedName()));
				} catch (final Exception e) {
					StackTrace.showStackTrace(e);
					return;
				}
			} else
				pt = (PartitionTable) ptSelected;
			// Make a default definition and add to selected partition table.
			pta = PartitionTableApplication.createDefault(pt, dimension
					.getDataSet(), dimension.getName());
			// If autoCol has been set, use it to modify the
			// partition table application.
			if (autoCol != null) {
				Relation rel = null;
				for (final Iterator j = ((DataSetTable) autoCol.getTable())
						.getTransformationUnits().iterator(); j.hasNext()
						&& rel == null;) {
					final TransformationUnit tu = (TransformationUnit) j.next();
					final Relation candRel = tu instanceof JoinTable ? ((JoinTable) tu)
							.getSchemaRelation()
							: null;
					for (final Iterator i = tu.getNewColumnNameMap().values()
							.iterator(); i.hasNext() && rel == null;) {
						final DataSetColumn dsCol = (DataSetColumn) i.next();
						if (dsCol == autoCol)
							rel = candRel;
					}
				}
				final PartitionAppliedRow row = new PartitionAppliedRow(
						autoCol.getModifiedName(), autoCol.getName(), 
						autoCol.getModifiedName(), rel);
				pta.getPartitionAppliedRows().clear();
				pta.getPartitionAppliedRows().add(row);
			}
			pt.applyTo(dimension.getDataSet(), dimension.getName(), pta);
			pta.syncCounts();
		}
		// Open selected table with dimension selected in appliedList.
		final PartitionTableDialog dialog = new PartitionTableDialog(martTab,
				(DataSet) mart.getDataSets().get(
						pta.getPartitionTable().getOriginalName()), dimension);
		dialog.setVisible(true);
		dialog.dispose();
	}

	/**
	 * Open a wizard which asks user which partition table they want to apply to
	 * the dataset, or which dataset they want to use to make such a partition
	 * table. It then applies a default application to the dataset and opens the
	 * editor with that application selected.
	 * 
	 * @param martTab
	 *            the mart tab the dataset lives in.
	 * @param dataset
	 *            the dataset we want to run the wizard on.
	 * @throws PartitionException
	 *             if the table is invalid.
	 */
	public static void showForDataSet(final MartTab martTab,
			final DataSet dataset) throws PartitionException {
		final Mart mart = martTab.getMart();
		// Does it already have a partition table? Select that one.
		PartitionTableApplication pta = dataset.getPartitionTableApplication();
		if (pta == null) {
			// If not, select an existing one.
			final List options = new ArrayList(mart.getPartitionTables());
			options.add(0, Resources.get("createNewPartitionTable"));
			Object ptSelected = JOptionPane.showInputDialog(null, Resources
					.get("wizardSelectPartitionTable"), Resources
					.get("questionTitle"), JOptionPane.QUESTION_MESSAGE, null,
					options.toArray(), null);
			if (ptSelected == null)
				return;
			WrappedColumn autoCol = null;
			Column sourceCol = null;
			PartitionTable pt = null;
			if (ptSelected == null)
				return;
			else if (ptSelected
					.equals(Resources.get("createNewPartitionTable"))) {
				// New dialog asking for a column to use from
				// the currently selected table.
				final Map newOptions = new TreeMap();
				for (final Iterator i = dataset.getMainTable().getColumns()
						.values().iterator(); i.hasNext();) {
					final DataSetColumn dsCol = (DataSetColumn) i.next();
					if (dsCol instanceof WrappedColumn)
						newOptions.put(dsCol.getModifiedName(), dsCol);
				}
				final String colName = (String) JOptionPane.showInputDialog(
						null, Resources.get("wizardCreatePartitionTable"),
						Resources.get("questionTitle"),
						JOptionPane.QUESTION_MESSAGE, null, newOptions.keySet()
								.toArray(), null);
				if (colName == null)
					return;
				// Magically build a dataset and new partition table
				// based on the selected column.
				autoCol = (WrappedColumn) newOptions.get(colName);
				sourceCol = autoCol.getWrappedColumn();
				try {
					final Collection candidates = mart
							.suggestDataSets(Collections
									.singletonList(sourceCol.getTable()));
					final DataSet ds = (DataSet) candidates.iterator().next();
					ds.setPartitionTable(true);
					pt = ds.asPartitionTable();
					// Find ds col for source col and select
					// ds col's name - NOT source col name as may be
					// different (e.g. in _key case).
					DataSetColumn realSourceCol = null;
					for (final Iterator i = ds.getMainTable().getColumns()
							.values().iterator(); i.hasNext()
							&& realSourceCol == null;) {
						final DataSetColumn cand = (DataSetColumn) i.next();
						if (cand instanceof WrappedColumn
								&& ((WrappedColumn) cand).getWrappedColumn()
										.equals(sourceCol))
							realSourceCol = cand;
					}
					if (realSourceCol == null)
						throw new BioMartError(); // Should never happen.
					ds.asPartitionTable().setSelectedColumnNames(
							Collections.singletonList(realSourceCol
									.getModifiedName()));
				} catch (final Exception e) {
					StackTrace.showStackTrace(e);
					return;
				}
			} else
				pt = (PartitionTable) ptSelected;
			// Make a default definition and add to selected partition table.
			pta = PartitionTableApplication.createDefault(pt, dataset);
			// If autoCol has been set, use it to modify the
			// partition table application.
			if (autoCol != null) {
				Relation rel = null;
				for (final Iterator j = ((DataSetTable) autoCol.getTable())
						.getTransformationUnits().iterator(); j.hasNext()
						&& rel == null;) {
					final TransformationUnit tu = (TransformationUnit) j.next();
					final Relation candRel = tu instanceof JoinTable ? ((JoinTable) tu)
							.getSchemaRelation()
							: null;
					for (final Iterator i = tu.getNewColumnNameMap().values()
							.iterator(); i.hasNext() && rel == null;) {
						final DataSetColumn dsCol = (DataSetColumn) i.next();
						if (dsCol == autoCol)
							rel = candRel;
					}
				}
				final PartitionAppliedRow row = new PartitionAppliedRow(
						sourceCol.getName(), autoCol.getName(), sourceCol
								.getName(), rel);
				pta.getPartitionAppliedRows().clear();
				pta.getPartitionAppliedRows().add(row);
			}
			pt.applyTo(dataset, PartitionTable.NO_DIMENSION, pta);
			pta.syncCounts();
		}
		// Open selected table with dimension selected in appliedList.
		final PartitionTableDialog dialog = new PartitionTableDialog(martTab,
				(DataSet) mart.getDataSets().get(
						pta.getPartitionTable().getOriginalName()), dataset);
		dialog.setVisible(true);
		dialog.dispose();
	}
}
