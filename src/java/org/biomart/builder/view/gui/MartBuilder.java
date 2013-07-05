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

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.ButtonGroup;
import javax.swing.ImageIcon;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JRadioButtonMenuItem;
import javax.swing.event.MenuEvent;
import javax.swing.event.MenuListener;
import javax.swing.undo.UndoManager;

import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Mart;
import org.biomart.builder.model.Schema;
import org.biomart.builder.model.DataSet.DataSetOptimiserType;
import org.biomart.builder.view.gui.MartTabSet.MartTab;
import org.biomart.common.resources.Resources;
import org.biomart.common.resources.Settings;
import org.biomart.common.view.gui.BioMartGUI;

/**
 * The main window housing the MartBuilder GUI. The {@link #main(String[])}
 * method starts the GUI and opens this window.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.80 $, $Date: 2008-03-05 13:00:28 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class MartBuilder extends BioMartGUI {
	private static final long serialVersionUID = 1L;

	/**
	 * Run this application and open the main window. The window stays open and
	 * the application keeps running until the window is closed.
	 * 
	 * @param args
	 *            any command line arguments that the user specified will be in
	 *            this array.
	 */
	public static void main(final String[] args) {
		// Initialise resources.
		Settings.setApplication(Settings.MARTBUILDER);
		Resources.setResourceLocation("org/biomart/builder/resources");
		// Start the application.
		new MartBuilder().launch();
	}

	private MartTabSet martTabSet;

	protected void initComponents() {
		// Make a menu bar and add it.
		this.setJMenuBar(new MartBuilderMenuBar(this));

		// Set up the set of tabs to hold the various marts.
		this.martTabSet = new MartTabSet(this);
		this.getContentPane().add(this.martTabSet, BorderLayout.CENTER);
		this.martTabSet.setOpaque(true);
		this.setBackground(this.martTabSet.getBackground());

		// Go straight to the 'New' page.
		this.martTabSet.requestNewMart();
	}

	/**
	 * Exits the application, but only with permission from the mart tabset.
	 */
	public boolean confirmExitApp() {
		return this.martTabSet.requestCloseAllMarts();
	}

	// This is the main menu bar.
	private static class MartBuilderMenuBar extends BioMartMenuBar {
		private static final long serialVersionUID = 1;

		private JMenuItem closeMart;

		private JMenuItem newMart;

		private JMenuItem openMart;

		private JMenuItem saveDDL;

		private JMenuItem saveMart;

		private JMenuItem saveMartAs;

		private JMenuItem monitorHost;

		private JMenuItem addSchema;

		private JMenuItem updateAllSchemas;

		private JMenuItem removeAllSchemaPartitions;

		private JMenuItem maskAllDataSets;

		private JMenuItem createDatasets;

		private JMenuItem removeAllDatasets;

		private JMenuItem keyguessingSchema;

		private JMenuItem updateSchema;

		private JMenuItem renameSchema;

		private JMenuItem removeSchema;

		private JMenuItem invisibleDataset;

		private JMenuItem maskedDataset;

		private JMenuItem explainDataset;

		private JMenuItem saveDatasetDDL;

		private JMenuItem renameDataset;

		private JMenuItem removeDataset;

		private JMenuItem extendDataset;

		private JMenu optimiseDatasetSubmenu;

		private JMenuItem indexOptimiser;

		private JMenuItem convertPartitionTable;

		private JMenuItem datasetAcceptAll;

		private JMenuItem datasetRejectAll;

		private JMenuItem datasetReplicate;

		private JMenuItem schemaAcceptAll;

		private JMenuItem schemaRejectAll;

		private JMenuItem updatePartitionCounts;

		private JMenuItem updateAllPartitionCounts;

		private JMenu nameCaseSubmenu;

		private JRadioButtonMenuItem nameCaseMixed;

		private JRadioButtonMenuItem nameCaseUpper;

		private JRadioButtonMenuItem nameCaseLower;

		private JMenuItem partitionDSWizard;

		/**
		 * Constructor calls super then sets up our menu items.
		 * 
		 * @param martBuilder
		 *            the mart builder gui to which we are attached.
		 */
		public MartBuilderMenuBar(final MartBuilder martBuilder) {
			super(martBuilder);
		}

		protected void buildMenus(final JMenuItem exit) {
			// New mart.
			this.newMart = new JMenuItem(Resources.get("newMartTitle"),
					new ImageIcon(Resources.getResourceAsURL("new.gif")));
			this.newMart
					.setMnemonic(Resources.get("newMartMnemonic").charAt(0));
			this.newMart.addActionListener(this);

			// Open existing mart.
			this.openMart = new JMenuItem(Resources.get("openMartTitle"),
					new ImageIcon(Resources.getResourceAsURL("open.gif")));
			this.openMart.setMnemonic(Resources.get("openMartMnemonic").charAt(
					0));
			this.openMart.addActionListener(this);

			// Save current mart.
			this.saveMart = new JMenuItem(Resources.get("saveMartTitle"),
					new ImageIcon(Resources.getResourceAsURL("save.gif")));
			this.saveMart.setMnemonic(Resources.get("saveMartMnemonic").charAt(
					0));
			this.saveMart.addActionListener(this);

			// Save current mart as.
			this.saveMartAs = new JMenuItem(Resources.get("saveMartAsTitle"),
					new ImageIcon(Resources.getResourceAsURL("save.gif")));
			this.saveMartAs.setMnemonic(Resources.get("saveMartAsMnemonic")
					.charAt(0));
			this.saveMartAs.addActionListener(this);

			// Create DDL for current mart.
			this.saveDDL = new JMenuItem(Resources.get("saveDDLTitle"),
					new ImageIcon(Resources.getResourceAsURL("saveText.gif")));
			this.saveDDL
					.setMnemonic(Resources.get("saveDDLMnemonic").charAt(0));
			this.saveDDL.addActionListener(this);

			// Monitor remote host.
			this.monitorHost = new JMenuItem(Resources.get("monitorHostTitle"));
			this.monitorHost.setMnemonic(Resources.get("monitorHostMnemonic")
					.charAt(0));
			this.monitorHost.addActionListener(this);

			// Close current mart.
			this.closeMart = new JMenuItem(Resources.get("closeMartTitle"));
			this.closeMart.setMnemonic(Resources.get("closeMartMnemonic")
					.charAt(0));
			this.closeMart.addActionListener(this);

			// Make a submenu for the name case type.
			this.nameCaseSubmenu = new JMenu(Resources.get("caseTitle"));
			this.nameCaseSubmenu.setMnemonic(Resources.get("caseMnemonic")
					.charAt(0));
			final ButtonGroup optGroupNC = new ButtonGroup();
			// Loop through the case types to create the submenu.
			// Mixed
			this.nameCaseMixed = new JRadioButtonMenuItem(Resources
					.get("caseMixedTitle"));
			this.nameCaseMixed.setMnemonic(Resources.get("caseMixedMnemonic")
					.charAt(0));
			this.nameCaseMixed.addActionListener(this);
			optGroupNC.add(this.nameCaseMixed);
			this.nameCaseSubmenu.add(this.nameCaseMixed);
			// Upper
			this.nameCaseUpper = new JRadioButtonMenuItem(Resources
					.get("caseUpperTitle"));
			this.nameCaseUpper.setMnemonic(Resources.get("caseUpperMnemonic")
					.charAt(0));
			this.nameCaseUpper.addActionListener(this);
			optGroupNC.add(this.nameCaseUpper);
			this.nameCaseSubmenu.add(this.nameCaseUpper);
			// Lower
			this.nameCaseLower = new JRadioButtonMenuItem(Resources
					.get("caseLowerTitle"));
			this.nameCaseLower.setMnemonic(Resources.get("caseLowerMnemonic")
					.charAt(0));
			this.nameCaseLower.addActionListener(this);
			optGroupNC.add(this.nameCaseLower);
			this.nameCaseSubmenu.add(this.nameCaseLower);

			// Add new schema.
			this.addSchema = new JMenuItem(Resources.get("addSchemaTitle"),
					new ImageIcon(Resources.getResourceAsURL("add.gif")));
			this.addSchema.setMnemonic(Resources.get("closeMartMnemonic")
					.charAt(0));
			this.addSchema.addActionListener(this);

			// Sync all schemas.
			this.updateAllSchemas = new JMenuItem(Resources
					.get("synchroniseAllSchemasTitle"), new ImageIcon(Resources
					.getResourceAsURL("refresh.gif")));
			this.updateAllSchemas.setMnemonic(Resources.get(
					"synchroniseAllSchemasMnemonic").charAt(0));
			this.updateAllSchemas.addActionListener(this);

			// Remove all schema partitions.
			this.removeAllSchemaPartitions = new JMenuItem(Resources
					.get("removeAllSchemaPartitionsTitle"));
			this.removeAllSchemaPartitions.setMnemonic(Resources.get(
					"removeAllSchemaPartitionsMnemonic").charAt(0));
			this.removeAllSchemaPartitions.addActionListener(this);

			// Mask all datasets.
			this.maskAllDataSets = new JMenuItem(Resources
					.get("maskAllDataSetsTitle"));
			this.maskAllDataSets.setMnemonic(Resources.get(
					"maskAllDataSetsMnemonic").charAt(0));
			this.maskAllDataSets.addActionListener(this);

			// Create datasets.
			this.createDatasets = new JMenuItem(Resources
					.get("suggestDataSetsTitle"), new ImageIcon(Resources
					.getResourceAsURL("add.gif")));
			this.createDatasets.setMnemonic(Resources.get(
					"suggestDataSetsMnemonic").charAt(0));
			this.createDatasets.addActionListener(this);

			// Remove all datasets.
			this.removeAllDatasets = new JMenuItem(Resources
					.get("removeAllDataSetsTitle"), new ImageIcon(Resources
					.getResourceAsURL("cut.gif")));
			this.removeAllDatasets.setMnemonic(Resources.get(
					"removeAllDataSetsMnemonic").charAt(0));
			this.removeAllDatasets.addActionListener(this);

			// Keyguessing.
			this.keyguessingSchema = new JCheckBoxMenuItem(Resources
					.get("enableKeyGuessingTitle"));
			this.keyguessingSchema.setMnemonic(Resources.get(
					"enableKeyGuessingMnemonic").charAt(0));
			this.keyguessingSchema.addActionListener(this);

			// Update schema.
			this.updateSchema = new JMenuItem(Resources
					.get("updateSchemaTitle"), new ImageIcon(Resources
					.getResourceAsURL("refresh.gif")));
			this.updateSchema.setMnemonic(Resources.get("updateSchemaMnemonic")
					.charAt(0));
			this.updateSchema.addActionListener(this);

			// Rename schema.
			this.renameSchema = new JMenuItem(Resources
					.get("renameSchemaTitle"));
			this.renameSchema.setMnemonic(Resources.get("renameSchemaMnemonic")
					.charAt(0));
			this.renameSchema.addActionListener(this);

			// Remove schema.
			this.removeSchema = new JMenuItem(Resources
					.get("removeSchemaTitle"), new ImageIcon(Resources
					.getResourceAsURL("cut.gif")));
			this.removeSchema.setMnemonic(Resources.get("removeSchemaMnemonic")
					.charAt(0));
			this.removeSchema.addActionListener(this);

			// Accept all changes.
			this.schemaAcceptAll = new JMenuItem(Resources
					.get("acceptChangesTitle"));
			this.schemaAcceptAll.setMnemonic(Resources.get(
					"acceptChangesMnemonic").charAt(0));
			this.schemaAcceptAll.addActionListener(this);

			// Reject all changes.
			this.schemaRejectAll = new JMenuItem(Resources
					.get("rejectChangesTitle"));
			this.schemaRejectAll.setMnemonic(Resources.get(
					"rejectChangesMnemonic").charAt(0));
			this.schemaRejectAll.addActionListener(this);

			// Invisible.
			this.invisibleDataset = new JCheckBoxMenuItem(Resources
					.get("invisibleDataSetTitle"));
			this.invisibleDataset.setMnemonic(Resources.get(
					"invisibleDataSetMnemonic").charAt(0));
			this.invisibleDataset.addActionListener(this);

			// Masked.
			this.maskedDataset = new JCheckBoxMenuItem(Resources
					.get("maskedDataSetTitle"));
			this.maskedDataset.setMnemonic(Resources.get(
					"maskedDataSetMnemonic").charAt(0));
			this.maskedDataset.addActionListener(this);

			// Partition dataset.
			this.partitionDSWizard = new JCheckBoxMenuItem(Resources
					.get("partitionWizardDataSetTitle"));
			this.partitionDSWizard.setMnemonic(Resources.get(
					"partitionWizardDataSetMnemonic").charAt(0));
			this.partitionDSWizard.addActionListener(this);

			// Update counts.
			this.updateAllPartitionCounts = new JMenuItem(Resources
					.get("updateAllPartitionCountsTitle"), new ImageIcon(
					Resources.getResourceAsURL("refresh.gif")));
			this.updateAllPartitionCounts.setMnemonic(Resources.get(
					"updateAllPartitionCountsMnemonic").charAt(0));
			this.updateAllPartitionCounts.addActionListener(this);

			// Update counts.
			this.updatePartitionCounts = new JMenuItem(Resources
					.get("updatePartitionCountsTitle"), new ImageIcon(Resources
					.getResourceAsURL("refresh.gif")));
			this.updatePartitionCounts.setMnemonic(Resources.get(
					"updatePartitionCountsMnemonic").charAt(0));
			this.updatePartitionCounts.addActionListener(this);

			// Explain dataset.
			this.explainDataset = new JMenuItem(Resources
					.get("explainDataSetTitle"), new ImageIcon(Resources
					.getResourceAsURL("help.gif")));
			this.explainDataset.setMnemonic(Resources.get(
					"explainDataSetMnemonic").charAt(0));
			this.explainDataset.addActionListener(this);

			// Save dataset DDL.
			this.saveDatasetDDL = new JMenuItem(Resources.get("saveDDLTitle"),
					new ImageIcon(Resources.getResourceAsURL("saveText.gif")));
			this.saveDatasetDDL.setMnemonic(Resources.get("saveDDLMnemonic")
					.charAt(0));
			this.saveDatasetDDL.addActionListener(this);

			// Rename dataset.
			this.renameDataset = new JMenuItem(Resources
					.get("renameDataSetTitle"));
			this.renameDataset.setMnemonic(Resources.get(
					"renameDataSetMnemonic").charAt(0));
			this.renameDataset.addActionListener(this);

			// Remove dataset.
			this.removeDataset = new JMenuItem(Resources
					.get("removeDataSetTitle"), new ImageIcon(Resources
					.getResourceAsURL("cut.gif")));
			this.removeDataset.setMnemonic(Resources.get(
					"removeDataSetMnemonic").charAt(0));
			this.removeDataset.addActionListener(this);

			// Extend dataset.
			this.extendDataset = new JMenuItem(Resources
					.get("suggestInvisibleDatasetsTitle"), new ImageIcon(
					Resources.getResourceAsURL("add.gif")));
			this.extendDataset.setMnemonic(Resources.get(
					"suggestInvisibleDatasetsMnemonic").charAt(0));
			this.extendDataset.addActionListener(this);

			// Convert dataset.
			this.convertPartitionTable = new JCheckBoxMenuItem(Resources
					.get("convertPartitionTableTitle"));
			this.convertPartitionTable.setMnemonic(Resources.get(
					"convertPartitionTableMnemonic").charAt(0));
			this.convertPartitionTable.addActionListener(this);

			// Accept all changes.
			this.datasetAcceptAll = new JMenuItem(Resources
					.get("acceptChangesTitle"));
			this.datasetAcceptAll.setMnemonic(Resources.get(
					"acceptChangesMnemonic").charAt(0));
			this.datasetAcceptAll.addActionListener(this);

			// Reject all changes.
			this.datasetRejectAll = new JMenuItem(Resources
					.get("rejectChangesTitle"));
			this.datasetRejectAll.setMnemonic(Resources.get(
					"rejectChangesMnemonic").charAt(0));
			this.datasetRejectAll.addActionListener(this);

			// Replicate.
			this.datasetReplicate = new JMenuItem(Resources
					.get("replicateDataSetTitle"));
			this.datasetReplicate.setMnemonic(Resources.get(
					"replicateDataSetMnemonic").charAt(0));
			this.datasetReplicate.addActionListener(this);

			// Make a submenu for the optimiser type.
			this.optimiseDatasetSubmenu = new JMenu(Resources
					.get("optimiserTitle"));
			this.optimiseDatasetSubmenu.setMnemonic(Resources.get(
					"optimiserMnemonic").charAt(0));
			final ButtonGroup optGroup = new ButtonGroup();
			// Loop through the map to create the submenu.
			for (final Iterator i = DataSetOptimiserType.getTypes().entrySet()
					.iterator(); i.hasNext();) {
				final Map.Entry entry = (Map.Entry) i.next();
				final String name = (String) entry.getKey();
				final DataSetOptimiserType value = (DataSetOptimiserType) entry
						.getValue();
				final JRadioButtonMenuItem opt = new JRadioButtonMenuItem(
						Resources.get("optimiser" + name + "Title"));
				opt.addActionListener(new ActionListener() {
					public void actionPerformed(final ActionEvent evt) {
						final DataSet ds = MartBuilderMenuBar.this
								.getMartBuilder().martTabSet
								.getSelectedMartTab().getDataSetTabSet()
								.getSelectedDataSet();
						MartBuilderMenuBar.this.getMartBuilder().martTabSet
								.getSelectedMartTab().getDataSetTabSet()
								.requestChangeOptimiserType(ds, value);
					}
				});
				optGroup.add(opt);
				this.optimiseDatasetSubmenu.add(opt);
			}
			this.optimiseDatasetSubmenu.addSeparator();
			this.indexOptimiser = new JCheckBoxMenuItem(Resources
					.get("indexOptimiserTitle"));
			this.indexOptimiser.setMnemonic(Resources.get(
					"indexOptimiserMnemonic").charAt(0));
			this.indexOptimiser.addActionListener(this);
			this.optimiseDatasetSubmenu.add(this.indexOptimiser);

			// Construct the file menu.
			final JMenu fileMenu = new JMenu(Resources.get("fileMenuTitle"));
			fileMenu.setMnemonic(Resources.get("fileMenuMnemonic").charAt(0));
			fileMenu.add(this.newMart);
			fileMenu.addSeparator();
			fileMenu.add(this.openMart);
			fileMenu.add(this.closeMart);
			fileMenu.addSeparator();
			fileMenu.add(this.saveMart);
			fileMenu.add(this.saveMartAs);
			// Add Quit option (only for non-Macs)
			if (exit != null) {
				fileMenu.addSeparator();
				fileMenu.add(exit);
			}
			final int firstRecentFileEntry = fileMenu.getMenuComponentCount();

			// Construct the edit menu.
			final JMenu editMenu = new JMenu(Resources.get("editMenuTitle"));
			editMenu.setMnemonic(Resources.get("editMenuMnemonic").charAt(0));

			// Construct the mart menu.
			final JMenu martMenu = new JMenu(Resources.get("martMenuTitle"));
			martMenu.setMnemonic(Resources.get("martMenuMnemonic").charAt(0));
			martMenu.add(this.saveDDL);
			martMenu.addSeparator();
			martMenu.add(this.nameCaseSubmenu);
			martMenu.addSeparator();
			martMenu.add(this.updateAllSchemas);
			martMenu.add(this.updateAllPartitionCounts);
			martMenu.add(this.removeAllSchemaPartitions);
			martMenu.addSeparator();
			martMenu.add(this.maskAllDataSets);
			martMenu.add(this.removeAllDatasets);
			martMenu.addSeparator();
			martMenu.add(this.monitorHost);

			// Construct the schema menu.
			final JMenu schemaMenu = new JMenu(Resources.get("schemaMenuTitle"));
			schemaMenu.setMnemonic(Resources.get("schemaMenuMnemonic")
					.charAt(0));
			schemaMenu.add(this.addSchema);
			schemaMenu.add(this.updateSchema);
			schemaMenu.addSeparator();
			schemaMenu.add(this.keyguessingSchema);
			schemaMenu.addSeparator();
			schemaMenu.add(this.renameSchema);
			schemaMenu.add(this.removeSchema);
			schemaMenu.addSeparator();
			schemaMenu.add(this.schemaAcceptAll);
			schemaMenu.add(this.schemaRejectAll);

			// Construct the dataset menu.
			final JMenu datasetMenu = new JMenu(Resources
					.get("datasetMenuTitle"));
			datasetMenu.setMnemonic(Resources.get("datasetMenuMnemonic")
					.charAt(0));
			datasetMenu.add(this.createDatasets);
			datasetMenu.addSeparator();
			datasetMenu.add(this.convertPartitionTable);
			datasetMenu.add(this.updatePartitionCounts);
			datasetMenu.addSeparator();
			datasetMenu.add(this.invisibleDataset);
			datasetMenu.add(this.maskedDataset);
			datasetMenu.add(this.partitionDSWizard);
			datasetMenu.add(this.optimiseDatasetSubmenu);
			datasetMenu.addSeparator();
			datasetMenu.add(this.saveDatasetDDL);
			datasetMenu.add(this.explainDataset);
			datasetMenu.addSeparator();
			datasetMenu.add(this.renameDataset);
			datasetMenu.add(this.removeDataset);
			datasetMenu.addSeparator();
			datasetMenu.add(this.datasetAcceptAll);
			datasetMenu.add(this.datasetRejectAll);
			datasetMenu.addSeparator();
			datasetMenu.add(this.datasetReplicate);
			datasetMenu.addSeparator();
			datasetMenu.add(this.extendDataset);

			// Add a listener which checks which options to enable each time the
			// menu is opened. This mean that if no mart is currently selected,
			// save and close will be disabled, and if the current mart is not
			// modified, save will be disabled, etc.
			martMenu.addMenuListener(new MenuListener() {
				public void menuCanceled(final MenuEvent e) {
				} // Interface requirement.

				public void menuDeselected(final MenuEvent e) {
				} // Interface requirement.

				public void menuSelected(final MenuEvent e) {
					final boolean hasMart = MartBuilderMenuBar.this
							.getMartBuilder().martTabSet.getSelectedMartTab() != null;
					MartBuilderMenuBar.this.saveDDL
							.setEnabled(hasMart
									&& MartBuilderMenuBar.this.getMartBuilder().martTabSet
											.getSelectedMartTab().getMart()
											.getDataSets().size() > 0);
				}
			});
			this.nameCaseSubmenu.addMenuListener(new MenuListener() {
				public void menuCanceled(final MenuEvent e) {
				} // Interface requirement.

				public void menuDeselected(final MenuEvent e) {
				} // Interface requirement.

				public void menuSelected(final MenuEvent e) {
					final MartTab martTab = MartBuilderMenuBar.this
							.getMartBuilder().martTabSet.getSelectedMartTab();
					if (martTab != null)
						switch (martTab.getMart().getCase()) {
						case Mart.USE_UPPER_CASE:
							MartBuilderMenuBar.this.nameCaseUpper
									.setSelected(true);
							break;
						case Mart.USE_LOWER_CASE:
							MartBuilderMenuBar.this.nameCaseLower
									.setSelected(true);
							break;
						default:
							MartBuilderMenuBar.this.nameCaseMixed
									.setSelected(true);
							break;
						}
				}
			});
			fileMenu.addMenuListener(new MenuListener() {
				public void menuCanceled(final MenuEvent e) {
				} // Interface requirement.

				public void menuDeselected(final MenuEvent e) {
				} // Interface requirement.

				public void menuSelected(final MenuEvent e) {
					final boolean hasMart = MartBuilderMenuBar.this
							.getMartBuilder().martTabSet.getSelectedMartTab() != null;
					MartBuilderMenuBar.this.saveMart
							.setEnabled(hasMart
									&& MartBuilderMenuBar.this.getMartBuilder().martTabSet
											.getModifiedStatus());
					MartBuilderMenuBar.this.saveMartAs.setEnabled(hasMart);
					MartBuilderMenuBar.this.closeMart.setEnabled(hasMart);
					MartBuilderMenuBar.this.nameCaseSubmenu.setEnabled(hasMart);
					// Wipe from the separator to the last non-separator/
					// non-numbered entry.
					// Then, insert after the separator a numbered list
					// of recent files, followed by another separator if
					// the list was not empty.
					while (fileMenu.getMenuComponentCount() > firstRecentFileEntry)
						fileMenu.remove(fileMenu
								.getMenuComponent(firstRecentFileEntry));
					final List names = Settings
							.getHistoryNamesForClass(MartTabSet.class);
					// We have to build this and reverse it separately
					// else the action of getting properties moves each
					// item to the top of the recently-accessed list, and
					// therefore flips the entire list at each menu request.
					final List newItems = new ArrayList();
					int position = names.size();
					for (final Iterator i = names.iterator(); i.hasNext(); position--) {
						final String name = (String) i.next();
						final File location = new File((String) Settings
								.getHistoryProperties(MartTabSet.class, name)
								.get("location"));
						final JMenuItem file = new JMenuItem(position + " "
								+ name);
						file.setMnemonic(("" + position).charAt(0));
						file.addActionListener(new ActionListener() {
							public void actionPerformed(final ActionEvent evt) {
								MartBuilderMenuBar.this.getMartBuilder().martTabSet
										.requestLoadMart(location);
							}
						});
						newItems.add(file);
					}
					if (newItems.size() > 0) {
						fileMenu.addSeparator();
						Collections.reverse(newItems);
						for (final Iterator i = newItems.iterator(); i
								.hasNext();)
							fileMenu.add((JMenuItem) i.next());
					}
				}
			});
			editMenu.addMenuListener(new MenuListener() {
				public void menuCanceled(final MenuEvent e) {
				} // Interface requirement.

				public void menuDeselected(final MenuEvent e) {
				} // Interface requirement.

				public void menuSelected(final MenuEvent e) {
					// Wipe the existing undo+redo entries.
					editMenu.removeAll();
					// Then, re-insert them based on the current undo manager
					// entries.
					final UndoManager undoManager = MartBuilderMenuBar.this
							.getMartBuilder().martTabSet.getUndoManager();
					final JMenuItem undo = new JMenuItem(undoManager
							.getUndoPresentationName());
					undo.setMnemonic(undoManager.getUndoPresentationName()
							.charAt(0));
					undo.addActionListener(new ActionListener() {
						public void actionPerformed(final ActionEvent e) {
							undoManager.undo();
						}
					});
					undo.setEnabled(undoManager.canUndo());
					editMenu.add(undo);
					final JMenuItem redo = new JMenuItem(undoManager
							.getRedoPresentationName());
					redo.setMnemonic(undoManager.getRedoPresentationName()
							.charAt(0));
					redo.addActionListener(new ActionListener() {
						public void actionPerformed(final ActionEvent e) {
							undoManager.redo();
						}
					});
					redo.setEnabled(undoManager.canRedo());
					editMenu.add(redo);
				}
			});
			martMenu.addMenuListener(new MenuListener() {
				public void menuCanceled(final MenuEvent e) {
				} // Interface requirement.

				public void menuDeselected(final MenuEvent e) {
				} // Interface requirement.

				public void menuSelected(final MenuEvent e) {
					final boolean hasMart = MartBuilderMenuBar.this
							.getMartBuilder().martTabSet.getSelectedMartTab() != null;
					MartBuilderMenuBar.this.updateAllSchemas
							.setEnabled(hasMart
									&& MartBuilderMenuBar.this.getMartBuilder().martTabSet
											.getSelectedMartTab().getMart()
											.getSchemas().size() > 0);
					MartBuilderMenuBar.this.removeAllDatasets
							.setEnabled(hasMart
									&& MartBuilderMenuBar.this.getMartBuilder().martTabSet
											.getSelectedMartTab().getMart()
											.getDataSets().size() > 0);
					MartBuilderMenuBar.this.removeAllSchemaPartitions
							.setEnabled(hasMart
									&& MartBuilderMenuBar.this.getMartBuilder().martTabSet
											.getSelectedMartTab().getMart()
											.getSchemas().size() > 0);
					MartBuilderMenuBar.this.maskAllDataSets
							.setEnabled(hasMart
									&& MartBuilderMenuBar.this.getMartBuilder().martTabSet
											.getSelectedMartTab().getMart()
											.getDataSets().size() > 0);
					MartBuilderMenuBar.this.updateAllPartitionCounts
							.setEnabled(hasMart
									&& MartBuilderMenuBar.this.getMartBuilder().martTabSet
											.getSelectedMartTab().getMart()
											.getPartitionTables().size() > 0);
				}
			});
			schemaMenu.addMenuListener(new MenuListener() {
				public void menuCanceled(final MenuEvent e) {
				} // Interface requirement.

				public void menuDeselected(final MenuEvent e) {
				} // Interface requirement.

				public void menuSelected(final MenuEvent e) {
					final boolean hasMart = MartBuilderMenuBar.this
							.getMartBuilder().martTabSet.getSelectedMartTab() != null;
					final Schema schema;
					if (hasMart)
						schema = MartBuilderMenuBar.this.getMartBuilder().martTabSet
								.getSelectedMartTab().getSchemaTabSet()
								.getSelectedSchema();
					else
						schema = null;
					MartBuilderMenuBar.this.addSchema.setEnabled(hasMart);
					MartBuilderMenuBar.this.keyguessingSchema
							.setEnabled(schema != null);
					MartBuilderMenuBar.this.keyguessingSchema
							.setSelected(schema != null
									&& schema.isKeyGuessing());
					MartBuilderMenuBar.this.updateSchema
							.setEnabled(schema != null);
					MartBuilderMenuBar.this.renameSchema
							.setEnabled(schema != null);
					MartBuilderMenuBar.this.removeSchema
							.setEnabled(schema != null);
					MartBuilderMenuBar.this.schemaAcceptAll
							.setEnabled(schema != null
									&& schema.isVisibleModified());
					MartBuilderMenuBar.this.schemaRejectAll
							.setEnabled(schema != null
									&& schema.isVisibleModified());
				}
			});
			datasetMenu.addMenuListener(new MenuListener() {
				public void menuCanceled(final MenuEvent e) {
				} // Interface requirement.

				public void menuDeselected(final MenuEvent e) {
				} // Interface requirement.

				public void menuSelected(final MenuEvent e) {
					final boolean hasMart = MartBuilderMenuBar.this
							.getMartBuilder().martTabSet.getSelectedMartTab() != null;
					MartBuilderMenuBar.this.createDatasets
							.setEnabled(hasMart
									&& MartBuilderMenuBar.this.getMartBuilder().martTabSet
											.getSelectedMartTab()
											.getSchemaTabSet()
											.getComponentCount() > 1);
					final DataSet ds;
					if (hasMart)
						ds = MartBuilderMenuBar.this.getMartBuilder().martTabSet
								.getSelectedMartTab().getDataSetTabSet()
								.getSelectedDataSet();
					else
						ds = null;
					MartBuilderMenuBar.this.invisibleDataset
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.invisibleDataset
							.setSelected(ds != null && ds.isInvisible());
					MartBuilderMenuBar.this.maskedDataset
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.maskedDataset
							.setSelected(ds != null && ds.isMasked());
					MartBuilderMenuBar.this.partitionDSWizard
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.partitionDSWizard
							.setSelected(ds != null
									&& ds.getPartitionTableApplication() != null);
					MartBuilderMenuBar.this.explainDataset
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.saveDatasetDDL
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.renameDataset
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.removeDataset
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.extendDataset
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.optimiseDatasetSubmenu
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.convertPartitionTable
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.convertPartitionTable
							.setSelected(ds != null && ds.isPartitionTable());
					MartBuilderMenuBar.this.updatePartitionCounts
							.setEnabled(ds != null && ds.isPartitionTable());
					MartBuilderMenuBar.this.datasetAcceptAll
							.setEnabled(ds != null && ds.isVisibleModified());
					MartBuilderMenuBar.this.datasetRejectAll
							.setEnabled(ds != null && ds.isVisibleModified());
					MartBuilderMenuBar.this.datasetReplicate
							.setEnabled(ds != null);
				}
			});
			this.optimiseDatasetSubmenu.addMenuListener(new MenuListener() {
				public void menuCanceled(final MenuEvent e) {
				} // Interface requirement.

				public void menuDeselected(final MenuEvent e) {
				} // Interface requirement.

				public void menuSelected(final MenuEvent e) {
					final DataSet ds;
					if (MartBuilderMenuBar.this.getMartBuilder().martTabSet
							.getSelectedMartTab() != null)
						ds = MartBuilderMenuBar.this.getMartBuilder().martTabSet
								.getSelectedMartTab().getDataSetTabSet()
								.getSelectedDataSet();
					else
						ds = null;
					MartBuilderMenuBar.this.indexOptimiser
							.setEnabled(ds != null);
					MartBuilderMenuBar.this.indexOptimiser
							.setSelected(ds != null && ds.isIndexOptimiser());
					int index = 0;
					for (final Iterator i = DataSetOptimiserType.getTypes()
							.values().iterator(); i.hasNext(); index++) {
						final DataSetOptimiserType value = (DataSetOptimiserType) i
								.next();
						if (ds.getDataSetOptimiserType().equals(value))
							((JMenuItem) MartBuilderMenuBar.this.optimiseDatasetSubmenu
									.getMenuComponent(index)).setSelected(true);
					}
				}
			});

			// Adds the menus to the menu bar.
			this.add(fileMenu);
			// this.add(editMenu); // FIXME Uncomment this when implemented.
			this.add(martMenu);
			this.add(schemaMenu);
			this.add(datasetMenu);
		}

		private MartBuilder getMartBuilder() {
			return (MartBuilder) this.getBioMartGUI();
		}

		public void actionPerformed(final ActionEvent e) {
			// Mart menu.
			if (e.getSource() == this.newMart)
				this.getMartBuilder().martTabSet.requestNewMart();
			else if (e.getSource() == this.openMart)
				this.getMartBuilder().martTabSet.requestLoadMart();
			else if (e.getSource() == this.saveMart)
				this.getMartBuilder().martTabSet.requestSaveMart();
			else if (e.getSource() == this.saveMartAs)
				this.getMartBuilder().martTabSet.requestSaveMartAs();
			else if (e.getSource() == this.closeMart)
				this.getMartBuilder().martTabSet.requestCloseMart();
			else if (e.getSource() == this.saveDDL)
				this.getMartBuilder().martTabSet.requestCreateDDL();
			else if (e.getSource() == this.monitorHost)
				this.getMartBuilder().martTabSet.requestMonitorRemoteHost();
			else if (e.getSource() == this.nameCaseLower)
				this.getMartBuilder().martTabSet
						.requestChangeNameCase(Mart.USE_LOWER_CASE);
			else if (e.getSource() == this.nameCaseUpper)
				this.getMartBuilder().martTabSet
						.requestChangeNameCase(Mart.USE_UPPER_CASE);
			else if (e.getSource() == this.nameCaseMixed)
				this.getMartBuilder().martTabSet
						.requestChangeNameCase(Mart.USE_MIXED_CASE);
			// Schema menu.
			else if (e.getSource() == this.addSchema)
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestAddSchema();
			else if (e.getSource() == this.updateAllSchemas)
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestSynchroniseAllSchemas();
			else if (e.getSource() == this.removeAllSchemaPartitions)
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestRemoveAllSchemaPartitions();
			else if (e.getSource() == this.maskAllDataSets)
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestMaskAllDataSets(true);
			else if (e.getSource() == this.createDatasets)
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestSuggestDataSets(null);
			else if (e.getSource() == this.removeAllDatasets)
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestRemoveAllDataSets();
			else if (e.getSource() == this.keyguessingSchema) {
				final Schema schema = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getSchemaTabSet()
						.getSelectedSchema();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestKeyGuessing(schema,
								this.keyguessingSchema.isSelected());
			} else if (e.getSource() == this.updateSchema) {
				final Schema schema = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getSchemaTabSet()
						.getSelectedSchema();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestModifySchema(schema);
			} else if (e.getSource() == this.renameSchema) {
				final Schema schema = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getSchemaTabSet()
						.getSelectedSchema();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestRenameSchema(schema);
			} else if (e.getSource() == this.removeSchema) {
				final Schema schema = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getSchemaTabSet()
						.getSelectedSchema();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestRemoveSchema(schema);
			} else if (e.getSource() == this.schemaAcceptAll) {
				final Schema schema = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getSchemaTabSet()
						.getSelectedSchema();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestAcceptAll(schema);
			} else if (e.getSource() == this.schemaRejectAll) {
				final Schema schema = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getSchemaTabSet()
						.getSelectedSchema();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getSchemaTabSet().requestRejectAll(schema);
			}
			// Dataset menu.
			else if (e.getSource() == this.invisibleDataset) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestInvisibleDataSet(ds,
								this.invisibleDataset.isSelected());
			} else if (e.getSource() == this.maskedDataset) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestMaskDataSet(ds,
								this.maskedDataset.isSelected());
			} else if (e.getSource() == this.partitionDSWizard) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestDataSetPartitionWizard(ds);
			} else if (e.getSource() == this.convertPartitionTable) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestConvertPartitionTable(ds);
			} else if (e.getSource() == this.updatePartitionCounts) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestUpdatePTCounts(
								ds.asPartitionTable());
			} else if (e.getSource() == this.updateAllPartitionCounts) {
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestUpdateAllPTCounts();
			} else if (e.getSource() == this.explainDataset) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestExplainDataSet(ds);
			} else if (e.getSource() == this.saveDatasetDDL) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestCreateDDL(ds);
			} else if (e.getSource() == this.renameDataset) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestRenameDataSet(ds);
			} else if (e.getSource() == this.removeDataset) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestRemoveDataSet(ds);
			} else if (e.getSource() == this.extendDataset) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestSuggestInvisibleDatasets(ds,
								ds.getMainTable());
			} else if (e.getSource() == this.indexOptimiser) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestIndexOptimiser(ds,
								this.indexOptimiser.isSelected());
			} else if (e.getSource() == this.datasetAcceptAll) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestAcceptAll(ds, null);
			} else if (e.getSource() == this.datasetRejectAll) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestRejectAll(ds, null);
			} else if (e.getSource() == this.datasetReplicate) {
				final DataSet ds = this.getMartBuilder().martTabSet
						.getSelectedMartTab().getDataSetTabSet()
						.getSelectedDataSet();
				this.getMartBuilder().martTabSet.getSelectedMartTab()
						.getDataSetTabSet().requestReplicateDataSet(ds);
			}
			// Others
			else
				super.actionPerformed(e);
		}
	}
}
