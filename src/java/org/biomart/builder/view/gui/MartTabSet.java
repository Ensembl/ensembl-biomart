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
import java.awt.CardLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;
import javax.swing.JToolBar;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.border.EmptyBorder;
import javax.swing.event.PopupMenuEvent;
import javax.swing.event.PopupMenuListener;
import javax.swing.filechooser.FileFilter;
import javax.swing.undo.AbstractUndoableEdit;
import javax.swing.undo.CannotRedoException;
import javax.swing.undo.CannotUndoException;
import javax.swing.undo.UndoManager;

import org.biomart.builder.controller.MartBuilderXML;
import org.biomart.builder.controller.MartConstructor.ConstructorRunnable;
import org.biomart.builder.exceptions.ConstructorException;
import org.biomart.builder.model.DataSet;
import org.biomart.builder.model.Mart;
import org.biomart.builder.model.Schema;
import org.biomart.builder.view.gui.diagrams.contexts.SchemaContext;
import org.biomart.builder.view.gui.dialogs.MartRunnerConnectionDialog;
import org.biomart.builder.view.gui.dialogs.MartRunnerMonitorDialog;
import org.biomart.builder.view.gui.dialogs.SaveDDLDialog;
import org.biomart.common.exceptions.TransactionException;
import org.biomart.common.resources.Resources;
import org.biomart.common.resources.Settings;
import org.biomart.common.utils.Transaction;
import org.biomart.common.utils.Transaction.TransactionEvent;
import org.biomart.common.utils.Transaction.TransactionListener;
import org.biomart.common.view.gui.LongProcess;
import org.biomart.common.view.gui.SwingWorker;
import org.biomart.common.view.gui.dialogs.ProgressDialog;
import org.biomart.common.view.gui.dialogs.StackTrace;

/**
 * Displays a set of tabs, one per mart currently loaded. Each tab keeps track
 * of the mart inside it, including all datasets and schemas.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.73 $, $Date: 2008-02-25 10:37:27 $, modified by
 *          $Author: rh4 $
 * @since 0.5
 */
public class MartTabSet extends JTabbedPane implements TransactionListener {
	private static final long serialVersionUID = 1;

	private MartBuilder martBuilder;

	// Mart hashcodes don't change, so it is safe to use a Map.
	private Map martModifiedStatus;

	// Mart hashcodes don't change, so it is safe to use a Map.
	private Map martXMLFile;

	private JFileChooser xmlFileChooser;

	private final UndoManager undoManager = new UndoManager();

	private final PropertyChangeListener updateListener = new PropertyChangeListener() {
		public void propertyChange(final PropertyChangeEvent evt) {
			final Mart mart = (Mart) evt.getSource();
			if (evt.getNewValue().equals(Boolean.TRUE)
					&& !Boolean.TRUE.equals(MartTabSet.this.martModifiedStatus
							.put(mart, Boolean.TRUE)))
				MartTabSet.this.updateMartTitle(mart);
		}
	};

	/**
	 * Creates a new set of tabs and associates them with a given MartBuilder
	 * GUI.
	 * 
	 * @param martBuilder
	 *            the GUI these tabs belong to.
	 */
	public MartTabSet(final MartBuilder martBuilder) {
		// Tabbed-pane stuff first.
		super();

		Transaction.addTransactionListener(this);

		// Create the file chooser for opening MartBuilder XML files.
		this.xmlFileChooser = new JFileChooser() {
			private static final long serialVersionUID = 1L;

			public File getSelectedFile() {
				File file = super.getSelectedFile();
				if (file != null && !file.exists()) {
					final String filename = file.getName();
					final String extension = Resources.get("xmlExtension");
					if (!filename.endsWith(extension)
							&& filename.indexOf('.') < 0)
						file = new File(file.getParentFile(), filename
								+ extension);
				}
				return file;
			}
		};
		this.xmlFileChooser.setFileFilter(new FileFilter() {
			// Accepts only files ending in ".xml".
			public boolean accept(final File f) {
				return f.isDirectory()
						|| f.getName().toLowerCase().endsWith(
								Resources.get("xmlExtension"));
			}

			public String getDescription() {
				return Resources.get("XMLFileFilterDescription");
			}
		});
		this.xmlFileChooser.setMultiSelectionEnabled(true);

		// Now set up and remember our variables.
		this.martBuilder = martBuilder;
		this.martModifiedStatus = new HashMap();
		this.martXMLFile = new HashMap();
	}

	/**
	 * Get the undo manager for this tabset.
	 * 
	 * @return the undo manager.
	 */
	public UndoManager getUndoManager() {
		return this.undoManager;
	}

	private Object stateBeforeTransaction = null;

	/**
	 * Save the application state for undo/redo and return that state as an
	 * object.
	 * 
	 * @return the state.
	 */
	public Object saveState() {
		try {
			final ByteArrayOutputStream so = new ByteArrayOutputStream();
			final ObjectOutputStream oo = new ObjectOutputStream(so);
			// FIXME At some point actually work out how to do this.
			oo.close();
			return so.toByteArray();
		} catch (final Exception e) {
			// Ignore.
			return null;
		}
	}

	/**
	 * Restore the application state from undo/redo.
	 * 
	 * @param state
	 *            the state to restore.
	 */
	public void restoreState(final Object state) {
		try {
			final ByteArrayInputStream si = new ByteArrayInputStream(
					(byte[]) state);
			final ObjectInputStream oi = new ObjectInputStream(si);
			// FIXME At some point actually work out how to do this.
			oi.close();
		} catch (final Exception e) {
			// Ignore.
		}
	}

	public boolean isDirectModified() {
		return false;
	}

	public boolean isVisibleModified() {
		return false;
	}

	public void setDirectModified(final boolean modified) {
		// Ignore.
	}

	public void setVisibleModified(final boolean modified) {
		// Ignore.
	}

	public void transactionEnded(final TransactionEvent evt)
			throws TransactionException {
		if (this.stateBeforeTransaction != null) {
			final Object preservedStateBeforeTransaction = this.stateBeforeTransaction;
			final Object stateAfterTransaction = this.saveState();
			this.undoManager.addEdit(new AbstractUndoableEdit() {
				private static final long serialVersionUID = 1L;

				public void redo() throws CannotRedoException {
					MartTabSet.this.restoreState(stateAfterTransaction);
				}

				public void undo() throws CannotUndoException {
					MartTabSet.this
							.restoreState(preservedStateBeforeTransaction);
				}

			});
		}
		this.stateBeforeTransaction = null;
	}

	public void transactionResetDirectModified() {
		// Ignore.
	}

	public void transactionResetVisibleModified() {
		// Ignore.
	}

	public void transactionStarted(final TransactionEvent evt) {
		this.stateBeforeTransaction = this.saveState();
	}

	/**
	 * Adds a new tab to the tabset representing a new mart.
	 * 
	 * @param mart
	 *            the mart to put in the tab.
	 * @param martXMLFile
	 *            the file the mart came from. May be <tt>null</tt> if the
	 *            mart is new.
	 */
	private synchronized void addMartTab(final Mart mart, final File martXMLFile) {
		this.martXMLFile.put(mart, martXMLFile);
		this.martModifiedStatus.put(mart, Boolean.FALSE);
		final MartTab martTab = new MartTab(this, mart);
		final String martTabName = this.suggestTabName(mart, true);
		this.addTab(martTabName, martTab);

		// Select the tab we just created.
		this.setSelectedIndex(this.getTabCount() - 1);

		// Within that tab, select the all-schemas and all-datasets tabs.
		martTab.getDataSetTabSet().setSelectedIndex(0);
		martTab.getSchemaTabSet().setSelectedIndex(0);

		// Listen to modified changes.
		mart.addPropertyChangeListener("directModified", this.updateListener);
	}

	private void updateMartTitle(final Mart mart) {
		// Update the tab title to indicate modification status.
		this.setTitleAt(this.getSelectedIndex(), this
				.suggestTabName(mart, true));
	}

	/**
	 * Construct a context menu for a given mart tab. This is the context menu
	 * on the tab itself, not it's contents.
	 * 
	 * @return the popup menu.
	 */
	private JPopupMenu getMartTabContextMenu() {
		final JPopupMenu contextMenu = new JPopupMenu();

		// The close option closes the selected mart, confirming first
		// that it's OK to do so.
		final JMenuItem close = new JMenuItem(Resources.get("closeMartTitle"));
		close.setMnemonic(Resources.get("closeMartMnemonic").charAt(0));
		close.addActionListener(new ActionListener() {
			public void actionPerformed(final ActionEvent evt) {
				MartTabSet.this.requestCloseMart();
			}
		});
		contextMenu.add(close);

		// Return the menu.
		return contextMenu;
	}

	/**
	 * Suggests a tab name based on a mart's filename. If the mart has no
	 * filename, "unsaved" is used.
	 */
	private String suggestTabName(final Mart mart, final boolean includeModified) {

		// Start with "unsaved".
		String basename = Resources.get("unsavedMart");

		// See if this mart came from a file. If so, use the filename.
		final File filename = (File) this.martXMLFile.get(mart);
		if (filename != null)
			basename = filename.getName();

		// If it's modified, append a "*" to make it obvious.
		return basename
				+ (includeModified
						&& this.martModifiedStatus.get(mart).equals(
								Boolean.TRUE) ? " *" : "");
	}

	protected void processMouseEvent(final MouseEvent evt) {
		boolean eventProcessed = false;
		// Is it a right-click?
		if (evt.isPopupTrigger()) {
			// Where was the click?
			final int selectedIndex = this.indexAtLocation(evt.getX(), evt
					.getY());
			// Did we actually click on any tab?
			if (selectedIndex >= 0) {
				// Select that tab.
				this.setSelectedIndex(selectedIndex);
				// Pop up the context menu for it.
				this.getMartTabContextMenu().show(this, evt.getX(), evt.getY());
				// We've processed the mouse event.
				eventProcessed = true;
			}
		}
		// Pass it on up if we're not interested.
		if (!eventProcessed)
			super.processMouseEvent(evt);
	}

	/**
	 * On a request to close all marts, check that none of them are modified. If
	 * any of them are, ask the user if they're sure they want to close them
	 * all.
	 * 
	 * @return <tt>true</tt> if its OK to close all the marts, <tt>false</tt>
	 *         if not.
	 */
	public boolean requestCloseAllMarts() {
		for (final Iterator i = this.martModifiedStatus.values().iterator(); i
				.hasNext();)
			if (i.next().equals(Boolean.TRUE)) {
				final int choice = JOptionPane.showConfirmDialog(null,
						Resources.get("okToCloseAll"), Resources
								.get("questionTitle"),
						JOptionPane.YES_NO_OPTION);
				return choice == JOptionPane.YES_OPTION;
			}
		return true;
	}

	/**
	 * On a request to close the current mart, check that it is not modified. If
	 * it is, ask the user if they're sure they want to close it. If they say
	 * yes, or if it is not modified, close it.
	 */
	public void requestCloseMart() {

		// If nothing is selected, forget it, they can't close!
		if (this.getSelectedMartTab() == null)
			return;

		// Work out the current selected mart.
		final MartTab currentMartTab = this.getSelectedMartTab();
		final Mart currentMart = currentMartTab.getMart();

		// Is it modified? If so, ask user for confirmation.
		boolean canClose = true;
		if (this.martModifiedStatus.get(currentMart).equals(Boolean.TRUE)) {
			// Modified, so must confirm action first.
			final int choice = JOptionPane.showConfirmDialog(null, Resources
					.get("okToClose"), Resources.get("questionTitle"),
					JOptionPane.YES_NO_OPTION);
			canClose = choice == JOptionPane.YES_OPTION;
		}

		// If it's OK to close, remove the tab and the mart itself.
		if (canClose) {
			// Remove the tab.
			this
					.removeTabAt(this.indexOfComponent(this
							.getSelectedComponent()));

			// Remove the mart from the modified map.
			this.martModifiedStatus.remove(currentMart);

			// Remove the XML file the mart came from from the file map.
			this.martXMLFile.remove(currentMart);
		}
	}

	/**
	 * Retrieves the parent MartBuilder GUI.
	 * 
	 * @return the parent mart builder GUI.
	 */
	public MartBuilder getMartBuilder() {
		return this.martBuilder;
	}

	/**
	 * Gets whether any currently open mart is modified.
	 * 
	 * @return <tt>true</tt> if any of them are, <tt>false</tt> if not.
	 */
	public boolean getModifiedStatus() {
		return this.martModifiedStatus.values().contains(Boolean.TRUE);
	}

	/**
	 * Works out which mart tab is selected, and return it.
	 * 
	 * @return the currently selected mart tab, or <tt>null</tt> if none is
	 *         selected.
	 */
	public MartTab getSelectedMartTab() {
		return (MartTab) this.getSelectedComponent();
	}

	/**
	 * Loads a schema from a user-specified file(s), by popping up a dialog
	 * allowing them to choose the file(s). If they choose a file, it is loaded
	 * and parsed and a new tab is added representing its contents.
	 */
	public void requestLoadMart() {
		// Open the file chooser.
		final String currentDir = Settings.getProperty("currentOpenDir");
		this.xmlFileChooser.setCurrentDirectory(currentDir == null ? null
				: new File(currentDir));
		if (this.xmlFileChooser.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
			// Update the load dialog.
			Settings.setProperty("currentOpenDir", this.xmlFileChooser
					.getCurrentDirectory().getPath());

			// Find out which files they selected.
			final File[] loadFiles = this.xmlFileChooser.getSelectedFiles();

			for (int i = 0; i < loadFiles.length; i++)
				this.requestLoadMart(loadFiles[i]);
		}
	}

	/**
	 * Loads a schema from a user-specified file(s), by popping up a dialog
	 * allowing them to choose the file(s). If they choose a file, it is loaded
	 * and parsed and a new tab is added representing its contents.
	 * 
	 * @param file
	 *            the file to load. If it does not exist, this delegates to the
	 *            normal load method.
	 */
	public void requestLoadMart(final File file) {
		// Open the file chooser.
		// In the background, load them in turn.
		new LongProcess() {
			public void run() throws Exception {
				// Do we need to close the existing unsaved
				// unmodified default tab?
				MartTab defaultTab = MartTabSet.this.getSelectedMartTab();
				final int defaultIndex = MartTabSet.this.getSelectedIndex();
				if (MartTabSet.this.getComponentCount() > 1
						|| defaultTab != null
						&& !MartTabSet.this.getTitleAt(defaultIndex).equals(
								Resources.get("unsavedMart")))
					defaultTab = null;

				// Load the files.
				Transaction.start(false);
				final Mart mart;
				try {
					mart = MartBuilderXML.load(file);
				} finally {
					Transaction.end();
				}

				MartTabSet.this.martModifiedStatus.put(mart, Boolean.FALSE);
				MartTabSet.this.addMartTab(mart, file);
				// Save XML filename in history of accessed
				// files.
				final Properties history = new Properties();
				history.setProperty("location", file.getPath());
				Settings.saveHistoryProperties(MartTabSet.class,
						MartTabSet.this.suggestTabName(mart, false), history);

				// Finally, remove the unsaved default tab if
				// we need to.
				if (defaultTab != null) {
					// Remove the tab.
					MartTabSet.this.removeTabAt(defaultIndex);

					// Remove the mart from the modified map.
					MartTabSet.this.martModifiedStatus.remove(defaultTab
							.getMart());

					// Remove the XML file the mart came from from
					// the file map.
					MartTabSet.this.martXMLFile.remove(defaultTab.getMart());
				}
			}
		}.start();
	}

	/**
	 * On a request to create DDL for the current mart, open the DDL creation
	 * window with all the datasets for this mart selected.
	 */
	public void requestCreateDDL() {
		this.requestSaveDDLDialog(SaveDDLDialog.VIEW_DDL);
	}

	/**
	 * On a request to run DDL for the current mart, open the DDL creation
	 * window with all the datasets for this mart selected and MartRunner option
	 * selected.
	 */
	public void requestRunDDL() {
		this.requestSaveDDLDialog(SaveDDLDialog.RUN_DDL);
	}

	private void requestSaveDDLDialog(final String generateOption) {

		// If nothing is selected, forget it, they can't close!
		if (this.getSelectedMartTab() == null)
			return;

		// Work out the current selected mart.
		final MartTab currentMartTab = this.getSelectedMartTab();

		// If the mart has no datasets, ignore the request.
		final Mart mart = currentMartTab.getMart();
		final Collection datasets = new ArrayList(mart.getDataSets().values());
		// Remove partition table datasets from the list.
		// Also remove masked datasets.
		for (final Iterator i = datasets.iterator(); i.hasNext();) {
			final DataSet ds = (DataSet) i.next();
			if (ds.isPartitionTable() || ds.isMasked())
				i.remove();
		}
		if (datasets.size() == 0)
			JOptionPane.showMessageDialog(null, Resources
					.get("noDatasetsToGenerate"),
					Resources.get("messageTitle"),
					JOptionPane.INFORMATION_MESSAGE);
		else
			// Open the DDL creation dialog and let it do it's stuff.
			(new SaveDDLDialog(currentMartTab, datasets, currentMartTab
					.getPartitionViewSelection() == null ? currentMartTab
					.getAllSchemaPrefixes() : Collections
					.singleton(currentMartTab.getPartitionViewSelection()),
					generateOption)).setVisible(true);
	}

	/**
	 * Sets the output database on the currently selected mart.
	 * 
	 * @param outputDatabase
	 *            the new output database.
	 */
	public void requestSetOutputDatabase(final String outputDatabase) {
		Transaction.start(false);
		this.getSelectedMartTab().getMart().setOutputDatabase(outputDatabase);
		Transaction.end();
	}

	/**
	 * Sets the output schema on the currently selected mart.
	 * 
	 * @param outputSchema
	 *            the new output schema.
	 */
	public void requestSetOutputSchema(final String outputSchema) {
		Transaction.start(false);
		this.getSelectedMartTab().getMart().setOutputSchema(outputSchema);
		Transaction.end();
	}

	/**
	 * Sets the output host on the currently selected mart.
	 * 
	 * @param host
	 *            the new output host.
	 */
	public void requestSetOutputHost(final String host) {
		Transaction.start(false);
		this.getSelectedMartTab().getMart().setOutputHost(host);
		Transaction.end();
	}

	/**
	 * Sets the output port on the currently selected mart.
	 * 
	 * @param port
	 *            the new output port.
	 */
	public void requestSetOutputPort(final String port) {
		Transaction.start(false);
		this.getSelectedMartTab().getMart().setOutputPort(port);
		Transaction.end();
	}

	/**
	 * Sets the override JDBC host on the currently selected mart.
	 * 
	 * @param host
	 *            the new host.
	 */
	public void requestSetOverrideHost(final String host) {
		Transaction.start(false);
		this.getSelectedMartTab().getMart().setOverrideHost(host);
		Transaction.end();
	}

	/**
	 * Sets the override JDBC port on the currently selected mart.
	 * 
	 * @param port
	 *            the new port.
	 */
	public void requestSetOverridePort(final String port) {
		Transaction.start(false);
		this.getSelectedMartTab().getMart().setOverridePort(port);
		Transaction.end();
	}

	/**
	 * Runs the given {@link ConstructorRunnable} and monitors it's progress.
	 * 
	 * @param constructor
	 *            the constructor that will build a mart.
	 */
	public void requestMonitorConstructorRunnable(
			final ConstructorRunnable constructor) {
		// Create a progress monitor.
		final ProgressDialog progressMonitor = new ProgressDialog(this, 0, 100,
				true);
		progressMonitor.setVisible(true);

		// Start the construction in a thread. It does not need to be
		// Swing-thread-safe because it will never access the GUI. All
		// GUI interaction is done through the Timer below.
		final SwingWorker worker = new SwingWorker() {
			public Object construct() {
				constructor.run();
				return null;
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
						// Did the job complete yet?
						if (constructor.isAlive()) {
							if (progressMonitor.isCanceled())
								// Stop the thread if required.
								constructor.cancel();
							// If not, update the progress report.
							progressMonitor.setProgress(constructor
									.getPercentComplete());
						} else {
							// If it completed, close the task and tidy up.
							// Stop the timer.
							timer.stop();
							// Close the progress dialog.
							progressMonitor.setVisible(false);
							progressMonitor.dispose();
							// If it failed, show the exception.
							final Exception failure = constructor
									.getFailureException();
							// By singling out ConstructorException we can show
							// users useful messages straight away.
							if (failure != null)
								StackTrace
										.showStackTrace(failure instanceof ConstructorException ? failure
												: new ConstructorException(
														Resources
																.get("martConstructionFailed"),
														failure));
							// Inform user of success, if it succeeded.
							else
								JOptionPane.showMessageDialog(null, Resources
										.get("martConstructionComplete"),
										Resources.get("messageTitle"),
										JOptionPane.INFORMATION_MESSAGE);
						}
					}
				});
			}
		});

		// Start the timer.
		timer.start();
	}

	/**
	 * Ask the user which remote host to monitor, then open the dialog box that
	 * monitors that host.
	 */
	public void requestMonitorRemoteHost() {
		final MartRunnerConnectionDialog d = new MartRunnerConnectionDialog(
				this.getSelectedMartTab() == null ? null : this
						.getSelectedMartTab().getMart());
		d.setVisible(true);
		// Cancelled by user?
		if (d.getHost() == null)
			return;
		else
			this.requestMonitorRemoteHost(d.getHost(), d.getPort());
		d.dispose();
	}

	/**
	 * Opens the dialog box to monitor the specified remote host and port.
	 * 
	 * @param host
	 *            the host to connect to.
	 * @param port
	 *            the port the host is listening on.
	 */
	public void requestMonitorRemoteHost(final String host, final String port) {
		Transaction.start(false);
		MartTabSet.this.getSelectedMartTab().getMart().setOutputHost(host);
		MartTabSet.this.getSelectedMartTab().getMart().setOutputPort(port);
		Transaction.end();
		// Open remote host monitor dialog.
		MartRunnerMonitorDialog.monitor(host, port);
	}

	/**
	 * Creates a new, empty mart and adds a tab for it.
	 */
	public void requestNewMart() {
		this.addMartTab(new Mart(), null);
	}

	/**
	 * Saves the current mart to the file currently defined for it.
	 */
	public void requestSaveMart() {
		// If nothing selected, refuse.
		if (this.getSelectedMartTab() == null)
			return;

		// Work out if we already have a file for this mart. If not,
		// do a save-as instead.
		final Mart currentMart = this.getSelectedMartTab().getMart();
		if (this.martXMLFile.get(currentMart) == null)
			this.requestSaveMartAs();
		else
			// Save it in the background to the existing file.
			new LongProcess() {
				public void run() throws Exception {
					// Save it.
					MartBuilderXML
							.save(currentMart,
									(File) MartTabSet.this.martXMLFile
											.get(currentMart));
					// We're not modified any more!
					MartTabSet.this.martModifiedStatus.put(currentMart,
							Boolean.FALSE);
					MartTabSet.this.updateMartTitle(currentMart);
				}
			}.start();
	}

	/**
	 * Saves the mart to a user-specified file, by popping up a file-chooser.
	 */
	public void requestSaveMartAs() {
		// If nothing selected at present, refuse.
		if (this.getSelectedMartTab() == null)
			return;

		// Work out the current mart.
		final Mart currentMart = this.getSelectedMartTab().getMart();

		// Show a file chooser. If the user didn't cancel it, process the
		// response.
		final String currentDir = Settings.getProperty("currentSaveDir");
		this.xmlFileChooser.setCurrentDirectory(currentDir == null ? null
				: new File(currentDir));
		if (this.xmlFileChooser.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
			Settings.setProperty("currentSaveDir", this.xmlFileChooser
					.getCurrentDirectory().getPath());

			// Find out the file the user chose.
			final File saveAsFile = this.xmlFileChooser.getSelectedFile();

			// Skip the rest if they cancelled the save box.
			if (saveAsFile == null)
				return;

			// Save it, and save the reference to the XML file for later.
			this.martXMLFile.put(currentMart, saveAsFile);
			this.requestSaveMart();
			// Save XML filename in history of accessed files.
			final Properties history = new Properties();
			history.setProperty("location", saveAsFile.getPath());
			Settings.saveHistoryProperties(MartTabSet.class, this
					.suggestTabName(currentMart, false), history);
		}
	}

	/**
	 * Change the name case for the selected mart.
	 * 
	 * @param nameCase
	 *            the new case.
	 */
	public void requestChangeNameCase(final int nameCase) {
		new LongProcess() {
			public void run() {
				Transaction.start(false);
				MartTabSet.this.getSelectedMartTab().getMart()
						.setCase(nameCase);
				Transaction.end();
			}
		}.start();
	}

	/**
	 * This represents a single mart XML file as a tab in the top-level tabbed
	 * pane set.
	 */
	public class MartTab extends JPanel {
		private static final long serialVersionUID = 1;

		private JRadioButton datasetButton;

		private DataSetTabSet datasetTabSet;

		private JPanel displayArea;

		private Mart mart;

		private MartTabSet martTabSet;

		private JRadioButton schemaButton;

		private SchemaTabSet schemaTabSet;

		private String partitionViewSelection = null;

		private final ArrayList listeners = new ArrayList();

		/**
		 * Constructs a new tab in the tabbed pane that represents the given
		 * mart.
		 * 
		 * @param martTabSet
		 *            the tabbed pane set we are adding ourselves to.
		 * @param mart
		 *            the mart we represent.
		 */
		public MartTab(final MartTabSet martTabSet, final Mart mart) {
			// Set up our layout.
			super(new BorderLayout());

			// Remember which mart and tabset we are working with.
			this.martTabSet = martTabSet;
			this.mart = mart;

			// Create display part of the tab. The display area consists of
			// two cards - one for the schema editor, one for the dataset
			// editor. Buttons in another area switch between the cards.
			this.displayArea = new JPanel(new CardLayout());

			// Create panel which contains the buttons.
			final JToolBar buttonsPanel = new JToolBar(Resources
					.get("martTabToolbarTitle"));

			// Add the Biomart logo to the buttons panel.
			final JLabel logo = new JLabel(new ImageIcon(Resources
					.getResourceAsURL("biomart-logo.gif")));
			logo.setBorder(new EmptyBorder(4, 4, 4, 4));
			buttonsPanel.add(logo);

			// Create the Run DDL button.
			final JButton runDDL = new JButton(Resources.get("runDDLButton"),
					new ImageIcon(Resources.getResourceAsURL("run.gif")));
			runDDL.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					MartTab.this.martTabSet.requestRunDDL();
				}
			});
			buttonsPanel.add(runDDL);

			// Create the schema tabset.
			this.schemaTabSet = new SchemaTabSet(this);

			// Create the button that selects the window card. It reattaches
			// it every time in case it has been attached somewhere else
			// whilst we weren't looking.
			this.schemaButton = new JRadioButton(Resources
					.get("schemaEditorButtonName"));
			this.schemaButton.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					if (e.getSource() == MartTab.this.schemaButton) {
						final SchemaContext context = new SchemaContext(
								MartTab.this);
						MartTab.this.schemaTabSet.setDiagramContext(context);
						MartTab.this.displayArea.add(MartTab.this.schemaTabSet,
								"SCHEMA_EDITOR_CARD");
						final CardLayout cards = (CardLayout) MartTab.this.displayArea
								.getLayout();
						new LongProcess() {
							public void run() throws Exception {
								SwingUtilities.invokeAndWait(new Runnable() {
									public void run() {
										cards.show(MartTab.this.displayArea,
												"SCHEMA_EDITOR_CARD");
									}
								});
							}
						}.start();
					}
				}
			});
			buttonsPanel.add(this.schemaButton);

			// Create the dataset tabset.
			this.datasetTabSet = new DataSetTabSet(this);

			// Dataset card.
			this.displayArea.add(this.datasetTabSet, "DATASET_EDITOR_CARD");

			// Create the button that selects the dataset card.
			this.datasetButton = new JRadioButton(Resources
					.get("datasetEditorButtonName"));
			this.datasetButton.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					if (e.getSource() == MartTab.this.datasetButton) {
						final CardLayout cards = (CardLayout) MartTab.this.displayArea
								.getLayout();
						new LongProcess() {
							public void run() throws Exception {
								SwingUtilities.invokeAndWait(new Runnable() {
									public void run() {
										cards.show(MartTab.this.displayArea,
												"DATASET_EDITOR_CARD");
									}
								});
							}
						}.start();
					}
				}
			});
			buttonsPanel.add(this.datasetButton);

			// Make buttons mutually exclusive.
			final ButtonGroup buttons = new ButtonGroup();
			buttons.add(this.schemaButton);
			buttons.add(this.datasetButton);

			// Drop-down menu to select current partition.
			final DefaultComboBoxModel partitionModel = new DefaultComboBoxModel();
			final JComboBox partitions = new JComboBox(partitionModel);
			partitions.setBorder(new EmptyBorder(4, 4, 4, 4));
			partitionModel.addElement(Resources.get("martTabAllPartitions"));
			partitions.setSelectedIndex(0);
			buttonsPanel.add(partitions);
			// On-click, before open, update contents to match
			// partitions of currently selected schema. Include an "All
			// partitions" option. If no schema currently selected,
			// show partitions from all.
			partitions.addPopupMenuListener(new PopupMenuListener() {
				public void popupMenuCanceled(PopupMenuEvent e) {
				}

				public void popupMenuWillBecomeInvisible(PopupMenuEvent e) {
				}

				public void popupMenuWillBecomeVisible(PopupMenuEvent e) {
					int items = partitionModel.getSize();
					final Object selection = partitionModel.getSelectedItem();

					partitionModel.removeAllElements();
					partitionModel.addElement(Resources
							.get("martTabAllPartitions"));
					for (final Iterator i = MartTab.this.getAllSchemaPrefixes()
							.iterator(); i.hasNext();)
						partitionModel.addElement(i.next());

					partitionModel.setSelectedItem(selection);

					if (items != partitionModel.getSize()) {
						partitions.hidePopup();
						partitions.showPopup();
					}
				}
			});
			// When drop-down changes, update local variable.
			partitions.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					if (partitions.getSelectedIndex() == 0)
						MartTab.this.setPartitionViewSelection(null);
					else
						MartTab.this
								.setPartitionViewSelection((String) partitions
										.getSelectedItem());
				}
			});

			// Add the buttons panel, and the display area containing the cards,
			// to the panel.
			this.add(buttonsPanel, BorderLayout.NORTH);
			this.add(this.displayArea, BorderLayout.CENTER);

			// Select the default button (which shows the schema card).
			this.selectSchemaEditor();
		}

		/**
		 * Return all possible schema prefixes, except the all-prefixes one.
		 * 
		 * @return the collection.
		 */
		public Collection getAllSchemaPrefixes() {
			// Rebuild list from all schemas.
			final Set sortedPrefixes = new TreeSet();
			for (final Iterator i = mart.getSchemas().values().iterator(); i
					.hasNext();)
				sortedPrefixes.addAll(((Schema) i.next())
						.getReferencedPartitions());
			return sortedPrefixes;
		}

		/**
		 * Set which schema partition (if any) the user has selected.
		 * 
		 * @param partitionViewSelection
		 *            the schema partition prefix. <tt>null</tt> if they have
		 *            selected All Partitions.
		 */
		public void setPartitionViewSelection(
				final String partitionViewSelection) {
			if (partitionViewSelection == this.partitionViewSelection
					|| (this.partitionViewSelection != null && this.partitionViewSelection
							.equals(partitionViewSelection)))
				return;
			this.partitionViewSelection = partitionViewSelection;
			for (final Iterator i = this.listeners.iterator(); i.hasNext();) {
				final WeakReference ref = (WeakReference) i.next();
				final PartitionViewSelectionListener listener = (PartitionViewSelectionListener) ref
						.get();
				if (listener == null)
					i.remove();
				else
					listener.partitionViewSelectionChanged();
			}
		}

		/**
		 * Add a listener to receive events when the partition view selection
		 * dropdown changes. The listener will be stored with a
		 * {@link WeakReference} and so will be dropped if it falls out of
		 * scope.
		 * 
		 * @param listener
		 *            the listener to register.
		 */
		public void addPartitionViewSelectionListener(
				final PartitionViewSelectionListener listener) {
			this.listeners.add(new WeakReference(listener));
		}

		/**
		 * Get which schema partition (if any) the user has selected.
		 * 
		 * @return the schema partition prefix. <tt>null</tt> if they have
		 *         selected All Partitions.
		 */
		public String getPartitionViewSelection() {
			return this.partitionViewSelection;
		}

		/**
		 * Obtain the tabbed pane set inside this one that represents the
		 * datasets in this mart.
		 * 
		 * @return the tabbed pane set showing the datasets in this mart.
		 */
		public DataSetTabSet getDataSetTabSet() {
			return this.datasetTabSet;
		}

		/**
		 * Find out what mart we represent.
		 * 
		 * @return the mart we represent.
		 */
		public Mart getMart() {
			return this.mart;
		}

		/**
		 * Find out what tabbed pane set we belong to.
		 * 
		 * @return the tabbed pane set we belong to.
		 */
		public MartTabSet getMartTabSet() {
			return this.martTabSet;
		}

		/**
		 * Obtain the tabbed pane set inside this one that represents the
		 * schemas in this mart.
		 * 
		 * @return the tabbed pane set showing the schemas in this mart.
		 */
		public SchemaTabSet getSchemaTabSet() {
			return this.schemaTabSet;
		}

		/**
		 * Fakes a click on the dataset editor radio button.
		 */
		public void selectDataSetEditor() {
			// May get called before button has been created.
			if (this.datasetButton != null && !this.datasetButton.isSelected())
				this.datasetButton.doClick();
		}

		/**
		 * Fakes a click on the schema editor radio button.
		 */
		public void selectSchemaEditor() {
			// May get called before button has been created.
			if (this.schemaButton != null && !this.schemaButton.isSelected())
				this.schemaButton.doClick();
		}
	}

	/**
	 * A listener that is called when the user changes the partition view
	 * selection dropdown.
	 */
	public interface PartitionViewSelectionListener {

		/**
		 * This method is called when the partition view selection dropdown is
		 * changed.
		 */
		public void partitionViewSelectionChanged();
	}
}