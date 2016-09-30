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
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFormattedTextField;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.ListCellRenderer;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.WindowConstants;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.ExpandVetoException;
import javax.swing.tree.TreeCellRenderer;
import javax.swing.tree.TreeNode;
import javax.swing.tree.TreePath;

import org.biomart.common.resources.Log;
import org.biomart.common.resources.Resources;
import org.biomart.common.view.gui.DraggableJTree;
import org.biomart.common.view.gui.LongProcess;
import org.biomart.common.view.gui.dialogs.StackTrace;
import org.biomart.runner.controller.MartRunnerProtocol.Client;
import org.biomart.runner.exceptions.ProtocolException;
import org.biomart.runner.model.JobPlan;
import org.biomart.runner.model.JobStatus;
import org.biomart.runner.model.JobPlan.JobPlanAction;
import org.biomart.runner.model.JobPlan.JobPlanSection;

/**
 * This dialog monitors and interacts with SQL being run on a remote host.
 * 
 * @author Richard Holland <holland@ebi.ac.uk>
 * @version $Revision: 1.31 $, $Date: 2007-12-20 15:38:46 $, modified by
 *          $Author: rh4 $
 * @since 0.6
 */
public class MartRunnerMonitorDialog extends JFrame {
	private static final long serialVersionUID = 1;

	private static final int DEFAULT_REFRESH = 60; // seconds

	private static final int MIN_REFRESH = 5; // seconds

	private static final Font PLAIN_FONT = Font.decode("SansSerif-PLAIN-12");

	private static final Font ITALIC_FONT = Font.decode("SansSerif-ITALIC-12");

	private static final Font BOLD_FONT = Font.decode("SansSerif-BOLD-12");

	private static final Font BOLD_ITALIC_FONT = Font
			.decode("SansSerif-BOLDITALIC-12");

	private static final Color PALE_BLUE = Color.decode("0xEEEEFF");

	private static final Color PALE_GREEN = Color.decode("0xEEFFEE");

	private static final Map STATUS_COLOR_MAP = new HashMap();

	private static final Map STATUS_FONT_MAP = new HashMap();

	private final JButton refreshJobList;

	private boolean listRefreshing = false;

	private final String host;

	private final String port;

	static {
		// Colours.
		MartRunnerMonitorDialog.STATUS_COLOR_MAP.put(JobStatus.NOT_QUEUED,
				Color.BLACK);
		MartRunnerMonitorDialog.STATUS_COLOR_MAP.put(JobStatus.INCOMPLETE,
				Color.CYAN);
		MartRunnerMonitorDialog.STATUS_COLOR_MAP.put(JobStatus.QUEUED,
				Color.MAGENTA);
		MartRunnerMonitorDialog.STATUS_COLOR_MAP.put(JobStatus.FAILED,
				Color.RED);
		MartRunnerMonitorDialog.STATUS_COLOR_MAP.put(JobStatus.RUNNING,
				Color.BLUE);
		MartRunnerMonitorDialog.STATUS_COLOR_MAP.put(JobStatus.STOPPED,
				Color.ORANGE);
		MartRunnerMonitorDialog.STATUS_COLOR_MAP.put(JobStatus.COMPLETED,
				Color.GREEN);
		MartRunnerMonitorDialog.STATUS_COLOR_MAP.put(JobStatus.UNKNOWN,
				Color.LIGHT_GRAY);
		// Fonts
		MartRunnerMonitorDialog.STATUS_FONT_MAP.put(JobStatus.NOT_QUEUED,
				MartRunnerMonitorDialog.PLAIN_FONT);
		MartRunnerMonitorDialog.STATUS_FONT_MAP.put(JobStatus.INCOMPLETE,
				MartRunnerMonitorDialog.ITALIC_FONT);
		MartRunnerMonitorDialog.STATUS_FONT_MAP.put(JobStatus.QUEUED,
				MartRunnerMonitorDialog.PLAIN_FONT);
		MartRunnerMonitorDialog.STATUS_FONT_MAP.put(JobStatus.FAILED,
				MartRunnerMonitorDialog.BOLD_FONT);
		MartRunnerMonitorDialog.STATUS_FONT_MAP.put(JobStatus.RUNNING,
				MartRunnerMonitorDialog.BOLD_ITALIC_FONT);
		MartRunnerMonitorDialog.STATUS_FONT_MAP.put(JobStatus.STOPPED,
				MartRunnerMonitorDialog.BOLD_FONT);
		MartRunnerMonitorDialog.STATUS_FONT_MAP.put(JobStatus.COMPLETED,
				MartRunnerMonitorDialog.PLAIN_FONT);
		MartRunnerMonitorDialog.STATUS_FONT_MAP.put(JobStatus.UNKNOWN,
				MartRunnerMonitorDialog.ITALIC_FONT);
	}

	/**
	 * Opens an explanation showing what a remote MartRunner host is up to.
	 * 
	 * @param host
	 *            the host to monitor.
	 * @param port
	 *            the port to connect to the host with.
	 */
	public static void monitor(final String host, final String port) {
		// Open the dialog.
		new MartRunnerMonitorDialog(host, port).setVisible(true);
	}

	private MartRunnerMonitorDialog(final String host, final String port) {
		// Create the blank dialog, and give it an appropriate title.
		super(Resources.get("monitorDialogTitle", new String[] { host, port }));
		this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

		// Make the RHS scrollpane containing job descriptions.
		final JobPlanPanel jobPlanPanel = new JobPlanPanel(this, host, port);
		this.host = host;
		this.port = port;

		// Make the LHS list of jobs.
		final JobPlanListModel jobPlanListModel = new JobPlanListModel(host,
				port);
		final JList jobList = new JList(jobPlanListModel);
		jobList.setCellRenderer(new JobPlanListCellRenderer());
		jobList.setBackground(Color.WHITE);
		jobList.setOpaque(true);
		jobList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		this.refreshJobList = new JButton(Resources.get("refreshButton"));
		final JTextField refreshRate = new JTextField(""
				+ MartRunnerMonitorDialog.DEFAULT_REFRESH, 5);
		final JPanel jobListPanel = new JPanel(new BorderLayout());
		jobListPanel.setBorder(new EmptyBorder(new Insets(2, 2, 2, 2)));
		jobListPanel.add(new JLabel(Resources.get("jobListTitle")),
				BorderLayout.PAGE_START);
		jobListPanel.add(new JScrollPane(jobList), BorderLayout.CENTER);
		final JPanel refreshPanel = new JPanel();
		refreshPanel.add(this.refreshJobList);
		refreshPanel.add(refreshRate);
		jobListPanel.add(refreshPanel, BorderLayout.PAGE_END);
		// Updates when refresh button is hit.
		this.refreshJobList.addActionListener(new ActionListener() {
			private boolean firstRun = true;

			public void actionPerformed(final ActionEvent e) {
				new LongProcess() {
					public void run() {
						if (MartRunnerMonitorDialog.this.listRefreshing)
							return;
						Object selection = jobList.getSelectedValue();
						try {
							MartRunnerMonitorDialog.this.listRefreshing = true;
							jobPlanListModel.updateList();
						} catch (final ProtocolException e) {
							StackTrace.showStackTrace(e);
						} finally {
							MartRunnerMonitorDialog.this.listRefreshing = false;
							// Attempt to select the first item on first run.
							if (firstRun && jobPlanListModel.size() > 0)
								selection = jobPlanListModel.lastElement();
							jobList.setSelectedValue(selection, true);
							firstRun = false;
						}
					}
				}.start();
			}
		});

		// Add a listener to the list to update the pane on the right.
		jobList.addListSelectionListener(new ListSelectionListener() {
			public void valueChanged(final ListSelectionEvent e) {
				final Object selection = jobList.getSelectedValue();
				if (!e.getValueIsAdjusting()
						&& !MartRunnerMonitorDialog.this.listRefreshing)
					// Update the panel on the right with the new job.
					jobPlanPanel
							.setJobPlan(selection instanceof JobPlan ? (JobPlan) selection
									: null);
			}
		});

		// Add context menu to the job list.
		jobList.addMouseListener(new MouseListener() {
			public void mouseClicked(final MouseEvent e) {
				this.doMouse(e);
			}

			public void mouseEntered(final MouseEvent e) {
				this.doMouse(e);
			}

			public void mouseExited(final MouseEvent e) {
				this.doMouse(e);
			}

			public void mousePressed(final MouseEvent e) {
				this.doMouse(e);
			}

			public void mouseReleased(final MouseEvent e) {
				this.doMouse(e);
			}

			private void doMouse(final MouseEvent e) {
				if (e.isPopupTrigger()) {
					final int index = jobList.locationToIndex(e.getPoint());
					if (index >= 0) {
						final JobPlan plan = (JobPlan) jobPlanListModel
								.getElementAt(index);
						final JPopupMenu menu = new JPopupMenu();

						// Remove job.
						final JMenuItem empty = new JMenuItem(Resources
								.get("emptyTableJobTitle"));
						empty.setMnemonic(Resources
								.get("emptyTableJobMnemonic").charAt(0));
						empty.addActionListener(new ActionListener() {
							public void actionPerformed(final ActionEvent evt) {
								new LongProcess() {
									public void run() throws Exception {
										// Do the job.
										final Socket clientSocket = Client
												.createClientSocket(host, port);
										Client.makeEmptyTableJob(clientSocket,
												plan.getJobId());
										clientSocket.close();
									}
								}.start();
							}
						});
						menu.add(empty);

						menu.addSeparator();

						// Remove job.
						final JMenuItem remove = new JMenuItem(Resources
								.get("removeJobTitle"));
						remove.setMnemonic(Resources.get("removeJobMnemonic")
								.charAt(0));
						remove.addActionListener(new ActionListener() {
							public void actionPerformed(final ActionEvent evt) {
								// Confirm.
								if (JOptionPane.showConfirmDialog(jobList,
										Resources.get("removeJobConfirm"),
										Resources.get("questionTitle"),
										JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION)
									new LongProcess() {
										public void run() throws Exception {
											// Remove the job.
											final Socket clientSocket = Client
													.createClientSocket(host,
															port);
											Client.removeJob(clientSocket, plan
													.getJobId());
											clientSocket.close();
											// Update the list.
											MartRunnerMonitorDialog.this.refreshJobList
													.doClick();
										}
									}.start();
							}
						});
						menu.add(remove);

						// Remove all job.
						final JMenuItem removeAll = new JMenuItem(Resources
								.get("removeAllJobsTitle"));
						removeAll.setMnemonic(Resources.get(
								"removeAllJobsMnemonic").charAt(0));
						removeAll.addActionListener(new ActionListener() {
							public void actionPerformed(final ActionEvent evt) {
								// Confirm.
								if (JOptionPane.showConfirmDialog(jobList,
										Resources.get("removeAllJobsConfirm"),
										Resources.get("questionTitle"),
										JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION)
									new LongProcess() {
										public void run() throws Exception {
											// Remove all jobs.
											final Enumeration e = jobPlanListModel
													.elements();
											final Socket clientSocket = Client
													.createClientSocket(host,
															port);
											while (e.hasMoreElements())
												Client.removeJob(clientSocket,
														((JobPlan) e
																.nextElement())
																.getJobId());
											clientSocket.close();
											// Update the list.
											MartRunnerMonitorDialog.this.refreshJobList
													.doClick();
										}
									}.start();
							}
						});
						menu.add(removeAll);

						// Show the menu.
						menu.show(jobList, e.getX(), e.getY());
						e.consume();
					}
				}
			}

		});

		// Set up a timer to update the list.
		final class TimerUpdate extends TimerTask {
			public void run() {
				MartRunnerMonitorDialog.this.refreshJobList.doClick();
			}
		}
		final class TimerListener extends WindowAdapter implements
				DocumentListener {
			private Timer timer = new Timer();

			// Called after the constructor.
			{
				this.timer.schedule(new TimerUpdate(), 0,
						MartRunnerMonitorDialog.DEFAULT_REFRESH * 1000);
			}

			public void changedUpdate(final DocumentEvent e) {
				this.updateTimer();
			}

			public void insertUpdate(final DocumentEvent e) {
				this.updateTimer();
			}

			public void removeUpdate(final DocumentEvent e) {
				this.updateTimer();
			}

			private void updateTimer() {
				String val = refreshRate.getText();
				if (val == null)
					val = "";
				int delay;
				try {
					delay = Integer.parseInt(val);
				} catch (final NumberFormatException ne) {
					delay = 0;
				}
				// Can't have it too short.
				delay = delay == 0 ? 0 : Math.max(delay,
						MartRunnerMonitorDialog.MIN_REFRESH) * 1000;
				// Reschedule.
				this.timer.cancel();
				if (delay > 0) {
					this.timer = new Timer();
					this.timer.schedule(new TimerUpdate(), delay, delay);
				}
			}

			public void windowClosed(final WindowEvent e) {
				this.timer.cancel();
			}

			public void windowClosing(final WindowEvent e) {
				this.timer.cancel();
			}
		}
		final TimerListener timerListener = new TimerListener();
		refreshRate.getDocument().addDocumentListener(timerListener);
		this.addWindowListener(timerListener);

		// Make the content pane.
		final JSplitPane splitPane = new JSplitPane(
				JSplitPane.HORIZONTAL_SPLIT, false, jobListPanel, jobPlanPanel);
		splitPane.setOneTouchExpandable(true);

		// Set up our content pane.
		this.setContentPane(splitPane);

		// Pack the window.
		this.pack();

		// Move ourselves.
		this.setLocationRelativeTo(null);
	}

	// Renders cells nicely.
	private static class JobPlanListCellRenderer implements ListCellRenderer {
		private static final long serialVersionUID = 1L;

		public Component getListCellRendererComponent(final JList list,
				final Object value, final int index, final boolean isSelected,
				final boolean cellHasFocus) {
			final JLabel label = new JLabel(value.toString());
			label.setOpaque(true);
			Color fgColor = Color.BLACK;
			Color bgColor = Color.WHITE;
			Font font = MartRunnerMonitorDialog.PLAIN_FONT;
			// A Job Plan entry node?
			if (value instanceof JobPlan) {
				final JobStatus status = ((JobPlan) value).getRoot()
						.getStatus();
				// White/Cyan stripes.
				bgColor = index % 2 == 0 ? Color.WHITE
						: MartRunnerMonitorDialog.PALE_BLUE;
				// Color-code text.
				fgColor = (Color) MartRunnerMonitorDialog.STATUS_COLOR_MAP
						.get(status);
				// Set font.
				font = (Font) MartRunnerMonitorDialog.STATUS_FONT_MAP
						.get(status);
			}
			// Always white-on-color or color-on-white.
			label.setFont(font);
			label.setForeground(isSelected ? bgColor : fgColor);
			label.setBackground(isSelected ? fgColor : bgColor);
			// Others get no extra material.
			return label;
		}
	}

	// Renders cells nicely.
	private static class JobPlanTreeCellRenderer implements TreeCellRenderer {
		private static final long serialVersionUID = 1L;

		public Component getTreeCellRendererComponent(final JTree tree,
				final Object value, final boolean sel, final boolean expanded,
				final boolean leaf, final int row, final boolean hasFocus) {
			final JLabel label = new JLabel(value.toString());
			label.setOpaque(true);
			Color fgColor = Color.BLACK;
			Color bgColor = Color.WHITE;
			Font font = MartRunnerMonitorDialog.PLAIN_FONT;
			// Sections are given text labels.
			if (value instanceof SectionNode) {
				final JobStatus status = ((SectionNode) value).getSection()
						.getStatus();
				// White/Cyan stripes.
				bgColor = row % 2 == 0 ? Color.WHITE
						: MartRunnerMonitorDialog.PALE_BLUE;
				// Color-code text.
				fgColor = (Color) MartRunnerMonitorDialog.STATUS_COLOR_MAP
						.get(status);
				// Set font.
				font = (Font) MartRunnerMonitorDialog.STATUS_FONT_MAP
						.get(status);
			}
			// Actions are given text labels.
			else if (value instanceof ActionNode) {
				final JobStatus status = ((ActionNode) value).getAction()
						.getStatus();
				// White/Cyan stripes.
				bgColor = row % 2 == 0 ? Color.WHITE
						: MartRunnerMonitorDialog.PALE_GREEN;
				// Color-code text.
				fgColor = (Color) MartRunnerMonitorDialog.STATUS_COLOR_MAP
						.get(status);
				// Set font.
				font = (Font) MartRunnerMonitorDialog.STATUS_FONT_MAP
						.get(status);
			}
			// Always white-on-color or color-on-white.
			label.setFont(font);
			label.setForeground(sel ? bgColor : fgColor);
			label.setBackground(sel ? fgColor : bgColor);
			// Everything else is default.
			return label;
		}
	}

	// A model for representing lists of jobs.
	private static class JobPlanListModel extends DefaultListModel {
		private static final long serialVersionUID = 1L;

		private final String host;

		private final String port;

		private JobPlanListModel(final String host, final String port) {
			super();
			this.host = host;
			this.port = port;
		}

		private void updateList() throws ProtocolException {
			try {
				// Communicate and update model.
				this.removeAllElements();
				final Socket clientSocket = Client.createClientSocket(
						this.host, this.port);
				for (final Iterator i = Client.listJobs(clientSocket)
						.getAllJobs().iterator(); i.hasNext();)
					this.addElement(i.next());
				clientSocket.close();
			} catch (final Throwable t) {
				throw new ProtocolException(t);
			}
		}
	}

	// A panel for showing the job plans in.
	private static class JobPlanPanel extends JPanel {

		private static final long serialVersionUID = 1L;

		private final String host;

		private final String port;

		private JTree tree;

		private JobPlanTreeModel treeModel;

		private String jobId;

		private final JTextField jobIdField;

		private final JSpinner threadSpinner;

		private final SpinnerNumberModel threadSpinnerModel;

		private final JTextField jdbcUrl;

		private final JTextField jdbcUser;

		private final JTextField contactEmail;

		private final JButton updateEmailButton;

		private final JFormattedTextField started;

		private final JFormattedTextField finished;

		private final JTextField elapsed;

		private final JTextField status;

		private final JTextArea messages;

		private final JButton startJob;

		private final JButton stopJob;

		private final JCheckBox skipDropTable;

		/**
		 * Create a new job description panel. In the top half goes two panes -
		 * an email settings pane, and the job tree view. In the bottom half
		 * goes an explanation panel.
		 * 
		 * @param parentDialog
		 *            the dialog we are displaying in.
		 * @param host
		 *            the host to talk to MartRunner at.
		 * @param port
		 *            the port to talk to MartRunner at.
		 */
		public JobPlanPanel(final MartRunnerMonitorDialog parentDialog,
				final String host, final String port) {
			super(new BorderLayout(2, 2));
			this.setBorder(new EmptyBorder(new Insets(2, 2, 2, 2)));
			this.host = host;
			this.port = port;

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

			// Create a panel to hold the header details.
			final JPanel headerPanel = new JPanel(new GridBagLayout());

			// Create the user-interactive bits of the panel.
			this.threadSpinnerModel = new SpinnerNumberModel(1, 1, 1, 1);
			this.threadSpinner = new JSpinner(this.threadSpinnerModel);
			// Spinner listener updates summary thread count instantly.
			this.threadSpinnerModel.addChangeListener(new ChangeListener() {
				public void stateChanged(final ChangeEvent e) {
					if (JobPlanPanel.this.jobId != null)
						try {
							final Socket clientSocket = Client
									.createClientSocket(host, port);
							Client
									.setThreadCount(
											clientSocket,
											JobPlanPanel.this.jobId,
											((Integer) JobPlanPanel.this.threadSpinnerModel
													.getValue()).intValue());
							clientSocket.close();
						} catch (final Throwable pe) {
							StackTrace.showStackTrace(pe);
						}
				}
			});

			// Populate the header panel.
			JLabel label = new JLabel(Resources.get("jobIdLabel"));
			headerPanel.add(label, labelConstraints);
			JPanel field = new JPanel();
			this.jobIdField = new JTextField(12);
			this.jobIdField.setEnabled(false);
			field.add(this.jobIdField);
			field.add(new JLabel(Resources.get("threadCountLabel")));
			field.add(this.threadSpinner);
			headerPanel.add(field, fieldConstraints);

			label = new JLabel(Resources.get("jdbcURLLabel"));
			headerPanel.add(label, labelConstraints);
			field = new JPanel();
			this.jdbcUrl = new JTextField(30);
			this.jdbcUrl.setEnabled(false);
			field.add(this.jdbcUrl);
			field.add(new JLabel(Resources.get("usernameLabel")));
			this.jdbcUser = new JTextField(12);
			this.jdbcUser.setEnabled(false);
			field.add(this.jdbcUser);
			headerPanel.add(field, fieldConstraints);

			label = new JLabel(Resources.get("contactEmailLabel"));
			headerPanel.add(label, labelConstraints);
			field = new JPanel();
			this.contactEmail = new JTextField(30);
			field.add(this.contactEmail);
			this.updateEmailButton = new JButton(Resources.get("updateButton"));
			// Listener on button to instantly update email address.
			this.updateEmailButton.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					if (JobPlanPanel.this.jobId != null)
						try {
							final Socket clientSocket = Client
									.createClientSocket(host, port);
							Client.setEmailAddress(clientSocket,
									JobPlanPanel.this.jobId,
									JobPlanPanel.this.contactEmail.getText()
											.trim());
							clientSocket.close();
						} catch (final Throwable pe) {
							StackTrace.showStackTrace(pe);
						}
				}
			});
			field.add(this.updateEmailButton);
			headerPanel.add(field, fieldConstraints);

			headerPanel.add(new JLabel(), labelLastRowConstraints);
			field = new JPanel();
			this.startJob = new JButton(Resources.get("startJobButton"));
			this.stopJob = new JButton(Resources.get("stopJobButton"));
			// Button listeners to start+stop jobs.
			this.startJob.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					if (JobPlanPanel.this.jobId != null)
						try {
							final Socket clientSocket = Client
									.createClientSocket(host, port);
							Client.startJob(clientSocket,
									JobPlanPanel.this.jobId);
							clientSocket.close();
							JobPlanPanel.this.startJob.setEnabled(false);
						} catch (final Throwable pe) {
							StackTrace.showStackTrace(pe);
						}
				}
			});
			this.stopJob.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					if (JobPlanPanel.this.jobId != null)
						try {
							final Socket clientSocket = Client
									.createClientSocket(host, port);
							Client.stopJob(clientSocket,
									JobPlanPanel.this.jobId);
							clientSocket.close();
							JobPlanPanel.this.stopJob.setEnabled(false);
						} catch (final Throwable pe) {
							StackTrace.showStackTrace(pe);
						}
				}
			});
			this.skipDropTable = new JCheckBox(Resources
					.get("skipDropTableLabel"));
			this.skipDropTable.addActionListener(new ActionListener() {
				public void actionPerformed(final ActionEvent e) {
					if (JobPlanPanel.this.jobId != null)
						try {
							final Socket clientSocket = Client
									.createClientSocket(host, port);
							Client.setSkipDropTable(clientSocket,
									JobPlanPanel.this.jobId,
									JobPlanPanel.this.skipDropTable
											.isSelected());
							clientSocket.close();
						} catch (final Throwable pe) {
							StackTrace.showStackTrace(pe);
						}
				}
			});
			field.add(this.startJob);
			field.add(this.stopJob);
			field.add(this.skipDropTable);
			headerPanel.add(field, fieldLastRowConstraints);

			// Create a panel to hold the footer details.
			final JPanel footerPanel = new JPanel(new GridBagLayout());

			// Populate the footer panel.
			label = new JLabel(Resources.get("statusLabel"));
			footerPanel.add(label, labelConstraints);
			field = new JPanel();
			this.status = new JTextField(12);
			this.status.setEnabled(false);
			field.add(this.status);
			field.add(new JLabel(Resources.get("elapsedLabel")));
			this.elapsed = new JTextField(12);
			this.elapsed.setEnabled(false);
			field.add(this.elapsed);
			field.add(new JLabel(Resources.get("startedLabel")));
			this.started = new JFormattedTextField(new SimpleDateFormat());
			this.started.setColumns(12);
			this.started.setEnabled(false);
			field.add(this.started);
			field.add(new JLabel(Resources.get("finishedLabel")));
			this.finished = new JFormattedTextField(new SimpleDateFormat());
			this.finished.setColumns(12);
			this.finished.setEnabled(false);
			field.add(this.finished);
			footerPanel.add(field, fieldConstraints);

			label = new JLabel(Resources.get("messagesLabel"));
			footerPanel.add(label, labelConstraints);
			field = new JPanel();
			this.messages = new JTextArea(6, 60);
			this.messages.setEnabled(false);
			field.add(new JScrollPane(this.messages));
			footerPanel.add(field, fieldConstraints);

			// Create the tree and default model.
			// Create a JTree to hold job details.
			this.treeModel = new JobPlanTreeModel(this.host, this.port, this,
					parentDialog);
			this.tree = new DraggableJTree(this.treeModel) {
				private static final long serialVersionUID = 1L;

				public boolean isPathEditable(final TreePath path) {
					return path.getPathCount() > 0
							&& path.getLastPathComponent() instanceof ActionNode;
				}

				public boolean isValidDragPath(final TreePath path) {
					// Drag-and-drop of level-1 nodes (ie. first level
					// below root only)
					return path.getPathCount() == 2;
				}

				public boolean isValidDropPath(final TreePath path) {
					// Drag-and-drop of level-1 nodes (ie. first level
					// below root only) PLUS level-0 node (root).
					return path.getPathCount() <= 2;
				}

				public void dragCompleted(final int action,
						final TreePath from, final TreePath to) {
					// On successful drag-and-drop, call move-section
					// with the identifiers of the source and target sections.
					final JobPlanSection fromSection = ((SectionNode) from
							.getLastPathComponent()).getSection();
					final JobPlanSection toSection = ((SectionNode) to
							.getLastPathComponent()).getSection();
					// Confirm.
					new LongProcess() {
						public void run() throws Exception {
							// Queue the job.
							final Socket clientSocket = Client
									.createClientSocket(host, port);
							Client.moveSection(clientSocket,
									JobPlanPanel.this.jobId, fromSection
											.getIdentifier(),
									toSection == treeModel.getRoot() ? null
											: toSection.getIdentifier());
							clientSocket.close();
							// Update the list.
							parentDialog.refreshJobList.doClick();
						}
					}.start();

				}
			};
			this.tree.setOpaque(true);
			this.tree.setBackground(Color.WHITE);
			this.tree.setEditable(true);
			this.tree.setRootVisible(true); // Always show the root node.
			this.tree.setShowsRootHandles(true); // Allow root expansion.
			this.tree.setCellRenderer(new JobPlanTreeCellRenderer());

			// Add context menu to the job plan tree.
			this.tree.addMouseListener(new MouseListener() {
				public void mouseClicked(final MouseEvent e) {
					this.doMouse(e);
				}

				public void mouseEntered(final MouseEvent e) {
					this.doMouse(e);
				}

				public void mouseExited(final MouseEvent e) {
					this.doMouse(e);
				}

				public void mousePressed(final MouseEvent e) {
					this.doMouse(e);
				}

				public void mouseReleased(final MouseEvent e) {
					this.doMouse(e);
				}

				private void doMouse(final MouseEvent e) {
					if (e.isPopupTrigger()) {
						final TreePath treePath = JobPlanPanel.this.tree
								.getPathForLocation(e.getX(), e.getY());
						if (treePath != null) {
							// Work out what was clicked on or
							// multiply selected.
							final TreePath[] selectedPaths;
							if (JobPlanPanel.this.tree.getSelectionCount() == 0)
								selectedPaths = new TreePath[] { treePath };
							else
								selectedPaths = JobPlanPanel.this.tree
										.getSelectionPaths();

							// Show menu.
							final JPopupMenu contextMenu = this
									.getContextMenu(Arrays
											.asList(selectedPaths));
							if (contextMenu != null
									&& contextMenu.getComponentCount() > 0) {
								contextMenu.show(JobPlanPanel.this.tree, e
										.getX(), e.getY());
								e.consume();
							}
						}
					}
				}

				private JPopupMenu getContextMenu(final Collection selectedPaths) {
					// Convert paths to identifiers.
					final Set identifiers = new HashSet();
					final List selectedNodes = new ArrayList();
					for (final Iterator i = selectedPaths.iterator(); i
							.hasNext();)
						selectedNodes.add(((TreePath) i.next())
								.getLastPathComponent());
					for (int i = 0; i < selectedNodes.size(); i++) {
						final Object node = selectedNodes.get(i);
						if (node instanceof ActionNode)
							identifiers.add(((ActionNode) node).getAction()
									.getIdentifier());
						else if (node instanceof SectionNode)
							identifiers.add(((SectionNode) node).getSection()
									.getIdentifier());
					}

					// Did we produce anything?
					if (identifiers.size() < 1)
						return null;

					// Build menu.
					final JPopupMenu contextMenu = new JPopupMenu();

					// Queue row.
					final JMenuItem queue = new JMenuItem(Resources
							.get("queueSelectionTitle"));
					queue.setMnemonic(Resources.get("queueSelectionMnemonic")
							.charAt(0));
					queue.addActionListener(new ActionListener() {
						public void actionPerformed(final ActionEvent evt) {
							// Confirm.
							new LongProcess() {
								public void run() throws Exception {
									// Queue the job.
									final Socket clientSocket = Client
											.createClientSocket(host, port);
									Client.queue(clientSocket,
											JobPlanPanel.this.jobId,
											identifiers);
									clientSocket.close();
									// Update the list.
									parentDialog.refreshJobList.doClick();
								}
							}.start();
						}
					});
					contextMenu.add(queue);

					// Unqueue row.
					final JMenuItem unqueue = new JMenuItem(Resources
							.get("unqueueSelectionTitle"));
					unqueue.setMnemonic(Resources.get(
							"unqueueSelectionMnemonic").charAt(0));
					unqueue.addActionListener(new ActionListener() {
						public void actionPerformed(final ActionEvent evt) {
							// Confirm.
							new LongProcess() {
								public void run() throws Exception {
									// Unqueue the job.
									final Socket clientSocket = Client
											.createClientSocket(host, port);
									Client.unqueue(clientSocket,
											JobPlanPanel.this.jobId,
											identifiers);
									clientSocket.close();
									// Update the list.
									parentDialog.refreshJobList.doClick();
								}
							}.start();
						}
					});
					contextMenu.add(unqueue);

					return contextMenu;
				}
			});

			// Listener on tree to update footer panel fields.
			this.tree.addTreeSelectionListener(new TreeSelectionListener() {

				public void valueChanged(final TreeSelectionEvent e) {
					// Default values.
					Date started = null;
					Date ended = null;
					JobStatus status = JobStatus.UNKNOWN;
					String messages = null;
					long elapsed = 0;

					// Check a path was actually selected.
					final TreePath path = e.getPath();
					if (path != null) {
						final Object selectedNode = e.getPath()
								.getLastPathComponent();

						// Get info.
						if (selectedNode instanceof SectionNode) {
							final JobPlanSection section = ((SectionNode) selectedNode)
									.getSection();
							status = section.getStatus();
							started = section.getStarted();
							ended = section.getEnded();
							messages = null;
						} else if (selectedNode instanceof ActionNode) {
							final JobPlanAction action = ((ActionNode) selectedNode)
									.getAction();
							status = action.getStatus();
							started = action.getStarted();
							ended = action.getEnded();
							messages = action.getMessage();
						}

						// Elapsed time calculation.
						if (started != null)
							if (ended != null)
								elapsed = ended.getTime() - started.getTime();
							else
								elapsed = new Date().getTime()
										- started.getTime();
					}

					// Elapsed time to string.
					elapsed /= 1000; // Un-millify.
					final long seconds = elapsed % 60;
					elapsed /= 60;
					final long minutes = elapsed % 60;
					elapsed /= 60;
					final long hours = elapsed % 24;
					elapsed /= 24;
					final long days = elapsed;

					// Update dialog.
					try {
						if (started != null) {
							JobPlanPanel.this.started.setValue(started);
							JobPlanPanel.this.started.commitEdit();
						} else
							JobPlanPanel.this.started.setText(null);
						if (ended != null) {
							JobPlanPanel.this.finished.setValue(ended);
							JobPlanPanel.this.finished.commitEdit();
						} else
							JobPlanPanel.this.finished.setText(null);
					} catch (final ParseException pe) {
						// Don't be so silly.
						Log.error(pe);
					}
					JobPlanPanel.this.elapsed.setText(Resources.get(
							"timeElapsedPattern", new String[] { "" + days,
									"" + hours, "" + minutes, "" + seconds }));
					JobPlanPanel.this.status.setText(status.toString());
					JobPlanPanel.this.messages.setText(messages);

					// Redraw.
					JobPlanPanel.this.revalidate();
				}
			});

			// Install an ExpansionListener on the tree which causes the model
			// to add actions dynamically from server.
			this.tree.addTreeWillExpandListener(this.treeModel);

			// Update the layout.
			this.add(headerPanel, BorderLayout.PAGE_START);
			this.add(new JScrollPane(this.tree), BorderLayout.CENTER);
			this.add(footerPanel, BorderLayout.PAGE_END);

			// Set the default values.
			this.setNoJob();
		}

		private void setNoJob() {
			this.jobId = null;
			this.jobIdField.setText(Resources.get("noJobSelected"));
			this.threadSpinnerModel.setValue(new Integer(1));
			this.threadSpinner.setEnabled(false);
			this.jdbcUrl.setText(null);
			this.jdbcUser.setText(null);
			this.contactEmail.setText(null);
			this.contactEmail.setEnabled(false);
			this.updateEmailButton.setEnabled(false);
			this.startJob.setEnabled(false);
			this.stopJob.setEnabled(false);
			this.skipDropTable.setSelected(false);
			this.skipDropTable.setEnabled(false);
			try {
				this.treeModel.setJobPlan(null);
			} catch (final ProtocolException e) {
				StackTrace.showStackTrace(e);
			}
		}

		private void setJobPlan(final JobPlan jobPlan) {
			if (jobPlan == null)
				this.setNoJob();
			else
				new LongProcess() {
					public void run() throws Exception {
						// Get new job ID.
						final String jobId = jobPlan.getJobId();

						// Update viewable fields.
						JobPlanPanel.this.jobIdField.setText(jobId);
						JobPlanPanel.this.threadSpinner.setEnabled(true);
						JobPlanPanel.this.contactEmail.setEnabled(true);
						JobPlanPanel.this.updateEmailButton.setEnabled(true);
						JobPlanPanel.this.skipDropTable.setEnabled(true);

						// Same job ID as before? Remember expansion set.
						final boolean jobIdChanged = !jobId
								.equals(JobPlanPanel.this.jobId);

						final List openRows = new ArrayList();
						if (!jobIdChanged) {
							// Remember tree state.
							final Enumeration openNodePaths = JobPlanPanel.this.tree
									.getExpandedDescendants(JobPlanPanel.this.tree
											.getPathForRow(0));
							while (openNodePaths != null
									&& openNodePaths.hasMoreElements()) {
								final TreePath openNodePath = (TreePath) openNodePaths
										.nextElement();
								openRows.add(new Integer(JobPlanPanel.this.tree
										.getRowForPath(openNodePath)));
							}
							// Sort the row numbers to prevent weirdness with
							// opening parents of already opened paths.
							Collections.sort(openRows);
						} else
							// Update our job ID.
							JobPlanPanel.this.jobId = jobId;

						// Update tree.
						JobPlanPanel.this.treeModel.setJobPlan(jobPlan);

						if (!jobIdChanged)
							// Re-expand tree.
							for (final Iterator i = openRows.iterator(); i
									.hasNext();)
								JobPlanPanel.this.tree.expandRow(((Integer) i
										.next()).intValue());
					}
				}.start();
		}
	}

	private static class JobPlanTreeModel extends DefaultTreeModel implements
			TreeWillExpandListener {
		private static final long serialVersionUID = 1L;

		private static final TreeNode LOADING_TREE = new DefaultMutableTreeNode(
				Resources.get("loadingTree"));

		private static final TreeNode EMPTY_TREE = new DefaultMutableTreeNode(
				Resources.get("emptyTree"));

		private final JobPlanPanel planPanel;

		private final String host;

		private final String port;

		private final MartRunnerMonitorDialog parentDialog;

		private JobPlanTreeModel(final String host, final String port,
				final JobPlanPanel planPanel,
				final MartRunnerMonitorDialog parentDialog) {
			super(JobPlanTreeModel.EMPTY_TREE, true);
			this.planPanel = planPanel;
			this.host = host;
			this.port = port;
			this.parentDialog = parentDialog;
		}

		/**
		 * Change the job this tree shows.
		 * 
		 * @param jobPlan
		 *            the job plan.
		 * @throws ProtocolException
		 *             if it was unable to do it.
		 */
		public void setJobPlan(final JobPlan jobPlan) throws ProtocolException {
			if (jobPlan == null) {
				this.setRoot(JobPlanTreeModel.EMPTY_TREE);
				this.reload();
			} else {
				// Set loading message.
				this.setRoot(JobPlanTreeModel.LOADING_TREE);
				this.reload();
				// Get job details.
				final SectionNode rootNode = new SectionNode(null, jobPlan
						.getRoot(), this.parentDialog);
				rootNode.expanded(this.host, this.port, this.planPanel.jobId);
				this.setRoot(rootNode);
				this.reload();
				// Update GUI bits from the updated plan.
				this.planPanel.threadSpinnerModel.setValue(new Integer(jobPlan
						.getThreadCount()));
				this.planPanel.threadSpinnerModel.setMaximum(new Integer(
						jobPlan.getMaxThreadCount()));
				this.planPanel.jdbcUrl.setText(jobPlan.getJDBCURL());
				this.planPanel.jdbcUser.setText(jobPlan.getJDBCUsername());
				this.planPanel.contactEmail.setText(jobPlan
						.getContactEmailAddress());
				this.planPanel.startJob.setEnabled(!jobPlan.getRoot()
						.getStatus().equals(JobStatus.RUNNING));
				this.planPanel.stopJob.setEnabled(jobPlan.getRoot().getStatus()
						.equals(JobStatus.RUNNING));
				this.planPanel.skipDropTable.setSelected(jobPlan
						.isSkipDropTable());
			}
		}

		public void treeWillCollapse(final TreeExpansionEvent event)
				throws ExpandVetoException {
			// Remove children.
			final Object collapsedNode = event.getPath().getLastPathComponent();
			if (collapsedNode instanceof SectionNode)
				((SectionNode) collapsedNode).collapsed();
		}

		public void treeWillExpand(final TreeExpansionEvent event)
				throws ExpandVetoException {
			// Insert children.
			final Object expandedNode = event.getPath().getLastPathComponent();
			if (expandedNode instanceof SectionNode)
				((SectionNode) expandedNode).expanded(this.host, this.port,
						this.planPanel.jobId);
		}
	}

	private static class SectionNode implements TreeNode {

		private final JobPlanSection section;

		private final SectionNode parent;

		private final MartRunnerMonitorDialog parentDialog;

		// Must use Vector to be able to provide enumeration.
		private final Vector children = new Vector();

		private SectionNode(final SectionNode parent,
				final JobPlanSection section,
				final MartRunnerMonitorDialog parentDialog) {
			this.section = section;
			this.parent = parent;
			this.parentDialog = parentDialog;
		}

		private JobPlanSection getSection() {
			return this.section;
		}

		private void collapsed() {
			// Forget children.
			this.children.clear();
		}

		private void expanded(final String host, final String port,
				final String jobId) {
			// Create children.
			// Actions first.
			try {
				final Socket clientSocket = Client.createClientSocket(host,
						port);
				final Collection actions = Client.getActions(clientSocket,
						jobId, this.section);
				for (final Iterator i = actions.iterator(); i.hasNext();)
					this.children.add(new ActionNode(this, (JobPlanAction) i
							.next(), this.parentDialog));
				clientSocket.close();
			} catch (final Throwable e) {
				// Log it.
				Log.error(e);
				// Add dummy actions instead.
				for (int i = 0; i < this.section.getActionCount(); i++)
					this.children.add(new DefaultMutableTreeNode(Resources
							.get("emptyTree")));
			}
			// Then subsections.
			for (final Iterator i = this.section.getSubSections().iterator(); i
					.hasNext();)
				this.children.add(new SectionNode(this, (JobPlanSection) i
						.next(), this.parentDialog));
		}

		public Enumeration children() {
			return this.children.elements();
		}

		public boolean getAllowsChildren() {
			return this.getChildCount() > 0;
		}

		public TreeNode getChildAt(final int childIndex) {
			return (TreeNode) this.children.get(childIndex);
		}

		public int getChildCount() {
			return this.section.getActionCount()
					+ this.section.getSubSections().size();
		}

		public int getIndex(final TreeNode node) {
			return this.children.indexOf(node);
		}

		public TreeNode getParent() {
			return this.parent;
		}

		public boolean isLeaf() {
			return this.getChildCount() == 0;
		}

		public String toString() {
			return this.section.toString();
		}
	}

	private static class ActionNode extends DefaultMutableTreeNode {
		private static final long serialVersionUID = 1L;

		private final JobPlanAction action;

		private final SectionNode parent;

		private final MartRunnerMonitorDialog parentDialog;

		private ActionNode(final SectionNode parent,
				final JobPlanAction action,
				final MartRunnerMonitorDialog parentDialog) {
			this.action = action;
			this.parent = parent;
			this.parentDialog = parentDialog;
		}

		private JobPlanAction getAction() {
			return this.action;
		}

		public Enumeration children() {
			return null;
		}

		public boolean getAllowsChildren() {
			return false;
		}

		public TreeNode getChildAt(final int childIndex) {
			return null;
		}

		public int getChildCount() {
			return 0;
		}

		public int getIndex(final TreeNode node) {
			return 0;
		}

		public TreeNode getParent() {
			return this.parent;
		}

		public boolean isLeaf() {
			return true;
		}

		public void setUserObject(final Object userObject) {
			// Set the actions.
			final String oldAction = this.action.getAction();
			this.action.setAction((String) userObject);
			final JobPlanSection section = this.parent.getSection();
			// Send the update to the server.
			try {
				final Socket clientSocket = Client.createClientSocket(
						this.parentDialog.host, this.parentDialog.port);
				Client.updateAction(clientSocket, section.getJobPlan()
						.getJobId(), section, this.action);
				clientSocket.close();
			} catch (final Throwable pe) {
				this.action.setAction(oldAction);
				StackTrace.showStackTrace(pe);
			}
		}

		public String toString() {
			return this.action.toString();
		}
	}
}
