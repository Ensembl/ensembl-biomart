package org.biomart.configurator.view;

import java.awt.Dimension;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import org.biomart.configurator.view.TreeView;

public class SplitPanel {
	
	private JButton    m_tempButton_1 = new JButton("LHS");
    private JButton    m_tempButton_2 = new JButton("RHS");
	
	private JSplitPane splitPane =  new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
	
	private TreeView treeViewObj = new TreeView();
	
	public SplitPanel() {
		// TODO Auto-generated constructor stub
		
		//splitPane.setLeftComponent(m_tempButton_1);
		//splitPane.setRightComponent(m_tempButton_2);
		splitPane.setOneTouchExpandable(true);
        splitPane.setDividerLocation(400);
        //Provide a preferred size for the split pane.
        splitPane.setPreferredSize(new Dimension(400, 200));
	}

	public JSplitPane getSplitPanel() {
		return this.splitPane;
	}
	
	public void addTreeView() {
		
		splitPane.setLeftComponent(treeViewObj.getTree());
	}
	
	public void addPropertEditor() {
		splitPane.setRightComponent(m_tempButton_2);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
