package org.biomart.configurator.view;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

public class ToolBarMenu {

	private JMenuBar m_menuBar =  new JMenuBar();
	private JMenu m_menu = new JMenu("File");
	
	private JMenuItem m_menuItem_new = new JMenuItem("New");
	private JMenuItem m_menuItem_open = new JMenuItem("Open");
	private JMenuItem m_menuItem_export = new JMenuItem("Export");
	private JMenuItem m_menuItem_saveAll = new JMenuItem("Save All");
	private JMenuItem m_menuItem_uploadAll = new JMenuItem("Upload All");
	private JMenuItem m_menuItem_exit = new JMenuItem("Exit");
	
	private JRadioButtonMenuItem rbMenuItem;
	private JCheckBoxMenuItem cbMenuItem;
	
	public ToolBarMenu() {
		// TODO Auto-generated constructor stub
		m_menu.add(m_menuItem_new);
		m_menu.add(m_menuItem_open);
		m_menu.add(m_menuItem_export);
		m_menu.addSeparator();
		m_menu.add(m_menuItem_saveAll);
		m_menu.add(m_menuItem_uploadAll);
		m_menu.addSeparator();
		m_menu.add(m_menuItem_exit);
		
		m_menuBar.add(m_menu);
				
	}
	
	public JMenuBar getMenuBar(){
		return this.m_menuBar;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
