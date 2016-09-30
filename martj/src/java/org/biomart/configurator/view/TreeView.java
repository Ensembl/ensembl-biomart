package org.biomart.configurator.view;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeSelectionModel;

public class TreeView {
	
	private JTree tree;
	
	public TreeView() {
		// TODO Auto-generated constructor stub
		
		//Create the nodes.
        DefaultMutableTreeNode top =
            new DefaultMutableTreeNode("The Java Series");
        
        top.add(new DefaultMutableTreeNode("child 1"));
        top.add(new DefaultMutableTreeNode("child 1"));
        
        

        //Create a tree that allows one selection at a time.
        tree = new JTree(top);
        tree.getSelectionModel().setSelectionMode
                (TreeSelectionModel.SINGLE_TREE_SELECTION);

	}

	public JTree getTree() {
		return this.tree;
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
