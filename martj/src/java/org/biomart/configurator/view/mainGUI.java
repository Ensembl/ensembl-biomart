package org.biomart.configurator.view;

import java.awt.*;
import javax.swing.*;
import java.awt.event.*;
import java.awt.image.BufferedImage;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.biomart.configurator.model.Initializer;
import org.biomart.configurator.view.ToolBarMenu;
import org.biomart.configurator.view.SplitPanel;

public class mainGUI {


	//... Constants
    private static final String INITIAL_VALUE = "1";
    
    //... Components
    private JFrame m_frame = new JFrame("Mart Configurator - a definitive answer to all the problems in modern science");
    
    private JTextField m_userInputTf = new JTextField(5);
    private JTextField m_totalTf     = new JTextField(20);
    private JButton    m_multiplyBtn = new JButton("Multiply");
    private JButton    m_clearBtn    = new JButton("Clear");
    
    private Initializer modelObj;
    private ToolBarMenu menuBarObj = new ToolBarMenu();
    private SplitPanel splitPanelObj = new SplitPanel();
    
    //======================================================= constructor
    /** Constructor */
    public mainGUI(Initializer model) {
        //... Set up the logic
        modelObj = model;
        modelObj.setValue(INITIAL_VALUE);
        
        //2. Optional: What happens when the frame closes?
        m_frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        //3. Create components and put them in the frame.
        //...create emptyLabel...
       
        // adding menuBar
        m_frame.setJMenuBar(menuBarObj.getMenuBar());
       
        // adding split panel
        splitPanelObj.addTreeView();
        splitPanelObj.addPropertEditor();
        m_frame.getContentPane().add(splitPanelObj.getSplitPanel());

        // Size the frame.
        m_frame.setSize(800,600);
        m_frame.setIconImage(new ImageIcon("/homes/syed/Desktop/martj/src/java/org/biomart/configurator/view/biomarticon.gif").getImage());
        // Show it.
        m_frame.setVisible(true);
        
        /*JPanel content = new JPanel();
        content.setLayout(new FlowLayout());
        content.add(new JLabel("Input"));
        content.add(m_userInputTf);
        content.add(m_multiplyBtn);
        content.add(new JLabel("Total"));
        content.add(m_totalTf);
        content.add(m_clearBtn);
        
        //... finalize layout
        this.setContentPane(content);
        this.pack();
        
        this.setTitle("Simple Calc - MVC");
        // The window closing event should probably be passed to the 
        // Controller in a real program, but this is a short example.
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        */
    }
    
    /**
     * 
     */

    public void reset() {
        m_totalTf.setText(INITIAL_VALUE);
    }
    
    /**
     * @return
     */
    public String getUserInput() {
        return m_userInputTf.getText();
    }
    
    /**
     * @param newTotal
     */
    public void setTotal(String newTotal) {
        m_totalTf.setText(newTotal);
    }
    
    /**
     * @param errMessage
     */
    public void showError(String errMessage) {
       // JOptionPane.showMessageDialog(this, errMessage);
    }
    
    /**
     * @param mal
     */
    public void addMultiplyListener(ActionListener mal) {
        m_multiplyBtn.addActionListener(mal);
    }
    
    public void addClearListener(ActionListener cal) {
        m_clearBtn.addActionListener(cal);
    }
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
