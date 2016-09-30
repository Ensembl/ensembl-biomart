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

package org.ensembl.mart.explorer;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;
import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;

import org.ensembl.mart.lib.InvalidQueryException;
import org.ensembl.mart.lib.Query;

import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.SequenceDescription;
import org.ensembl.mart.lib.config.AttributeCollection;
import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.lib.config.AttributeGroup;
import org.ensembl.mart.lib.config.AttributePage;
import org.ensembl.mart.lib.config.ConfigurationException;
import org.ensembl.mart.lib.config.DatasetConfig;
import org.ensembl.mart.util.LoggingUtil;

/**
 * Widget for viewing the current sequence attribute on a query and enabling the user to
 * add, remove or select a different one. This class implements the Model View Controller Design pattern where 
 * the query is the model, actionPerformed(...) handles user control actions and 
 * sequenceDescriptionChanged(...) updates the view when the model changes.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 *
  * TODO Modify SequenceDescription.left/right to use -1 for unset, then update this to reflect that.
 */
public class SequenceGroupWidget
  extends InputPage
  implements ActionListener, TreeSelectionListener {

  private static final Logger logger =
    Logger.getLogger(SequenceGroupWidget.class.getName());

  private class LabelledTextField extends JTextField {
    private LabelledTextField(String initialValue) {
      super(initialValue);
      Dimension d = new Dimension(100, 24);
      setPreferredSize(d);
      setMaximumSize(d);
    }

    public int getTextAsInt() {
      if (getText() == null || getText().length() < 1)
          return 0;
      return Integer.parseInt(getText());
    }
  }

  private final int IMAGE_WIDTH = 248;

  private final int IMAGE_HEIGHT = 69;

  private final int UNSUPPORTED = -100;

  private Feedback feedback = new Feedback(this);

  private LabelledTextField flank5 = new LabelledTextField("0");
  private LabelledTextField flank3 = new LabelledTextField("0");

  private JButton clearButton = new JButton("Clear");

  private JRadioButton transcript = new JRadioButton("Transcripts/proteins");

  private JRadioButton gene = new JRadioButton("Genes");
  
  private JRadioButton none = new JRadioButton();

  private final String[] gene_disables = new String[] {"transcript_exon_intron",
                                                       "transcript_exon",
                                                       "transcript_flank",
                                                       "3utr",
                                                       "5utr",
                                                       "cdna", 
                                                       "coding",
                                                       "coding_transcript_flank",
                                                       "peptide"
                                                       };
  
  private JRadioButton includeNone = new JRadioButton();

  private JRadioButton[] typeButtons = { transcript, gene,  none };
  
  private JRadioButton[] genericSeqButtons = { none };

  private JRadioButton[] includeButtons;
  
  private JRadioButton[] tranButtons;

  private JComponent[] rightColumn;

  private JComponent[] leftColumn;

  private JRadioButton[] geneButtons;
  
  private JLabel schematicSequenceImageHolder = new JLabel();

  private ImageIcon blankIcon;

  private DatasetConfig dsv;
  
  private AdaptorManager manager;
  
  private String[] seqTypes;
  
  private JRadioButton lastButton = null;
  
  private String iname = null;
  
  private boolean containsImage = false;
  
  /**
  * @param name
  * @param query
  * @param tree
 * @throws ConfigurationException 
  */
  public SequenceGroupWidget(
    String name,
    String iname,
    Query query,
    QueryTreeView tree,
    DatasetConfig dsv,
    AdaptorManager manager) {

    super(query, name, tree);
    
    this.dsv = dsv;
    this.manager = manager;
    this.iname = iname;
    
    if (tree != null)
      tree.addTreeSelectionListener(this);

    buildGUI();
    sequenceDescriptionChanged(query, null, query.getSequenceDescription());
  }

  private void buildGUI() {
    if (iname.matches("\\w+seq_scope"))
      buildGUIGeneric();
    else
      buildGUIEnsembl();
  }
  
  private void buildGUIGeneric() {
  Box b = Box.createVerticalBox();

  b.add(addAll(Box.createHorizontalBox(), new JComponent[]{clearButton}, true));
  
  containsImage = false;

  Box columns = Box.createHorizontalBox();
  
  //need to get all sequence types from the Registry, and make JRadioButtons for them
  AttributePage seqPage = dsv.getAttributePageByInternalName("sequences");
  if (seqPage == null ) seqPage = dsv.getAttributePageByInternalName("sequence");
  AttributeGroup seqGroup = (AttributeGroup) seqPage.getAttributeGroupByName("sequence");
  AttributeCollection seqCol = seqGroup.getAttributeCollectionByName(iname);
  List seq_atts = seqCol.getAttributeDescriptions();
  includeButtons = new JRadioButton[seq_atts.size()];
  leftColumn = new JComponent[seq_atts.size()];
  seqTypes = new String[seq_atts.size()];
  
  for (int i = 0, n = seq_atts.size(); i < n; i++) {
      AttributeDescription pointer = (AttributeDescription) seq_atts.get(i);
      AttributeDescription realAtt =  manager.getPointerAttribute(pointer);
      
      JRadioButton button = new JRadioButton(realAtt.getDisplayName());
      button.addActionListener(this);
      
      //all buttons go in includeButtons, and their root type descriptions map with them
      includeButtons[i] = button;
      leftColumn[i] = button;
      seqTypes[i] = pointer.getInternalName();
  }
    
  columns.add(addAll(Box.createVerticalBox(), leftColumn, true));
  columns.add(Box.createHorizontalGlue());
  b.add(columns);

  if (iname.matches("snp\\w+")) {
      b.add(
              addAll(
                      Box.createHorizontalBox(),
                      new Component[] {
                          new JLabel("5' Flank (bp)"),
                          flank5,
                          Box.createHorizontalStrut(50),
                          new JLabel("3' Flank (bp)"),
                          flank3 },
                          false));
            
      flank3.addActionListener(this);
      flank5.addActionListener(this);
  }

  add(b);
  
  none.setSelected(true);
  ButtonGroup bg = new ButtonGroup();
  for (int i = 0; i < genericSeqButtons.length; i++) {
    bg.add(genericSeqButtons[i]);
    genericSeqButtons[i].addActionListener(this);
  }

  clearButton.addActionListener(this);
  clearButton.addActionListener(new ActionListener() {
    public void actionPerformed(ActionEvent e) {
      none.doClick();
    }
  });

  bg = new ButtonGroup();
  for (int i = 0; i < includeButtons.length; i++) {
    bg.add(includeButtons[i]);
    includeButtons[i].addActionListener(this);
  }
  bg.add(includeNone);
  
  //default state
  setDefaultStateGeneric();    
  }
  
  private void buildGUIEnsembl() {
    containsImage = true;
    gene.setToolTipText(
      " Transcript information ignored (one output per gene)");

    Box b = Box.createVerticalBox();

    b.add(addAll(Box.createHorizontalBox(), new JComponent[]{transcript,gene,clearButton}, true));

    b.add(
      addAll(
        Box.createHorizontalBox(),
        new JComponent[] { schematicSequenceImageHolder },
        true));

    Box columns = Box.createHorizontalBox();
    
    //need to get all sequence types from the Registry, and make JRadioButtons for them    
    AttributePage seqPage = dsv.getAttributePageByInternalName("sequences");
    // another hack this time for wormart
    if (seqPage == null) seqPage = dsv.getAttributePageByInternalName("sequence");
    AttributeGroup seqGroup = (AttributeGroup) seqPage.getAttributeGroupByName("sequence");
    AttributeCollection seqCol = seqGroup.getAttributeCollectionByName("seq_scope_type");
    List seq_atts = seqCol.getAttributeDescriptions();
    includeButtons = new JRadioButton[seq_atts.size()];
    seqTypes = new String[seq_atts.size()];
    
    //turn the gene_exclude Array into a List for easier reference
    List gene_excludes = Arrays.asList(gene_disables);
    ArrayList gene_atts = new ArrayList();
    ArrayList tran_atts = new ArrayList();
    
    for (int i = 0, n = seq_atts.size(); i < n; i++) {
        AttributeDescription pointer = (AttributeDescription) seq_atts.get(i);
        AttributeDescription realAtt =  manager.getPointerAttribute(pointer);
        
        JRadioButton button = new JRadioButton(realAtt.getDisplayName());
        button.addActionListener(this);
        
        //all buttons go in includeButtons, and their root type descriptions map with them
        includeButtons[i] = button;
        seqTypes[i] = pointer.getInternalName();
        
        if (pointer.getPointerAttribute()!=null && gene_excludes.contains(pointer.getPointerAttribute()))
            tran_atts.add(button);
        else
            gene_atts.add(button);
    }
    
    geneButtons = new JRadioButton[gene_atts.size()];    
    gene_atts.toArray(geneButtons);
    rightColumn = new JComponent[gene_atts.size()];
    gene_atts.toArray(rightColumn);
    
    tranButtons = new JRadioButton[tran_atts.size()];
    tran_atts.toArray(tranButtons);
    leftColumn = new JComponent[tran_atts.size()];
    tran_atts.toArray(leftColumn);
    
    columns.add(addAll(Box.createVerticalBox(), leftColumn, true));
    columns.add(addAll(Box.createVerticalBox(), rightColumn, false));
    columns.add(Box.createHorizontalGlue());
    b.add(columns);

    b.add(
      addAll(
        Box.createHorizontalBox(),
        new Component[] {
          new JLabel("5' Flank (bp)"),
          flank5,
          Box.createHorizontalStrut(50),
          new JLabel("3' Flank (bp)"),
          flank3 },
        false));

    add(b);

    BufferedImage blank =
      new BufferedImage(IMAGE_WIDTH, IMAGE_HEIGHT, BufferedImage.TYPE_INT_ARGB);
    Graphics2D g = blank.createGraphics();
    g.setBackground(Color.WHITE);
    g.fillRect(0, 0, IMAGE_WIDTH, IMAGE_HEIGHT);
    blankIcon = new ImageIcon(blank);

    none.setSelected(true);
    ButtonGroup bg = new ButtonGroup();
    for (int i = 0; i < typeButtons.length; i++) {
      bg.add(typeButtons[i]);
      typeButtons[i].addActionListener(this);
    }

    clearButton.addActionListener(this);
    clearButton.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        none.doClick();
      }
    });

    bg = new ButtonGroup();
    for (int i = 0; i < includeButtons.length; i++) {
      bg.add(includeButtons[i]);
      includeButtons[i].addActionListener(this);
    }
    bg.add(includeNone);

    flank3.addActionListener(this);
    flank5.addActionListener(this);
    
    //default state
    setDefaultStateEns();
  }

  private void setDefaultState() {
    if (iname.matches("\\w+seq_scope"))
      setDefaultStateGeneric();
    else
      setDefaultStateEns();
  }
  
  private void setDefaultStateGeneric() {
    lastButton = null;
    
    if (iname.matches("snp\\w+")) {
        flank3.setText("100");
        flank5.setText("100");
        flank3.setEnabled(true);
        flank5.setEnabled(true);
    }
    setButtonsEnabled(includeButtons, true);    
  }
  
  private void setDefaultStateEns() {
      schematicSequenceImageHolder.setIcon(blankIcon);
      lastButton = null;
      flank3.setEnabled(true);
      flank5.setEnabled(true);
      gene.setSelected(false);
      transcript.setSelected(true);
      setButtonsEnabled(includeButtons, false);
      setButtonsEnabled(tranButtons, true);
  }
  
  private Box addAll(
    Box container,
    Component[] components,
    boolean addGlueAtEnd) {
    for (int i = 0; i < components.length; i++)
      container.add(components[i]);
    if (addGlueAtEnd)
      container.add(Box.createGlue());
    return container;
  }

  private ImageIcon loadIcon(String filepath) {
    ImageIcon icon = null;
    URL testImage = getClass().getClassLoader().getResource(filepath);

    if (testImage != null)
      icon = new ImageIcon(testImage);
    else {
      System.err.println("Problem loading file: " + filepath);
    }

    return icon;
  }

  /**
   * Runs a graphical test of this widget. 
   * @param args ignored
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    LoggingUtil.setAllRootHandlerLevelsToFinest();
    logger.setLevel(Level.ALL);

    //DSAttributeGroup g = new DSAttributeGroup("sequences");
    Query q = new Query();
    //q.addQueryChangeListener(new DebugQueryListener(System.out));

    //TODO: load defaultMartRegistry.xml, get human sequence config
    //SequenceGroupWidget w = new SequenceGroupWidget("seq widget", q, null);

    //new QuickFrame("Sequence Attribute Widget test", w);
  }

  /**
   * Updates the query in response to a user action (control part of model-control-view pattern).
   * Adds / removes a filtter to/from query as necessary.
   */
  public void actionPerformed(ActionEvent e) {

    Object src = e.getSource();

    if (src == clearButton) {        
      changeQuery(null, 0, 0);
      setDefaultState();
    } else if (src == transcript) {        
        transcript.setSelected(true);
        if (lastButton == null)
          changeQuery(null, 0, 0);
        setButtonsEnabled(includeButtons, false);
        setButtonsEnabled(tranButtons, true);

    } else if (src == gene) {
        gene.setSelected(true);
        
        if (lastButton != null) {
            String seqType = null;
            for (int i = 0, n = geneButtons.length; i < n; i++) {
                JRadioButton button = geneButtons[i];
                if (lastButton == button) {
                    seqType = seqTypes[i];
                    break;
                }
            }
            
            //seqType may be null if a transcript only type button was chosen before the gene switch was applied
            if (seqType == null) {
                //runs setDefaultState by virtue of null sequenceDescription sent to the changed method
                changeQuery(null, 0, 0);
                transcript.setSelected(false);
                gene.setSelected(true);
            } else
              changeQuery( seqType, 
                           flank5.getTextAsInt(), 
                           flank3.getTextAsInt());            
        }
        setButtonsEnabled(includeButtons, false);
        setButtonsEnabled(geneButtons, true);
    } else if (src == flank5 ) {        
        if (lastButton != null) {
            String seqType = null;
            for (int i = 0, n = includeButtons.length; i < n; i++) {
                JRadioButton button = includeButtons[i];
                if (lastButton == button) {
                    seqType = seqTypes[i];
                    lastButton = button;
                    break;
                }
            }
            
            changeQuery( seqType, 
                         flank5.getTextAsInt(), 
                         flank3.getTextAsInt());
        }
    } else if (src == flank3) {        
        if (lastButton != null) {
            String seqType = null;
            for (int i = 0, n = includeButtons.length; i < n; i++) {
                JRadioButton button = includeButtons[i];
                if (lastButton == button) {
                    seqType = seqTypes[i];
                    lastButton = button;
                    break;
                }
            }
            
            changeQuery( seqType, 
                         flank5.getTextAsInt(), 
                         flank3.getTextAsInt());            
        }
    } else {        
          String seqType = null;
          for (int i = 0, n = includeButtons.length; i < n; i++) {
              JRadioButton button = includeButtons[i];
              if (src == button) {
                  seqType = seqTypes[i];
                  lastButton = button;
                  break;
              }
          }
          
          changeQuery( seqType, 
                       flank5.getTextAsInt(), 
                       flank3.getTextAsInt());
    }
  }

  /**
   * Updates the view (GUI state) to represent the sequence description.
   * @param description
   */
  public void sequenceDescriptionChanged(
    Query sourceQuery,
    SequenceDescription oldSequenceDescription,
    SequenceDescription sd) {

    if (sd == null)      
      setDefaultState();      
    else {
      if (containsImage) {
        //just need to know which image to load
    	String seqD = sd.getSeqDescription();
        if (sd.getLeftFlank() > 0)
          seqD += "_5";
        if (sd.getRightFlank() > 0)
          seqD += "_3";
        
        
        String imagePath = "data/image/gene_schematic_"+seqD+".gif";
        
        schematicSequenceImageHolder.setIcon(
            loadIcon(imagePath));
      }
    } 
  }
  
  /**
   * Updates sequence description on the query
   * @param imageFilePath image to be displayed
   * @param sequenceType sequence type (constant from SeequenceDescription), or UNSUPPORTED if unsupported
   * @param leftFlank left flank in base pairs
   * @param rightFlank rightt flank in base pairs
   */
  private void changeQuery(String sequenceType, int leftFlank, int rightFlank) {

  if (sequenceType == null) {

      query.setSequenceDescription(null);

    } else {

      try {

			AttributePage seqPage = dsv.getAttributePageByInternalName("sequences");
			// for wormmart
			if (seqPage == null) seqPage = dsv.getAttributePageByInternalName("sequence");

			AttributeDescription attrDesc = seqPage.getAttributeDescriptionByInternalName(sequenceType);
			String seqDs = attrDesc.getPointerDataset();
			if (seqDs==null || "".equals(seqDs)) seqDs = dsv.getDataset();
			
        SequenceDescription newAttribute =
          new SequenceDescription(dsv.getDataset(), seqDs, sequenceType, manager.getRootAdaptor(), leftFlank, rightFlank);

        SequenceDescription oldAttribute = query.getSequenceDescription();

        if (oldAttribute != newAttribute
         && !newAttribute.equals(oldAttribute)) {

          query.setSequenceDescription(newAttribute);
            		
		  	// try to add remove atts logic here
		  			
		  	ArrayList attsToRemove = new ArrayList();       
			boolean removeSeq = false;
          
			Attribute[] queryAtts = query.getAttributes();
			for (int i = 0, n = queryAtts.length; i < n; i++) {
				Attribute thisAtt = queryAtts[i];
              
				if (seqPage.getAttributeDescriptionByFieldNameTableConstraint(thisAtt.getField(), thisAtt.getTableConstraint()) == null) {
					attsToRemove.add(thisAtt);
				}
			}
          
			if (query.getSequenceDescription() != null) {
				if (!seqPage.getInternalName().equals("sequences") && !seqPage.getInternalName().equals("sequence") )
							removeSeq = true;
			}
          
			if (attsToRemove.size() > 0 || removeSeq) {
				feedback.info("Removing attributes from pages not compatible with " + seqPage.getDisplayName());
              
				for (int i = 0, n = attsToRemove.size(); i < n; i++) {
					Attribute attToRemove = (Attribute) attsToRemove.get(i);
					
					System.out.println("revmoving "+attToRemove.getField());
					
					query.removeAttribute(attToRemove);
				}
              
				if (removeSeq)
						query.setSequenceDescription(null);
			}
          
     
          //System.out.println(" seq11 descripiton "+query.getSequenceDescription());
          
          sequenceDescritpionChanged(query,oldAttribute,newAttribute);
          
          
        }

      } catch (InvalidQueryException e) {
        feedback.warning("Invalid sequence attribute. " + e.getMessage());
        e.printStackTrace();
      }

    }

  }

  private void setButtonsEnabled(JRadioButton[] buttons, boolean enabled) {
    for (int i = 0; i < buttons.length; i++)
      buttons[i].setEnabled(enabled);
  }

  /**
   * Callback method called when an item in the tree is selected.
   * Brings this widget to the front if the selected node in the tree is a sequence description.
   * TODO get scrolling to a selected attribute working properly
   */
  public void valueChanged(TreeSelectionEvent e) {

    if (query.getSequenceDescription() != null) {

      if (e.getNewLeadSelectionPath() != null
        && e.getNewLeadSelectionPath().getLastPathComponent() != null) {

        DefaultMutableTreeNode node =
          (DefaultMutableTreeNode) e
            .getNewLeadSelectionPath()
            .getLastPathComponent();

        if (node != null) {

          TreeNodeData tnd = (TreeNodeData) node.getUserObject();
          if (tnd.getSequenceDescription() != null)
            for (Component p, c = this; c != null; c = p) {
              p = c.getParent();
              if (p instanceof JTabbedPane)
                 ((JTabbedPane) p).setSelectedComponent(c);
              else if (p instanceof JScrollPane) {
                // not sure if this is being used
                Point pt = c.getLocation();
                Rectangle r = new Rectangle(pt);
                ((JScrollPane) p).scrollRectToVisible(r);

              }

            }
        }
      }
    }

  }

}
