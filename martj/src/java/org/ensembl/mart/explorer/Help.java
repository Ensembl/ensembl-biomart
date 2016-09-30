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
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Frame;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.swing.JDialog;
import javax.swing.JEditorPane;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.event.HyperlinkEvent;
import javax.swing.event.HyperlinkListener;
import javax.swing.text.html.HTMLDocument;
import javax.swing.text.html.HTMLFrameHyperlinkEvent;

import org.ensembl.mart.guiutils.QuickFrame;

/**
 * Displays the MartExplorer help file
 * <code>file data/martexplorer_help.html</code>.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 * 
 */
public class Help extends JEditorPane implements HyperlinkListener {

  private final int WIDTH = 500;
  private final int HEIGHT = 500;

  public Help() {

    //  enable hyperlinks
    setEditable(false);
    addHyperlinkListener(this);

    setPreferredSize(new Dimension(WIDTH, HEIGHT));

    try {
      URL url =
        getClass().getClassLoader().getResource("data/martexplorer_help.html");
      setPage(url);

    } catch (MalformedURLException e) {
      setText(e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      setText(e.getMessage());
      e.printStackTrace();
    }

  }

  /**
   * Displays the Help page in a non-modal dialog box.
   * The dialog is displayed over the bottom right corner
   * of the parent.
   * @param parent parent frame
   */
  public void showDialog(Frame parent) {
    JDialog d = new JDialog(parent, "MartExplorer Documentation", false);
    Container c = d.getContentPane();
    JScrollPane sp =
      new JScrollPane(
        this,
        JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,
        JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
    c.add(sp);
    int x = Math.max(0, parent.getWidth()-WIDTH);
    int y = Math.max(0, parent.getHeight()-HEIGHT);
    d.setBounds(x,y,WIDTH,HEIGHT);
    d.setVisible(true);
  }

  public void hyperlinkUpdate(HyperlinkEvent e) {
    if (e.getEventType() == HyperlinkEvent.EventType.ACTIVATED) {
      JEditorPane pane = (JEditorPane) e.getSource();
      if (e instanceof HTMLFrameHyperlinkEvent) {
        HTMLFrameHyperlinkEvent evt = (HTMLFrameHyperlinkEvent) e;
        HTMLDocument doc = (HTMLDocument) pane.getDocument();
        doc.processHTMLFrameHyperlinkEvent(evt);
      } else {
        try {
          pane.setPage(e.getURL());
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args) {
    QuickFrame f =
      new QuickFrame(
        "Help test",
        new JLabel("Nothing here, use the close button below to close the test program."));
    new Help().showDialog(f);
  }
}
