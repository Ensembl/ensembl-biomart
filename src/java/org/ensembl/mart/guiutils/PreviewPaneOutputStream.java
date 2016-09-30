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
	Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
package org.ensembl.mart.guiutils;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

import javax.swing.JEditorPane;

/** 
* @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
* @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
*/
public class PreviewPaneOutputStream extends FilterOutputStream {

  private Logger logger = Logger.getLogger(PreviewPaneOutputStream.class.getName());
  
  private JEditorPane outpane;
  private int maxBytes;
  private int pos = 0;
  private OutputStream out;
  
  private ByteArrayOutputStream preview;
  
  public PreviewPaneOutputStream(JEditorPane outpane, int maxBytes) {
    this(null, outpane, maxBytes);
  }
  
  /**
   * @param out
   */
  public PreviewPaneOutputStream(OutputStream out, JEditorPane outpane, int maxBytes) {
    super(out);
    this.out = out;
    this.outpane = outpane;
    this.maxBytes = maxBytes;
    preview = new ByteArrayOutputStream();
  }



  /* (non-Javadoc)
   * @see java.io.OutputStream#close()
   */
  public void close() throws IOException {
    if (preview != null)
      writePreview();

    if (out != null)
      out.close();
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#flush()
   */
  public void flush() throws IOException {
    if (out != null)
      out.flush();
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#write(byte[], int, int)
   */
  public void write(byte[] b, int off, int len) throws IOException {
    if (pos < maxBytes) {
      int newlen = Math.min(len, maxBytes - pos);
      pos += newlen;
      preview.write(b, off, newlen);
    } else if (preview != null)
      writePreview();
    if (out != null)
      out.write(b, off, len);
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#write(byte[])
   */
  public void write(byte[] b) throws IOException {
    if (pos < maxBytes) {
      int len = b.length;
      int newlen = Math.min(len, maxBytes - pos);
      pos += newlen;
      preview.write(b, 0, newlen);
    } else if (preview != null)
      writePreview();
      
    if (out != null)
      out.write(b);
  }

  /* (non-Javadoc)
   * @see java.io.OutputStream#write(int)
   */
  public void write(int b) throws IOException {
    if (pos < maxBytes) {
      preview.write(b);
      pos++;
    } else if (preview != null)
      writePreview();
      
    if (out != null)
      out.write(b);
  }
  
  private void writePreview() throws IOException {
     outpane.setText(preview.toString());
     preview.close();
     preview = null;
  }
}
