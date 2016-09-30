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

package org.ensembl.mart.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A wrapper for an OutputStream that calls flush on the contained stream
 * when the bufferSize is reached.
 */
public class AutoFlushOutputStream extends FilterOutputStream {

  private int pos = 0;
  private int bufferSize; 



	public AutoFlushOutputStream(OutputStream out, int bufferSize) {

		super(out);
    this.bufferSize = bufferSize;

	}

  

	public void write(byte[] b, int off, int len) throws IOException {
		super.write(b, off, len);
    autoFlush( len );
	}


	public void write(byte[] b) throws IOException {
		super.write(b);
    autoFlush( b.length ); 
	}



	public void write(int b) throws IOException {
		super.write(b);
    autoFlush( 1 );
	}

  /**
   * Causes flush if pos + numBytesWritten excedes bufferSize.
   * @param numBytesWritten
   */
  private void autoFlush(int numBytesWritten) throws IOException {
    pos += numBytesWritten;
    if ( pos>bufferSize ) {
      flush();
      pos = 0;
    }
  }

}
