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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Effectively closes itself once maxBytes is reached by returning -1 from all
 * the read(...) methods. This class will n bytes where n<=maxBytes even if the contained
 * inputStream has more than n bytes available.
 */
public class MaximumBytesInputFilter extends FilterInputStream {

  private int maxBytes;
  private int pos = 0;

	/**
	 * @param in
	 */
	public MaximumBytesInputFilter(InputStream in, int maxBytes) {
		super(in);
    this.maxBytes = maxBytes;
	}



	public int read() throws IOException {
    return update( super.read() );
	}

	public int read(byte[] b, int off, int len) throws IOException {
		return update( super.read(b, off, len) );
	}

	public int read(byte[] b) throws IOException {
    return update( super.read(b) );
	}

  private int update(int c ) {
    if ( c==-1 ) return -1;
    else {
      pos += c;
      if ( pos>maxBytes ) return c - (pos-maxBytes );
      else return c; 
    }
    
  }

}
