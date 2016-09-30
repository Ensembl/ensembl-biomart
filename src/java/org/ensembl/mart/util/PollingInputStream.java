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
 * Rather than simply returning -1 from read(...) if it's contained inputStream
 * does it will keep sleeping for 500ms before trying again if alive==true.
 *  This behaviour is repeated
 * until it is not alive. Note that this sends the calling Thread to sleep.
 */
public class PollingInputStream extends FilterInputStream {

	private boolean live;
	private InputStream is;
	private IOException exception;


	public PollingInputStream(InputStream in) {
		super(in);
		this.is = in;
	}


	public int read() throws IOException {
		int c = -1;
		for (c = is.read(); c == -1 && live; c = is.read())
			sleep();
		return c;
	}




	public int read(byte[] b, int off, int len) throws IOException {
    int c = -1;
    for (c = is.read(b, off, len); c == -1 && live; c = is.read(b, off, len))
      sleep();
    return c;
	}

	public int read(byte[] b) throws IOException {
    int c = -1;
    for (c = is.read(b); c == -1 && live; c = is.read(b))
      sleep();
    return c;

	}


  private void sleep() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


	public boolean isLive() {
		return live;
	}


	public void setLive(boolean b) {
		live = b;
	}

}
