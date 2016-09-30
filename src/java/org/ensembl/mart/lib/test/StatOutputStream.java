package org.ensembl.mart.lib.test;

import java.io.IOException;
import java.io.OutputStream;

public class StatOutputStream extends OutputStream {

	private int bytecount = 0; // will hold the total number of bytes written
    private int linecount = 0; // will count newline bytes
    private byte newline = new String("\n").getBytes()[0]; // byte for a newline
    
    public void close() {
        bytecount = 0;
	}

    public void flush() {
	}

    public void write(int b) throws IOException {
        if (b == 0)
            throw new IOException("Dumpster Error: must write 1 byte of output\n");
        bytecount++;

        if (b == newline)
             linecount++;
	}

    public void write(byte[] b) throws IOException {
        if (b.length == 0)
            throw new IOException("Dumpster Error: must write 1 byte of output\n");
        bytecount += b.length;
        
        for(int i = 0; i < b.length; ++i) {
            if (b[i] == newline)
                linecount++;
        }
    }

    public void write(byte[] b, int off, int len) throws IOException {
        if(b.length == 0)
            throw new IOException("Dumpster Error: must write 1 byte of output\n");

        if (off > b.length || len > b.length || off + len > b.length)
            throw new IOException("Dumpster Error: provided byte array does not have enough bytes for offset and length\n");

        bytecount += len;
        
        for (int i = off; i < off + len; ++i) {
            if (b[i] == newline)
                linecount++;
        }
	}
        
    public int getCharCount() {
        int thisbytecount = bytecount;
        bytecount = 0;
        return thisbytecount;
	}
    
    public int getLineCount() {
        int thislinecount = linecount;
        linecount = 0;
        return thislinecount;
    }
    
    public String toString() {
        StringBuffer buf = new StringBuffer();

        buf.append("[");
        buf.append(" ,charCount=").append( bytecount );
        buf.append(" ,lineCount=").append( linecount );
        buf.append("]");

              return buf.toString();
    }
}
