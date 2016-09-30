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
 
package org.ensembl.mart.lib;

/**
 * Class for holding the location of a portion of sequence in the dna chunks
 * table.  A Sequence Location is specified by chromosome, start, end, and strand.
 * Start and end can be modified by extendRightFlank and extendLeftFlank.
 * Start and end can be updated to new values.
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 */
public class SequenceLocation {

     private final String chr;
     private final int start;
     private final int end;
     private final int strand; // -1 is revearse, 1 is forward
     private final int hashcode; // cache the hashcode once it is calculated

	public SequenceLocation(String chr, int start, int end, int strand) {
     	this.chr = chr;
     	if (start > 1)
     	  this.start = start;
     else
        this.start = 1; // sometimes client will ask for more sequence than is available, give as much as is available
     	this.end = end;
     	this.strand = strand;
     	
        int tmp = chr.hashCode();
        tmp = (31 * tmp) + start;
        tmp = (31 * tmp) + end;
        tmp = (31 * tmp) + strand;
        
        hashcode = tmp;
     }

	/**
	 * Returns the start position.
	 * 
	 * @return int start
	 */
	public int getStart() {
		return start;
	}
       
	/**
	 * Returns the end position.
	 * 
	 * @return int end
	 */
	public int getEnd() {
		return end;
	}
    
	/**
	 * Returns the strand
	 * 
	 * @return int strand
	 */
	public int getStrand() {
		return strand;
	}
    
	/**
	 * Returns the Chromosome name.
	 * 
	 * @return int chromosome
	 */
	public String getChr() {
		return chr;
	}
    
	/**
	 * Returns a new SequenceLocation object with extended RightFlank 
	 * coordinate according to strand.
	 * 
	 * @param int length
	 * @return SequenceLocation
	 */
	public SequenceLocation extendRightFlank(int length) {
		// one of these will get updated
		int newstart = start;
		int newend = end;
		
		if (strand == -1) {
			newstart = start - length;
			if (start < 1)
				newstart = 1; // sometimes requested flank length exceeds available sequence.
		}
		else
			newend = end + length;
	    
	    return new SequenceLocation(this.chr, newstart, newend, this.strand);
	}
    
	/**
	 * Returns a new SequenceLocation with extended LeftFlank coordinate by length, 
	 * according to the strand.
	 * 
	 * @param int length
	 * @return SequenceLocation
	 */
	public SequenceLocation extendLeftFlank(int length) {
		int newstart = start;
		int newend = end;
		
		if (strand == -1)
			newend = end + length;
		else {
			newstart = start - length;
			if (start < 1)
				newstart = 1;
		}
		return new SequenceLocation(this.chr, newstart, newend, this.strand);
	}
	
	public SequenceLocation getLeftFlankOnly(int length) {
		int newstart = start;
		int newend = end;
		
		if (strand == -1) {
		  newstart = end + 1;
		  newend = end + length;
		}
		else {
		   newstart = start - length;
		   newend = start - 1;
		}
		return new SequenceLocation(this.chr, newstart, newend, this.strand);
	}
	
	public SequenceLocation getRightFlankOnly(int length) {
		int newstart = start;
		int newend = end;
		
		if (strand == -1) {
			newstart = start - length;
			newend = start - 1;
		}
		else {
			newstart = end + 1;
			newend = end + length;
		}
		return new SequenceLocation(this.chr, newstart, newend, this.strand);
	}
	
	public boolean equals(Object o) {
		// test object
		if (! (o instanceof SequenceLocation) )
		   return false;
		
		SequenceLocation l = (SequenceLocation) o;
		
		// test chr
		if (! (this.chr.equals(l.getChr()) ) )
		    return false;
		    
		// test start
		if (! (this.start == l.getStart()))
		    return false;
		    
		// test end
		if (! (this.end == l.getEnd()))
			return false;
			
	    // test strand
		if (! (this.strand == l.getStrand()))
		    return false;
		
		return true;
	}
	
	public int hashCode() {
		
		return hashcode;
	}
	
	/**
	 * Prints out a descriptive representation for debugging.
	 */
	public String toString() {
		 StringBuffer location = new StringBuffer();
		 
		 location.append("chr="+getChr());
		 location.append(" "+"start="+getStart());
		 location.append(" "+"end="+getEnd());
		 location.append(" "+"strand="+getStrand());
		 
		 return location.toString();    
	}
}
