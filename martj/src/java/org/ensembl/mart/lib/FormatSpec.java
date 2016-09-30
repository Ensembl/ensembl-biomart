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
 * Object for defining the format of the output for a Query.
 * Output can be tabulated, or fasta.  Tabulated output should
 * have a separator defined, but, currently, this is not enforced by
 * the object.  Fasta formats should not have a separator defined, although
 * this is also not enforced. 
 * 
 * @author <a href="mailto:dlondon@ebi.ac.uk">Darin London</a>
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 */
public class FormatSpec {

    public static FormatSpec TABSEPARATEDFORMAT = new FormatSpec(FormatSpec.TABULATED, "\t");
    public static FormatSpec FASTAFORMAT = new FormatSpec(FormatSpec.FASTA);
    
    private String separator = null;
    private int format = -1;

    /**
     *  enums over TABULATED and FASTA, extend as needed
     *  client can setFormat with FormatSpec.TABULATED or FormatSpec.FASTA
     */
	  public static final int TABULATED = 1;
    public static final int FASTA = 2;

    /**
     * default constructor
     */
    public FormatSpec() {
    }
    
    /**
     * constructor for a fully qualified FormatSpec
     * eg., format and separator are set.  format can be set
     * with explicit use of the static TABULATED or FASTA variables.
     * 
     * @param format -- int
     * @param separator -- String field separator
     */
     public FormatSpec(int format, String separator) {
         this.format = format;
         this.separator = separator;    
     }
     
     /**
      * Constructor for when you just want to set FASTA, or 
      * you want to set the separator later. Defaults to
      * setting separator to tab ("\t").
      * 
      * @param format int
      */
     public FormatSpec(int format) {
     	this.format = format;
     	this.separator = "\t";
     }
     /**
      * set the Format for the FormatSpec.
      * Formats can be set with explicit use of the static TABULATED and FASTA
      * variables.
      * 
      * @param format int
      */
     public void setFormat(int format) {
         this.format = format;
	 }

     /**
      * returns the current format (one of TABULATED or FASTA)
      * 
      * @return format -- int
      */
     public int getFormat() { return format; }

     /**
      * set the field separator
      * 
      * @param separator -- String
      */
     public void setSeparator(String separator) {
         this.separator = separator;
	 }
            
     /**
      * get the field separator
      * 
      * @return String separator
      */
     public String getSeparator() {
         return separator;
	 }
	 
	 
	/** Object state.
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		
		StringBuffer buf = new StringBuffer();
		buf.append("[");
		buf.append("format=");
		if (format==FASTA)buf.append("FASTA");
		else if (format==TABULATED ) {
			buf.append("TABULATED, ");
			buf.append("separator=").append(separator);
		}
		buf.append("]");
		
		return buf.toString();
			}

}
