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

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ensembl.mart.util.FormattedSequencePrintStream;

public class SNPSeqQueryRunner extends BaseSeqQueryRunner {

    private String alleleField = null;
    private int alleleIndex = -1;
    private SequenceLocation curLocation = null;
    private String curAllele = null;
    private Hashtable headerinfo = new Hashtable();
    private Logger logger = Logger.getLogger(TranscriptFlankSeqQueryRunner.class.getName());
    
    public SNPSeqQueryRunner(Query query, FormatSpec format, OutputStream os) {
        super(query);
        this.format = format;
        this.osr = new FormattedSequencePrintStream(maxColumnLen, os, true); //autoflush true
        
        switch (format.getFormat()) {
        case FormatSpec.TABULATED :
            this.separator = format.getSeparator();
            this.seqWriter = tabulatedWriter;
            break;
            
        case FormatSpec.FASTA :
            this.separator = "|";
            this.seqWriter = fastaWriter;
            break;
        }
    }

    protected void updateQuery() {
        queryID = "snp_id_key";
        qualifiedQueryID = "main.snp_id_key";
        
        query.addAttribute(new FieldAttribute(queryID, "main", queryID));
        Attribute[] exportable = query.getSequenceDescription().getFinalLink();
        
        chrField = exportable[0].getField();
        coordStart = exportable[1].getField();
        strandField = exportable[2].getField();
        alleleField = exportable[3].getField();
    }

    protected void processResultSet(Connection conn, ResultSet rs)
            throws IOException, SQLException {

      if (queryIDindex < 0) {
        ResultSetMetaData rmeta = rs.getMetaData();
        
        // process columnNames for required attribute indices
        for (int i = 1, nColumns = rmeta.getColumnCount(); i <= nColumns; ++i) {
          String column = rmeta.getColumnName(i);
          if (column.equals(queryID) && queryIDindex < 0)
            queryIDindex = i;
          else if (column.equals(coordStart) && startIndex < 0)
            startIndex = i;
          else if (column.equals(alleleField) && alleleIndex < 0)
            alleleIndex = i;
          else if (column.equals(chrField) && chromIndex < 0)
            chromIndex = i;
          else if (column.equals(strandField) && strandIndex < 0)
            strandIndex = i;
          else
            otherIndices.add(new Integer(i));
        }
      }
        while (rs.next()) {
          lastID = rs.getInt(queryIDindex);
          int start = rs.getInt(startIndex);

          if (start > 0) {
              // if start is not null, create a new SequenceLocation object from the chr, start, end, and strand
              String chr = rs.getString(chromIndex);
              curAllele = rs.getString(alleleIndex);
              int strand = rs.getInt(strandIndex);
                            
              curLocation = new SequenceLocation(chr, start, start, strand);
              
              // Rest can be duplicates, or novel values for a given field, collect lists of values for each field
              // currindex is now the last index of the DisplayIDs.  Increment it, and iterate over the rest of the ResultSet to print the description
              
              for (int i = 0, n = otherIndices.size(); i < n; i++) {
                  int currindex = ((Integer) otherIndices.get(i)).intValue();
                  if (rs.getString(currindex) != null) {
                      String field = attributes[currindex - 1].getField();
                      if (!fields.contains(field))
                          fields.add(field);
                      
                      String value = rs.getString(currindex);
                      
                      if (headerinfo.containsKey(field)) {
                          if (!((ArrayList) headerinfo.get(field)).contains(value))
                              ((ArrayList) headerinfo.get(field)).add(value);
                      } else {
                          List values = new ArrayList();
                          values.add(value);
                          headerinfo.put(field, values);
                      }
                  }
              }
              
              //will only do each exon once
              seqWriter.writeSequences(new Integer(0), conn);
              headerinfo = new Hashtable();
              curLocation = null;
              curAllele = null;
          }
          
          totalRows++;
          totalRowsThisExecute++;
          resultSetRowsProcessed++;
        }
    }

    private final SeqWriter tabulatedWriter = new SeqWriter() {
        void writeSequences(Integer geneID, Connection conn) throws SequenceException {
            if (curLocation != null) {
                try {
                    for (int j = 0, n = fields.size(); j < n; j++) {
                        if (j > 0)
                            osr.print(separator);
                        
                        String field = (String) fields.get(j);
                        if (headerinfo.containsKey(field)) {
                            ArrayList values = (ArrayList) headerinfo.get(field);
                            
                            for (int vi = 0; vi < values.size(); vi++) {
                                if (vi > 0)
                                    osr.print(",");
                                osr.print((String) values.get(vi));
                            }
                        }
                    }
                    
                    osr.print(separator);
                    
                    if (osr.checkError())
                        throw new IOException();
                    
                    SequenceDescription seqd = query.getSequenceDescription();
                    int start = 0;
                    int end = 0;
                    int leftFlank = 0;
                    int rightFlank = 0;                
                    
                    // modify snp curLocation coordinates depending on flank requested
                    if (seqd.getLeftFlank() > 0)
                        leftFlank = seqd.getLeftFlank();
                    if (seqd.getRightFlank() > 0)
                        rightFlank = seqd.getRightFlank();
                    
                    int offset = leftFlank;
                    if (curLocation.getStrand() < 0) {
                        start = curLocation.getStart() - rightFlank;
                        if (start < 1)
                            start = 1;
                        end = curLocation.getStart() + leftFlank;
                    } else {
                        start = curLocation.getStart() - leftFlank;
                        if (start < 1) {
                            offset = leftFlank + start - 1;
                            start = 1;
                        }
                        end = curLocation.getStart() + rightFlank;                    
                    }
                    
                    SequenceLocation thisLocation = new SequenceLocation(curLocation.getChr(), start, end, curLocation.getStrand());                
                    
                    byte[] sequence = dna.getSequence(thisLocation.getChr(), thisLocation.getStart(), thisLocation.getEnd());
                    
                    osr.write(sequence, 0, offset);
                    osr.print(" - " + curAllele + " - ");
                    osr.write(sequence, offset, sequence.length - offset);                
                    
                    osr.print("\n");
                    
                    if (osr.checkError())
                        throw new IOException();
                } catch (SequenceException e) {
                    if (logger.isLoggable(Level.WARNING))
                        logger.warning(e.getMessage());
                    throw e;
                } catch (IOException e) {
                    if (logger.isLoggable(Level.WARNING))
                        logger.warning("Couldnt write to OutputStream\n" + e.getMessage());
                    throw new SequenceException(e);
                }
            }
        }
    };
    
    private final SeqWriter fastaWriter = new SeqWriter() {
        void writeSequences(Integer geneID, Connection conn) throws SequenceException {
            if (curLocation != null) {
                try {
                    osr.print(">");
                    for (int j = 0, n = fields.size(); j < n; j++) {
                        if (j > 0)
                            osr.print(separator);
                        
                        String field = (String) fields.get(j);
                        if (headerinfo.containsKey(field)) {
                            ArrayList values = (ArrayList) headerinfo.get(field);
                            
                            for (int vi = 0; vi < values.size(); vi++) {
                                if (vi > 0)
                                    osr.print(",");
                                osr.print((String) values.get(vi));
                            }
                        }
                    }
                    
                    osr.print("\n");
                    
                    if (osr.checkError())
                        throw new IOException();
                    
                    SequenceDescription seqd = query.getSequenceDescription();
                    int start = 0;
                    int end = 0;
                    int leftFlank = 0;
                    int rightFlank = 0;                
                    
                    // modify snp curLocation coordinates depending on flank requested
                    if (seqd.getLeftFlank() > 0)
                        leftFlank = seqd.getLeftFlank();
                    if (seqd.getRightFlank() > 0)
                        rightFlank = seqd.getRightFlank();
                    
                    int offset = leftFlank;
                    if (curLocation.getStrand() < 0) {
                        start = curLocation.getStart() - rightFlank;
                        if (start < 1)
                            start = 1;
                        end = curLocation.getStart() + leftFlank;
                    } else {
                        start = curLocation.getStart() - leftFlank;
                        if (start < 1) {
                            offset = leftFlank + start - 1;
                            start = 1;
                        }
                        end = curLocation.getStart() + rightFlank;                    
                    }
                    
                    SequenceLocation thisLocation = new SequenceLocation(curLocation.getChr(), start, end, curLocation.getStrand());                
                    
                    byte[] sequence = dna.getSequence(thisLocation.getChr(), thisLocation.getStart(), thisLocation.getEnd());
                    
                    osr.write(sequence, 0, offset);
                    osr.print("\n" + curAllele + "\n");
                    osr.write(sequence, offset, sequence.length - offset);                
                    
                    osr.print("\n");
                    
                    if (osr.checkError())
                        throw new IOException();
                } catch (SequenceException e) {
                    if (logger.isLoggable(Level.WARNING))
                        logger.warning(e.getMessage());
                    throw e;
                } catch (IOException e) {
                    if (logger.isLoggable(Level.WARNING))
                        logger.warning("Couldnt write to OutputStream\n" + e.getMessage());
                    throw new SequenceException(e);
                }
            }
        }
    };
}
