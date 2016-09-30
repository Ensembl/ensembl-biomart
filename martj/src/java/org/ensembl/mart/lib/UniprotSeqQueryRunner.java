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

import org.ensembl.mart.lib.config.AttributeDescription;
import org.ensembl.mart.util.FormattedSequencePrintStream;

public class UniprotSeqQueryRunner extends BaseSeqQueryRunner {

    private String seqField = null;
    private int seqIndex = -1;
    private String curSequence = null;
    private Hashtable headerinfo = new Hashtable();
    private Logger logger = Logger.getLogger(TranscriptFlankSeqQueryRunner.class.getName());
    
    public UniprotSeqQueryRunner(Query query, FormatSpec format, OutputStream os) {
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
        Attribute[] exportable = query.getSequenceDescription().getFinalLink();
        
        queryID = exportable[0].getField();
        qualifiedQueryID = exportable[0].getTableConstraint()+"."+queryID;
        
        String seqType = query.getSequenceDescription().getSeqType();
        AttributeDescription seqDesc = query.getSequenceDescription().getRefDataset().getAttributeDescriptionByInternalName(seqType);
        query.addAttribute(new FieldAttribute(seqDesc.getField(), 
                                              seqDesc.getTableConstraint(), 
                                              seqDesc.getKey()));
        seqField = seqDesc.getField();
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
          else if (column.equals(seqField) && seqIndex < 0)
            seqIndex = i;
          else
            otherIndices.add(new Integer(i));
        }
      }
        while (rs.next()) {
          lastID = rs.getInt(queryIDindex);
          curSequence = rs.getString(seqIndex);
          
          if (curSequence != null) {
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
              curSequence = null;
          }
          
          totalRows++;
          totalRowsThisExecute++;
          resultSetRowsProcessed++;
        }
    }

    private final SeqWriter tabulatedWriter = new SeqWriter() {
        void writeSequences(Integer geneID, Connection conn) throws SequenceException {
            if (curSequence != null) {
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
                    
                    osr.print(curSequence); 
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
            if (curSequence != null) {
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
                    
                    osr.print(curSequence); 
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
