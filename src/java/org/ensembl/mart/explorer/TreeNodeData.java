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
import org.ensembl.mart.lib.Attribute;
import org.ensembl.mart.lib.BooleanFilter;
import org.ensembl.mart.lib.Filter;
import org.ensembl.mart.lib.IDListFilter;
import org.ensembl.mart.lib.Query;
import org.ensembl.mart.lib.SequenceDescription;
import org.ensembl.mart.lib.config.FilterDescription;
/**
 * Used to create the label on the TreeNode it added to and store optional
 * attribute and filter objects.
 * 
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp </a>
 */
public class TreeNodeData {
  public static final class Type {
    private String label;

    public Type(String label) {
      //assert label != null;
      this.label = label;
    }

    /**
     * @return Returns the label.
     */
    public String getLabel() {
      return label;
    }
  }
  private SequenceDescription sequenceDescription;
  ;
  public static final Type DATASOURCE = new Type("Mart database");
  public static final Type DATASET = new Type("Dataset");
  public static final Type ATTRIBUTES = new Type("Attributes");
  public static final Type FILTERS = new Type("Filters");
  public static final Type FORMAT = new Type("Format");

  public static final TreeNodeData createDataSourceNode() {
    return new TreeNodeData(DATASOURCE, ":", null);
  };
  public static final TreeNodeData createDatasetNode() {
    return new TreeNodeData(DATASET, ":", null);
  };
  public static final TreeNodeData createAttributesNode() {
    return new TreeNodeData(ATTRIBUTES, null, null);
  };
  public static final TreeNodeData createFilterNode() {
    return new TreeNodeData(FILTERS, null, null);
  }
  public static final TreeNodeData createFormatNode() {
    return new TreeNodeData(FORMAT, null, null);
  }
  private Type type;
  private String separator;
  private String rightText;
  private Attribute attribute;
  private Filter filter;

  private boolean useLeftText = true;

  private TreeNodeData(
    Type type,
    String separator,
    String rightText,
    Attribute attribute,
    Filter filter) {
    this.type = type;
    this.separator = separator;
    this.rightText = rightText;
    this.attribute = attribute;
    this.filter = filter;
  }

  public TreeNodeData(
    Type type,
    String separator,
    String rightText,
    Attribute attribute) {
    this(type, separator, rightText, attribute, null);
  }

  public TreeNodeData(
    Type type,
    String separator,
    String rightText,
    Filter filter) {
    this(type, separator, rightText, null, filter);
  }

  public TreeNodeData(Type type, String separator, String rightText) {
    this(type, separator, rightText, null, null);
  }

  /**
   * Creates a TreeNodeData instance containing the filter and
   * with a label derived from the query.datasetConfig and the filter.
   * @param query
   * @param filter
   */
  public TreeNodeData(Query query, Filter filter) {

    this.filter = filter;

    // use rawfield as default for label
    String fieldName = filter.getField();
    
    // Try to get a user friendly fieldName, 
    // otherwise use the raw one from filter
    if (query.getDatasetConfig() != null) {
//TODO: fix faulty search for Boolean filters
      FilterDescription fd =
        query
          .getDatasetConfig()
          .getFilterDescriptionByFieldNameTableConstraint(
          filter.getField(),
          filter.getTableConstraint(),
          filter.getQualifier());

      String tmpName = null;
      if (fd != null)
        tmpName =
          fd.getDisplayNameByFieldNameTableConstraint(
            filter.getField(),
            filter.getTableConstraint(),
            filter.getQualifier());
            
        if (tmpName != null)
          fieldName = tmpName;

    }
    
    // Try to make the qualifier prettier
    String qualifier = filter.getQualifier();
    if (filter instanceof BooleanFilter && qualifier != null) {
      qualifier = qualifier.toLowerCase();

      if (qualifier.matches("\\s*is\\s+null\\s*"))
        qualifier = "excluded";
      else if (qualifier.matches("\\s*is\\s+not\\s+null\\s*"))
        qualifier = "required";
    }

    if (filter instanceof IDListFilter) {
      IDListFilter f = (IDListFilter) filter;
      if (f.getFile() != null)
        qualifier = "in " + f.getFile();
      else if (f.getUrl() != null)
        qualifier = "in " + f.getUrl();
      else if (f.getIdentifiers() != null && f.getIdentifiers().length != 0)
        qualifier = "in list";
    }

    if (qualifier == null)
      qualifier = "";

    String value = filter.getValue();
    if (value == null)
      value = "";

    String tmp = fieldName + " " + qualifier + " " + value;

    // we need to make the special characters < and > safe for display in html
    this.rightText = tmp.replaceAll("<", "&lt;").replaceAll(">", "&gt;");

  }

  public TreeNodeData(SequenceDescription sequenceDescription) {
    this.sequenceDescription = sequenceDescription;
    this.rightText = sequenceDescription.getSeqDescription();
    this.type = ATTRIBUTES;
    this.separator = "";
    useLeftText = false;
  }


  /**
   * Generates a small piece of html that is appears as the label on
   * tree nodes.
   */
  public String toString() {
    
    StringBuffer buf = new StringBuffer();
    
    buf.append("<html>");

    if (useLeftText) {
      buf.append("<b>");
      if (type != null)
        buf.append(type.label);
      if (separator != null)
        buf.append(separator);
      buf.append("</b> ");
    }
    
    if (rightText != null)
      buf.append(rightText);
    buf.append("</html>");
    
    return buf.toString();
  }
  
  
  /**
   * @return attribute if set, otherwise null
   */
  public Attribute getAttribute() {
    return attribute;
  }
  /**
   * @return filter if set, otherwise null
   */
  public Filter getFilter() {
    return filter;
  }

  public String getLabel() {
    return type.label;
  }

  public String getRightText() {
    return rightText;
  }

  public String getSeparator() {
    return separator;
  }

  public void setRightText(String string) {
    rightText = string;
  }

  public Type getType() {
    return type;
  }

  public SequenceDescription getSequenceDescription() {
    return sequenceDescription;

  }
}
