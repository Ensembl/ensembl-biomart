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

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * @author <a href="mailto:craig@ebi.ac.uk">Craig Melsopp</a>
 *
 */
public class StringUtil {

  /**
   * Splits str into separate lines suitable for "wrapped" displays. 
   * The length of each string is <= maxLength.
   * Splits are made at word boundaries unless a word is longer than the
   * maxLineLength in which case the word is split into to two or more lines. 
   * @param str string to be split
   * @param maxLineLength maximum number of characters in a line.
   * @return array containing zero or more lines.
   */
  public static String[] splitLines(String str, int maxLineLength) {

    List l = new ArrayList();

    // extract the words
    for (StringTokenizer st = new StringTokenizer(str); st.hasMoreTokens();)
      l.add(st.nextToken());

    // split long strings
    int i = 0;
    while (i < l.size()) {
      String s = (String) l.get(i);
      if (s.length() > maxLineLength) {
        l.remove(i);
        l.add(i, s.substring(0, maxLineLength - 1));
        l.add(++i, s.substring(maxLineLength - 1));
      } else {
        ++i;
      }
    }

    // join the words to make lines
    StringBuffer line = new StringBuffer();
    List l2 = new ArrayList();
    int j = 0, n = l.size();
    while (j < n) {
      String s = (String) l.get(j);
      if (line.length() + s.length() > maxLineLength) {
        // remove space inserted at end of line
        l2.add(line.toString());
        line.delete(0, line.length());
      } else {
        line.append(s).append(" ");
        j++;
      }
    }

    l2.add(line.toString());

    return (String[]) l2.toArray(new String[l2.size()]);
  }

  /**
   * Converts str into a multiline HTML string. If str.length()<=maxLineLength and convertShortLines is
   * false then str is returned unchanged.
   * 
   * The returned string begins &lt;HTML&gt; and ends with &lt;/HTML&gt;. 
   * All but the first line are wrapped in   
   * &lt;BR&gt; and &lt;/br&gt; tags. 
   * 
   * @param str string to be converted to multiline html string.
   * @param maxLineLength maximum number of characters in a line.
   * @param convertShortLines whether short lines should be wrapped in HTML or returned unchanged.
   * @return HTML representation of str, or str .
   */
  public static String wrapLinesAsHTML(String str, int maxLineLength, boolean convertShortLines) {
    
    if (str==null) return "";

    if (!convertShortLines && str.length()<=maxLineLength)
      return str;

    String[] lines = splitLines(str, maxLineLength);

    StringBuffer buf = new StringBuffer();
    buf.append("<HTML>");

    for (int i = 0; i < lines.length; i++) {
    
      if (i > 0)
        buf.append("<BR>");
    
      buf.append(lines[i]);
    
      if (i > 0)
        buf.append("</BR>");
    }

    buf.append("</HTML>");

    return buf.toString();
  }

  public static void main(String[] args) {
    String s = "please bring the glass to the bar called somereallylongname";
    System.out.println("in = " + s);
    System.out.println(
      "out = " + org.ensembl.util.StringUtil.toString(splitLines(s, 10)));

    System.out.println("out = " + wrapLinesAsHTML(s, 10,true));
    
    s = "bob";
    System.out.println("in = " + s);
    System.out.println("out = " + wrapLinesAsHTML(s,10,true));
    System.out.println("out = " + wrapLinesAsHTML(s, 10,false));
  }

}
