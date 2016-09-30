/*
 * Created on Jan 22, 2004
 *
 * To change the template for this generated file go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
package org.ensembl.mart.explorer;

import java.awt.Component;

import javax.swing.JOptionPane;

import org.ensembl.mart.util.StringUtil;

/**
 * @author craig
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */
public class Feedback {

  private Component src;

  public Feedback(Component src) {
    this.src = src;
  }

  private void showMessageDialog(
    String label,
    String message,
    int dialogType) {

    JOptionPane.showMessageDialog(
      src,
      StringUtil.wrapLinesAsHTML(message, 80, true),
      label,
      dialogType);

  }

  /**
   * Displays message in information dialog box.
   * @param message message to be displayed
   */
  public void info(String message) {

    showMessageDialog("Information", message, JOptionPane.INFORMATION_MESSAGE);

  }

  /**
   * Displays message in warning dialog box.
   * @param message message to be displayed
   */
  public void warning(String message) {

    showMessageDialog("Warning", message, JOptionPane.WARNING_MESSAGE);

  }

  /**
   * Displays message plus exception.messge in warning dialog box 
   * and prints stack trace to console.
   * @param message message to display
   * @param e exception exception to be displayed
   */
  public void warning(String message, Exception e) {
    warning(message, e, true);
  }

  /**
   * Prints stacktrace and warning message to screen.
   * @param message message to display
   * @param e exception exception to be displayed
   * @param includeExceptionInWarning whether e.getMessage() should be displayed in warning dialog.
   */
  public void warning(
    String message,
    Exception e,
    boolean includeExceptionInWarning) {
    e.printStackTrace();
    if (includeExceptionInWarning)
      message = message + ":" + e.getMessage();
    warning(message);

  }

  /**
   * Displays warning dialog and prints stack trace to console.
   * @param e exception
   */
  public void warning(Exception e) {
    warning(e.getMessage());
    e.printStackTrace();
  }

  public static void main(String[] args) {
    Feedback f = new Feedback(null);
    f.warning(
      "A very long test message that goes on and on. It should have been reformated for better display. If not the code needs fixing!");
  }

}
