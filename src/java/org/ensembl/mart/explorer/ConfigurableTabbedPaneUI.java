/*
 * This class is derived from a file downloaded from the internet. The original source 
 * of that file is unclear (accroding to the preson publishing it)
 * so there is no copyright restriction on the file.  
 */

package org.ensembl.mart.explorer;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Insets;

import javax.swing.UIManager;
import javax.swing.plaf.basic.BasicTabbedPaneUI;

/**
 * UI component for rendering tabs where the background color of the
 * selected tab can be set.
 */
public class ConfigurableTabbedPaneUI extends BasicTabbedPaneUI {
  
 
	// default color is decided by the Default UI.
	Color selectedTabBgColor = UIManager.getColor("TabbedPane.lightHighlight");
  
  // No insets so that the "tab" part of the tab and it's page have 
  // no gaps.
	Insets contentBorderInsets = new Insets(0, 0, 0, 0);

	public ConfigurableTabbedPaneUI(Color color) {
		selectedTabBgColor = color;
	}

	public void setSelectedTabBgColor(Color color) {
		selectedTabBgColor = color;
	}

	// Overriding method. defines the offset from the top of the
	// tabbed pane to the component it contains.
	protected Insets getContentBorderInsets(int tabPlacement) {
		return contentBorderInsets;
	}

	// Overriding method. paints the background of the tabs.
	protected void paintTabBackground(
		Graphics g,
		int tabPlacement,
		int tabIndex,
		int x,
		int y,
		int w,
		int h,
		boolean isSelected) {
		g.setColor(tabPane.getBackgroundAt(tabIndex));
		// these two lines are all that makes this method
		// different from the paintTabBackground() in 
		// BasicTabbedPaneUI. we want to force the background
		// color of the tab to be selectedTabBgColor, not
		// the default one from the tabbed pane. (light gray)
		if (isSelected)
			g.setColor(selectedTabBgColor);

		switch (tabPlacement) {
			case LEFT :
				g.fillRect(x + 1, y + 1, w - 2, h - 3);
				break;
			case RIGHT :
				g.fillRect(x, y + 1, w - 2, h - 3);
				break;
			case BOTTOM :
				g.fillRect(x + 1, y, w - 3, h - 1);
				break;
			case TOP :
			default :
				// draw a rectangle with a "cropped" top left corner
        final int crop = 7;
        int[] xPos = { x+1+crop,  x+w+1,    x+w+1,   x+1,  x+1 };
        int[] yPos = { y+1,       y+1,    y+h,   y+h,  y+1+crop };
        g.fillPolygon( xPos, yPos, 5 );
		}
	}
}
