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

package org.ensembl.mart.editor;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;


public class DatasetConfigTreeNodeSelection implements Transferable {

    DatasetConfigTreeNode node;

    public DatasetConfigTreeNodeSelection(DatasetConfigTreeNode node) {
            this.node = node;
    }

    public DataFlavor[] getTransferDataFlavors() {
        //Return an array of DataFlavors that represents the object
        DataFlavor[] df = new DataFlavor[1];
        try {
            df[1] = new DataFlavor(Class.forName("org.ensembl.mart.editor.DatasetConfigTreeNode"), "treeNode");
            return df;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    public boolean isDataFlavorSupported(DataFlavor df) {
        //Test if the DataFlavor supplied is supported
        if(df.getDefaultRepresentationClassAsString().equals("org.ensembl.mart.editor.DatasetConfigTreeNode"))
            return true;
        else
            return false;
    }

    public Object getTransferData(DataFlavor df) {
        //Return the object represented by the supplied DataFlavor
        return node;
    }

}
