package de.dhbw.db2.youdb.storage;

/**
 * Page managetr to creates new pages
 * @author cbinnig
 *
 */
public class PageManager {
	protected static int nextPageNumber = 0; //counter for unique page numbers in table
	
	/**
	 * Creates a page for a given page type and slot size
	 * @param type Enum PageType
	 * @param slotSize Size of lot in bytes
	 * @return
	 */
	public static AbstractPage createPage(EnumPageType type, int slotSize){
		AbstractPage page = null;
		
		switch(type){
		case RowPageType:
			page = new RowPage(slotSize);
			break;
		}
		page.setPageNumber(nextPageNumber++);
		return page;
	}
	
	/**
	 * Creates a page with a default pyge type and a given slot size
	 * @param slotSize
	 * @return
	 */
	public static AbstractPage createDefaultPage(int slotSize){
		return createPage(EnumPageType.RowPageType, slotSize);
	}
}
