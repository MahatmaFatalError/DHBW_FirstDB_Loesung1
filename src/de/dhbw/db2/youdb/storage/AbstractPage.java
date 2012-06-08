package de.dhbw.db2.youdb.storage;

import de.dhbw.db2.youdb.storage.types.AbstractSQLValue;
import de.dhbw.db2.youdb.storage.types.SQLInteger;

/**
 * Defines the interface of a page 
 * 
 * @author cbinnig
 *
 */
public abstract class AbstractPage {
	public static int PAGE_SIZE = 64; //kBytes
	public static int HEADER_SIZE = 128; //Bytes
	
	protected byte[] data = new byte[PAGE_SIZE*1024]; //array for page data
	protected int slotSize = 0; //fixed slot size in bytes
	protected int numRecords = 0; //number of records in page
	protected int offset = 0; //offset to next free slot (from page begin)
	protected int offsetEnd = data.length - HEADER_SIZE;  //offset for next variable length value (from page end)
	protected int pageNumber = 0;
	
	/**
	 * Creates a page with a given slot size in bytes
	 * @param recordLength
	 */
	public AbstractPage(int slotSize){
		this.slotSize = slotSize;
	}
	
	/**
	 * Get number of records in page
	 * @return
	 */
	public int getNumRecords() {
		return numRecords;
	}

	/**
	 * Returns page number
	 * @return
	 */
	public int getPageNumber() {
		return pageNumber;
	}

	/**
	 * Sets page number
	 * @param pageNumber
	 */
	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}
	
	/**
	 * Calculates the number of free bytes in a page
	 * @return
	 */
	public int getFreeSpace(){
		return offsetEnd-offset;
	}
	
	/**
	 * Calculates if a given record fits into page
	 * @param record
	 * @return
	 */
	public boolean recordFitsIntoPage(AbstractRecord record){
		int fixedLength = record.getFixedLength();
		int variableLength = record.getVariableLength();
		
		return (fixedLength+variableLength<=getFreeSpace());
	}
	
	/**
	 * Estimates minimum number of records which fit page 
	 * @param record
	 * @return
	 */
	public static int estimateRecords(AbstractRecord record){
		int maxRecordSize = 0;
		for(AbstractSQLValue value: record.getValues()){
			maxRecordSize += value.getMaxLength();
			if(!value.isFixedLength())
				maxRecordSize += (2*SQLInteger.LENGTH);
		}
		return PAGE_SIZE*1024/maxRecordSize;
	}

	/**
	 * Inserts a record into a page at a given slot number 
	 * while record could be updated (in place) or inserted (into new slot).
	 * @param slotNumber
	 * @param record
	 * @param insert
	 */
	public abstract void insert(int slotNumber, AbstractRecord record, boolean insert);
	
	/**
	 * Inserts a record into a page at the next free slot
	 * @param record
	 * @return slot number where record was inserted
	 */
	public abstract int insert(AbstractRecord record);
	
	/**
	 * Returns the record stored at a given slot number
	 * @param slotNumber Slot number
	 * @param record Returned record (must be filled default SqlValues with corresponding types)
	 */
	public abstract void read(int slotNumber, AbstractRecord record);
}
