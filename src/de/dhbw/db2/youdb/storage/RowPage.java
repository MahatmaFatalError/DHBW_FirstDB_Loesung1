package de.dhbw.db2.youdb.storage;

import de.dhbw.db2.youdb.storage.types.SQLInteger;
import de.dhbw.db2.youdb.storage.types.SQLVarchar;



public class RowPage extends AbstractPage {

	/**
	 * Constructor for a row page with a given (fixed) slot size
	 * @param slotSize
	 */
	public RowPage(int slotSize) {
		super(slotSize);
	}
	
	@Override
	public void insert(int slotNumber, AbstractRecord record, boolean doInsert) {
		//TODO: Implement this method
		
		
	}
	
	@Override
	public int insert(AbstractRecord record){
		//TODO: Implement this method
		
		byte[] recordValues = new byte[slotSize];
		byte[] attributeValue = new byte[record.values.length];
		
				
		if(recordFitsIntoPage(record)){
			for (int i = 0; i < record.values.length; i++) {
				attributeValue = record.getValue(i).serialize();
				
				if(record.getValue(i).isFixedLength()){	//int
					
					for (int j = 0; j < attributeValue.length; i++){
		            	data[offset+j] = attributeValue[j];	
		            }
		            offset += attributeValue.length;
		            
				} else {								//varchar
					offsetEnd -= attributeValue.length;
					
					for (int j = 0; j < attributeValue.length; i++){
		            	data[offsetEnd+j] = attributeValue[j];	
		            }
					
					offsetEnd -= attributeValue.length;
					
					data[offset] = (byte) attributeValue.length;
					data[++offset] = (byte) offsetEnd;
					
				}
				numRecords++;
				      
	        }			
			
		} else{
			throw new IllegalArgumentException();
		}		
		
		return numRecords;
	}
	
	@Override
	public void read(int slotNumber, AbstractRecord record){
		//TODO: Implement this method
		int readPos = slotSize*slotNumber;
		
		byte[] byteValue = new byte[slotSize];	// = data[readPos];
		byte[] tmp = new byte[1];
		
		//TODO: herausfinden, wie viele Attribute im Datensatz sind
		
		for (int i = 0; i < record.values.length; i++){	//woher weis ich denn, wie lang der scheiss value ist?
			if(record.getValue(i).isFixedLength()){		//int lesen
				SQLInteger sqlint = new SQLInteger();
				
				for (int j = 0; i < 4; j++){
					byteValue[j] = data[readPos+j];
				}		
				
				readPos += 4;
				
				sqlint.deserialize(byteValue);
				
				record.setValue(i, sqlint);
				
				
			} else {									//varchar lesen
				SQLVarchar sqlchar= new SQLVarchar(record.getVariableLength());
				
				int length = data[readPos];
				int varOffset = data[++readPos]; 
				
				for(int j = 0; j < length; j++){
					byteValue[j] = data[readPos+j];
				}
				
				readPos += length;
				
				sqlchar.deserialize(byteValue);
				
				record.setValue(i, sqlchar);
				
			}
			
		}
		
	}
}
