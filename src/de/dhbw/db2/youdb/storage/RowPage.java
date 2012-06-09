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
		
		//byte[] recordValues = new byte[slotSize];
		byte[] attributeValue = new byte[record.values.length];
		
		int writePos = slotNumber * slotSize;
		
		
		//TODO: Alten Datensetz lesen, um varchars komplett zu loeschen	
		//Record oldRecord = new Record();
		//this.read(slotNumber, oldRecord);
		
		if(doInsert){
			if(recordFitsIntoPage(record)){
				
			}
						
			//RowPage newPage = new RowPage(slotSize);
			
			
			
			
		} else{
			
			for (int i = 0; i < record.values.length; i++) {
				attributeValue = record.getValue(i).serialize();
				
				if(record.getValue(i).isFixedLength()){	//int					
					
					//Datensatz loeschen, Varchar values loeschen fehlt...
					for (int j = 0; j < slotSize; i++){
		            	data[writePos+j] = (byte) 0;	
		            }
					
					//neuen 
					for (int j = 0; j < attributeValue.length; j++){
		            	data[writePos+j] = attributeValue[j];	
		            }
					writePos += attributeValue.length;
		            
				} else {								//varchar
					offsetEnd -= attributeValue.length;
					
					for (int j = 0; j < attributeValue.length; j++){
		            	data[offsetEnd+j] = attributeValue[j];	
		            }
					
					offsetEnd -= attributeValue.length;
					
					data[writePos] = (byte) attributeValue.length;
					data[++writePos] = (byte) offsetEnd;
					
				}
			}
		}
		
		
		
	}
	
	@Override
	public int insert(AbstractRecord record){
		//TODO: Implement this method
		
		
		byte[] attributeValue = new byte[record.values.length];	
				
		if(recordFitsIntoPage(record)){
			for (int i = 0; i < record.values.length; i++) {
				attributeValue = record.getValue(i).serialize();
				
				if(record.getValue(i).isFixedLength()){	//int
					
					for (int j = 0; j < attributeValue.length; j++){
		            	data[offset+j] = attributeValue[j];	
		            }
		            offset += attributeValue.length;
		            
				} else {								//varchar
					offsetEnd -= attributeValue.length;
					
					for (int j = 0; j < attributeValue.length; j++){
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
		
		//TODO: herausfinden, wie viele Attribute im Datensatz sind
		
		for (int i = 0; i < record.values.length; i++){	//woher weis ich denn, wie lang der scheiss value ist?
			if(record.getValue(i).isFixedLength()){		//int lesen
				SQLInteger sqlint = new SQLInteger();
				
				for (int j = 0; j < record.getValue(i).getFixedLength(); j++){
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
					byteValue[j] = data[varOffset+j];
				}
				
				sqlchar.deserialize(byteValue);
				
				record.setValue(i, sqlchar);
				
			}
			
		}
		
	}
}
