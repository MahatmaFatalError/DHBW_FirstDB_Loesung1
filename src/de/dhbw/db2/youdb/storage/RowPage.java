package de.dhbw.db2.youdb.storage;

import java.util.Arrays;

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
		
		int writePos = slotNumber * slotSize;
		
		if(doInsert){
			if(recordFitsIntoPage(record)){
				
				//Daten nach dem Slot slotNumber auslagern
				byte[] shifted = Arrays.copyOfRange(data, writePos, offset);

				offset = writePos;
				insert(record);
				
				//ausgelagerte Daten wieder in urspruenlches Array nach dem neuen Datensatz anhaengen
				for (int i = 0; i < shifted.length; i++) {
					data[offset++] = shifted[i];
				}				
			}
			
		} else{
			
			int oldOffset = offset;
			
			offset = writePos;
			
			insert(record);
			
			//reset
			numRecords--;
			offset= oldOffset;
		}		
		
	}
	
	@Override
	public int insert(AbstractRecord record){		
		
		byte[] attributeValue;	
				
		if(recordFitsIntoPage(record)){
			for (int i = 0; i < record.values.length; i++) {
				attributeValue = record.getValue(i).serialize();
				
				if(record.getValue(i).isFixedLength()){	//int
					
					for (int j = 0; j < attributeValue.length; j++){
		            	data[offset++] = attributeValue[j];	
		            }
		            
				} else {								//varchar
					offsetEnd -= record.getValue(i).getVariableLength();
					
					SQLInteger varcharReference = new SQLInteger(offsetEnd);
					SQLInteger varcharLength = new SQLInteger(record.getValue(i).getVariableLength());
					
					byte[] referenceBytes = varcharReference.serialize();
					byte[] lengthBytes = varcharLength.serialize();
					
					for (int j = 0; j < referenceBytes.length; j++){
		            	data[offset++] = referenceBytes[j];	
		            }
					
					for (int j = 0; j < lengthBytes.length; j++){
		            	data[offset++] = lengthBytes[j];	
		            }
					
					for (int j = 0; j < attributeValue.length; j++){
		            	data[offsetEnd++] = attributeValue[j];	
		            }
					
					//reset (um beim neachsten varchar den alten nicht zu ueberschreiben)
					offsetEnd -= record.getValue(i).getVariableLength();
					
				}
				numRecords++;				      
	        }			
			
		} else{
			throw new IllegalArgumentException("Record is too big for this page");
		}		
		
		// return Slotnumber
		return numRecords;
	}
	
	@Override
	public void read(int slotNumber, AbstractRecord record){
		//TODO: Implement this method
		int readPos = slotSize*slotNumber;		
				
		for (int i = 0; i < record.values.length; i++){
			if(record.getValue(i).isFixedLength()){		//int lesen
				SQLInteger sqlint = new SQLInteger();
				byte[] byteValue = new byte[slotSize];
				
				for (int j = 0; j < record.getValue(i).getFixedLength(); j++){
					byteValue[j] = data[readPos++];
				}				
				
				sqlint.deserialize(byteValue);
				
				record.setValue(i, sqlint);				
				
			} else {									//varchar lesen
				SQLInteger sqlint = new SQLInteger();
				SQLVarchar sqlvarchar;
				
				int referencePosition;
				int length;
				
				byte[] referenceBytes = new byte[SQLInteger.LENGTH];
				byte[] lengthBytes = new byte[SQLInteger.LENGTH]; 
				
				for(int j = 0; j < referenceBytes.length; j++){
					referenceBytes[j] = data[readPos++];
				}
				
				sqlint.deserialize(referenceBytes);
				referencePosition = sqlint.getValue();
				
				for(int j = 0; j < lengthBytes.length; j++){
					lengthBytes[j] = data[readPos++];
				}
				
				sqlint.deserialize(lengthBytes);
				length = sqlint.getValue();
				
				//position und laenge des varchars ausgelesen, nun Zeichenkette lesen
				byte[] byteValue = new byte[length];
				for(int j = 0; j < length; j++){
					byteValue[j] = data[referencePosition+j];
				}
				
				sqlvarchar =  new SQLVarchar(length);
				
				sqlvarchar.deserialize(byteValue);
				
				record.setValue(i, sqlvarchar);
				
			}
			
		}
		
	}
}
