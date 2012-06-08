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
	// Offset richtig als schreib-zeiger?
	public int insert(AbstractRecord record){
		//TODO: Implement this method
		
		byte[] recVal = new byte[slotSize];
		
		if(recordFitsIntoPage(record)){
			for (int i = 0; i < record.values.length; i++) {
	            recVal = record.getValue(i).serialize();
				
	            
	            for (int j = 0; j < recVal.length; i++){
	            	data[offset+j] = recVal[j];	
	            }
	            offset += recVal.length;
				            
	        }			
			
		} else{
			//throw new Exception;
		}		
		
		return offset;
	}
	
	@Override
	public void read(int slotNumber, AbstractRecord record){
		//TODO: Implement this method
		int readPos = slotSize*slotNumber;
		
		byte b = data[readPos];
		byte[] tmp = new byte[1];
		
		//TODO: herausfinden, wie viele Attribute im Datensatz sind
			
		if(record.getVariableLength()<0){	//Wenn fixed lengt => SQLInteger
			SQLInteger sqlint = new SQLInteger();
			
			
			tmp[0] = b;
			
			sqlint.deserialize(tmp);
			record.setValue(0, sqlint);
			
		}else{ //else SQLVarchar
			SQLVarchar sqlchar= new SQLVarchar(record.getVariableLength());
			
			tmp[0] = b;
			
			sqlchar.deserialize(tmp);
			
			record.setValue(0, sqlchar);
		}
		
	}
}
