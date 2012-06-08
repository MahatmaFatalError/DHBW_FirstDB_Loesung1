package de.dhbw.db2.youdb.storage;

import de.dhbw.db2.youdb.storage.types.AbstractSQLValue;

public class Record extends AbstractRecord {

	/**
	 * Constructor for a record with a given number of attributes
	 * @param attNumber
	 */
	public Record(int attNumber) {
		super(attNumber);
	}

	@Override
	public int getFixedLength(){
		int length = 0;
		for(AbstractSQLValue value: this.values){
			length += value.getFixedLength();
		}
		return length;
	}
	
	@Override
	public int getVariableLength(){
		int length = 0;
		for(AbstractSQLValue value: this.values){
			length += value.getVariableLength();
		}
		return length;
	}
	
	@Override
	public String toString(){
		StringBuffer buffer = new StringBuffer();
		buffer.append("(");
		for(AbstractSQLValue value: this.values){
			buffer.append(value);
			buffer.append(" ");
		}
		buffer.append(")");
		return buffer.toString();
	}
	
	@Override
	public boolean equals(Object o){
		AbstractRecord cmp = (AbstractRecord)o;
		
		if(this.values.length != cmp.getValues().length)
			return false;
		
		int i=0;
		for(AbstractSQLValue cmpValue: cmp.getValues()){
			//check if types match
			if(cmpValue.getType()!=this.values[i].getType())
				return false;
			
			//check if values match
			if(!cmpValue.equals(this.values[i]))
				return false;
			
			i++;
		}
		return true;
	}
	
	@Override
	public Record clone(){
		Record record = new Record(this.values.length);
		int i=0;
		for(AbstractSQLValue value: this.values){
			record.values[i] = value.clone();
			i++;
		}
		return record;
	}

	@Override
	public AbstractRecord append(AbstractRecord rec) {
		Record newRec = new Record(this.values.length + rec.values.length);
		
		int i=0;
		for(AbstractSQLValue val: this.values)
			newRec.values[i++] = val;
		
		for(AbstractSQLValue val: rec.values)
			newRec.values[i++] = val;
		
		return newRec;
	}
}
