package de.dhbw.db2.youdb.storage.types;

/**
 * SQL varchar value
 * @author cbinnig
 *
 */
public class SQLVarchar extends AbstractSQLValue {
	public static int LENGTH = 8; //fixed length
	
	private byte[] data; //cache for byte representation
	private String value; //String value
	
	/**
	 * Constructor with default value and max. length 
	 * @param maxLength
	 */
	public SQLVarchar(int maxLength){
		super(SQLType.SqlVarchar, maxLength, false);
		this.value = "";
		this.data = this.serialize();
	}
	
	/**
	 * Constructor with string value and max. length 
	 * @param value
	 * @param maxLength
	 */
	public SQLVarchar(String value, int maxLength){
		super(SQLType.SqlVarchar, maxLength, false);
		this.value = value;
		this.data = this.serialize();
	}
	
	/**
	 * Get string value of SQLVarchar
	 * @return
	 */
	public String getValue() {
		return value;
	}
	
	/**
	 * Set string value of SQLVarchar
	 * @param value
	 */
	public void setValue(String value) {
		this.value = value;
		this.data = this.serialize();
	}
	
	@Override
	public int getFixedLength() {
		return LENGTH;
	}
	
	@Override
	public int getVariableLength() {
		return this.data.length;
	}
	
	@Override
	public byte[] serialize() {
		data = value.getBytes();
		return data;
	}

	@Override
	public void deserialize(byte[] data) {
		value = new String (data);
	}
	
	@Override
	public String toString(){
		return this.value;
	}
	
	@Override
	public boolean equals(Object o){
		SQLVarchar cmp = (SQLVarchar)o;
		if(this.getMaxLength() == cmp.getMaxLength() && this.value.equals(cmp.value))
			return true;
		
		return false;
	}
	
	@Override
	public int compareTo(AbstractSQLValue o) {
		SQLVarchar cmp = (SQLVarchar)o;
		return this.value.compareTo(cmp.value);
	}
	
	@Override
	public SQLVarchar clone(){
		return new SQLVarchar(this.value, this.maxLength);
	}

	@Override
	public void parseValue(String value) {
		this.value = value;
		this.data = value.getBytes();
	}
}
