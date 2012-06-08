package de.dhbw.db2.youdb.storage.types;

/**
 * SQL integer value
 * @author cbinnig
 *
 */
public class SQLInteger extends AbstractSQLValue {
	public static int LENGTH = 4; //fixed length
	private int value = 0; //integer value
	
	/**
	 * Constructor with default value
	 */
	public SQLInteger(){
		super(SQLType.SqlInteger, LENGTH);
		this.value = 0;
	}
	
	/**
	 * Constructor with value
	 * @param value Integer value
	 */
	public SQLInteger(int value){
		super(SQLType.SqlInteger, LENGTH);
		this.value = value;
	}

	/**
	 * Returns integer value of SQLInteger
	 * @return
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Sets integer value of SQLInteger
	 * @param value
	 */
	public void setValue(int value) {
		this.value = value;
	}
	
	@Override
	public byte[] serialize() {
		//TODO: Implement this method
            byte[] data = new byte[LENGTH];
            
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (value >>> (8*i));               
            }            
            return data;

	}

	@Override
	public void deserialize(byte[] data) {
		//TODO: Implement this method
            final int MASK = 0xFF;
            
            value = data[data.length-1];
            
            for (int i = data.length-2; i >= 0; i--) {
                byte b = data[i];
                
                value = value << 8;
                
                value = (value | (b & MASK));                    
            }                    
            
        }
	
	@Override
	public String toString(){
		return ""+this.value;
	}
	
	@Override
	public boolean equals(Object o){
		SQLInteger cmp = (SQLInteger)o;
		if(this.value==cmp.value)
			return true;
		
		return false;
	}

	@Override
	public int compareTo(AbstractSQLValue o) {
		SQLInteger cmp = (SQLInteger)o;
		
		if(this.value<cmp.value)
			return 1;
		else if(this.value>cmp.value)
			return -1;
		return 0;
	}
	
	@Override
	public SQLInteger clone(){
		return new SQLInteger(this.value);
	}

	@Override
	public void parseValue(String data) {
		this.value = Integer.parseInt(data);
	}

	@Override
	public int getFixedLength() {
		return LENGTH;
	}

	@Override
	public int getVariableLength() {
		return 0;
	}
}
