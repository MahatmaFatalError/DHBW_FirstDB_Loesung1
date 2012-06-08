package de.dhbw.db2.youdb.storage.types;

/**
 * Enumeration of implemented types for SqlValues
 * @author cbinnig
 *
 */
public enum SQLType {
	SqlInteger(new SQLInteger()),
	SqlVarchar(new SQLVarchar(255));
	
	private AbstractSQLValue prototype = null;
	
	SQLType(AbstractSQLValue prototype){
		this.prototype = prototype;
	}
	
	public AbstractSQLValue getInstance(int length){
		AbstractSQLValue value = this.prototype.clone();
		if(length>0)
			value.setMaxLength(length);
		
		return value;
	}
}
