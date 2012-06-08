package de.dhbw.db2.youdb.storage;

import java.util.Vector;

import de.dhbw.db2.youdb.storage.types.AbstractSQLValue;

/**
 * Defines the interface for a record stored in a page
 * @author cbinnig
 *
 */
public abstract class AbstractRecord implements Cloneable{
	protected AbstractSQLValue[] values;
	
	/**
	 * Creates a record with a given number of attributes
	 * @param length number of attributes
	 */
	public AbstractRecord(int attNumber){
		this.values = new AbstractSQLValue[attNumber];
	}
	
	/**
	 * Set value i of record
	 * @param i
	 * @param value
	 */
	public void setValue(int i, AbstractSQLValue value){
		this.values[i] = value;
	}
	
	/**
	 * Return value of attribute with a given number
	 * @param i attribute number
	 * @return
	 */
	public AbstractSQLValue getValue(int i){
		return this.values[i];
	}
	
	/**
	 * Returns all values of the recors as array
	 * @return
	 */
	public AbstractSQLValue[] getValues(){
		return this.values;
	}
	
	/**
	 * Keeps only listed column numbers
	 * @param colNumbers
	 */
	public void keepValues(Vector<Integer> colNumbers){
		AbstractSQLValue[] tmpValues = this.values;
		
		this.values = new AbstractSQLValue[colNumbers.size()];
		int i=0;
		for(Integer colNum: colNumbers){
			this.values[i++] = tmpValues[colNum];
		}
	}
	
	/**
	 * Returns the fixed size of a record needed for for storing 
	 * in a slot in a page
	 * @return
	 */
	public abstract int getFixedLength();
	
	/**
	 * Returns the variable size of a record needed for storing 
	 * the record the variable part in a page
	 * @return
	 */
	public abstract int getVariableLength();

	/**
	 * Creates a new record form current record and rec by 
	 * appending rec to the end of the current record
	 * @param rec
	 * @return
	 */
	public abstract AbstractRecord append(AbstractRecord rec);
	
	@Override
	public abstract AbstractRecord clone();
}
