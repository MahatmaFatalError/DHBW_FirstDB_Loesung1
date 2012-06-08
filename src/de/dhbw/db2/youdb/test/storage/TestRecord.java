package de.dhbw.db2.youdb.test.storage;

import junit.framework.Assert;
import de.dhbw.db2.youdb.storage.AbstractRecord;
import de.dhbw.db2.youdb.storage.Record;
import de.dhbw.db2.youdb.storage.types.SQLInteger;
import de.dhbw.db2.youdb.storage.types.SQLVarchar;
import de.dhbw.db2.youdb.test.TestCase;


public class TestRecord extends TestCase{
	public void testCreate1(){
		AbstractRecord r = new Record(2);
		SQLInteger v1 = new SQLInteger(123456789);
		SQLVarchar v2 = new SQLVarchar("Hello111", 10);
		r.setValue(0, v1);
		r.setValue(1, v2);
		
		Assert.assertEquals(12, r.getFixedLength());
		Assert.assertEquals(((String)v2.getValue()).getBytes().length, r.getVariableLength());
	}
}
