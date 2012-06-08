package de.dhbw.db2.youdb.test.storage.types;

import de.dhbw.db2.youdb.storage.types.SQLInteger;
import de.dhbw.db2.youdb.test.TestCase;
import junit.framework.Assert;
import org.junit.Test;



public class TestSQLInteger extends TestCase {
         
        @Test
	public void testSerializeDeserialize1(){
		int value = 123456789;
		
		SQLInteger sqlInt = new SQLInteger(value);
		byte[] content = sqlInt.serialize();
		
		SQLInteger sqlInt2 = new SQLInteger();
		sqlInt2.deserialize(content);
		
		Assert.assertEquals(sqlInt.getValue(), sqlInt2.getValue());
	}

}
