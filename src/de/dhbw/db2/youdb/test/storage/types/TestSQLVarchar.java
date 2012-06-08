package de.dhbw.db2.youdb.test.storage.types;

import de.dhbw.db2.youdb.storage.types.SQLVarchar;
import de.dhbw.db2.youdb.test.TestCase;
import junit.framework.Assert;
import org.junit.Test;


public class TestSQLVarchar  extends TestCase {
    
    @Test
    public void test(){
        testSerializeDeserialize1();
    }
    
    
    
    public void testSerializeDeserialize1(){
            String value = "123456789";

            SQLVarchar sqlVarchar = new SQLVarchar(value, 255);
            byte[] data = sqlVarchar.serialize();

            SQLVarchar sqlVarchar2 = new SQLVarchar(255);
            sqlVarchar2.deserialize(data);

            Assert.assertEquals(sqlVarchar.getValue(), sqlVarchar2.getValue());
    }
}
