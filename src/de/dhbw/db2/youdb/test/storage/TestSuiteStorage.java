package de.dhbw.db2.youdb.test.storage;

import de.dhbw.db2.youdb.test.storage.types.TestSQLInteger;
import de.dhbw.db2.youdb.test.storage.types.TestSQLVarchar;
import junit.framework.Test;
import junit.framework.TestSuite;

public class TestSuiteStorage extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "YouDB-Storage" );
    suite.addTestSuite( TestPage.class );
    suite.addTestSuite( TestRecord.class );
    suite.addTestSuite( TestSQLInteger.class );
    suite.addTestSuite( TestSQLVarchar.class );
    return suite;
  }
}