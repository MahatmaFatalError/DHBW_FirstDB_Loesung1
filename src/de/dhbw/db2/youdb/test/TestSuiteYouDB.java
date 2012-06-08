package de.dhbw.db2.youdb.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import de.dhbw.db2.youdb.test.storage.TestSuiteStorage;

public class TestSuiteYouDB extends TestSuite
{
  public static Test suite()
  {
    TestSuite suite = new TestSuite( "YouDB-All" );
    suite.addTest(TestSuiteStorage.suite());
    return suite;
  }
}