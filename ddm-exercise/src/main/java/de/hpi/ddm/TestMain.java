package de.hpi.ddm;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import de.hpi.ddm.actors.LargeMessageProxyTest;

public class TestMain {

    public static void main(String[] args) throws Exception {
        System.out.println("TESTS");
        JUnitCore junit = new JUnitCore();
        Result result = junit.run(LargeMessageProxyTest.class);
        System.out.println(result);
    }
}