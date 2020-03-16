package com.experoinc.janusgraph;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class FoundationDBContainerTest {

    @Container
    public FoundationDBContainer fdbc = new FoundationDBContainer();

    @Test
    public void testContainerInit () {
        assertTrue(fdbc.isRunning());
    }
}