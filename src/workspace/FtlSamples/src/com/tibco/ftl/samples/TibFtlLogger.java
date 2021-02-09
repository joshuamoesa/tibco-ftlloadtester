/*
 * Copyright (c) 2010-2020 TIBCO Software Inc.
 * All Rights Reserved. Confidential & Proprietary.
 * For more information, please contact:
 * TIBCO Software Inc., Palo Alto, California, USA
 */

package com.tibco.ftl.samples;

public class TibFtlLogger
{
    private static class Loader
    {
        static final TibFtlLogger INSTANCE = new TibFtlLogger();
    }

    private TibFtlLogger () {}

    public static TibFtlLogger getInstance()
    {
        return Loader.INSTANCE;
    }

    public synchronized void log(String format, Object... list)
    {
        System.out.printf(format + "\n", list);
    }
}
