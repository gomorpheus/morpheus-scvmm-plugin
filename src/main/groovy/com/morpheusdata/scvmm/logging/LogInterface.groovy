// (C) Copyright 2025 Hewlett Packard Enterprise Development LP

package com.morpheusdata.scvmm.logging

interface LogInterface {
    void info(String format, ... args)

    void warn(String format, ... args)

    void error(String format, ... args)

    void debug(String format, ... args)

    boolean isDebugEnabled()
}
