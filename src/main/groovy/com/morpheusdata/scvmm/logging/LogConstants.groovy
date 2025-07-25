// (c) Copyright 2025 Hewlett Packard Enterprise Development LP

package com.morpheusdata.scvmm.logging

import groovy.transform.CompileStatic

/**
 * Customizations for the LogWrapper behavior.
 */
@CompileStatic
class LogConstants {
    static final String MESSAGE_PREFIX = '[SCVMMPlugin]'

    // Package prefix for callers making log requests. A file name and line number can be included in log messages
    // only for classes with this package prefix.
    static final String CALLER_PACKAGE_PREFIX = "com.morpheusdata.scvmm"
}
