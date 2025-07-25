// (C) Copyright 2025 Hewlett Packard Enterprise Development LP

package com.morpheusdata.scvmm.logging

import groovy.transform.CompileStatic
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * A logging wrapper around SLF4J which enables a Morpheus plugin to log messages with a custom prefix
 * and with the file name and line number from which the log request was made.
 */

@Singleton
@CompileStatic
class LogWrapper implements LogInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(LogWrapper)

    boolean isDebugEnabled() {
        return LOGGER.debugEnabled
    }

    void info(String format, ... args) {
        if (LOGGER.infoEnabled) {
            LOGGER.info("${LogConstants.MESSAGE_PREFIX}${callerDetails} $format", args)
        }
    }

    void warn(String format, ... args) {
        if (LOGGER.warnEnabled) {
            LOGGER.warn("${LogConstants.MESSAGE_PREFIX}${callerDetails} $format", args)
        }
    }

    void error(String format, ... args) {
        if (LOGGER.errorEnabled) {
            LOGGER.error("${LogConstants.MESSAGE_PREFIX}${callerDetails} $format", args)
        }
    }

    void debug(String format, ... args) {
        if (LOGGER.debugEnabled) {
            LOGGER.debug("${LogConstants.MESSAGE_PREFIX}${callerDetails} $format", args)
        }
    }

    /**
     * This function traverses the stack trace to determine which function is making a log
     * entry. A typical stack might look like this:
     * ...
     * com.hpe.morpheus.arubacxdss.logging.LogWrapper.getCallerDetails(LogWrapper.groovy:69)
     * com.hpe.morpheus.arubacxdss.logging.LogWrapper.error(LogWrapper.groovy:36)
     * ...
     * java.base/java.lang.reflect.Method.invoke(Unknown Source)
     * ...
     * com.hpe.morpheus.arubacxdss.HpeArubaCxDssPlugin.initialize(HpeArubaCxDssPlugin.groovy:28)
     * com.hpe.morpheus.arubacxdss.HpeArubaCxDssPlugin$initialize.call(Unknown Source)
     * ...
     *
     * In this example, the caller is at "HpeArubaCxDssPlugin.groovy:28". The additional Java layers
     * can complicate finding that stack trace entry. We first need to find the topmost
     * stack trace entry from the com.hpe.mp.storage.logging package. We then traverse the
     * stack above that entry looking for a package prefix match of PACKAGE_PREFIX_CALLER.
     *
     * @return string of " [fileName:lineNumber]" from the caller (empty if unknown)
     */
    private static String getCallerDetails() {
        try {
            // Retrieve stack trace dump for the current thread
            StackTraceElement[] stackTraceElements = Thread.currentThread().stackTrace

            // Traverse the stack trace backwards looking for the topmost index from this
            // logging package.
            int lastPackageIndex = -1
            for (int i = stackTraceElements.length - 1; i >= 0; i--) {
                StackTraceElement stackTrace = stackTraceElements[i]
                if (stackTrace.className == LogWrapper.name) {
                    lastPackageIndex = i
                    break
                }
            }

            // As a safety measure, for a condition which should not occur, return an empty
            // string if the logging package was not detected.
            if (lastPackageIndex < 0) {
                return ''
            }

            // Scan just above the topmost logger stack trace element looking for the first
            // one to match the PACKAGE_PREFIX_CALLER. If a match is found, return the log
            // entry (e.g. [fileName:lineNumber]).
            for (int i = lastPackageIndex + 1; i < stackTraceElements.length; i++) {
                StackTraceElement stackTrace = stackTraceElements[i]
                if (stackTrace.className.startsWith(LogConstants.CALLER_PACKAGE_PREFIX)) {
                    return " [${stackTrace.fileName}:${stackTrace.lineNumber}]"
                }
            }
        } catch (e) {
            // Capture any unexpected exception to ensure method always returns a string
            return " [${e.message}]"
        }

        // Return an empty string if unable to determine the caller.
        return ''
    }
}
