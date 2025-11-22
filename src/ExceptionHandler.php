<?php

class ExceptionHandler
{
    public static function register()
    {
        set_exception_handler([self::class, 'handleException']);
        set_error_handler([self::class, 'handleError']);
    }

    public static function handleException($e)
    {
        try {
            if (class_exists('Logger')) {
                $logger = Logger::getInstance();
                $logger->error('Uncaught Exception: ' . $e->getMessage() . " in " . $e->getFile() . ':' . $e->getLine() . "\n" . $e->getTraceAsString());
            }
        } catch (Throwable $t) {
            // ignore
        }
    }

    public static function handleError($errno, $errstr, $errfile, $errline)
    {
        // Convert to ErrorException so set_exception_handler handles it
        throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
    }
}

?>
