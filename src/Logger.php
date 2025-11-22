<?php

class Logger
{
    private static $instance = null;
    private $handle;

    private function __construct()
    {
        $logDir = __DIR__ . '/../logs';
        if (!is_dir($logDir)) {
            @mkdir($logDir, 0777, true);
        }

        $file = $logDir . '/' . date('Y-m-d') . '.log';
        $this->handle = fopen($file, 'a');
    }

    public static function getInstance()
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public function write($level, $message)
    {
        if (!$this->handle) return false;
        $time = date('Y-m-d H:i:s');
        $line = "[$time] [$level] $message\n";
        fwrite($this->handle, $line);
    }

    public function info($message)
    {
        $this->write('INFO', $message);
    }

    public function warn($message)
    {
        $this->write('WARN', $message);
    }

    public function error($message)
    {
        $this->write('ERROR', $message);
    }

    public function __destruct()
    {
        if ($this->handle) {
            fclose($this->handle);
        }
    }
}

?>
