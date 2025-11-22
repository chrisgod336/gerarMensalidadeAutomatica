<?php
// Bootstrap: initialize logger and exception handling for scripts
require_once __DIR__ . '/src/Logger.php';
require_once __DIR__ . '/src/ExceptionHandler.php';

// register exception handler
ExceptionHandler::register();

function logger()
{
    return Logger::getInstance();
}

// ensure logs dir exists
if (!is_dir(__DIR__ . '/logs')) {
    @mkdir(__DIR__ . '/logs', 0777, true);
}

/**
 * Global execQuery helper.
 * Parameters:
 *  - $conn: pg connection resource
 *  - $sql: SQL string
 *  - $id_empresa: optional id_empresa for logging (if null, will not call fc_gera_log)
 *  - $id_passo: optional passo id for logging
 *  - $id_registro: optional registro id for logging
 *  - $tx_tipo_registro: optional tipo registro for logging
 */
function execQuery($conn, $sql, $id_empresa = null, $id_passo = 0, $id_registro = null, $tx_tipo_registro = null)
{
    $ret = @pg_query($conn, $sql);
    if ($ret === false) {
        $msg = "Erro ao executar query: " . pg_last_error($conn) . " -- SQL: " . $sql;
        // only call fc_gera_log if id_empresa is provided and function exists
        if ($id_empresa !== null && function_exists('fc_gera_log')) {
            try { fc_gera_log($id_empresa, $id_registro ?? 0, $id_passo, 'N', $msg, $id_registro, $tx_tipo_registro); } catch (Throwable $t) {}
        }
        throw new Exception($msg);
    }
    return $ret;
}

?>
