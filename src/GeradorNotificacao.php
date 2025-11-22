<?php
require_once __DIR__ . '/../bootstrap.php';

class GeradorNotificacao
{
    private $id_empresa;
    private $id_cobranca_mensalidade;

    public function __construct($id_empresa, $id_cobranca_mensalidade)
    {
        $this->id_empresa = intval($id_empresa);
        $this->id_cobranca_mensalidade = intval($id_cobranca_mensalidade);
    }

    private function execQuery($conn, $sql, $id_passo = 4, $id_registro = null, $tx_tipo_registro = null)
    {
        $ret = @pg_query($conn, $sql);
        if ($ret === false) {
            $msg = "Erro ao executar query: " . pg_last_error($conn) . " -- SQL: " . $sql;
            fc_gera_log($this->id_empresa, $this->id_cobranca_mensalidade, $id_passo, 'N', $msg, $id_registro, $tx_tipo_registro);
            throw new Exception($msg);
        }
        return $ret;
    }

    public function run()
    {
        $id_empresa = $this->id_empresa;
        $id_cobranca_mensalidade = $this->id_cobranca_mensalidade;

        logger()->info("GeradorNotificacao: start id_empresa={$id_empresa} id_cobranca={$id_cobranca_mensalidade}");

        try {
            // Fallback: if full migration not yet completed, call the procedural implementation.
            $proc = __DIR__ . '/../gerarNotificacao.php';
            if (file_exists($proc)) {
                require_once $proc;
                if (function_exists('gera_notificacao')) {
                    // The procedural function should accept the same parameters
                    return call_user_func('gera_notificacao', $id_empresa, $id_cobranca_mensalidade);
                }
            }

            // If procedural function not present, throw to indicate incomplete migration
            throw new Exception('Procedural function gera_notificacao not found; migration pending.');

        } catch (Throwable $e) {
            logger()->error('GeradorNotificacao error: ' . $e->getMessage());
            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 4, 'N', $e->getMessage(), 0, 'Sistema');
            return [ 'success' => false, 'message' => $e->getMessage() ];
        }
    }
}

class GeradorNotificacao
{
    private $id_empresa;
    private $id_cobranca_mensalidade;

    public function __construct($id_empresa, $id_cobranca_mensalidade)
    {
        $this->id_empresa = intval($id_empresa);
        $this->id_cobranca_mensalidade = intval($id_cobranca_mensalidade);
    }

    private function execQuery($conn, $sql, $id_passo = 4, $id_registro = null, $tx_tipo_registro = null)
    {
        $ret = @pg_query($conn, $sql);
        if ($ret === false) {
            $msg = "Erro ao executar query: " . pg_last_error($conn) . " -- SQL: " . $sql;
            fc_gera_log($this->id_empresa, $this->id_cobranca_mensalidade, $id_passo, 'N', $msg, $id_registro, $tx_tipo_registro);
            throw new Exception($msg);
        }
        return $ret;
    }

    public function run()
    {
        $id_empresa = $this->id_empresa;
        $id_cobranca_mensalidade = $this->id_cobranca_mensalidade;

        logger()->info("GeradorNotificacao: start id_empresa={$id_empresa} id_cobranca={$id_cobranca_mensalidade}");

        if (!class_exists('Conexao')) {
            $var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
            require_once($var_caminho_con);
        }

        $conexao = new Conexao();
        $conn = $conexao->open(dbNameCob($id_empresa));

        try {
            // Ensure this passo (4) is present. If exists and not EM_PROGRESSO, abort; otherwise create it.
            $sql_check_passo = "SELECT tx_status FROM db_gol.tb_passo_mensalidade_automatica WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade AND id_passo = 4;";
            $ret_check = $this->execQuery($conn, $sql_check_passo, 4);
            $row_check = $ret_check ? (pg_fetch_all($ret_check)[0] ?? null) : null;
            if ($row_check) {
                $status_passo = $row_check['tx_status'];
                if ($status_passo !== 'EM_PROGRESSO') {
                    logger()->info("Passo 4 existe com status={$status_passo}, abortando Notificacao.");
                    return [ 'success' => true, 'message' => "Passo 4 status={$status_passo}, nada a fazer" ];
                }
            } else {
                $sql_insert_passo = "INSERT INTO db_gol.tb_passo_mensalidade_automatica (id_empresa, id_cobranca_mensalidade, id_passo, tx_status, dt_inc) VALUES($id_empresa, $id_cobranca_mensalidade, 4, 'EM_PROGRESSO', NOW());";
                $this->execQuery($conn, $sql_insert_passo, 4);
            }

            // If procedural implementation exists, delegate to it for now
            $proc = __DIR__ . '/../gerarNotificacao.php';
            if (file_exists($proc)) {
                require_once $proc;
                if (function_exists('gera_notificacao')) {
                    $result = call_user_func('gera_notificacao', $id_empresa, $id_cobranca_mensalidade);
                    logger()->info('GeradorNotificacao: finished procedural run');
                    return $result;
                }
            }

            throw new Exception('Procedural function gera_notificacao not found; migration pending.');

        } catch (Throwable $e) {
            logger()->error('GeradorNotificacao error: ' . $e->getMessage());
            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 4, 'N', $e->getMessage(), 0, 'Sistema');
            return [ 'success' => false, 'message' => $e->getMessage() ];
        } finally {
            try{ $conexao->close(); } catch(Throwable $t){}
        }
    }

}

?>
