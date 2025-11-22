<?php
require_once __DIR__ . '/../bootstrap.php';

class GeradorFaturamento
{
    private $id_empresa;
    private $id_cobranca_mensalidade;

    public function __construct($id_empresa, $id_cobranca_mensalidade)
    {
        $this->id_empresa = intval($id_empresa);
        $this->id_cobranca_mensalidade = intval($id_cobranca_mensalidade);
    }

    private function execQuery($conn, $sql, $id_passo = 2, $id_registro = null, $tx_tipo_registro = null)
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

        logger()->info("GeradorFaturamento: start id_empresa={$id_empresa} id_cobranca={$id_cobranca_mensalidade}");

        if (!class_exists('Conexao')) {
            $var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
            require_once($var_caminho_con);
        }

        $conexao = new Conexao();
        $conn = $conexao->open(dbNameCob($id_empresa));

        try {
            // Ensure this passo (2) is created here. If already exists and not EM_PROGRESSO, abort.
            $sql_check_passo = "SELECT tx_status FROM db_gol.tb_passo_mensalidade_automatica WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade AND id_passo = 2;";
            $ret = $this->execQuery($conn, $sql_check_passo, 2);
            $row = $ret ? (pg_fetch_all($ret)[0] ?? null) : null;
            if ($row) {
                $status_passo = $row['tx_status'];
                if ($status_passo !== 'EM_PROGRESSO') {
                    logger()->info("Passo 2 existe com status={$status_passo}, abortando processamento de faturamento.");
                    return [ 'success' => true, 'message' => "Passo 2 status={$status_passo}, nada a fazer" ];
                }
            } else {
                // create passo 2 as EM_PROGRESSO
                $sql_insert_passo = "INSERT INTO db_gol.tb_passo_mensalidade_automatica (id_empresa, id_cobranca_mensalidade, id_passo, tx_status, dt_inc) VALUES($id_empresa, $id_cobranca_mensalidade, 2, 'EM_PROGRESSO', NOW());";
                $this->execQuery($conn, $sql_insert_passo, 2);
            }

            // Busca vendas a faturar
            $ds_vd = "SELECT id_venda, id_empresa_emitente FROM db_gol.tb_venda WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade AND status = 1";
            $ret = $this->execQuery($conn, $ds_vd, 2);
            $result_venda = pg_fetch_all($ret);

            if (!$result_venda || empty($result_venda)) {
                fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', 'Não foi encontrada nenhuma venda a ser faturada! Favor verifique.');
                $this->registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 2);
                logger()->info('Nenhuma venda encontrada para faturar.');
                return [ 'success' => false, 'message' => 'Nenhuma venda encontrada' ];
            }

            $id_venda_chked = [];
            foreach ($result_venda as $linha_vd) {
                $var_id_venda = $linha_vd['id_venda'];
                $id_empresa_emitente = $linha_vd['id_empresa_emitente'];
                $var_pk_venda = $id_empresa . $var_id_venda . $id_empresa_emitente;
                $var_tipo_operacao = 1;
                $id_venda_chked[$var_pk_venda] = [
                    'id_empresa' => $id_empresa,
                    'id_venda' => $var_id_venda,
                    'id_empresa_emitente' => $id_empresa_emitente,
                    'retorno' => true,
                    'mensagem' => '',
                    'tipo_operacao' => $var_tipo_operacao
                ];
            }

            $count_error = 0;

            foreach ($id_venda_chked as $venda) {
                if (!isset($venda['id_venda'], $venda['id_empresa'], $venda['id_empresa_emitente'])) {
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', 'Erro: Dados da venda incompletos.', $venda['id_venda'] ?? false, $venda['id_venda'] ? 'Venda' : false);
                    continue;
                }

                $id_venda = $venda['id_venda'];
                $id_empresa_emitente = $venda['id_empresa_emitente'];

                $soapUrl = '';
                if ($id_empresa == 1) {
                    $soapUrl = "https://srvdsv3.axmsolucoes.com.br/scriptcase915/app/GOL/mob001_1/mob001_1.php?id_venda=$id_venda&var_id_empresa=$id_empresa_emitente&var_glo_empresa=$id_empresa&var_glo_login=MENS_AUTOMATICA";
                } else {
                    $soapUrl = "https://" . NOME_SERVIDOR . ".sempretecnologia.com.br/mob001_1/mob001_1.php?id_venda=$id_venda&var_id_empresa=$id_empresa_emitente&var_glo_empresa=$id_empresa&var_glo_login=MENS_AUTOMATICA";
                }

                $paramXML = '<urn:mobile_faturar soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">' .
                    '<venda xsi:type="xsd:int">' . $id_venda . '</venda>' .
                    '<empresa xsi:type="xsd:int">' . $id_empresa . '</empresa>' .
                    '<glo_empresa xsi:type="xsd:int">' . $id_empresa_emitente . '</glo_empresa>' .
                    '<login xsi:type="xsd:string">MENS_AUTOMATICA</login>' .
                    '</urn:mobile_faturar>';

                $headers = ["Content-Type: text/xml;charset=UTF-8", "SOAPAction: urn:mobile_faturar"];

                $soapEnvelope = '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:mobile_faturar"><soapenv:Header/><soapenv:Body>' . $paramXML . '</soapenv:Body></soapenv:Envelope>';

                $options = [
                    'location' => $soapUrl,
                    'uri' => 'urn:mobile_faturar',
                    'trace' => true,
                    'cache_wsdl' => WSDL_CACHE_NONE,
                    'stream_context' => stream_context_create(['http' => ['header' => implode("\r\n", $headers)]])
                ];

                try {
                    $soapClient = new SoapClient(null, $options);
                    $response = $soapClient->__doRequest($soapEnvelope, $soapUrl, "urn:mobile_faturar", 1);

                    if (mb_detect_encoding($response, 'UTF-8', true) === false) {
                        $response = iconv('ISO-8859-1', 'UTF-8//TRANSLIT', $response);
                    }

                    $response_clean = $this->substituirAcentos($response);
                    $parts = explode('#@#', $response_clean);
                    $filtered_parts = array_filter($parts);
                    $last_two = array_slice($filtered_parts, -2);

                    if (count($last_two) >= 2) {
                        $status = htmlentities($last_two[0], ENT_QUOTES, 'UTF-8');
                        $message = pg_escape_string($conn, $last_two[1]);

                        if (trim($status) == 'S') {
                            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'S', $message, $id_venda ?: false, $id_venda ? 'Venda' : false);
                        } else {
                            $count_error++;
                            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', $message, $id_venda ?: false, $id_venda ? 'Venda' : false);
                        }
                    } else {
                        $count_error++;
                        fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', 'Erro de servidor ao tentar faturar venda ' . $id_venda, $id_venda ?: false, $id_venda ? 'Venda' : false);
                    }

                } catch (SoapFault $fault) {
                    $count_error++;
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', 'Erro ao tentar faturar venda ' . $id_venda . ' - ' . $fault->getMessage(), $id_venda ?: false, $id_venda ? 'Venda' : false);
                }
            }

                if ($count_error) {
                    $this->registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 2);
                } else {
                    $sql_update = "UPDATE db_gol.tb_passo_mensalidade_automatica SET tx_status = 'CONCLUIDO', dt_fim = NOW() WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade AND id_passo = 2";
                    $this->execQuery($conn, $sql_update, 2);

                    $sql_update = "UPDATE db_gol.tb_cobranca_mensalidade SET lo_faturamento_gerado = 'S' WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade";
                    $this->execQuery($conn, $sql_update, 2);

                    // Do NOT create the next passo here. The next passo must be started by its own module (GeradorNossoNumero).
                }

            return [ 'success' => true ];

        } catch (Exception $e) {
            logger()->error('GeradorFaturamento::run error: ' . $e->getMessage());
            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', $e->getMessage(), 0, 'Sistema');
            return [ 'success' => false, 'message' => $e->getMessage() ];
        } finally {
            try { $conexao->close(); } catch (Throwable $t) {}
        }
    }

    private function substituirAcentos($string) {
        $comAcento = ['À','Á','Â','Ã','Ä','Å','Æ','Ç','È','É','Ê','Ë','Ì','Í','Î','Ï','Ð','Ñ','Ò','Ó','Ô','Õ','Ö','Ù','Ú','Û','Ü','Ý','Þ','ß','à','á','â','ã','ä','å','æ','ç','è','é','ê','ë','ì','í','î','ï','ð','ñ','ò','ó','ô','õ','ö','ù','ú','û','ü','ý','þ','ÿ'];
        $semAcento = ['A','A','A','A','A','A','AE','C','E','E','E','E','I','I','I','I','D','N','O','O','O','O','O','U','U','U','U','Y','Th','s','a','a','a','a','a','a','ae','c','e','e','e','e','i','i','i','i','d','n','o','o','o','o','o','u','u','u','u','y','th','y'];
        return str_replace($comAcento, $semAcento, $string);
    }

    private function curlMultiplasURLS($urls)
    {
        $mh = curl_multi_init();
        $handles = [];
        $results = [];
        $errors = [];

        foreach ($urls as $url) {
            $ch = curl_init($url);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_HEADER, true);
            curl_setopt($ch, CURLOPT_NOBODY, false);
            curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
            curl_setopt($ch, CURLOPT_TIMEOUT, 30);
            curl_multi_add_handle($mh, $ch);
            $handles[$url] = $ch;
        }

        $active = null;
        do { $mrc = curl_multi_exec($mh, $active); } while ($mrc === CURLM_CALL_MULTI_PERFORM);
        while ($active && $mrc === CURLM_OK) {
            if (curl_multi_select($mh) !== -1) {
                do { $mrc = curl_multi_exec($mh, $active); } while ($mrc === CURLM_CALL_MULTI_PERFORM);
            }
        }

        foreach ($handles as $url => $ch) {
            $response = curl_multi_getcontent($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $error = curl_error($ch);
            $errno = curl_errno($ch);
            if ($httpCode >= 200 && $httpCode < 300) {
                $results[$url] = $response;
            } else {
                $errors[$url] = ['error_message' => $error, 'error_code' => $errno, 'http_code' => $httpCode];
            }
            curl_multi_remove_handle($mh, $ch);
            curl_close($ch);
        }

        curl_multi_close($mh);
        return ['results' => $results, 'errors' => $errors];
    }

    private function registra_erro_passo($id_empresa, $id_cobranca_mensalidade, $id_passo)
    {
        if (!class_exists('Conexao')) {
            $var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
            require_once($var_caminho_con);
        }
        $conexao = new Conexao();
        try{
            $conn = $conexao->open(dbNameCob($id_empresa));
            $sql_update = "UPDATE db_gol.tb_passo_mensalidade_automatica SET tx_status = 'ERRO', dt_fim = NOW() WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade AND id_passo = $id_passo";
            try { $this->execQuery($conn, $sql_update, $id_passo); } catch (Throwable $t) { }
            echo 'Email: <pre>';
            print_r(fc_monta_relatorio($id_empresa, $id_cobranca_mensalidade));
            echo '</pre>';
        }catch(Throwable $t){ }
        finally{ try{ $conexao->close(); }catch(Throwable $t){} }
    }
}

?>
