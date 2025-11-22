<?php
require_once __DIR__ . '/../bootstrap.php';

class GeradorNossoNumero
{
    private $id_empresa;
    private $id_cobranca_mensalidade;

    public function __construct($id_empresa, $id_cobranca_mensalidade)
    {
        $this->id_empresa = intval($id_empresa);
        $this->id_cobranca_mensalidade = intval($id_cobranca_mensalidade);
    }

    private function execQuery($conn, $sql, $id_passo = 3, $id_registro = null, $tx_tipo_registro = null)
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

        logger()->info("GeradorNossoNumero: start id_empresa={$id_empresa} id_cobranca={$id_cobranca_mensalidade}");

        if (!class_exists('Conexao')) {
            $var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
            require_once($var_caminho_con);
        }

        $conexao = new Conexao();
        $conn = $conexao->open(dbNameCob($id_empresa));

        try{
            // Ensure this passo (3) is present. If exists and not EM_PROGRESSO, abort; otherwise create it.
            $sql_check_passo = "SELECT tx_status FROM db_gol.tb_passo_mensalidade_automatica WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade AND id_passo = 3;";
            $ret_check = @pg_query($conn, $sql_check_passo);
            $row_check = $ret_check ? pg_fetch_all($ret_check)[0] ?? null : null;
            if ($row_check) {
                $status_passo = $row_check['tx_status'];
                if ($status_passo !== 'EM_PROGRESSO') {
                    logger()->info("Passo 3 existe com status={$status_passo}, abortando NossoNumero.");
                    return [ 'success' => true, 'message' => "Passo 3 status={$status_passo}, nada a fazer" ];
                }
            } else {
                $sql_insert_passo = "INSERT INTO db_gol.tb_passo_mensalidade_automatica (id_empresa, id_cobranca_mensalidade, id_passo, tx_status, dt_inc) VALUES($id_empresa, $id_cobranca_mensalidade, 3, 'EM_PROGRESSO', NOW());";
                $this->execQuery($conn, $sql_insert_passo, 3);
            }
            $sql_search = "SELECT id_boleto FROM db_gol.tb_modelo_cobranca_automatica_mensalidade WHERE id_empresa = $id_empresa AND id_modelo = 1";
            $ret = $this->execQuery($conn, $sql_search, 3);
            $modelo = pg_fetch_all($ret)[0] ?? null;

            if(!$modelo){
                fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro ao tantar buscar dados do modelo de mensalidade automática.');
                $this->registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);
                return [ 'success' => false, 'message' => 'Modelo não encontrado' ];
            }

            $id_boleto = $modelo['id_boleto'];
            $id_banco = 0;
            $id_empresa_emitente = $id_empresa;
            $tipo_pedido = '';
            $serie = 0;

            if($id_boleto){
                $sql_search = "SELECT id_banco FROM db_gol.tb_conta_financeira WHERE id_empresa = $id_empresa AND id_boleto = $id_boleto";
                $ret = $this->execQuery($conn, $sql_search, 3, $id_boleto, 'Boleto');
                $row = pg_fetch_all($ret)[0] ?? null;
                if(!$row){
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro ao tentar buscar o banco vinculado ao boleto', $id_boleto, 'Boleto');
                    $this->registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);
                    return [ 'success' => false, 'message' => 'Boleto ou banco não encontrado' ];
                }
                $id_banco = $row['id_banco'];
            }

            $sql_search = "SELECT id_empresa,id_mov,id_desd,tipo,id_venda,id_notfat,tipo_pedido,id_empresa_emitente,nr_boleto,id_pessoa FROM db_gol.vw_rec_pag_lote WHERE id_empresa= $id_empresa AND tipo= 'RC' AND lo_tipo_boleto='S' AND situacao IN ('A') AND id_venda IN (SELECT id_venda FROM db_gol.tb_venda WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade) ORDER BY dt_emissao DESC, id_mov DESC, id_desd DESC";
            $ret = $this->execQuery($conn, $sql_search, 3);
            $array_vendas = pg_fetch_all($ret) ?: [];

            $array_gera_boleto_nosso_numero = [];
            $erro_passo = false;

            foreach($array_vendas as $index => $venda){
                $id_venda = $venda['id_venda'];
                $venda_err = false;

                if($id_boleto && $id_banco){
                    $array_vendas[$index]['id_boleto'] = $id_boleto;
                    $array_vendas[$index]['id_banco'] = $id_banco;
                }else{
                    $sql_search = "SELECT id_boleto,id_banco FROM db_gol.tb_pessoa WHERE id_empresa = $id_empresa AND id_pessoa = " . $venda['id_pessoa'];
                    $ret = @pg_query($conn, $sql_search);
                    if(!$ret){
                        $venda_err = true;
                        fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro ao tentar buscar o boleto do cliente '.$venda['id_pessoa'], $venda['id_pessoa'], 'Cliente');
                        $this->registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);
                    } else {
                        $pessoa = pg_fetch_all($ret)[0] ?? null;
                        if(!$pessoa){
                            $venda_err = true;
                            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro nenhum boleto cadastrado no cliente '.$venda['id_pessoa'], $venda['id_pessoa'], 'Cliente');
                            $erro_passo = true;
                            continue;
                        }
                        $boleto = $pessoa['id_boleto'];
                        $banco = $pessoa['id_banco'];
                        $array_vendas[$index]['id_boleto'] = $boleto;
                        $array_vendas[$index]['id_banco'] = $banco;
                        if(!$boleto || !$banco){
                            $venda_err = true;
                            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro nenhum boleto/banco cadastrado no cliente '.$venda['id_pessoa'], $venda['id_pessoa'], 'Cliente');
                            $erro_passo = true;
                            continue;
                        }
                    }
                }

                if(!$venda_err && $id_venda > 0){
                    $sql_search = "SELECT id_empresa_emitente,tipo_pedido FROM db_gol.tb_venda WHERE id_empresa = $id_empresa AND id_venda = $id_venda";
                    $ret = $this->execQuery($conn, $sql_search, 3);
                    $row = pg_fetch_all($ret)[0] ?? null;
                    if($row){
                        $id_empresa_emitente = $row['id_empresa_emitente'];
                        $serie = $row['tipo_pedido'];
                        switch($serie){ case 0: $tipo_pedido='FF'; break; case 2: $tipo_pedido='CF'; break; default: $tipo_pedido='UN'; }
                    }
                }

                if(!$venda_err){
                    $array_gera_boleto_nosso_numero[] = [
                        'id_empresa' => $id_empresa,
                        'id_empresa_emitente' => $id_empresa_emitente,
                        'id_venda' => $id_venda,
                        'id_notfat' => $venda['id_notfat'] ?? null,
                        'serie' => $tipo_pedido,
                        'id_mov' => $venda['id_mov'],
                        'id_desd' => $venda['id_desd'],
                        'operacao_boleto' => 3,
                        'controle_lote' => false,
                        'id_boleto' => $array_vendas[$index]['id_boleto'] ?? null,
                        'id_banco' => $array_vendas[$index]['id_banco'] ?? null
                    ];
                }
            }

            $count_lote_boleto = 0;
            $count_nosso_numero = count($array_gera_boleto_nosso_numero);
            $login = 'MENS_AUTOMATICA';

            while($count_lote_boleto < $count_nosso_numero){
                $array_gera_boleto = [];
                $var_aux = $count_lote_boleto;
                $item = $array_gera_boleto_nosso_numero[$var_aux];
                $var_id_empresa = $item['id_empresa'];
                $var_id_empresa_emitente = $item['id_empresa_emitente'];
                $var_id_venda = $item['id_venda'];
                $var_id_notfat = $item['id_notfat'];
                $var_serie = $item['serie'];
                $var_id_mov = $item['id_mov'];
                $var_id_desd = $item['id_desd'];
                $var_operacao_boleto = $item['operacao_boleto'];
                $var_controle_lote = $item['controle_lote'];
                $var_id_boleto = $item['id_boleto'];
                $var_banco = $item['id_banco'];

                $var_pk_arr = $var_id_empresa.$var_id_mov.$var_id_desd.'RC';
                $array_gera_boleto[$var_pk_arr] = [
                    'id_empresa' => $var_id_empresa,
                    'id_empresa_emitente' => $var_id_empresa_emitente,
                    'id_venda' => $var_id_venda,
                    'id_notfat' => $var_id_notfat,
                    'serie' => $var_serie,
                    'id_mov' => $var_id_mov,
                    'id_desd' => $var_id_desd,
                    'var_operacao_boleto' => $var_operacao_boleto,
                    'var_id_boleto' => $var_id_boleto,
                    'var_banco' => $var_banco,
                    'controle_lote' => $var_controle_lote,
                    'registro_online' => 'N'
                ];

                $array_retorno = fcProcessaBoleto($array_gera_boleto, dbNameCob($var_id_empresa), $login);

                if(isset($array_retorno['sucesso']) && $array_retorno['sucesso']){
                    $index = $var_id_venda.$var_id_desd;
                    $dados_boleto = $array_retorno[$id_empresa.$index.'RC']['retorno'][$index]['dadosboleto'] ?? null;
                    if($dados_boleto && ($dados_boleto['sucesso'] ?? false)){
                        $nosso_numero = $dados_boleto['nosso_numero'] ?? '';
                        $dt_vencimento = $dados_boleto['dt_vencimento'] ?? '';
                        if($dt_vencimento){ $data = new DateTime($dt_vencimento); $dt_vencimento = $data->format('d/m/Y'); }
                        $valor = $dados_boleto['valor'] ?? 0; $valor = $valor ? number_format((float)$valor, 2, ',', '.') : '0,00';
                        $linha_digitavel = $dados_boleto['linha_digitavel'] ?? '';
                        fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'S', "Nr.: $var_id_mov - $var_id_desd Nr. Nosso Número: /$nosso_numero - Valor: $valor - Linha Digitável: $linha_digitavel", $var_id_mov, 'Contas a receber');
                    }else{
                        fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', ($dados_boleto['mensagem'] ?? "Falha ao gerar os dados do boleto. Favor verificar!") . "\nNr.: $var_id_mov - $var_id_desd", $var_id_mov, 'Contas a receber');
                        $erro_passo = true;
                    }
                }else{
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', "Falha ao gerar os dados do boleto. Favor verificar!\nNr.: $var_id_mov - $var_id_desd", $var_id_mov, 'Contas a receber');
                    $erro_passo = true;
                }

                $count_lote_boleto++;
            }

            if($erro_passo){
                $this->registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);
                return [ 'success' => false, 'message' => 'Erro passo boleto' ];
            }else{
                $sql_update = "UPDATE db_gol.tb_passo_mensalidade_automatica SET tx_status = 'CONCLUIDO', dt_fim = NOW() WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade AND id_passo = 3";
                $this->execQuery($conn, $sql_update, 3);

                $sql_update = "UPDATE db_gol.tb_cobranca_mensalidade SET lo_boleto_gerado = 'S' WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade";
                $this->execQuery($conn, $sql_update, 3);

                // Do NOT create passo 4 here. The GeradorNotificacao must create passo 4 at its own start.
            }

            return [ 'success' => true ];

        }catch(Exception $e){
            logger()->error('GeradorNossoNumero::run error: '.$e->getMessage());
            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', $e->getMessage(), 0, 'Sistema');
            return [ 'success' => false, 'message' => $e->getMessage() ];
        }finally{
            try{ $conexao->close(); } catch(Throwable $t){}
        }
    }

    private function curlMultiplasURLS($urls)
    {
        $mh = curl_multi_init();
        $handles = array();
        $results = array();
        $errors = array();

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
                $errors[$url] = [ 'error_message' => $error, 'error_code' => $errno, 'http_code' => $httpCode ];
            }
            curl_multi_remove_handle($mh, $ch);
            curl_close($ch);
        }

        curl_multi_close($mh);
        return [ 'results' => $results, 'errors' => $errors ];
    }

    private function registra_erro_passo($id_empresa, $id_cobranca_mensalidade, $id_passo)
    {
        $conexao = new Conexao();
        try{
            $conn = $conexao->open(dbNameCob($id_empresa));
            $sql_update = "UPDATE db_gol.tb_passo_mensalidade_automatica SET tx_status = 'ERRO', dt_fim = NOW() WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade AND id_passo = $id_passo";
            @pg_query($conn, $sql_update);
            echo 'Email: <pre>';
            print_r(fc_monta_relatorio($id_empresa, $id_cobranca_mensalidade));
            echo '</pre>';
        }catch(Throwable $t){}
        finally{ try{ $conexao->close(); }catch(Throwable $t){} }
    }
}

?>
