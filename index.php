<?php

/*DEFINE A CONSTANTE CAMINHO_INSTALACAO COM O ENDEREÇO DE DESENVOLVIMENTO OU PRODUÇÃO*/
include_once('../defineVariavelAmbiente.php');

/*ARQUIVO COM FUNÇÕES RELATIVAS A CRIAÇÃO E BUSCA DE LOGS*/
include_once('./log.php');

require_once __DIR__ . '/bootstrap.php';

/*ARQUIVO COM FUNÇÃO REALIVAS A GERAÇÃO DE RELATÓRIOS DE MENSALIDADE AUTOMÁTICA*/
include_once('./relatorio.php');

/*Classe de geração de mensalidade (OOP)*/
require_once __DIR__ . '/src/GeradorMensalidade.php';
require_once __DIR__ . '/src/GeradorFaturamento.php';
require_once __DIR__ . '/src/GeradorNossoNumero.php';
require_once __DIR__ . '/src/GeradorNotificacao.php';

/*ARQUIVO COM FUNÇÃO PARA REALIZAR O PROCESSAMENTO DO BOLETO*/
include_once('../dfe.php');

if (!class_exists('Conexao')) {
    $var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
    require_once($var_caminho_con);
}

$array_empresa = [];

try {

    $result = [
        'clientes_total' => 0,
        'clientes_ok'    => 0,
        'clientes_falha' => 0,
        'dlpds_ok'       => array(),
        'dlpds_falha'    => array()
    ];

    if($_SERVER['SERVER_NAME'] == 'srvdsv3.axmsolucoes.com.br'){
        $array_empresa[]['id_empresa'] = 1;
    }else{

        $base_contrato = 'db_contrato';

        $conexao = new Conexao();
        $conn = $conexao->open($base_contrato);

        /*$sql_search = "SELECT DISTINCT(id_empresa) 
                        FROM db_contratos.tb_contratos
                        ORDER BY id_empresa
        ";*/
        /*AJUSTE REALIZADO PARA ACABAR COM OS ERROS DE CONEXÃO NOS LOGS DE TODOS OS SERVIDORES - 21-02-2025*/
        $sql_search = "SELECT DISTINCT SUBSTRING(tx_nome_bd, 6, 6)::integer AS id_empresa FROM db_contratos.tb_contratos
                    JOIN pg_database ON pg_database.datname = 'db_'||SUBSTRING(tx_nome_bd, 6, 6)
        ";
        $ret = execQuery($conn, $sql_search);
        $array_empresa = pg_fetch_all($ret);

    }

        $result['clientes_total'] = count($array_empresa);

        echo "Empresas: <pre>";
        print_r($array_empresa);
        echo '</pre>';

        foreach($array_empresa as $empresa){

            $id_empresa = $empresa['id_empresa'];
            $res = null;

        $conexao = new Conexao();
        $conn = $conexao->open(dbNameCob($id_empresa));

        $sql_search = "SELECT lo_ativo, nu_dia_referencia 
                        FROM db_gol.tb_modelo_cobranca_automatica_mensalidade
                        WHERE id_empresa = $id_empresa
                        AND id_modelo = 1";

        $ret = execQuery($conn, $sql_search, $id_empresa);

        if($ret){

            $lo_ativo = pg_fetch_all($ret)[0]['lo_ativo'];

            $nu_dia_referencia = intval(pg_fetch_all($ret)[0]['nu_dia_referencia']);

            echo "Modelo: <pre>";
            print_r(pg_fetch_all($ret));
            echo '</pre>';

            if($nu_dia_referencia > intval(date('t'))){
                $nu_dia_referencia  = intval(date('t'));
            }
            //Confere se a rotina está ativa e se o dia atual é igual ao dia de referência
            if($lo_ativo == 'S' && $nu_dia_referencia == intval(date('d'))){

                $id_cobranca_mensalidade = 0;
                
                $sql_search = "SELECT id_cobranca_mensalidade
                                    FROM db_gol.tb_cobranca_mensalidade
                                    WHERE dt_referencia = CURRENT_DATE
                                    AND id_empresa = $id_empresa
                                    ORDER BY id_cobranca_mensalidade ASC
                                    LIMIT 1
                ";

                $ret = execQuery($conn, $sql_search, $id_empresa);

                if(isset(pg_fetch_all($ret)[0])){
                    $id_cobranca_mensalidade = pg_fetch_all($ret)[0]['id_cobranca_mensalidade'];
                }

                $sql_search = "SELECT  id_passo, tx_status
                                FROM db_gol.tb_passo_mensalidade_automatica 
                                WHERE id_empresa = $id_empresa 
                                AND id_cobranca_mensalidade = $id_cobranca_mensalidade
                                ORDER BY id_passo
                ";

                $array_passos = array();

                if($id_cobranca_mensalidade){
                    $ret = execQuery($conn, $sql_search, $id_empresa);
                    $array_passos = pg_fetch_all($ret);
                }

                // Normalize passos into a map by id_passo => tx_status
                $passos_map = [];
                if (!empty($array_passos)) {
                    foreach ($array_passos as $p) {
                        $passos_map[intval($p['id_passo'])] = $p['tx_status'];
                    }
                }

                $sql_search = "SELECT lo_mensalidade_gerada, lo_faturamento_gerado, lo_boleto_gerado, lo_cobranca_gerada 
                                FROM db_gol.tb_cobranca_mensalidade
                                WHERE id_empresa = $id_empresa
                                AND id_cobranca_mensalidade = $id_cobranca_mensalidade";

                $array_cobranca = array();

                if($id_cobranca_mensalidade){
                    $ret = execQuery($conn, $sql_search, $id_empresa);
                    $array_cobranca = pg_fetch_all($ret)[0];
                }

                echo "Id empresa: $id_empresa<hr>";
                echo "Id Cobrança: $id_cobranca_mensalidade<hr>";
                echo '<pre> Passos: ';
                print_r($array_passos);
                echo '</pre>';
                echo '<pre> Cobrança: ';
                print_r($array_cobranca);
                echo '</pre>';

                // New flow: trigger the gerador that corresponds to the current passo.
                // If there is no cobranca yet, start passo 1 (GeradorMensalidade).
                if (empty($id_cobranca_mensalidade) || empty($passos_map)) {
                    echo "Iniciou Mensalidade (passo 1) <hr>";
                    $gerador = new GeradorMensalidade($id_empresa, $id_cobranca_mensalidade);
                    $res = $gerador->run();
                    echo "Finalizou Mensalidade<hr>";

                    if (is_array($res) && isset($res['success'])) {
                        if ($res['success'] === true) {
                            $result['clientes_ok']++;
                            $result['dlpds_ok'][] = $id_empresa;
                        } else {
                            $result['clientes_falha']++;
                            $result['dlpds_falha'][] = $id_empresa;
                        }
                    }

                } else {
                    // Check passos 1..4 and only invoke the class if its passo is in EM_PROGRESSO
                    // passo mapping: 1=GeradorMensalidade, 2=GeradorFaturamento, 3=GeradorNossoNumero, 4=GeradorNotificacao
                    for ($passo = 1; $passo <= 4; $passo++) {
                        $status = isset($passos_map[$passo]) ? $passos_map[$passo] : null;
                        if ($status === 'EM_PROGRESSO') {
                            switch ($passo) {
                                case 1:
                                    echo "Resumindo Mensalidade (passo 1) <hr>";
                                    $g = new GeradorMensalidade($id_empresa, $id_cobranca_mensalidade);
                                    $res = $g->run();
                                    echo "Finalizou Mensalidade (passo 1) <hr>";
                                    break;
                                case 2:
                                    echo "Iniciando Faturamento (passo 2) <hr>";
                                    $g = new GeradorFaturamento($id_empresa, $id_cobranca_mensalidade);
                                    $res = $g->run();
                                    echo "Finalizou Faturamento (passo 2) <hr>";
                                    break;
                                case 3:
                                    echo "Iniciando NossoNumero (passo 3) <hr>";
                                    $g = new GeradorNossoNumero($id_empresa, $id_cobranca_mensalidade);
                                    $res = $g->run();
                                    echo "Finalizou NossoNumero (passo 3) <hr>";
                                    break;
                                case 4:
                                    echo "Iniciando Notificacao (passo 4) <hr>";
                                    $g = new GeradorNotificacao($id_empresa, $id_cobranca_mensalidade);
                                    $res = $g->run();
                                    echo "Finalizou Notificacao (passo 4) <hr>";
                                    break;
                            }
                            // After handling one EM_PROGRESSO passo, map result to summary counters
                            if (isset($g) && method_exists($g, 'run')) {
                                // If $res was set by the generator, use it; some generators may not return it, so skip.
                                if (isset($res) && is_array($res) && array_key_exists('success', $res)) {
                                    if ($res['success'] === true) {
                                        $result['clientes_ok']++;
                                        $result['dlpds_ok'][] = $id_empresa;
                                    } else {
                                        $result['clientes_falha']++;
                                        $result['dlpds_falha'][] = $id_empresa;
                                    }
                                }
                            }
                            // Only run the first EM_PROGRESSO passo found
                            break;
                        }
                    }
                }

                // If the rotina is inactive or the day doesn't match, consider it OK (no-op)
                if (!($lo_ativo == 'S' && $nu_dia_referencia == intval(date('d')))) {
                    $result['clientes_ok']++;
                    $result['dlpds_ok'][] = $id_empresa;
                }

            }
        }

        if (($result['clientes_total'] == 0) || ($result['clientes_ok'] > 0 && $result['clientes_falha'] == 0)) {
            http_response_code(200);
            $result['mensagem'] = 'Todos os clientes processados com sucesso';
        } elseif ($result['clientes_ok'] > 0) {
            http_response_code(206);
            $result['mensagem'] = 'Alguns clientes falharam, mas houve clientes processados com sucesso.';
        } else {
            http_response_code(500);
            $result['mensagem'] = 'Falha geral - nenhum cliente processado.';
        }

} catch (Throwable $e) {
    http_response_code(500);
    $result = ['mensagem' => 'Erro fatal: ' . $e->getMessage()];
}

echo json_encode($result, JSON_PRETTY_PRINT);

//Função para trazer o nome da base de dados 
function dbNameCob($id_empresa)
{

    if ($id_empresa  == 1) {
        $nome_banco = "db_gol9_dsv";
    } else {
        $nome_banco = "db_" . str_pad($id_empresa, 6, "0", STR_PAD_LEFT);
    }

    return $nome_banco;
}

//Função para mudar o stats do passo para erro
function registra_erro_passo($id_empresa, $id_cobranca_mensalidade, $id_passo){

    $conexao = new Conexao();
    $conn = $conexao->open(dbNameCob($id_empresa));

    $sql_update = "UPDATE db_gol.tb_passo_mensalidade_automatica
    SET tx_status = 'ERRO',
    dt_fim = NOW()
    WHERE id_empresa = $id_empresa
    AND id_cobranca_mensalidade = $id_cobranca_mensalidade
    AND id_passo = $id_passo";

    execQuery($conn, $sql_update, $id_empresa, $id_passo);

}

//função para buscar parâmetro no banco
function buscaParam($id_empresa, $tx_descricao){

    $conexao = new Conexao();
    $conn = $conexao->open(dbNameCob($id_empresa));

    $sql_search = "SELECT tx_valor
                        FROM db_gol.tb_parametro
                        WHERE id_empresa = $id_empresa 
                        AND tx_descricao = '$tx_descricao';";

    $ret = execQuery($conn, $sql_search, $id_empresa);
    return pg_fetch_all($ret)[0]['tx_valor'];
}

//Função para rodar em paralelo
function curlMultiplasURLS($urls)
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
    do {
        $mrc = curl_multi_exec($mh, $active);
    } while ($mrc === CURLM_CALL_MULTI_PERFORM);

    while ($active && $mrc === CURLM_OK) {
        if (curl_multi_select($mh) !== -1) {
            do {
                $mrc = curl_multi_exec($mh, $active);
            } while ($mrc === CURLM_CALL_MULTI_PERFORM);
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
            $errors[$url] = [
                'error_message' => $error,
                'error_code' => $errno,
                'http_code' => $httpCode
            ];
        }

        curl_multi_remove_handle($mh, $ch);
        curl_close($ch);
    }

    curl_multi_close($mh);

    return [
        'results' => $results,
        'errors' => $errors
    ];
}


?>