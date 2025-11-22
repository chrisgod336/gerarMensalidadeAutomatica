<?php

/*DEFINE A CONSTANTE CAMINHO_INSTALACAO COM O ENDEREÇO DE DESENVOLVIMENTO OU PRODUÇÃO*/
include_once('../defineVariavelAmbiente.php');

/*ARQUIVO COM FUNÇÕES RELATIVAS A CRIAÇÃO E BUSCA DE LOGS*/
include_once('./log.php');

/*ARQUIVO COM FUNÇÃO REALIVAS A GERAÇÃO DE RELATÓRIOS DE MENSALIDADE AUTOMÁTICA*/
include_once('./relatorio.php');

/*ARQUIVO COM FUNÇÃO REALIVAS A GERAÇÃO DE COBRANÇA E MENSALIDADE*/
include_once('./gerarMensalidade.php');

/*ARQUIVO COM FUNÇÃO PARA REALIZAR O PROCESSAMENTO DO BOLETO*/
include_once('../dfe.php');

if (!class_exists('Conexao')) {
    $var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
    require_once($var_caminho_con);
}

$array_empresa = [];

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
    $ret = pg_query($conn, $sql_search);
    $array_empresa = pg_fetch_all($ret);

}

echo "Empresas: <pre>";
print_r($array_empresa);
echo '</pre>';

foreach($array_empresa as $empresa){

    $id_empresa = $empresa['id_empresa'];

    $conexao = new Conexao();
    $conn = $conexao->open(dbNameCob($id_empresa));

    $sql_search = "SELECT lo_ativo, nu_dia_referencia 
                    FROM db_gol.tb_modelo_cobranca_automatica_mensalidade
                    WHERE id_empresa = $id_empresa
                    AND id_modelo = 1";

    $ret = pg_query($conn, $sql_search);

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

            $ret = pg_query($conn, $sql_search);

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
                $ret = pg_query($conn, $sql_search);
                $array_passos = pg_fetch_all($ret);
            }

            $sql_search = "SELECT lo_mensalidade_gerada, lo_faturamento_gerado, lo_boleto_gerado, lo_cobranca_gerada 
                            FROM db_gol.tb_cobranca_mensalidade
                            WHERE id_empresa = $id_empresa
                            AND id_cobranca_mensalidade = $id_cobranca_mensalidade";

            $array_cobranca = array();

            if($id_cobranca_mensalidade){
                $ret = pg_query($conn, $sql_search);
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

            if(!$id_cobranca_mensalidade){
                echo "Iniciou Mensalidade<hr>";
                gera_mensalidade($id_empresa);
                echo "Finalizou Mensalidade<hr>";
            }

        }
    }


}

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

    pg_query($conn, $sql_update);

}

//função para buscar parâmetro no banco
function buscaParam($id_empresa, $tx_descricao){

    $conexao = new Conexao();
    $conn = $conexao->open(dbNameCob($id_empresa));

    $sql_search = "SELECT tx_valor
                        FROM db_gol.tb_parametro
                        WHERE id_empresa = $id_empresa 
                        AND tx_descricao = '$tx_descricao';";

    $ret = pg_query($conn, $sql_search);
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