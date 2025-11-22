<?php

include_once('../defineVariavelAmbiente.php');
include_once('./log.php');
include_once('./relatorio.php');
include_once('../dfe.php');

if (!class_exists('Conexao')) {
    $var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
    require_once($var_caminho_con);
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

if(isset($_GET) && isset($_GET['id_empresa']) && isset($_GET['id_cobranca_mensalidade'])){
    $id_empresa = $_GET['id_empresa'];
    $id_cobranca_mensalidade = $_GET['id_cobranca_mensalidade'];

    echo "Id empresa: $id_empresa<hr>";
    echo "Id cobrança: $id_cobranca_mensalidade<hr>";

    gera_nosso_numero($id_empresa, $id_cobranca_mensalidade);
}else{
    echo "Erro! parâmetros não recebidos via GET";
    exit;
}

//Função para gerar o nosso número de todos os boletos de uma determinada base
function gera_nosso_numero($id_empresa, $id_cobranca_mensalidade) {

    echo 'Gera Nosso Númemro<hr>';
    
    $conexao = new Conexao();
    $conn = $conexao->open(dbNameCob($id_empresa));
        
        $sql_search = "SELECT id_boleto
                        FROM db_gol.tb_modelo_cobranca_automatica_mensalidade
                        WHERE id_empresa = $id_empresa
                        AND id_modelo = 1";

        $ret = pg_query($conn, $sql_search);

        if(!$ret){
            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro ao tantar buscar dados do modelo de mensalidade automática.');
            registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);
            return;
        }

        $modelo = pg_fetch_all($ret)[0];

        $id_boleto = $modelo['id_boleto'];
        $id_banco = 0;
        $id_empresa_emitente = $id_empresa;
        $tipo_pedido = '';
        $serie = 0;

        if($id_boleto){
            $sql_search = " SELECT id_banco 
                                FROM db_gol.tb_conta_financeira
                                WHERE id_empresa = $id_empresa
                                AND id_boleto = $id_boleto";

            $ret = pg_query($conn, $sql_search);

            if(!$ret){
                fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro ao tentar buscar o banco vinculado ao boleto', $id_boleto, 'Boleto');
                registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);
                return;
            }

            if(!pg_fetch_all($ret)[0]){
                fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro ao tentar buscar o banco vinculado ao boleto', $id_boleto, 'Boleto');
                registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);
                return;
            }

            $id_banco = pg_fetch_all($ret)[0]['id_banco'];
        }

        $sql_search = "SELECT 
                        id_empresa,
                        id_mov, 
                        id_desd,
                        tipo,
                        id_venda,
                        id_notfat,
                        tipo_pedido,
                        id_empresa_emitente,
                        nr_boleto,
                        id_pessoa
                    FROM  db_gol.vw_rec_pag_lote
                    WHERE id_empresa= $id_empresa
                    AND tipo= 'RC'
                    AND lo_tipo_boleto='S'
                    AND situacao IN ('A')
                    AND id_venda IN (
                        SELECT id_venda
                        FROM db_gol.tb_venda 
                        WHERE id_empresa = $id_empresa 
                        AND id_cobranca_mensalidade = $id_cobranca_mensalidade
                    )
                    ORDER BY
                    dt_emissao DESC, id_mov DESC, id_desd DESC";

        $ret = pg_query($conn, $sql_search);
        $array_vendas = pg_fetch_all($ret);
        $array_gera_boleto_nosso_numero = array();

        $erro_passo = false;

        echo "Array de vendas: <pre>";
        print_r($array_vendas);
        echo "</pre>";

        foreach($array_vendas as $index => $venda){

            $id_venda = $venda['id_venda'];

            echo "Id venda boleto: $id_venda<hr>";

            $venda_err = false;

      
            if($id_boleto&&$id_banco){

                $array_vendas[$index]['id_boleto'] = $id_boleto;
                $array_vendas[$index]['id_banco'] = $id_banco;
                
            }else{
                $sql_search = "SELECT id_boleto,
                                id_banco
                        FROM db_gol.tb_pessoa 
                        WHERE id_empresa = $id_empresa 
                        AND id_pessoa = ".$venda['id_pessoa'];

                $ret = pg_query($conn, $sql_search);

                if(!$ret){
                    $venda_err = true;
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro ao tentar buscar o boleto do cliente '.$venda['id_pessoa'], $venda['id_pessoa'], 'Cliente');
                    registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);
                }

                if(!isset(pg_fetch_all($ret)[0])){
                    $venda_err = true;
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro nenhum boleto cadastrado no cliente '.$venda['id_pessoa'], $venda['id_pessoa'], 'Cliente');
                    $erro_passo = true;
                    continue;
                }

                echo $sql_search.'<hr>';
                echo '<pre>';
                print_r(pg_fetch_all($ret)[0]);
                echo '</pre>';

                $boleto = pg_fetch_all($ret)[0]['id_boleto'];
                $banco = pg_fetch_all($ret)[0]['id_banco'];

                $array_vendas[$index]['id_boleto'] = $boleto;
                $array_vendas[$index]['id_banco'] = $banco;

                if(!$boleto || !$banco){
                    $venda_err = true;
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', 'Erro nenhum boleto/banco cadastrado no cliente '.$venda['id_pessoa'], $venda['id_pessoa'], 'Cliente');
                    $erro_passo = true;
                    continue;
                }
            }

            if(!$venda_err && $id_venda > 0){

                $sql_search = "SELECT id_empresa_emitente
                                ,tipo_pedido
                            FROM db_gol.tb_venda
                            WHERE id_empresa = $id_empresa
                            AND id_venda = $id_venda";

                $ret = pg_query($conn, $sql_search);

                if($ret && isset(pg_fetch_all($ret)[0])){
                    $id_empresa_emitente = pg_fetch_all($ret)[0]['id_empresa_emitente'];
                    $serie = pg_fetch_all($ret)[0]['tipo_pedido'];

                    echo "Empresa emitente $id_empresa_emitente<hr>";
                    echo "Serie: $serie";

                    switch($serie){
                        case 0:
                            $tipo_pedido = 'FF';
                            break;
                        case 2:
                            $tipo_pedido = 'CF';
                            break;
                        default:
                            $tipo_pedido = 'UN';
                    }

                }
            }

            if(!$venda_err){
                $array_gera_boleto_nosso_numero[] = [
                    'id_empresa' => $id_empresa,
                    'id_empresa_emitente' =>  $id_empresa_emitente,
                    'id_venda' => $id_venda,
                    'id_notfat' => $venda['id_notfat'],
                    'serie' => $tipo_pedido,
                    'id_mov' => $venda['id_mov'],
                    'id_desd' => $venda['id_desd'],
                    'operacao_boleto' => 3,
                    'controle_lote' => false,
                    'id_boleto' => $boleto,
                    'id_banco' => $banco
                ];
            }
        }

        echo '<pre>';
        print_r($array_gera_boleto_nosso_numero);
        echo '</pre>';

        $count_lote_boleto = 0;
        $count_nosso_numero = count($array_gera_boleto_nosso_numero);
        $login = 'MENS_AUTOMATICA';
        
        while($count_lote_boleto < $count_nosso_numero){
            $array_gera_boleto = array();

            echo 'Array Nosso Num: <pre>';
            print_r($array_gera_boleto_nosso_numero);
            echo '</pre>';

            $var_aux = $count_lote_boleto;
            $var_id_empresa = $array_gera_boleto_nosso_numero[$var_aux]['id_empresa'];
            $var_id_empresa_emitente = $array_gera_boleto_nosso_numero[$var_aux]['id_empresa_emitente'];
            $var_id_venda = $array_gera_boleto_nosso_numero[$var_aux]['id_venda'];
            $var_id_notfat = $array_gera_boleto_nosso_numero[$var_aux]['id_notfat'];
            $var_serie = $array_gera_boleto_nosso_numero[$var_aux]['serie'];
            $var_id_mov = $array_gera_boleto_nosso_numero[$var_aux]['id_mov'];
            $var_id_desd = $array_gera_boleto_nosso_numero[$var_aux]['id_desd'];
            $var_operacao_boleto = $array_gera_boleto_nosso_numero[$var_aux]['operacao_boleto'];
            $var_id_boleto_email_aux = '';
            $var_controle_lote = $array_gera_boleto_nosso_numero[$var_aux]['controle_lote'];
            $var_banco_email_aux = '';
            $var_id_boleto = $array_gera_boleto_nosso_numero[$var_aux]['id_boleto'];
            $var_banco = $array_gera_boleto_nosso_numero[$var_aux]['id_banco'];
            $var_rec_pag_agrupada = '';

            $var_pk_arr = $var_id_empresa.$var_id_mov.$var_id_desd.'RC';

            $array_gera_boleto[$var_pk_arr] = array(
                'id_empresa'                => $var_id_empresa
                , 'id_empresa_emitente'     => $var_id_empresa_emitente
                , 'id_venda'                => $var_id_venda
                , 'id_notfat'               => $var_id_notfat
                , 'serie'                   => $var_serie
                , 'id_mov'                  => $var_id_mov
                , 'id_desd'                 => $var_id_desd
                , 'var_operacao_boleto'     => $var_operacao_boleto
                , 'var_id_boleto_email_aux' => $var_id_boleto_email_aux
                , 'controle_lote'           => $var_controle_lote
                , 'var_banco_email_aux'     => $var_banco_email_aux
                , 'var_id_boleto'           => $var_id_boleto
                , 'var_banco'               => $var_banco
                , 'rec_pag_agrupada'        => $var_rec_pag_agrupada
                , 'app_anterior'            => ''
                , 'registro_online'         => 'N'
            );

            echo'Dados: <pre>';
            print_r([$array_gera_boleto, dbNameCob($var_id_empresa), $login]);
            echo '</pre>';

            $array_retorno = fcProcessaBoleto($array_gera_boleto, dbNameCob($var_id_empresa), $login);

            echo "ARRAY RETORNO: <pre>";
            print_r($array_retorno);
            echo "</pre>";

            if(isset($array_retorno['sucesso']) && $array_retorno['sucesso']){
                echo 'Sucesso:<pre>';
                print_r($array_retorno);
                echo '</pre>';

                $index = $var_id_venda.$var_id_desd;
                echo "Index: $index<hr>";

                $dados_boleto = $array_retorno[$id_empresa.$index.'RC']['retorno'][$index]['dadosboleto'];

                echo "Dados boleto: <pre>";
                print_r($dados_boleto);
                echo "</pre>";

                if($dados_boleto['sucesso']){

                    $nosso_numero = $dados_boleto['nosso_numero']??'';
                    $dt_vencimento = $dados_boleto['dt_vencimento']??'';

                    if($dt_vencimento){
                        $data = new DateTime($dt_vencimento);
                        $dt_vencimento = $data->format('d/m/Y');
                    }
                    $valor = $dados_boleto['valor']??0;
                    $valor = $valor? number_format((float)$valor, 2, ',', '.') : '0,00';
                    $linha_digitavel = $dados_boleto['linha_digitavel']??'';

                    echo "Nosso número: $nosso_numero<hr>";
                    echo "Data vencimento: $dt_vencimento<hr>";
                    echo "Valor: $valor<hr>";
                    echo "Linha Digitável: $linha_digitavel<hr>";

                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'S', "Nr.: $var_id_mov - $var_id_desd Nr. Nosso Número: /$nosso_numero - Valor: $valor - Linha Digitável: $linha_digitavel", $var_id_mov, 'Contas a receber');
                    
                }else{
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', ($dados_boleto['mensagem']??"Falha ao gerar os dados do boleto. Favor verificar!")."\n
                    Nr.: $var_id_mov - $var_id_desd", $var_id_mov, 'Contas a receber');
                    $erro_passo = true;
                }

                
            }else{
                echo 'Erro:<pre>';
                print_r($array_retorno);
                echo '</pre>';
                fc_gera_log($id_empresa, $id_cobranca_mensalidade, 3, 'N', "Falha ao gerar os dados do boleto. Favor verificar!\n
                    Nr.: $var_id_mov - $var_id_desd", $var_id_mov, 'Contas a receber');
                $erro_passo = true;
            }
            $count_lote_boleto++;
        }

        if($erro_passo){
            registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 3);

            return;
        }else{


            $conexao = new Conexao();
            $conn = $conexao->open(dbNameCob($id_empresa));

            $sql_update = "UPDATE db_gol.tb_passo_mensalidade_automatica
                                    SET tx_status = 'CONCLUIDO',
                                    dt_fim = NOW()
                                    WHERE id_empresa = $id_empresa
                                    AND id_cobranca_mensalidade = $id_cobranca_mensalidade
                                    AND id_passo = 3";

            $ret = pg_query($conn, $sql_update);

            if($ret){
                echo "Atualizou o passo!<hr>";

                 $sql_update = "UPDATE db_gol.tb_cobranca_mensalidade
                            SET lo_boleto_gerado = 'S'
                            WHERE id_empresa = $id_empresa AND id_cobranca_mensalidade = $id_cobranca_mensalidade";

                if(pg_query($conn, $sql_update)){
                    echo "Atualizou a cobrança!<hr>";

                    $sql_insert = "INSERT INTO db_gol.tb_passo_mensalidade_automatica
                        (id_empresa, id_cobranca_mensalidade, id_passo, tx_status, dt_inc)
                        VALUES($id_empresa, $id_cobranca_mensalidade, 4, 'EM_PROGRESSO', NOW());
                    ";

                    if(pg_query($conn, $sql_insert)){
                        echo "Inseriu o próximo passo!<hr>";

                        $arrayServidor = array('https://'.$_SERVER['SERVER_NAME']."/includes/geraMensalidadeAutomatica/gerarNotificacao.php?id_empresa=$id_empresa&id_cobranca_mensalidade=$id_cobranca_mensalidade");

                        if($_SERVER['SERVER_NAME'] == 'srvdsv3.axmsolucoes.com.br'){
                          $arrayServidor = array("https://srvdsv3.axmsolucoes.com.br/scriptcase915/app/GOL/includes/geraMensalidadeAutomatica/gerarNotificacao.php?id_empresa=$id_empresa&id_cobranca_mensalidade=$id_cobranca_mensalidade");
                        }
                      
                        $resultados = curlMultiplasURLS($arrayServidor);
  
                        echo '<pre>';
                        print_r($resultados);
                        echo '</pre>';
                    }  
                }
            }
        }
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

    echo 'Email: <pre>';
    print_r(fc_monta_relatorio($id_empresa, $id_cobranca_mensalidade));
    echo '</pre>';

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