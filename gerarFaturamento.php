<?php

//require CAMINHO_INSTALACAO.'/includes/sendgrid-php-master/vendor/autoload.php'; // If you're using Composer (recommended)
include_once('../defineVariavelAmbiente.php');
include_once('./log.php');
include_once('./relatorio.php');

if (!class_exists('Conexao')) {
	$var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
	require_once($var_caminho_con);
}

$retMensalidade['sucesso'] = false;
$retMensalidade['dados']  = "";
$arr_venda_faturar_loop = array();
$id_venda_chked = array();
$var_cont_lote_faturar_loop = 0;               
$var_id_venda = 0;
$id_empresa_emitente = 0;
$var_tipo_operacao = ''; 
$var_dt_carga = '';
$var_erro = false;
$var_msg_erro = '';
$var_erro = 0;


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

    echo "Id Empresa: $id_empresa<hr>";
    echo "Id Cobrança Mensalidade: $id_cobranca_mensalidade<hr>";

    gera_faturamento($id_empresa, $id_cobranca_mensalidade);
}else{
    echo "Erro! parâmetros não recebidos via GET";
    exit;
}

function gera_faturamento($id_empresa, $id_cobranca_mensalidade)
{

    echo "gerarFaturamento.php ==> <hr>";
    
    $conexao = new Conexao();
    $conn = $conexao->open(dbNameCob($id_empresa));

    //Forçando a parada para evitar bug
    $sql_search = "
    SELECT COUNT(*) AS count 
    FROM db_gol.tb_passo_mensalidade_automatica
    WHERE id_empresa = $id_empresa
    AND id_cobranca_mensalidade = $id_cobranca_mensalidade
    AND id_passo = 3;
    ";

    $ret = pg_query($conn, $sql_search);
    $count = pg_fetch_all($ret)[0]['count'];

    if($count){
        return;
    }

    faturar($id_empresa, $id_cobranca_mensalidade, $conn);

    $conexao->close();
}

gera_faturamento(1, 465);
    
function faturar($id_empresa, $id_cobranca_mensalidade, $conn){
    
    $var_erro = '0';
    $var_msg_erro= '';
    $login = "MENS_AUTOMATICA";

    $ds_vd = "
    SELECT id_venda, id_empresa_emitente
        FROM db_gol.tb_venda 
        WHERE id_empresa = $id_empresa
        AND id_cobranca_mensalidade = $id_cobranca_mensalidade
        AND status = 1";

//---------------------Realizar a busca das vendas e serem faturadas da mesma forma que ocorre nas aplicações fat242_2

    $count_error = 0;

    $ret = pg_query($conn, $ds_vd);
    $result_venda = pg_fetch_all($ret);

    echo $ds_vd.'<hr>Vendas encontradas: <pre>';
    print_r($result_venda);
    echo '</pre>';

    if($result_venda && !empty($result_venda)){
        foreach($result_venda as $key_vd => $linha_vd){
            $var_id_venda = $linha_vd['id_venda'];
            $id_empresa_emitente = $linha_vd['id_empresa_emitente'];
            $var_pk_venda = $id_empresa.$var_id_venda.$id_empresa_emitente;
            $var_tipo_operacao = 1;
            $id_venda_chked[$var_pk_venda] = array(
                'id_empresa'             => $id_empresa
                , 'id_venda'             => $var_id_venda
                , 'id_empresa_emitente'  => $id_empresa_emitente
                , 'retorno'              => true
                , 'mensagem'             => ''
                , 'tipo_operacao'       => $var_tipo_operacao
            );
            
        }
    }else{
        fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', 'Não foi encontrada nenhuma venda a ser faturada! Favor verifique.');
        registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 2);

        echo 'Email: <pre>';
        print_r(fc_monta_relatorio($id_empresa, $id_cobranca_mensalidade));
        echo '</pre>';
        return;
    }

    echo '$id_venda_chked => <pre>';
    print_r($id_venda_chked);
    echo '</pre>';

    foreach ($id_venda_chked as $venda) {

        if (isset($venda['id_venda'], $venda['id_empresa'], $venda['id_empresa_emitente'])) {
            $id_venda = $venda['id_venda']; 
            $id_empresa = $venda['id_empresa'];
            $id_empresa_emitente = $venda['id_empresa_emitente'];
        } else {
            echo "Erro: Dados da venda incompletos.";
            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', 'Erro: Dados da venda incompletos.', $id_venda?$id_venda:false, $id_venda?'Venda':false);
            continue; 
        }

        echo 'Dados da venda: <pre>';
        print_r([$id_venda, $id_empresa_emitente, $id_empresa, $login]);
        echo '</pre>';

        $soapUrl = '';

        if ($id_empresa == 1) {
            $soapUrl = "https://srvdsv3.axmsolucoes.com.br/scriptcase915/app/GOL/mob001_1/mob001_1.php?id_venda=$id_venda&var_id_empresa=$id_empresa_emitente&var_glo_empresa=$id_empresa&var_glo_login=$login";
        } else {
            $soapUrl = "https://" . NOME_SERVIDOR . ".sempretecnologia.com.br/mob001_1/mob001_1.php?id_venda=$id_venda&var_id_empresa=$id_empresa_emitente&var_glo_empresa=$id_empresa&var_glo_login=$login";
        }

        $paramXML = '
            <urn:mobile_faturar soapenv:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
                <venda xsi:type="xsd:int">' . $id_venda . '</venda>
                <empresa xsi:type="xsd:int">' . $id_empresa . '</empresa>
                <glo_empresa xsi:type="xsd:int">' . $id_empresa_emitente . '</glo_empresa>
                <login xsi:type="xsd:string">' . $login . '</login>
            </urn:mobile_faturar>
        ';

        $headers = [
            "Content-Type: text/xml;charset=UTF-8",
            "SOAPAction: urn:mobile_faturar"
        ];

        $soapEnvelope = '
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:mobile_faturar">
           <soapenv:Header/>
           <soapenv:Body>' . $paramXML . '</soapenv:Body>
        </soapenv:Envelope>
        ';

        $options = [
            'location' => $soapUrl,
            'uri' => 'urn:mobile_faturar', 
            'trace' => true,
            'cache_wsdl' => WSDL_CACHE_NONE,  
            'stream_context' => stream_context_create([
                'http' => [
                    'header' => implode("\r\n", $headers),
                ]
            ]),
        ];

        try {
            $soapClient = new SoapClient(null, $options); 
            $response = $soapClient->__doRequest($soapEnvelope, $soapUrl, "urn:mobile_faturar", 1);
        
            // Tenta detectar a codificação da string para garantir que esteja em UTF-8
            if (mb_detect_encoding($response, 'UTF-8', true) === false) {
                // Se a string não estiver em UTF-8, força a conversão
                $response = iconv('ISO-8859-1', 'UTF-8//TRANSLIT', $response);
            }
        
            // Remover acentuação
            $response_clean = substituirAcentos($response);
        
            // Separar as partes da resposta
            $parts = explode('#@#', $response_clean);
        
            echo 'Parts: <pre>';
            print_r($parts);
            echo '</pre>';
        
            // Filtrando partes válidas
            $filtered_parts = array_filter($parts);
        
            echo 'Filtered Parts: <pre>';
            print_r($filtered_parts);
            echo '</pre>';
        
            // Pegando os dois últimos elementos
            $last_two = array_slice($filtered_parts, -2);
        
            echo 'Last Two: <pre>';
            print_r($last_two);
            echo '</pre>';
        
            if (count($last_two) >= 2) {
                $status = htmlentities($last_two[0], ENT_QUOTES, 'UTF-8');
                $message = pg_escape_string($conn, $last_two[1]);
        
                echo "Status: " . $status . '<br>';
                echo "Message: " . $message . '<hr>';
        
                if (trim($status) == 'S') {
                    echo 'SIM<hr>';
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'S', $message, $id_venda?$id_venda:false, $id_venda?'Venda':false);
                } else {
                    $count_error++;
                    echo 'NAO<hr>';
                    echo "Message: $message<hr>";
                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', $message, $id_venda?$id_venda:false, $id_venda?'Venda':false);
                }
            } else {
                $count_error++;
                echo "Erro: Resposta inesperada do servidor. Dados SOAP incompletos.";
                fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', 'Erro de servidor ao tentar faturar venda ' . $id_venda, $id_venda?$id_venda:false, $id_venda?'Venda':false);
            }
        
        } catch (SoapFault $fault) {
            echo "Erro SOAP: Código de erro " . $fault->faultcode . " - " . $fault->getMessage();
            $count_error++;
            fc_gera_log($id_empresa, $id_cobranca_mensalidade, 2, 'N', 'Erro ao tentar faturar venda '.$id_venda, $id_venda?$id_venda:false, $id_venda?'Venda':false);
        }
        
        
    }

	
	if($count_error){
        echo 'ERRO<hr>';
		registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 2);
	}else{
        echo 'CONC<hr>';
		$sql_update = "UPDATE db_gol.tb_passo_mensalidade_automatica
		SET tx_status = 'CONCLUIDO',
		dt_fim = NOW()
		WHERE id_empresa = $id_empresa
		AND id_cobranca_mensalidade = $id_cobranca_mensalidade
		AND id_passo = 2";

		pg_query($conn, $sql_update);

        $sql_update = "
            UPDATE db_gol.tb_cobranca_mensalidade
                SET lo_faturamento_gerado = 'S'
            WHERE id_empresa = $id_empresa
            AND id_cobranca_mensalidade = $id_cobranca_mensalidade";

        pg_query($conn, $sql_update);

        $sql_insert = "INSERT INTO db_gol.tb_passo_mensalidade_automatica
                    (id_empresa, id_cobranca_mensalidade, id_passo, tx_status, dt_inc)
                    VALUES($id_empresa, $id_cobranca_mensalidade, 3, 'EM_PROGRESSO', NOW());
        ";

        pg_query($conn, $sql_insert);

	}

    $arrayServidor = array('https://'.$_SERVER['SERVER_NAME']."/includes/geraMensalidadeAutomatica/gerarNossoNumero.php?id_empresa=$id_empresa&id_cobranca_mensalidade=$id_cobranca_mensalidade");

    if($_SERVER['SERVER_NAME'] == 'srvdsv3.axmsolucoes.com.br'){
      $arrayServidor = array("https://srvdsv3.axmsolucoes.com.br/scriptcase915/app/GOL/includes/geraMensalidadeAutomatica/gerarNossoNumero.php?id_empresa=$id_empresa&id_cobranca_mensalidade=$id_cobranca_mensalidade");
    }
  
    $resultados = curlMultiplasURLS($arrayServidor);
  
    echo '<pre>';
    print_r($resultados);
    echo '</pre>';

} 

function substituirAcentos($string) {
    $comAcento = array('À', 'Á', 'Â', 'Ã', 'Ä', 'Å', 'Æ', 'Ç', 'È', 'É', 'Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ', 'Ò', 'Ó', 'Ô', 'Õ', 'Ö', 'Ù', 'Ú', 'Û', 'Ü', 'Ý', 'Þ', 'ß', 'à', 'á', 'â', 'ã', 'ä', 'å', 'æ', 'ç', 'è', 'é', 'ê', 'ë', 'ì', 'í', 'î', 'ï', 'ð', 'ñ', 'ò', 'ó', 'ô', 'õ', 'ö', 'ù', 'ú', 'û', 'ü', 'ý', 'þ', 'ÿ');
    $semAcento = array('A', 'A', 'A', 'A', 'A', 'A', 'AE', 'C', 'E', 'E', 'E', 'E', 'I', 'I', 'I', 'I', 'D', 'N', 'O', 'O', 'O', 'O', 'O', 'U', 'U', 'U', 'U', 'Y', 'Th', 's', 'a', 'a', 'a', 'a', 'a', 'a', 'ae', 'c', 'e', 'e', 'e', 'e', 'i', 'i', 'i', 'i', 'd', 'n', 'o', 'o', 'o', 'o', 'o', 'u', 'u', 'u', 'u', 'y', 'th', 'y');
    
    return str_replace($comAcento, $semAcento, $string);
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