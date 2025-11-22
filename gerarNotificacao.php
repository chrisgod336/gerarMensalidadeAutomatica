<?php

include_once('../defineVariavelAmbiente.php');
include_once('./log.php');
include_once('./relatorio.php');

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

    gera_notificacao($id_empresa, $id_cobranca_mensalidade);
}else{
    echo "Erro! parâmetros não recebidos via GET";
    exit;
}

function gera_notificacao($id_empresa, $id_cobranca_mensalidade) {
    echo "Gerar Notificacao!<hr>";

    $conexao = new Conexao();
    echo "Gera notificação: ".dbNameCob($id_empresa).'<hr>';
    $conn = $conexao->open(dbNameCob($id_empresa));
        
    $arr_logs = gera_regua_cobranca($id_empresa);

    if(!$arr_logs || empty($arr_logs)){
        fc_gera_log($id_empresa, $id_cobranca_mensalidade, 4, 'N', 'Não foi possível gerar as notificações de cobrança.');
        registra_erro_passo($id_empresa, $id_cobranca_mensalidade, 4);
    }else{
        foreach($arr_logs as $logs){
            $titulo = $logs['notificacao']['tituloNotificacao'];

            $rows = $logs['atencao']['linhas'];

            foreach($rows as $row){
                echo "row: <pre>";
                print_r($row);
                echo "</pre>";
                $id = explode('/', $row['codigo'])[0];
                fc_gera_log($id_empresa, $id_cobranca_mensalidade, 4, 'S', "$titulo: ${row['mensagem']}", $id, 'Contas a receber');
            }
        }

        pg_query($conn, "UPDATE db_gol.tb_cobranca_mensalidade
            SET lo_cobranca_gerada = 'S'
            WHERE id_empresa = $id_empresa
            AND id_cobranca_mensalidade = $id_cobranca_mensalidade");

        pg_query($conn, "UPDATE db_gol.tb_passo_mensalidade_automatica
                                    SET tx_status = 'CONCLUIDO',
                                    dt_fim = NOW()
                                    WHERE id_empresa = $id_empresa
                                    AND id_cobranca_mensalidade = $id_cobranca_mensalidade
                                    AND id_passo = 4");
    }

    #Envia o relatório
      echo 'Email: <pre>';
      print_r(fc_monta_relatorio($id_empresa, $id_cobranca_mensalidade));
      echo '</pre>';
    
}

function fc_data_dia_fixo($nm_not_4, $var_id_empresa_4, $nu_dia_fixo, $nu_notificacao_4, $email_host_4, $email_user_4, $email_pwd_4, $email_porta_4, $email_from_4, $conn){
    echo 'fc_data_dia_fixo ==========> <hr>';
    $falha          = 0;
    $result_insert  = 0;
    $redir          = false;
    $msg            = "";
    $arrMsgReturn   = array();
    $arrMsgInsert   = array();
    $arrMsgUpdate   = array();

    $msg_header     = "7ª NOTIFICAÇÃO";
    $tituloNotificacao = '7ª Notificação';

    if($nu_dia_fixo > 30){
        $nu_dia_fixo = 30;
    } 

    $nu_dia_fixo = str_pad($nu_dia_fixo, 2, "0", STR_PAD_LEFT); 

    if($var_id_empresa_4 == 8858){
        $sql_search = "
            SELECT  
                    a.id_empresa AS id_empresa
                    , a.id_mov AS id_mov
                    , a.id_desd AS id_desd
                    , a.tipo AS tipo
                    , a.dt_vencimento AS dt_vencimento
            /*5*/   , TRIM(b.email) AS email_cliente
                    , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                    , c.cnpj AS cnpj_empresa
                    , TRIM(c.razao_social) AS razao_social_empresa
                    , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
            /*10*/  , c.email AS email_empresa
                    , c.telefone AS telefone_empresa
                    , c.tx_logo_nfe AS logo_empresa
                    , a.id_venda AS id_venda
                    , a.dt_emissao AS dt_emissao    
            /*15*/  , CASE WHEN a.dt_emissao::date  < ((EXTRACT('YEAR' FROM now()::date))
                                                        ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                                        ||'-'|| '01')::Date
                           THEN ((EXTRACT('YEAR' FROM now()::date))
                                ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                ||'-'|| '$nu_dia_fixo')::Date
                           ELSE ((EXTRACT('YEAR' FROM a.dt_emissao))
                                ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                  THEN 
                                                     CASE WHEN '$nu_dia_fixo' > '28'
                                                     THEN '28'
                                                     ELSE '$nu_dia_fixo'                                            
                                                  END
                                        ELSE '$nu_dia_fixo'
                                        END
                               ))::date 
                           END as dt_envio
                    , SUM(a.vr_rec_original) AS valor_fianceiro
                    , b.razao_social
                    , e.dt_venda
                    , e.dt_vencimento
            /*20*/  , f.telefone1
                    , b.contato
                    , e.nr_pedido_talao AS referencia
            FROM db_gol.tb_rec_pag a
            INNER JOIN db_gol.tb_pessoa b
            ON a.id_empresa = b.id_empresa
            AND a.id_pessoa = b.id_pessoa
            INNER JOIN db_gol.tb_empresa c
            ON a.id_empresa = c.id_empresa                              
            INNER JOIN db_gol.tb_formapg d
            ON a.id_empresa = d.id_empresa
            AND a.id_forma = d.id_forma
            INNER JOIN db_gol.tb_venda e
            ON a.id_empresa = e.id_empresa
            AND a.id_venda = e.id_venda
            LEFT JOIN db_gol.tb_pessoa_endereco f
            ON f.tipo_endereco = 1 
            AND f.id_empresa = a.id_empresa
            AND f.id_pessoa_endereco = a.id_pessoa
            WHERE a.id_empresa = $var_id_empresa_4
            AND a.id_condominio = 0
            AND a.situacao = 'A'
            AND a.tipo = 'RC'
            AND a.dt_emissao >= (NOW()::date - interval '55 days')
            AND a.dt_vencimento >= ((EXTRACT('YEAR' FROM a.dt_emissao))
                                    ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                    ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                             THEN 
                                                CASE WHEN '$nu_dia_fixo' > '28'
                                                THEN '28'
                                                ELSE '$nu_dia_fixo'                                         
                                                END
                                             ELSE '$nu_dia_fixo'
                                             END
                                  ))::date
            AND b.lo_cobranca_automatica = 'S'
            AND d.lo_tipo_boleto = 'S'
            AND a.id_mov IN(SELECT
                                vd.id_venda
                            FROM db_gol.tb_venda vd
                            WHERE vd.id_empresa = a.id_empresa
                            AND vd.tx_origem_venda = 'MS'                                               
                            AND vd.status IN(4, 5, 8)
                            AND vd.dt_venda >= (NOW()::date - interval '55 days'))  
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22, e.nr_pedido_talao
            ORDER BY a.dt_vencimento ASC"; 
    }else{
        $sql_search = "SELECT  
                    a.id_empresa AS id_empresa
                    , a.id_mov AS id_mov
                    , a.id_desd AS id_desd
                    , a.tipo AS tipo
                    , a.dt_vencimento AS dt_vencimento
            /*5*/   , TRIM(b.email) AS email_cliente
                    , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                    , c.cnpj AS cnpj_empresa
                    , TRIM(c.razao_social) AS razao_social_empresa
                    , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
            /*10*/  , c.email AS email_empresa
                    , c.telefone AS telefone_empresa
                    , c.tx_logo_nfe AS logo_empresa
                    , a.id_venda AS id_venda
                    , a.dt_emissao AS dt_emissao    
            /*15*/  , CASE WHEN a.dt_emissao::date  < ((EXTRACT('YEAR' FROM now()::date))
                                                        ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                                        ||'-'|| '01')::Date
                           THEN ((EXTRACT('YEAR' FROM now()::date))
                                ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                ||'-'|| '$nu_dia_fixo')::Date
                           ELSE ((EXTRACT('YEAR' FROM a.dt_emissao))
                                ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                  THEN 
                                                     CASE WHEN '$nu_dia_fixo' > '28'
                                                     THEN '28'
                                                     ELSE '$nu_dia_fixo'                                            
                                                  END
                                        ELSE '$nu_dia_fixo'
                                        END
                               ))::date 
                           END as dt_envio
                    , SUM(a.vr_rec_original) AS valor_fianceiro
                    , b.razao_social
                    , e.dt_venda
                    , e.dt_vencimento
            /*20*/  , f.telefone1
                    , b.contato
                    , e.nr_pedido_talao AS referencia
            FROM db_gol.tb_rec_pag a
            INNER JOIN db_gol.tb_pessoa b
            ON a.id_empresa = b.id_empresa
            AND a.id_pessoa = b.id_pessoa
            INNER JOIN db_gol.tb_empresa c
            ON a.id_empresa = c.id_empresa                              
            INNER JOIN db_gol.tb_formapg d
            ON a.id_empresa = d.id_empresa
            AND a.id_forma = d.id_forma
            INNER JOIN db_gol.tb_venda e
            ON a.id_empresa = e.id_empresa
            AND a.id_venda = e.id_venda
            LEFT JOIN db_gol.tb_pessoa_endereco f
            ON f.tipo_endereco = 1 
            AND f.id_empresa = a.id_empresa
            AND f.id_pessoa_endereco = a.id_pessoa
            WHERE a.id_empresa = $var_id_empresa_4
            AND a.id_condominio = 0
            AND a.situacao = 'A'
            AND a.tipo = 'RC'
            AND a.dt_emissao >= (NOW()::date - interval '25 days')
            AND a.dt_vencimento >= ((EXTRACT('YEAR' FROM a.dt_emissao))
                                    ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                    ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                             THEN 
                                                CASE WHEN '$nu_dia_fixo' > '28'
                                                THEN '28'
                                                ELSE '$nu_dia_fixo'                                         
                                                END
                                             ELSE '$nu_dia_fixo'
                                             END
                                  ))::date
            AND b.lo_cobranca_automatica = 'S'
            AND d.lo_tipo_boleto = 'S'
            AND a.id_mov IN(SELECT
                                vd.id_venda
                            FROM db_gol.tb_venda vd
                            WHERE vd.id_empresa = a.id_empresa
                            AND vd.tx_origem_venda = 'MS'                                               
                            AND vd.status IN(4, 5, 8)
                            AND vd.dt_venda >= (NOW()::date - interval '25 days'))  
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22, e.nr_pedido_talao
            ORDER BY a.dt_vencimento ASC";
    }

    $ret = pg_query($conn, $sql_search);
    $dia_fixo = pg_fetch_all($ret);      

    if(empty($dia_fixo)){
        $arrMsgReturn[$nm_not_4] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Não há recebimentos em aberto para gerar notificações de cobrança.'
        );
        return $arrMsgReturn;

    }

    #INFORMAÇÕES DOS DADOS DA NOTIFICAÇÃO
    $sql_search = "
        SELECT 
            tx_assunto_".$nm_not_4.",
            tx_texto_".$nm_not_4." ,
            tx_texto_sms_".$nm_not_4." ,
            lo_sms_ativo_".$nm_not_4." ,
            tx_texto_whatsapp_".$nm_not_4." ,
            lo_whatsapp_ativo_".$nm_not_4.",
            tx_tipo_anexo_email 
        FROM db_gol.tb_regua_cobranca 
        WHERE id_empresa = $var_id_empresa_4
        AND id_condominio = 0 
    ";

    $ret = pg_query($conn, $sql_search); 
    $dados_notificacoes_dias_fixos = pg_fetch_all($ret);             

    $var_assunto          = $dados_notificacoes_dias_fixos[0]["tx_assunto_".$nm_not_4];
    $var_texto            = $dados_notificacoes_dias_fixos[0]["tx_texto_".$nm_not_4];
    $var_texto_sms        = $dados_notificacoes_dias_fixos[0]["tx_texto_sms_".$nm_not_4];
    $var_envia_sms        = $dados_notificacoes_dias_fixos[0]["lo_sms_ativo_".$nm_not_4];
    $var_texto_whatsapp   = $dados_notificacoes_dias_fixos[0]["tx_texto_whatsapp_".$nm_not_4];
    $var_envia_whatsapp   = $dados_notificacoes_dias_fixos[0]["lo_whatsapp_ativo_".$nm_not_4];
    $var_tipo_anexo_email = $dados_notificacoes_dias_fixos[0]["tx_tipo_anexo_email"];

    $param_permissao_sms = buscaParam($var_id_empresa_4, 'utiliza_notificacao_sms');
    $param_permissao_whatsapp = buscaParam($var_id_empresa_4, 'utiliza_notificacao_whatsapp');

    $var_permissao_sms         = $param_permissao_sms ? $param_permissao_sms : 'N';
    $var_permissao_whatsapp    = $param_permissao_whatsapp ? $param_permissao_whatsapp : 'N';

    if($var_permissao_sms == 'N'){
        $var_envia_sms = 'N'; 
    }

    if($var_permissao_whatsapp == 'N'){
        $var_envia_whatsapp = 'N' ;
    }

    $var_valida_qtd_bilhetagem_sms = fc_valida_bilhetagem('SMS', $var_id_empresa_4, $conn);

    if($var_valida_qtd_bilhetagem_sms == false){
        $var_envia_sms = 'N';
    }

    $var_valida_qtd_bilhetagem_whatsapp = fc_valida_bilhetagem('WHATSAPP', $var_id_empresa_4, $conn);

    if($var_valida_qtd_bilhetagem_whatsapp == false){
        $var_envia_whatsapp = 'N';
    }

    $var_assunto    = pg_escape_string($var_assunto);
    $var_texto      = pg_escape_string($var_texto);
    $var_texto_sms  = pg_escape_string($var_texto_sms);

    if(empty($var_assunto)){
        $arrMsgReturn[$nm_not_4] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo assunto do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    
    }
    if(empty($var_texto)){
        $arrMsgReturn[$nm_not_4] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo mensagem do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }

    #PERCORRE OS DADOS E INSERE NO BANCO
    $size = count($dia_fixo);
    for($i = 0; $i < $size ; $i++){
        $var_id_empresa           = $dia_fixo[$i]['id_empresa'];
        $var_id_mov               = $dia_fixo[$i]['id_mov'];
        $var_id_desd              = $dia_fixo[$i]['id_desd'];
        $var_tipo                 = $dia_fixo[$i]['tipo'];
        $var_dt_vencimento        = $dia_fixo[$i]['dt_vencimento'];
        $email_cliente            = $dia_fixo[$i]['email_cliente'];
        $var_nm_fantasia_cliente  = $dia_fixo[$i]['nm_fantasia_cliente'];
        $var_cnpj_empresa         = $dia_fixo[$i]['cnpj_empresa'];
        $var_razao_social_empresa = $dia_fixo[$i]['razao_social_empresa'];
        $var_nm_fantasia_empresa  = $dia_fixo[$i]['nm_fantasia_empresa'];
        $var_email_empresa        = $dia_fixo[$i]['email_empresa'];
        $var_telefone_empresa     = $dia_fixo[$i]['telefone_empresa'];
        $var_logo_empresa         = $dia_fixo[$i]['logo_empresa'];
        $var_id_venda             = $dia_fixo[$i]['id_venda'];
        $var_dt_envio             = $dia_fixo[$i]['dt_envio'];
        $var_valor_fianceiro      = $dia_fixo[$i]['valor_fianceiro'];
        $var_razao_social         = $dia_fixo[$i]['razao_social'];
        $var_dt_venda             = $dia_fixo[$i]['dt_venda'];
        $var_contato              = $dia_fixo[$i]['dt_vencimento'];
        $var_dt_vencimento_venda  = $dia_fixo[$i]['telefone1'];
        $var_numero_telefone      = $dia_fixo[$i]['contato'];
        $var_referencia           = $dia_fixo[$i]['referencia'];

        $arr_valor_hashtag = array();
        $arr_valor_hashtag['nm_fantasia_cliente'] = $var_nm_fantasia_cliente;                                       
        $arr_valor_hashtag['cnpj_empresa'] = $var_cnpj_empresa;
        $arr_valor_hashtag['razao_social_empresa'] = $var_razao_social_empresa;
        $arr_valor_hashtag['nm_fantasia_empresa'] = $var_nm_fantasia_empresa;
        $arr_valor_hashtag['email_empresa'] = $var_email_empresa;
        $arr_valor_hashtag['telefone_empresa'] = $var_telefone_empresa;
        $arr_valor_hashtag['logo_empresa'] = $var_logo_empresa;
        $arr_valor_hashtag['id_venda'] = $var_id_venda;
        $arr_valor_hashtag['texto'] = $var_texto;
        $arr_valor_hashtag['assunto'] = $var_assunto;
        $arr_valor_hashtag['valor_fianceiro'] = $var_valor_fianceiro;
        $arr_valor_hashtag['razao_social'] = $var_razao_social;
        $arr_valor_hashtag['data_vencimento'] = $var_dt_vencimento;
        $arr_valor_hashtag['data_venda'] = $var_dt_venda; 
        $arr_valor_hashtag['data_vencimento_venda'] = $var_dt_vencimento_venda;
        $arr_valor_hashtag['contato'] = $var_contato;
        $arr_valor_hashtag['referencia'] = $var_referencia;

        //metodo criado para trocar os valores das hashtags
        $var_novo_texto = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        //metodo criado para trocar os valores das hashtags
        $var_novo_assunto = fc_adiciona_hashtag_assunto($arr_valor_hashtag, $var_id_empresa);

        // TROCO APENAS A VARIAL DO TEXT SMS
        $arr_valor_hashtag['texto'] = $var_texto_sms;
        $var_novo_texto_sms = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        // TROCO APENAS A VARIAL DO TEXT WHATSAPP
        $arr_valor_hashtag['texto'] = $var_texto_whatsapp;
        $var_novo_texto_whatsapp = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        $conexao = new Conexao();
        $conn = null;

        if($var_id_empresa == 1){
            echo 'Contrato do dsv<hr>';
            $conn = $conexao->open("conn_contratos_dsv");
        }else{
            echo 'Contrato do prod<hr>';
            $conn = $conexao->open("conn_contratos");
        }

        $sql_search = "   
            SELECT 
                * 
            FROM db_contratos.tb_notificacao 
            WHERE id_empresa    = $var_id_empresa 
            AND id_mov          = $var_id_mov
            AND id_desd         = $var_id_desd
            AND nu_notificacao  = $nu_notificacao_4";

        $ret = pg_query($conn, $sql);

        if(empty($check_notificacao)){
            $sql_insert = "
                INSERT INTO db_contratos.tb_notificacao(
                    id_empresa,
                    id_mov,
                    id_desd,
                    nu_notificacao,
                    tipo,
                    tx_email_host,
                    tx_email_smtp_secure,
                    tx_email_username,
                    tx_email_password,
                    tx_email_porta,
                    tx_email_from,
                    tx_email_address,
                    tx_email_subject,
                    tx_email_body,
                    tx_login,
                    tx_retorno,
                    lo_enviado,
                    dt_vencimento,
                    dt_inc,
                    dt_envio,
                    tx_texto_sms,
                    lo_envia_sms,
                    nu_numero_telefone,
                    tx_texto_whatsapp,
                    lo_envia_whatsapp,
                    tx_tipo_anexo_email 
                ) VALUES (
                    $var_id_empresa,
                    $var_id_mov,
                    $var_id_desd,
                    $nu_notificacao_4,
                    '$var_tipo',
                    '$email_host_4',
                    'tls',
                    '$email_user_4',
                    '$email_pwd_4',
                    '$email_porta_4',
                    '$email_from_4',
                    '$email_cliente',
                    '$var_novo_assunto',
                    '$var_novo_texto',
                    '$id_empresa',
                    '',
                    'N',
                    '$var_dt_vencimento',
                    now(),
                    '$var_dt_envio',
                    '$var_novo_texto_sms',
                    '$var_envia_sms',
                    '$var_numero_telefone',
                    '$var_novo_texto_whatsapp',
                    '$var_envia_whatsapp',
                    '$var_tipo_anexo_email'
                )";
            $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");
            $result_insert++;
        }else{
            $var_lo_enviado = $check_notificacao[0]['lo_enviado'];
            if($var_lo_enviado == 'N'){
                $arr = array(
                    'tipo'                      => $var_tipo,
                    'tx_email_host'             => $email_host_4,
                    'tx_email_smtp_secure'      => 'tls',
                    'tx_email_username'         => $email_user_4,
                    'tx_email_password'         => $email_pwd_4,
                    'tx_email_porta'            => $email_porta_4,
                    'tx_email_from'             => $email_from_4,
                    'tx_email_address'          => $email_cliente,
                    'tx_email_subject'          => $var_novo_assunto,
                    'tx_email_body'             => $var_novo_texto,
                    'tx_login'                  => 'MENS_AUTOMATICA',
                    'tx_retorno'                => '',
                    'lo_enviado'                => 'N',
                    'dt_vencimento'             => $var_dt_vencimento,
                    'dt_inc'                    => 'NOW()',
                    'dt_envio'                  => $var_dt_envio,
                    'tx_tipo_anexo_email'       => $var_tipo_anexo_email
                );

                $tb_valor = "";
                $tb_coluna = array();

                foreach ($arr as $key => $value) {
                    $tb_valor       = $value;
                    $tb_coluna[]    = "$key = '$value'";        
                }

                $implode_result = implode(', ', $tb_coluna);

                $sql_update = "
                    UPDATE db_contratos.tb_notificacao 
                        SET $implode_result
                    WHERE id_empresa = $var_id_empresa
                    AND id_mov = $var_id_mov        
                    AND id_desd = $var_id_desd
                    AND nu_notificacao = $nu_notificacao_4
                ";

                pg_query($conn, $sql_update);
                $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                $falha++;
            }
        }
    }
    $conexao = new Conexao();
    echo "Id empresa 4: ".dbNameCob($var_id_empresa_4).'<hr>';
    $conn = $conexao->open($dbNameCob($var_id_empresa_4));

    $arrCountNotificacao = array(
        'tituloNotificacao' => $tituloNotificacao,
        'sucesso' => array(
            'count' => "0"
        ),
        'atencao' => array(
            'count' => "0"
        )
    );

    if($result_insert > 0){
        $countInserted = count($arrMsgInsert);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'sucesso' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'mensagem' => "$countInserted novas notificações de cobrança inseridas."
                ),
                'linhas' => $arrMsgInsert
            )
        );
    
    }

    if($falha > 0){
        $countUpdated = count($arrMsgUpdate);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'atencao' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'subtitulo' => "$countUpdated notificações de cobrança atualizadas."
                ),
                'linhas' => $arrMsgUpdate
            )
        );

    }

    if(isset($arrMsgReturn['sucesso'], $arrMsgReturn['sucesso']['linhas'])){
        $countSucesso = count($arrMsgReturn['sucesso']['linhas']);
        $arrCountNotificacao['notificacao']['sucesso']['count'] = $countSucesso;
    }

    if(isset($arrMsgReturn['atencao'], $arrMsgReturn['atencao']['linhas'])){
        $countAtencao = count($arrMsgReturn['atencao']['linhas']);
        $arrCountNotificacao['notificacao']['atencao']['count'] = $countAtencao;
    }

    $arrMsgReturn['notificacao'] = $arrCountNotificacao;

    return $arrMsgReturn;
}

function fc_valida_bilhetagem($var_tipo, $id_empresa, $conn){
    echo 'fc_valida_bilhetagem ==========> <hr>';
    $var_retorno = false;

    $varUrl = "https://srvdsv3.axmsolucoes.com.br/scriptcase915/app/GOL/includes/apiRestSempre/index.php/consultaFinanceiroMensalidadeNovo?";
    $param = "ID_EMPRESA_CONEXAO=1&ID_EMPRESA=1&ID_PESSOA=9";

    $curl = curl_init();

    curl_setopt_array($curl, [
      CURLOPT_URL => $varUrl.$param,
      CURLOPT_RETURNTRANSFER => true,
      CURLOPT_ENCODING => "",
      CURLOPT_MAXREDIRS => 10,
      CURLOPT_TIMEOUT => 30,
      CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
      CURLOPT_CUSTOMREQUEST => "GET",
      CURLOPT_POSTFIELDS => "",
      CURLOPT_HTTPHEADER => [
        "Content-Type: application/json"
      ],
    ]);

    $response = curl_exec($curl);
    $err = curl_error($curl);

    curl_close($curl);
    $arrResponse = json_decode($response, true);

    $varQtdSmS = 0;
    $varQtdWhatsapp = 0; 

    if($arrResponse){
        $varQtdSmS = $arrResponse['qtd_sms'];
        $varQtdWhatsapp = $arrResponse['qtd_whatsapp'];

    }

    if($var_tipo == 'SMS'){

        $sql_search = "SELECT COUNT(id_notificacao_sms_whatsapp) AS count
                         FROM db_gol.vw_notificacao_sms_whatsapp 
                         WHERE id_empresa = $id_empresa
                         AND tx_tipo = 'SMS' 
                         AND status IN('delivered','sent')";

        $ret = pg_query($conn, $sql_search);

        if($ret){
            $var_qtd_envio_sms = pg_fetch_all($ret)[0]['count'];
        }
        
        if($var_qtd_envio_sms < $varQtdSmS){
            $var_retorno =  true;
        }else{
            $var_retorno =  false;
        }

    }


    if($var_tipo == 'WHATSAPP'){
        
        $sql_search = "SELECT COUNT(id_notificacao_sms_whatsapp) AS count
                                          FROM db_gol.vw_notificacao_sms_whatsapp 
                                          WHERE id_empresa = $id_empresa 
                                          AND tx_tipo = 'WHATSAPP' 
                                          AND status IN('delivered','sent')";

        $ret = pg_query($conn, $sql_search);

        if($ret){
            $var_qtd_envio_whatsapp = pg_fetch_all($ret)[0]['count'];
        }
        
        if($var_qtd_envio_whatsapp < $varQtdWhatsapp){
            $var_retorno =  true;
        }else{
            $var_retorno =  false;
        }

    }

    return true;

}

function fc_altera_hashtag($arr_valor_hashtag, $id_empresa){
    echo 'fc_altera_hashtag ==========> <hr>';
    $antes = array();
    $depois = array();
    $var_logo = '';

    $var_nm_fantasia_cliente = $arr_valor_hashtag['nm_fantasia_cliente'];
    $var_cnpj_empresa = $arr_valor_hashtag['cnpj_empresa'];
    $var_razao_social_empresa = $arr_valor_hashtag['razao_social_empresa'];
    $var_nm_fantasia_empresa = $arr_valor_hashtag['nm_fantasia_empresa'];
    $var_email_empresa = $arr_valor_hashtag['email_empresa'];
    $var_telefone_empresa = $arr_valor_hashtag['telefone_empresa'];
    $var_logo_empresa = $arr_valor_hashtag['logo_empresa'];
    $var_id_venda = $arr_valor_hashtag['id_venda'];
    $var_texto = $arr_valor_hashtag['texto'];
    $var_valor_fianceiro = number_format($arr_valor_hashtag['valor_fianceiro'],2,',','.');
    $var_razao_social = $arr_valor_hashtag['razao_social'];
    $var_dt_vencimento = $arr_valor_hashtag['data_vencimento'];
    $var_dt_venda = $arr_valor_hashtag['data_venda'];
    $var_dt_vencimento_venda = $arr_valor_hashtag['data_vencimento_venda'];
    $var_contato = $arr_valor_hashtag['contato'];
    $var_referencia = $arr_valor_hashtag['referencia'];



    $var_id_empresa_aux = str_pad($id_empresa, 6, '0', STR_PAD_LEFT);

    $var_caminho_logo = CAMINHO_INSTALACAO."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if( isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] == 'on' ) {
        $var_endereco = "https://";
    }else{
        $var_endereco = "http://";
    }

    $var_endereco_logo = $var_endereco.ENDERECO_SISTEMA."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if ((TRIM($var_logo_empresa) != '') && (is_file($var_caminho_logo))){
        $var_logo = "<img src=$var_endereco_logo width=300px height=120px/>";
    }

    $antes =  [ "#CNPJ_EMPRESA"
              , "#RAZAO_SOCIAL_EMPRESA"
              , "#NOME_FANTASIA_EMPRESA"
              , "#EMAIL_EMPRESA"
              , "#TELEFONE_EMPRESA"
              , "#LOGO_EMPRESA"
              , "#NOME_FANTASIA_CLIENTE"
              , "#NUMERO_VENDA"
              , "#FINANCEIRO_VALOR"
              , "#RAZAO_SOCIAL_CLIENTE"
              , "#DATA_VENCIMENTO_COMPLETA"
              , "#DATA_VENCIMENTO_VENDA"
              , "#DATA_VENCIMENTO"
              , "#DATA_VENDA"
              , "#CONTATO_CLIENTE"
              , "#REFERENCIA"];

    $depois = [$var_cnpj_empresa
              , $var_razao_social_empresa
              , $var_nm_fantasia_empresa
              , $var_email_empresa
              , fc_formatar_telefone($var_telefone_empresa)
              , $var_logo 
              , $var_nm_fantasia_cliente
              , $var_id_venda
              , $var_valor_fianceiro
              , $var_razao_social
              , date('d/m/Y', strtotime($var_dt_vencimento))
              , date('m/Y', strtotime($var_dt_vencimento_venda))
              , date('m/Y', strtotime($var_dt_vencimento))
              , date('m/Y', strtotime($var_dt_venda))
              , $var_contato
              , $var_referencia];
              
    $var_novo_texto = str_replace($antes, $depois, $var_texto);

    return $var_novo_texto;

}

function fc_adiciona_hashtag_assunto($arr_valor_hashtag, $id_empresa) {
    echo 'fc_adiciona_hashtag_assunto ==========> <hr>';
    $antes = array();
    $depois = array();
    $var_logo = '';

    $var_nm_fantasia_cliente = $arr_valor_hashtag['nm_fantasia_cliente'];
    $var_cnpj_empresa = $arr_valor_hashtag['cnpj_empresa'];
    $var_razao_social_empresa = $arr_valor_hashtag['razao_social_empresa'];
    $var_nm_fantasia_empresa = $arr_valor_hashtag['nm_fantasia_empresa'];
    $var_email_empresa = $arr_valor_hashtag['email_empresa'];
    $var_telefone_empresa = $arr_valor_hashtag['telefone_empresa'];
    $var_logo_empresa = $arr_valor_hashtag['logo_empresa'];
    $var_id_venda = $arr_valor_hashtag['id_venda'];
    $var_assunto = $arr_valor_hashtag['assunto'];
    $var_valor_fianceiro = number_format($arr_valor_hashtag['valor_fianceiro'],2,',','.');
    $var_razao_social = $arr_valor_hashtag['razao_social'];
    $var_dt_vencimento = $arr_valor_hashtag['data_vencimento'];
    $var_dt_venda = $arr_valor_hashtag['data_venda'];
    $var_dt_vencimento_venda = $arr_valor_hashtag['data_vencimento_venda'];
    $var_contato = $arr_valor_hashtag['contato'];
    $var_referencia = $arr_valor_hashtag['referencia'];

    $var_id_empresa_aux = str_pad($id_empresa, 6, '0', STR_PAD_LEFT);

    $var_caminho_logo = CAMINHO_INSTALACAO."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if( isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] == 'on' ) {
        $var_endereco = "https://";
    }else{
        $var_endereco = "http://";
    }

    $var_endereco_logo = $var_endereco.ENDERECO_SISTEMA."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if ((TRIM($var_logo_empresa) != '') && (is_file($var_caminho_logo))){
        $var_logo = "<img src=$var_endereco_logo width=300px height=120px/>";
    }

    $antes =  [ "#CNPJ_EMPRESA"
              , "#RAZAO_SOCIAL_EMPRESA"
              , "#NOME_FANTASIA_EMPRESA"
              , "#EMAIL_EMPRESA"
              , "#TELEFONE_EMPRESA"
              , "#LOGO_EMPRESA"
              , "#NOME_FANTASIA_CLIENTE"
              , "#NUMERO_VENDA"
              , "#FINANCEIRO_VALOR"
              , "#RAZAO_SOCIAL_CLIENTE"
              , "#DATA_VENCIMENTO_COMPLETA"
              , "#DATA_VENCIMENTO"
              , "#DATA_VENDA"
              , "#DATA_VENCIMENTO_VENDA"
              , "#CONTATO_CLIENTE"
              , "#REFERENCIA"];

    $depois = [$var_cnpj_empresa
              , $var_razao_social_empresa
              , $var_nm_fantasia_empresa
              , $var_email_empresa
              , fc_formatar_telefone($var_telefone_empresa)
              , $var_logo 
              , $var_nm_fantasia_cliente
              , $var_id_venda
              , $var_valor_fianceiro
              , $var_razao_social
              , date('d/m/Y', strtotime($var_dt_vencimento))
              , date('m/Y', strtotime($var_dt_vencimento))
              , date('m/Y', strtotime($var_dt_venda))
              , date('m/Y', strtotime($var_dt_vencimento_venda))
              , $var_contato
              , $var_referencia];
              
              

    $var_novo_assunto = str_replace($antes, $depois, $var_assunto);

    return $var_novo_assunto;


}

function fc_data_interval_antes_vencimento($nm_not, $var_id_empresa, $nu_dias_venc, $nu_notificacao, $email_host, $email_user, $email_pwd, $email_porta, $email_from, $conn){
    echo 'fc_data_interval_antes_vencimento ==========> <hr>';
    $falha                = 0;
    $result_insert      = 0;
    $redir              = false;
    $msg                = "";
    $var_dt_enviado     = "";   
    $arrMsgReturn       = array();
    $arrMsgInsert       = array();
    $arrMsgUpdate       = array();
    $tituloNotificacao = '';

    switch ($nm_not) {
        case 'primeira':
            $msg_header = "1ª NOTIFICAÇÃO";
            $tituloNotificacao = "1ª Notificação";      
        break;      
        case 'segunda':
            $msg_header = "2ª NOTIFICAÇÃO";
            $tituloNotificacao = "2ª Notificação";
        break;
        case 'terceira':
            $msg_header = "3ª NOTIFICAÇÃO";
            $tituloNotificacao = "3ª Notificação";
        break;
        case 'quarta':
            $msg_header = "4ª NOTIFICAÇÃO";
            $tituloNotificacao = "4ª Notificação";
        break;
        case 'quinta':
            $msg_header = "5ª NOTIFICAÇÃO";
            $tituloNotificacao = "5ª Notificação";
        break;
        case 'sexta':
            $msg_header = "6ª NOTIFICAÇÃO";
            $tituloNotificacao = "6ª Notificação";
        break;              
    }

    $sql_search = "
        SELECT  
        /*0*/   a.id_empresa AS id_empresa
                , a.id_mov AS id_mov
                , a.id_desd AS id_desd
                , a.tipo AS tipo
                , a.dt_vencimento AS dt_vencimento
        /*5*/   , TRIM(b.email) AS email_cliente
                , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                , c.cnpj AS cnpj_empresa
                , TRIM(c.razao_social) AS razao_social_empresa
                , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
        /*10*/  , c.email AS email_empresa
                , c.telefone AS telefone_empresa
                , c.tx_logo_nfe AS logo_empresa
                , a.id_venda AS id_venda
                , SUM(a.vr_rec_original) AS valor_fianceiro
        /*15*/  , b.razao_social
                , d.telefone1
                , e.dt_venda
                , e.dt_vencimento
                , b.contato
        /*20*/  , e.nr_pedido_talao AS referencia
        FROM db_gol.tb_rec_pag a
        INNER JOIN db_gol.tb_pessoa b
        ON a.id_empresa = b.id_empresa
        AND a.id_pessoa = b.id_pessoa
        INNER JOIN db_gol.tb_empresa c
        ON a.id_empresa = c.id_empresa
        LEFT JOIN db_gol.tb_pessoa_endereco d
        ON d.tipo_endereco = 1 
        AND d.id_empresa = a.id_empresa
        AND d.id_pessoa_endereco = a.id_pessoa
        INNER JOIN db_gol.tb_venda e
        ON a.id_empresa = e.id_empresa
        AND a.id_venda = e.id_venda
        WHERE a.id_empresa = $var_id_empresa
        AND a.id_condominio = 0
        AND a.situacao = 'A'
        AND a.tipo = 'RC'
        AND b.lo_cobranca_automatica = 'S'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20, e.nr_pedido_talao
        ORDER BY a.dt_vencimento ASC
    ";

    $ret = pg_query($conn, $sql_search);
    $dt_interval_venc_antes = pg_fetch_all($ret);

    if(empty($dt_interval_venc_antes)){
        $arrMsgReturn = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Não há recebimentos em aberto para gerar notificações de cobrança.'
        );
        return $arrMsgReturn;
    }

    #INFORMAÇÕES DOS DADOS DA NOTIFICAÇÃO
    $sql_search = "
        SELECT 
            tx_assunto_".$nm_not.",
            tx_texto_".$nm_not." ,
            tx_texto_sms_".$nm_not." ,
            lo_sms_ativo_".$nm_not.",
            tx_texto_whatsapp_".$nm_not." ,
            lo_whatsapp_ativo_".$nm_not.",
            tx_tipo_anexo_email
        FROM db_gol.tb_regua_cobranca 
        WHERE id_empresa = $var_id_empresa
        AND id_condominio = 0 
    ";

    $ret = pg_query($conn, $sql_search);
    $dados_notificacao_antes = pg_fetch_all($ret);

    $var_assunto          = $dados_notificacao_antes[0]["tx_assunto_".$nm_not];
    $var_texto            = $dados_notificacao_antes[0]["tx_texto_".$nm_not];
    $var_texto_sms        = $dados_notificacao_antes[0]["tx_texto_sms_".$nm_not];
    $var_envia_sms        = $dados_notificacao_antes[0]["lo_sms_ativo_".$nm_not];
    $var_texto_whatsapp   = $dados_notificacao_antes[0]["tx_texto_whatsapp_".$nm_not];
    $var_envia_whatsapp   = $dados_notificacao_antes[0]["lo_whatsapp_ativo_".$nm_not];
    $var_tipo_anexo_email = $dados_notificacao_antes[0]["tx_tipo_anexo_email"];

    //PARAMETRO PARA OCULTAR OS CAMPOS Enviar SMS E Enviar WHATSAPP
    $param_permissao_sms = buscaParam($var_id_empresa, 'utiliza_notificacao_sms');
    $param_permissao_whatsapp = buscaParam($var_id_empresa, 'utiliza_notificacao_whatsapp');

    $var_permissao_sms         = $param_permissao_sms ? $param_permissao_sms : 'N';
    $var_permissao_whatsapp    = $param_permissao_whatsapp ? $param_permissao_whatsapp : 'N';

    if($var_permissao_sms == 'N'){
        $var_envia_sms = 'N'; 
    }

    if($var_permissao_whatsapp == 'N'){
        $var_envia_whatsapp = 'N' ;
    }

    $var_valida_qtd_bilhetagem_sms = true;

    if($var_valida_qtd_bilhetagem_sms == false){
        $var_envia_sms = 'N';
    }

    $var_valida_qtd_bilhetagem_whatsapp = true;

    if($var_valida_qtd_bilhetagem_whatsapp == false){
        $var_envia_whatsapp = 'N';
    }

    $var_assunto     = pg_escape_string($var_assunto);
    $var_texto       = pg_escape_string($var_texto);                            
    $var_texto_sms   = pg_escape_string($var_texto_sms);

    if(empty($var_assunto)){
        $arrMsgReturn = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo assunto do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }
    if(empty($var_texto)){
        $arrMsgReturn = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo mensagem do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }

    #PERCORRE OS DADOS E INSERE NO BANCO
    $size = count($dt_interval_venc_antes);

    for($i = 0; $i < $size ; $i++){
        $var_id_empresa           = $dt_interval_venc_antes[$i]['id_empresa'];
        $var_id_mov               = $dt_interval_venc_antes[$i]['id_mov'];
        $var_id_desd              = $dt_interval_venc_antes[$i]['id_desd'];
        $var_tipo                 = $dt_interval_venc_antes[$i]['tipo'];
        $var_dt_vencimento        = $dt_interval_venc_antes[$i]['dt_vencimento'];
        $var_email_address        = $dt_interval_venc_antes[$i]['email_cliente'];                                                
        $var_nm_fantasia_cliente  = $dt_interval_venc_antes[$i]['nm_fantasia_cliente'];
        $var_cnpj_empresa         = $dt_interval_venc_antes[$i]['cnpj_empresa'];
        $var_razao_social_empresa = $dt_interval_venc_antes[$i]['razao_social_empresa'];
        $var_nm_fantasia_empresa  = $dt_interval_venc_antes[$i]['nm_fantasia_empresa'];
        $var_email_empresa        = $dt_interval_venc_antes[$i]['email_empresa'];
        $var_telefone_empresa     = $dt_interval_venc_antes[$i]['telefone_empresa'];
        $var_logo_empresa         = $dt_interval_venc_antes[$i]['logo_empresa'];
        $var_id_venda             = $dt_interval_venc_antes[$i]['id_venda'];
        $var_valor_fianceiro      = $dt_interval_venc_antes[$i]['valor_fianceiro'];
        $var_razao_social         = $dt_interval_venc_antes[$i]['razao_social'];
        $var_numero_telefone      = $dt_interval_venc_antes[$i]['telefone1'];
        $var_dt_venda             = $dt_interval_venc_antes[$i]['dt_venda'];
        $var_dt_vencimento_venda  = $dt_interval_venc_antes[$i]['dt_vencimento'];
        $var_contato              = $dt_interval_venc_antes[$i]['contato'];
        $var_referencia           = $dt_interval_venc_antes[$i]['referencia'];

        $arr_valor_hashtag = array();
        $arr_valor_hashtag['nm_fantasia_cliente'] = $var_nm_fantasia_cliente;
        $arr_valor_hashtag['cnpj_empresa'] = $var_cnpj_empresa;
        $arr_valor_hashtag['razao_social_empresa'] = $var_razao_social_empresa;
        $arr_valor_hashtag['nm_fantasia_empresa'] = $var_nm_fantasia_empresa;
        $arr_valor_hashtag['email_empresa'] = $var_email_empresa;
        $arr_valor_hashtag['telefone_empresa'] = $var_telefone_empresa;
        $arr_valor_hashtag['logo_empresa'] = $var_logo_empresa;
        $arr_valor_hashtag['id_venda'] = $var_id_venda;
        $arr_valor_hashtag['texto'] = $var_texto;
        $arr_valor_hashtag['assunto'] = $var_assunto;
        $arr_valor_hashtag['valor_fianceiro'] = $var_valor_fianceiro;
        $arr_valor_hashtag['razao_social'] = $var_razao_social; 
        $arr_valor_hashtag['data_vencimento'] = $var_dt_vencimento; 
        $arr_valor_hashtag['data_venda'] = $var_dt_venda; 
        $arr_valor_hashtag['data_vencimento_venda'] = $var_dt_vencimento_venda;
        $arr_valor_hashtag['contato'] = $var_contato;
        $arr_valor_hashtag['referencia'] = $var_referencia;

        //metodo criado para trocar os valores das hashtags
        $var_novo_texto = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        //metodo criado para trocar os valores das hashtags
        $var_novo_assunto = fc_adiciona_hashtag_assunto($arr_valor_hashtag, $var_id_empresa);

        // TROCO APENAS A VARIAL DO TEXT SMS
        $arr_valor_hashtag['texto'] = $var_texto_sms;
        $var_novo_texto_sms = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

       // TROCO APENAS A VARIAL DO TEXT WHATSAPP
        $arr_valor_hashtag['texto'] = $var_texto_whatsapp;
        $var_novo_texto_whatsapp = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        #verifica o intervalo entre o vencimento e o dia estipulado     
        $sql_search = "SELECT CAST('$var_dt_vencimento'::DATE - interval '$nu_dias_venc day' AS DATE) AS cast";
        $ret = pg_query($conn, $sql_search);
        $dt_enviar = pg_fetch_all($ret);
        $var_dt_envio        = $dt_enviar[0]['cast']; 

        #COMPARA AS DATAS SE FOR ULTRAPASSADO A DATA ESPULADA A DATA DE VENCIMENTO, NÃO IRÁ INSERIR
        $date_venc           = date_create($var_dt_vencimento);
        $date_now            = date_create(date('Y-m-d'));
        $date_env            = date_create($var_dt_envio);

        if($date_env >= $date_now){
            $conexao = new Conexao();
            $conn = null;

            if($var_id_empresa == 1){
                echo 'Contrato do dsv<hr>';
                $conn = $conexao->open("conn_contratos_dsv");
            }else{
                echo 'Contrato do prod<hr>';
                $conn = $conexao->open("conn_contratos");
            }
            #verifica se existe dados
            $sql_search = "       
                SELECT  
                    * 
                FROM db_contratos.tb_notificacao 
                WHERE id_empresa    = $var_id_empresa 
                AND id_mov          = $var_id_mov
                AND id_desd         = $var_id_desd
                AND nu_notificacao  = $nu_notificacao";

            $ret = pg_query($conn, $sql_search);
            $check_notificacao = pg_fetch_all($ret);

            if(empty($check_notificacao)){
                $sql_insert = "
                    INSERT INTO db_contratos.tb_notificacao (
                        id_empresa,
                        id_mov,
                        id_desd,
                        nu_notificacao,
                        tipo,
                        tx_email_host,
                        tx_email_smtp_secure,
                        tx_email_username,
                        tx_email_password,
                        tx_email_porta,
                        tx_email_from,
                        tx_email_address,
                        tx_email_subject,
                        tx_email_body,
                        tx_login,
                        tx_retorno,
                        lo_enviado,
                        dt_vencimento,
                        dt_inc,
                        dt_envio,
                        tx_texto_sms,
                        lo_envia_sms,
                        nu_numero_telefone,
                        tx_texto_whatsapp,
                        lo_envia_whatsapp,
                        tx_tipo_anexo_email
                    ) VALUES (
                        $var_id_empresa,
                        $var_id_mov,
                        $var_id_desd,
                        $nu_notificacao,
                        '$var_tipo',
                        '$email_host',
                        'tls',
                        '$email_user',
                        '$email_pwd',
                        '$email_porta',
                        '$email_from',
                        '$var_email_address',
                        '$var_novo_assunto',
                        '$var_novo_texto',
                        'ADMIN',
                        '',
                        'N',
                        '$var_dt_vencimento',
                        now(),
                        '$var_dt_envio',
                        '$var_novo_texto_sms',
                        '$var_envia_sms',
                        '$var_numero_telefone',
                        '$var_novo_texto_whatsapp',
                        '$var_envia_whatsapp',
                        '$var_tipo_anexo_email'
                    )";

                    pg_query($conn, $sql_insert);

                $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");
                $result_insert++;
            }else{
                $var_lo_enviado = $check_notificacao[0]['lo_enviado'];
                if($var_lo_enviado == 'N'){
                    $arr = array(
                        'tipo'                      => $var_tipo,
                        'tx_email_host'             => $email_host,
                        'tx_email_smtp_secure'      => 'tls',
                        'tx_email_username'         => $email_user,
                        'tx_email_password'         => $email_pwd,
                        'tx_email_porta'            => $email_porta,
                        'tx_email_from'             => $email_from,
                        'tx_email_address'          => $var_email_address,
                        'tx_email_subject'          => $var_novo_assunto,
                        'tx_email_body'             => $var_novo_texto,
                        'tx_login'                  => 'ADMIN',
                        'tx_retorno'                => '',
                        'lo_enviado'                => 'N',
                        'dt_vencimento'             => $var_dt_vencimento,
                        'dt_inc'                    => 'NOW()',
                        'dt_envio'                  => $var_dt_envio,
                        'tx_tipo_anexo_email'       => $var_tipo_anexo_email
                    );

                    $tb_valor = "";
                    $tb_coluna = array();

                    foreach($arr as $key => $value){
                        $tb_valor       = $value;
                        $tb_coluna[]    = "$key = '$value'";        
                    }

                    $implode_result = implode(', ', $tb_coluna);

                    $sql_update = "
                        UPDATE db_contratos.tb_notificacao 
                        SET $implode_result 
                        WHERE id_empresa = $var_id_empresa
                        AND id_mov = $var_id_mov        
                        AND id_desd = $var_id_desd
                        AND nu_notificacao = $nu_notificacao
                    ";
                    pg_query($conn, $sql_update);
                    $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                    $falha++;
                }
            }
        }
    }
    $conexao = new Conexao();
    echo "Id empresa : ".dbNameCob($var_id_empresa).'<hr>';
    $conn = $conexao->open(dbNameCob($var_id_empresa));

    $arrCountNotificacao = array(
        'tituloNotificacao' => $tituloNotificacao,
        'sucesso' => array(
            'count' => "0"
        ),
        'atencao' => array(
            'count' => "0"
        )
    );

    if($result_insert > 0){
        $countInserted = count($arrMsgInsert);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'sucesso' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'mensagem' => "$countInserted novas notificações de cobrança inseridas."
                ),
                'linhas' => $arrMsgInsert
            )
        );
    }

    if($falha > 0){
        $countUpdated = count($arrMsgUpdate);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'atencao' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'subtitulo' => "$countUpdated notificações de cobrança atualizadas."
                ),
                'linhas' => $arrMsgUpdate
            )
        );
    }

    if(isset($arrMsgReturn['sucesso'], $arrMsgReturn['sucesso']['linhas'])){
        $countSucesso = count($arrMsgReturn['sucesso']['linhas']);
        $arrCountNotificacao['notificacao']['sucesso']['count'] = $countSucesso;
    }

    if(isset($arrMsgReturn['atencao'], $arrMsgReturn['atencao']['linhas'])){
        $countAtencao = count($arrMsgReturn['atencao']['linhas']);
        $arrCountNotificacao['notificacao']['atencao']['count'] = $countAtencao;
    }

    $arrMsgReturn['notificacao'] = $arrCountNotificacao;

    return $arrMsgReturn;  
}

function fc_data_interval_depois_vencimento($nm_not_3, $var_id_empresa_3, $nu_dias_venc_3, $nu_notificacao_3, $email_host_3, $email_usar_3, $email_pwd_3, $email_porta_3, $email_from_3, $conn){
    echo 'fc_data_data_interval_depois_vencimento ==========> <hr>';
    $falha          = 0;
    $result_insert  = 0;
    $redir          = false;
    $msg            = "";
    $arrMsgReturn   = array();
    $arrMsgInsert   = array();
    $arrMsgUpdate   = array();
    $tituloNotificacao = '';

    switch ($nm_not_3) {
        case 'primeira':
            $msg_header = "1ª NOTIFICAÇÃO";
            $tituloNotificacao = "1ª Notificação";      
        break;      
        case 'segunda':
            $msg_header = "2ª NOTIFICAÇÃO";
            $tituloNotificacao = "2ª Notificação";
        break;
        case 'terceira':
            $msg_header = "3ª NOTIFICAÇÃO";
            $tituloNotificacao = "3ª Notificação";
        break;
        case 'quarta':
            $msg_header = "4ª NOTIFICAÇÃO";
            $tituloNotificacao = "4ª Notificação";
        break;
        case 'quinta':
            $msg_header = "5ª NOTIFICAÇÃO";
            $tituloNotificacao = "5ª Notificação";
        break;
        case 'sexta':
            $msg_header = "6ª NOTIFICAÇÃO";
            $tituloNotificacao = "6ª Notificação";
        break;              
    }

    $sql_search = "SELECT  
        /*0*/   a.id_empresa AS id_empresa
                , a.id_mov AS id_mov
                , a.id_desd AS id_desd
                , a.tipo AS tipo
                , a.dt_vencimento AS dt_vencimento
        /*5*/   , TRIM(b.email) AS email_cliente
                , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                , c.cnpj AS cnpj_empresa
                , TRIM(c.razao_social) AS razao_social_empresa
                , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
        /*10*/  , c.email AS email_empresa
                , c.telefone AS telefone_empresa
                , c.tx_logo_nfe AS logo_empresa
                , a.id_venda AS id_venda
                , SUM(a.vr_rec_original) AS valor_fianceiro
        /*15*/  , b.razao_social
                , d.telefone1
                , e.dt_venda
                , e.dt_vencimento
                , b.contato
        /*20*/  , e.nr_pedido_talao AS referencia
        FROM db_gol.tb_rec_pag a
        INNER JOIN db_gol.tb_pessoa b
        ON a.id_empresa = b.id_empresa
        AND a.id_pessoa = b.id_pessoa
        INNER JOIN db_gol.tb_empresa c
        ON a.id_empresa = c.id_empresa
        LEFT JOIN db_gol.tb_pessoa_endereco d
        ON d.tipo_endereco = 1 
        AND d.id_empresa = a.id_empresa
        AND d.id_pessoa_endereco = a.id_pessoa
        INNER JOIN db_gol.tb_venda e
        ON a.id_empresa = e.id_empresa
        AND a.id_venda = e.id_venda
        WHERE a.id_empresa = $var_id_empresa_3
        AND a.id_condominio = 0
        AND a.situacao = 'A'
        AND a.tipo = 'RC'
        AND b.lo_cobranca_automatica = 'S'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20, e.nr_pedido_talao
        ORDER BY a.dt_vencimento ASC
    ";

    $ret = pg_query($conn, $sql_search);
    $dt_interval_venc_d = pg_fetch_all($ret); 

    if(empty($dt_interval_venc_d)){
        $arrMsgReturn[$nm_not_3] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Não há recebimentos em aberto para gerar notificações de cobrança.'
        );
        return $arrMsgReturn;
    }
    #INFORMAÇÕES DOS DADOS DA NOTIFICAÇÃO
    $sql_search = "
        SELECT 
            tx_assunto_".$nm_not_3.",
            tx_texto_".$nm_not_3.",
            tx_texto_sms_".$nm_not_3." ,
            lo_sms_ativo_".$nm_not_3.",
            tx_texto_whatsapp_".$nm_not_3." ,
            lo_whatsapp_ativo_".$nm_not_3.",
            tx_tipo_anexo_email
        FROM db_gol.tb_regua_cobranca 
        WHERE id_empresa = $var_id_empresa_3
        AND id_condominio = 0 
    ";

    $ret = pg_query($conn, $sql_search);
    $dados_notificacao_depois = pg_fetch_all($ret);                 

    $var_assunto     = $dados_notificacao_depois[0]["tx_assunto_".$nm_not_3];
    $var_texto       = $dados_notificacao_depois[0]["tx_texto_".$nm_not_3];
    $var_texto_sms   = $dados_notificacao_depois[0]["tx_texto_sms_".$nm_not_3];
    $var_envia_sms   = $dados_notificacao_depois[0]["lo_sms_ativo_".$nm_not_3];
    $var_texto_whatsapp   = $dados_notificacao_depois[0]["tx_texto_whatsapp_".$nm_not_3];
    $var_envia_whatsapp   = $dados_notificacao_depois[0]["lo_whatsapp_ativo_".$nm_not_3];
    $var_tipo_anexo_email = $dados_notificacao_depois[0]["tx_tipo_anexo_email"];

    $param_permissao_sms = buscaParam($var_id_empresa_3, 'utiliza_notificacao_sms');
    $param_permissao_whatsapp = buscaParam($var_id_empresa_3, 'utiliza_notificacao_whatsapp');

    $var_permissao_sms         = $param_permissao_sms ? $param_permissao_sms : 'N';
    $var_permissao_whatsapp    = $param_permissao_whatsapp ? $param_permissao_whatsapp : 'N';

    if($var_permissao_sms == 'N'){
        $var_envia_sms = 'N'; 
    }

    if($var_permissao_whatsapp == 'N'){
        $var_envia_whatsapp = 'N' ;
    }

    $var_valida_qtd_bilhetagem_sms = fc_valida_bilhetagem('SMS', $var_id_empresa_3, $conn);

    if($var_valida_qtd_bilhetagem_sms == false){
        $var_envia_sms = 'N';
    }

    $var_valida_qtd_bilhetagem_whatsapp = fc_valida_bilhetagem('WHATSAPP', $var_id_empresa_3, $conn);

    if($var_valida_qtd_bilhetagem_whatsapp == false){
        $var_envia_whatsapp = 'N';
    }

    $var_assunto = pg_escape_string($var_assunto);
    $var_texto   = pg_escape_string($var_texto);
    $var_texto_sms   = pg_escape_string($var_texto_sms);

    if(empty($var_assunto)){
        $arrMsgReturn[$nm_not_3] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo assunto do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }
    if(empty($var_texto)){
        $arrMsgReturn[$nm_not_3] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo mensagem do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }

    #PERCORRE OS DADOS E INSERE NO BANCO
    $size = count($dt_interval_venc_d);

    for($i = 0; $i < $size ; $i++){
        $var_id_empresa           = $dt_interval_venc_d[$i]['id_empresa'];
        $var_id_mov               = $dt_interval_venc_d[$i]['id_mov'];
        $var_id_desd              = $dt_interval_venc_d[$i]['id_desd'];
        $var_tipo                 = $dt_interval_venc_d[$i]['tipo'];
        $var_dt_vencimento        = $dt_interval_venc_d[$i]['dt_vencimento'];
        $var_email_address        = $dt_interval_venc_d[$i]['email_cliente'];
        $var_nm_fantasia_cliente  = $dt_interval_venc_d[$i]['nm_fantasia_cliente'];
        $var_cnpj_empresa         = $dt_interval_venc_d[$i]['cnpj_empresa'];
        $var_razao_social_empresa = $dt_interval_venc_d[$i]['razao_social_empresa'];
        $var_nm_fantasia_empresa  = $dt_interval_venc_d[$i]['nm_fantasia_empresa'];
        $var_email_empresa        = $dt_interval_venc_d[$i]['email_empresa'];
        $var_telefone_empresa     = $dt_interval_venc_d[$i]['telefone_empresa'];
        $var_logo_empresa         = $dt_interval_venc_d[$i]['logo_empresa'];
        $var_id_venda             = $dt_interval_venc_d[$i]['id_venda'];
        $var_valor_fianceiro      = $dt_interval_venc_d[$i]['valor_fianceiro'];
        $var_razao_social         = $dt_interval_venc_d[$i]['razao_social'];
        $var_numero_telefone      = $dt_interval_venc_d[$i]['telefone1'];
        $var_dt_venda             = $dt_interval_venc_d[$i]['dt_venda'];
        $var_dt_vencimento_venda  = $dt_interval_venc_d[$i]['dt_vencimento'];
        $var_contato              = $dt_interval_venc_d[$i]['contato'];
        $var_referencia           = $dt_interval_venc_d[$i]['referencia'];

        $arr_valor_hashtag = array();
        $arr_valor_hashtag['nm_fantasia_cliente'] = $var_nm_fantasia_cliente;                                       
        $arr_valor_hashtag['cnpj_empresa'] = $var_cnpj_empresa;
        $arr_valor_hashtag['razao_social_empresa'] = $var_razao_social_empresa;
        $arr_valor_hashtag['nm_fantasia_empresa'] = $var_nm_fantasia_empresa;
        $arr_valor_hashtag['email_empresa'] = $var_email_empresa;
        $arr_valor_hashtag['telefone_empresa'] = $var_telefone_empresa;
        $arr_valor_hashtag['logo_empresa'] = $var_logo_empresa;
        $arr_valor_hashtag['id_venda'] = $var_id_venda;
        $arr_valor_hashtag['texto'] = $var_texto;
        $arr_valor_hashtag['assunto'] = $var_assunto;
        $arr_valor_hashtag['valor_fianceiro'] = $var_valor_fianceiro;
        $arr_valor_hashtag['razao_social'] = $var_razao_social;
        $arr_valor_hashtag['data_vencimento'] = $var_dt_vencimento;
        $arr_valor_hashtag['data_venda'] = $var_dt_venda; 
        $arr_valor_hashtag['data_vencimento_venda'] = $var_dt_vencimento_venda;
        $arr_valor_hashtag['contato'] = $var_contato;   
        $arr_valor_hashtag['referencia'] = $var_referencia;

        //metodo criado para trocar os valores das hashtags
        $var_novo_texto = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);  

        //metodo criado para trocar os valores das hashtags
        $var_novo_assunto = fc_adiciona_hashtag_assunto($arr_valor_hashtag, $var_id_empresa);  

        // TROCO APENAS A VARIAL DO TEXT SMS
        $arr_valor_hashtag['texto'] = $var_texto_sms;
        $var_novo_texto_sms = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        // TROCO APENAS A VARIAL DO TEXT WHATSAPP
        $arr_valor_hashtag['texto'] = $var_texto_whatsapp;
        $var_novo_texto_whatsapp = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        #verifica o intervalo entre o vencimento e o dia estipulado     
        $sql_search = "SELECT CAST('$var_dt_vencimento'::DATE + interval '$nu_dias_venc_3 day' AS DATE) AS cast";
        $ret = pg_query($conn, $sql_search);
        $dt_enviar = pg_fetch_all($ret);
        $var_dt_envio       = $dt_enviar[0]['cast']; 

        #COMPARA AS DATAS SE FOR ULTRAPASSADO A DATA ESPULADA A DATA DE VENCIMENTO, NÃO IRÁ INSERIR
        $date_venc           = date_create($var_dt_vencimento);
        $date_now            = date_create(date('Y-m-d'));
        $date_env            = date_create($var_dt_envio);

        if($date_env >= $date_now){
            $conexao = new Conexao();
            $conn = null;

            if($var_id_empresa == 1){
                echo 'Contrato do dsv<hr>';
                $conn = $conexao->open("conn_contratos_dsv");
            }else{
                echo 'Contrato do prod<hr>';
                $conn = $conexao->open("conn_contratos");
            }

            $sql_search = "   
                SELECT
                    * 
                FROM db_contratos.tb_notificacao 
                WHERE id_empresa    = $var_id_empresa 
                AND id_mov          = $var_id_mov
                AND id_desd         = $var_id_desd
                AND nu_notificacao  = $nu_notificacao_3";

            $ret = pg_query($conn, $sql_search);
            $check_notificacao = pg_fetch_all($ret);

            if(empty($check_notificacao)){
                $sql_insert = "
                    INSERT INTO db_contratos.tb_notificacao(
                        id_empresa,
                        id_mov,
                        id_desd,
                        nu_notificacao,
                        tipo,
                        tx_email_host,
                        tx_email_smtp_secure,
                        tx_email_username,
                        tx_email_password,
                        tx_email_porta,
                        tx_email_from,
                        tx_email_address,
                        tx_email_subject,
                        tx_email_body,
                        tx_login,
                        tx_retorno,
                        lo_enviado,
                        dt_vencimento,
                        dt_inc,
                        dt_envio,
                        tx_texto_sms,
                        lo_envia_sms,
                        nu_numero_telefone,
                        tx_texto_whatsapp,
                        lo_envia_whatsapp,
                        tx_tipo_anexo_email
                    ) VALUES (
                        $var_id_empresa,
                        $var_id_mov,
                        $var_id_desd,
                        $nu_notificacao_3,
                        '$var_tipo',
                        '$email_host_3',
                        'tls',
                        '$email_user_3',
                        '$email_pwd_3',
                        '$email_porta_3',
                        '$email_from_3',
                        '$var_email_address',
                        '$var_novo_assunto',
                        '$var_novo_texto',
                        'ADMIN',
                        '',
                        'N',
                        '$var_dt_vencimento',
                        now(),
                        '$var_dt_envio',
                        '$var_novo_texto_sms',
                        '$var_envia_sms',
                        '$var_numero_telefone',
                        '$var_novo_texto_whatsapp',
                        '$var_envia_whatsapp',
                        '$var_tipo_anexo_email'
                    )";

                pg_query($conn, $sql_insert);
            
                $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");                               
                $result_insert++;
            }else{
                $var_lo_enviado = $check_notificacao[0]['lo_enviado'];
                if($var_lo_enviado == 'N'){
                    $arr = array(
                        'tipo'                      => $var_tipo,
                        'tx_email_host'             => $email_host_3,
                        'tx_email_smtp_secure'      => 'tls',
                        'tx_email_username'         => $email_user_3,
                        'tx_email_password'         => $email_pwd_3,
                        'tx_email_porta'            => $email_porta_3,
                        'tx_email_from'             => $email_from_3,
                        'tx_email_address'          => $var_email_address,
                        'tx_email_subject'          => $var_novo_assunto,
                        'tx_email_body'             => $var_novo_texto,
                        'tx_login'                  => 'ADMIN',
                        'tx_retorno'                => '',
                        'lo_enviado'                => 'N',
                        'dt_vencimento'             => $var_dt_vencimento,
                        'dt_inc'                    => 'NOW()',
                        'dt_envio'                  => $var_dt_envio,
                        'tx_tipo_anexo_email'       => $var_tipo_anexo_email
                    );

                    $tb_valor = "";
                    $tb_coluna = array();

                    foreach ($arr as $key => $value) {
                        $tb_valor       = $value;
                        $tb_coluna[]    = "$key = '$value'";        
                    }

                    $implode_result = implode(', ', $tb_coluna);

                    $sql_update = "
                        UPDATE db_contratos.tb_notificacao 
                        SET $implode_result
                        WHERE id_empresa = $var_id_empresa
                        AND id_mov = $var_id_mov        
                        AND id_desd = $var_id_desd
                        AND nu_notificacao = $nu_notificacao_3
                     ";
                    pg_query($conn, $sql_update);
                    $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                    $falha++;
                }
            }
        }else{
            $msg .= '</br> Não há notificação a ser enviada para este título: '.$var_id_mov.'/'.$var_id_desd;
            $falha++;
        }
    }
    $conexao = new Conexao();
    echo "Id empresa 3: ".dbNameCob($var_id_empresa_3).'<hr>';
    $conn = $conexao->open(dbNameCob($var_id_empresa_3));

    $arrCountNotificacao = array(
        'tituloNotificacao' => $tituloNotificacao,
        'sucesso' => array(
            'count' => "0"
        ),
        'atencao' => array(
            'count' => "0"
        )
    );

    if($result_insert > 0){
        $countInserted = count($arrMsgInsert);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'sucesso' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'mensagem' => "$countInserted novas notificações de cobrança inseridas."
                ),
                'linhas' => $arrMsgInsert
            )
        );
    }

    if($falha > 0){
        $countUpdated = count($arrMsgUpdate);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'atencao' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'subtitulo' => "$countUpdated notificações de cobrança atualizadas."
                ),
                'linhas' => $arrMsgUpdate
            )
        );
    }

    if(isset($arrMsgReturn['sucesso'], $arrMsgReturn['sucesso']['linhas'])){
        $countSucesso = count($arrMsgReturn['sucesso']['linhas']);
        $arrCountNotificacao['notificacao']['sucesso']['count'] = $countSucesso;
    }

    if(isset($arrMsgReturn['atencao'], $arrMsgReturn['atencao']['linhas'])){
        $countAtencao = count($arrMsgReturn['atencao']['linhas']);
        $arrCountNotificacao['notificacao']['atencao']['count'] = $countAtencao;
    }

    $arrMsgReturn['notificacao'] = $arrCountNotificacao;

    return $arrMsgReturn;
}

function fc_data_interval_no_vencimento($nm_not_2, $var_id_empresa_2, $nu_dias_venc_2, $nu_notificacao_2, $email_host_2, $email_user_2, $email_pwd_2, $email_porta_2, $email_from_2, $conn){
    echo 'fc_data_interval_no_vencimento ==========> <hr>';
    print_r([
        'nm_not_2' => $nm_not_2, 
        'var_id_empresa_2' => $var_id_empresa_2, 
        'nu_dias_venc_2' => $nu_dias_venc_2, 
        'nu_notificacao_2' => $nu_notificacao_2, 
        'email_host_2' => $email_host_2, 
        'email_user_2' => $email_user_2, 
        'email_pwd_2' => $email_pwd_2, 
        'email_porta_2' => $email_porta_2, 
        'email_from_2' => $email_from_2
    ]);
    echo '</pre>';

    $falha          = 0;
    $result_insert  = 0;
    $redir          = false;
    $msg            = "";
    $arrMsgReturn   = array();
    $arrMsgInsert   = array();
    $arrMsgUpdate   = array();
    $tituloNotificacao = '';

    switch ($nm_not_2) {
        case 'primeira':
            $msg_header = "1ª NOTIFICAÇÃO";
            $tituloNotificacao = "1ª Notificação";      
        break;      
        case 'segunda':
            $msg_header = "2ª NOTIFICAÇÃO";
            $tituloNotificacao = "2ª Notificação";
        break;
        case 'terceira':
            $msg_header = "3ª NOTIFICAÇÃO";
            $tituloNotificacao = "3ª Notificação";
        break;
        case 'quarta':
            $msg_header = "4ª NOTIFICAÇÃO";
            $tituloNotificacao = "4ª Notificação";
        break;
        case 'quinta':
            $msg_header = "5ª NOTIFICAÇÃO";
            $tituloNotificacao = "5ª Notificação";
        break;
        case 'sexta':
            $msg_header = "6ª NOTIFICAÇÃO";
            $tituloNotificacao = "6ª Notificação";
        break;              
    }

    $sql_search = "
        SELECT  
    /*0*/   a.id_empresa AS id_empresa
            , a.id_mov AS id_mov
            , a.id_desd AS id_desd
            , a.tipo AS tipo
            , a.dt_vencimento AS dt_vencimento
    /*5*/   , TRIM(b.email) AS email_cliente
            , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
            , c.cnpj AS cnpj_empresa
            , TRIM(c.razao_social) AS razao_social_empresa
            , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
    /*10*/  , c.email AS email_empresa
            , c.telefone AS telefone_empresa
            , c.tx_logo_nfe AS logo_empresa
            , a.id_venda AS id_venda
            , SUM(a.vr_rec_original) AS valor_fianceiro
    /*15*/  , b.razao_social
            , d.telefone1
            , e.dt_venda
            , e.dt_vencimento
            , b.contato
    /*20*/  , e.nr_pedido_talao AS referencia
        FROM db_gol.tb_rec_pag a
        INNER JOIN db_gol.tb_pessoa b
        ON a.id_empresa = b.id_empresa
        AND a.id_pessoa = b.id_pessoa
        INNER JOIN db_gol.tb_empresa c
        ON a.id_empresa = c.id_empresa
        LEFT JOIN db_gol.tb_pessoa_endereco d
        ON d.tipo_endereco = 1 
        AND d.id_empresa = a.id_empresa
        AND d.id_pessoa_endereco = a.id_pessoa
        INNER JOIN db_gol.tb_venda e
        ON a.id_empresa = e.id_empresa
        AND a.id_venda = e.id_venda
        WHERE a.id_empresa = $var_id_empresa_2
        AND a.id_condominio = 0
        AND a.situacao = 'A'
        AND a.tipo = 'RC'
        AND b.lo_cobranca_automatica = 'S'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20, e.nr_pedido_talao
        ORDER BY a.dt_vencimento ASC
    ";

    $ret = pg_query($conn, $sql_search);
    $dt_interval_venc_v  = pg_fetch_all($ret);

    echo '$dt_interval_venc_v => <pre>';
    print_r($dt_interval_venc_v);
    echo '</pre>';


    if(empty($dt_interval_venc_v)){
        $arrMsgReturn[$nm_not_2] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Não há recebimentos em aberto para gerar notificações de cobrança.'
        );
        return $arrMsgReturn;
    }

    #INFORMAÇÕES DOS DADOS DA NOTIFICAÇÃO
    $sql_search = "
        SELECT tx_assunto_".$nm_not_2.",
               tx_texto_".$nm_not_2.",
                tx_texto_sms_".$nm_not_2." ,
                lo_sms_ativo_".$nm_not_2." ,
                tx_texto_whatsapp_".$nm_not_2." ,
                lo_whatsapp_ativo_".$nm_not_2.",
                tx_tipo_anexo_email
        FROM db_gol.tb_regua_cobranca 
        WHERE id_empresa = $var_id_empresa_2
        AND id_condominio = 0 
    ";

    $ret = pg_query($conn, $sql_search);
    $dados_notificacao_venc = pg_fetch_all($ret);

    echo '$dados_notificacao_venc => <pre>';
    print_r($dados_notificacao_venc);
    echo '</pre>';

    $var_assunto     = $dados_notificacao_venc[0]["tx_assunto_".$nm_not_2];
    $var_texto       = $dados_notificacao_venc[0]["tx_texto_".$nm_not_2];
    $var_texto_sms   = $dados_notificacao_venc[0]["tx_texto_sms_".$nm_not_2];
    $var_envia_sms        = $dados_notificacao_venc[0]["tx_texto_sms_".$nm_not_2];
    $var_texto_whatsapp   = $dados_notificacao_venc[0]["lo_sms_ativo_".$nm_not_2];
    $var_envia_whatsapp   = $dados_notificacao_venc[0]["tx_texto_whatsapp_".$nm_not_2];
    $var_tipo_anexo_email = $dados_notificacao_venc[0]["tx_tipo_anexo_email"];

    $param_permissao_sms = buscaParam($var_id_empresa_2, 'utiliza_notificacao_sms');
    $param_permissao_whatsapp = buscaParam($var_id_empresa_2, 'utiliza_notificacao_whatsapp');

    echo '$param_permissao_sms => '.$param_permissao_sms.'<hr>';
    echo '$param_permissao_whatsapp => '.$param_permissao_whatsapp.'<hr>';

    $var_permissao_sms         = $param_permissao_sms ? $param_permissao_sms : 'N';
    $var_permissao_whatsapp    = $param_permissao_whatsapp ? $param_permissao_whatsapp : 'N';

    if($var_permissao_sms == 'N'){
        $var_envia_sms = 'N'; 
    }

    if($var_permissao_whatsapp == 'N'){
        $var_envia_whatsapp = 'N' ;
    }

    $var_valida_qtd_bilhetagem_sms = fc_valida_bilhetagem('SMS', $var_id_empresa_2, $conn);
    echo '$var_valida_qtd_bilhetagem_sms => '.$var_valida_qtd_bilhetagem_sms.'<hr>';

    if($var_valida_qtd_bilhetagem_sms == false){
        $var_envia_sms = 'N';
    }

    $var_valida_qtd_bilhetagem_whatsapp = fc_valida_bilhetagem('WHATSAPP', $var_id_empresa_2, $conn);
    echo '$var_valida_qtd_bilhetagem_whatsapp  => '.$var_valida_qtd_bilhetagem_whatsapp .'<hr>';


    if($var_valida_qtd_bilhetagem_whatsapp == false){
        $var_envia_whatsapp = 'N';
    }

    $var_assunto     = pg_escape_string($var_assunto);
    $var_texto       = pg_escape_string($var_texto);
    $var_texto_sms   = pg_escape_string($var_texto_sms);

    if(empty($var_assunto)){
        $arrMsgReturn[$nm_not_2] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo assunto do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }
    if(empty($var_texto)){
        $arrMsgReturn[$nm_not_2] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo mensagem do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }

    #PERCORRE OS DADOS E INSERE NO BANCO
    $size = count($dt_interval_venc_v);

    for($i = 0; $i < $size ; $i++){
        $var_id_empresa           = $dt_interval_venc_v[$i]['id_empresa'];
        $var_id_mov               = $dt_interval_venc_v[$i]['id_mov'];
        $var_id_desd              = $dt_interval_venc_v[$i]['id_desd'];
        $var_tipo                 = $dt_interval_venc_v[$i]['tipo'];
        $var_dt_vencimento        = $dt_interval_venc_v[$i]['dt_vencimento'];
        $var_email_address        = $dt_interval_venc_v[$i]['email_cliente'];
        $var_nm_fantasia_cliente  = $dt_interval_venc_v[$i]['nm_fantasia_cliente'];
        $var_cnpj_empresa         = $dt_interval_venc_v[$i]['cnpj_empresa'];
        $var_razao_social_empresa = $dt_interval_venc_v[$i]['razao_social_empresa'];
        $var_nm_fantasia_empresa  = $dt_interval_venc_v[$i]['nm_fantasia_empresa'];
        $var_email_empresa        = $dt_interval_venc_v[$i]['email_empresa'];
        $var_telefone_empresa     = $dt_interval_venc_v[$i]['telefone_empresa'];
        $var_logo_empresa         = $dt_interval_venc_v[$i]['logo_empresa'];
        $var_id_venda             = $dt_interval_venc_v[$i]['id_venda'];
        $var_valor_fianceiro      = $dt_interval_venc_v[$i]['valor_fianceiro'];
        $var_razao_social         = $dt_interval_venc_v[$i]['razao_social'];
        $var_numero_telefone      = $dt_interval_venc_v[$i]['telefone1'];
        $var_dt_venda             = $dt_interval_venc_v[$i]['dt_venda'];
        $var_dt_vencimento_venda  = $dt_interval_venc_v[$i]['dt_vencimento'];
        $var_contato              = $dt_interval_venc_v[$i]['contato'];
        $var_referencia           = $dt_interval_venc_v[$i]['referencia'];

        $arr_valor_hashtag = array();
        $arr_valor_hashtag['nm_fantasia_cliente'] = $var_nm_fantasia_cliente;
        $arr_valor_hashtag['cnpj_empresa'] = $var_cnpj_empresa;
        $arr_valor_hashtag['razao_social_empresa'] = $var_razao_social_empresa;
        $arr_valor_hashtag['nm_fantasia_empresa'] = $var_nm_fantasia_empresa;
        $arr_valor_hashtag['email_empresa'] = $var_email_empresa;
        $arr_valor_hashtag['telefone_empresa'] = $var_telefone_empresa;
        $arr_valor_hashtag['logo_empresa'] = $var_logo_empresa;
        $arr_valor_hashtag['id_venda'] = $var_id_venda;
        $arr_valor_hashtag['texto'] = $var_texto;
        $arr_valor_hashtag['assunto'] = $var_assunto;
        $arr_valor_hashtag['valor_fianceiro'] = $var_valor_fianceiro;
        $arr_valor_hashtag['razao_social'] = $var_razao_social;
        $arr_valor_hashtag['data_vencimento'] = $var_dt_vencimento;
        $arr_valor_hashtag['data_venda'] = $var_dt_venda; 
        $arr_valor_hashtag['data_vencimento_venda'] = $var_dt_vencimento_venda; 
        $arr_valor_hashtag['contato'] = $var_contato;
        $arr_valor_hashtag['referencia'] = $var_referencia;

        //metodo criado para trocar os valores das hashtags
        $var_novo_texto = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        //metodo criado para trocar os valores das hashtags
        $var_novo_assunto = fc_adiciona_hashtag_assunto($arr_valor_hashtag, $var_id_empresa);

        // TROCO APENAS A VARIAL DO TEXT SMS
        $arr_valor_hashtag['texto'] = $var_texto_sms;
        $var_novo_texto_sms = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        // TROCO APENAS A VARIAL DO TEXT WHATSAPP
        $arr_valor_hashtag['texto'] = $var_texto_whatsapp;
        $var_novo_texto_whatsapp = fc_altera_hashtag($arr_valor_hashtag, $var_id_empresa);

        #verifica o intervalo entre o vencimento e o dia estipulado     
        $sql_search = "SELECT CAST('$var_dt_vencimento'::DATE - interval '$nu_dias_venc_2 day' AS DATE) AS cast";
        $ret = pg_query($conn, $sql_search);
        $dt_enviar = pg_fetch_all($conn, $sql_search);
        $var_dt_envio = $dt_enviar[0]['cast']; 

        #COMPARA AS DATAS SE FOR ULTRAPASSADO A DATA ESTIPULADA A DATA DE VENCIMENTO, NÃO IRÁ INSERIR
        $date_venc = date_create($var_dt_vencimento);
        $date_now  = date_create(date('Y-m-d'));
        $date_env  = date_create($var_dt_envio);

        if($date_env >= $date_now){

            $conexao = new Conexao();
            $conn = null;

            if($var_id_empresa == 1){
                echo 'Contrato do dsv<hr>';
                $conn = $conexao->open("conn_contratos_dsv");
            }else{
                echo 'Contrato do prod<hr>';
                $conn = $conexao->open("conn_contratos");
            }

            $sql_search = "
                SELECT
                    * 
                FROM db_contratos.tb_notificacao 
                WHERE id_empresa    = $var_id_empresa 
                AND id_mov          = $var_id_mov
                AND id_desd         = $var_id_desd
                AND nu_notificacao  = $nu_notificacao_2";

            $ret = pg_query($conn, $sql_search);    
            $check_notificacao = pg_fetch_all($ret);

            echo 'check_notificacao => <pre>';
            print_r($check_notificacao);
            echo '</pre>';

            if(empty($check_notificacao)){
                $sql_insert = "
                    INSERT INTO db_contratos.tb_notificacao(
                        id_empresa,
                        id_mov,
                        id_desd,
                        nu_notificacao,
                        tipo,
                        tx_email_host,
                        tx_email_smtp_secure,
                        tx_email_username,
                        tx_email_password,
                        tx_email_porta,
                        tx_email_from,
                        tx_email_address,
                        tx_email_subject,
                        tx_email_body,
                        tx_login,
                        tx_retorno,
                        lo_enviado,
                        dt_vencimento,
                        dt_inc,
                        dt_envio,
                        tx_texto_sms,
                        lo_envia_sms,
                        nu_numero_telefone,
                        tx_texto_whatsapp,
                        lo_envia_whatsapp,
                        tx_tipo_anexo_email
                    ) VALUES (
                        $var_id_empresa,
                        $var_id_mov,
                        $var_id_desd,
                        $nu_notificacao_2,
                        '$var_tipo',
                        '$email_host_2',
                        'tls',
                        '$email_user_2',
                        '$email_pwd_2',
                        '$email_porta_2',
                        '$email_from_2',
                        '$var_email_address',
                        '$var_novo_assunto',
                        '$var_novo_texto',
                        'ADMIN',
                        '',
                        'N',
                        '$var_dt_vencimento',
                        now(),
                        '$var_dt_envio',
                        '$var_novo_texto_sms',
                        '$var_envia_sms',
                        '$var_numero_telefone',
                        '$var_novo_texto_whatsapp',
                        '$var_envia_whatsapp',
                        '$var_tipo_anexo_email'
                    )";

                    pg_query($conn, $sql_insert);

                $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");
                $result_insert++;
            }else{
                $var_lo_enviado = $check_notificacao[0]['lo_enviado'];
                if($var_lo_enviado == 'N'){
                    $arr = array(
                        'tipo'                      => $var_tipo,
                        'tx_email_host'             => $email_host_2,
                        'tx_email_smtp_secure'      => 'tls',
                        'tx_email_username'         => $email_user_2,
                        'tx_email_password'         => $email_pwd_2,
                        'tx_email_porta'            => $email_porta_2,
                        'tx_email_from'             => $email_from_2,
                        'tx_email_address'          => $var_email_address,
                        'tx_email_subject'          => $var_novo_assunto,
                        'tx_email_body'             => $var_novo_texto,
                        'tx_login'                  => 'ADMIN',
                        'tx_retorno'                => '',
                        'lo_enviado'                => 'N',
                        'dt_vencimento'             => $var_dt_vencimento,
                        'dt_inc'                    => 'NOW()',
                        'dt_envio'                  => $var_dt_envio,
                        'tx_tipo_anexo_email'       => $var_tipo_anexo_email
                    );

                    $tb_valor = "";
                    $tb_coluna = array();

                    foreach ($arr as $key => $value) {
                        $tb_valor    = $value;
                        $tb_coluna[] = "$key = '$value'";       
                    }

                    $implode_result = implode(', ', $tb_coluna);

                    $sql_update = "
                        UPDATE db_contratos.tb_notificacao 
                        SET $implode_result 
                        WHERE id_empresa = $var_id_empresa
                        AND id_mov = $var_id_mov
                        AND id_desd = $var_id_desd
                        AND nu_notificacao = $nu_notificacao_2
                    ";
                    pg_query($conn, $sql_update);
                    $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                    $falha++;
                }
            }
        }
    }

    $conexao = new Conexao();
    echo "Id empresa 2: ".dbNameCob($var_id_empresa_2).'<hr>';
    $conn = $conexao->open(dbNameCob($var_id_empresa_2));

    $arrCountNotificacao = array(
        'tituloNotificacao' => $tituloNotificacao,
        'sucesso' => array(
            'count' => "0"
        ),
        'atencao' => array(
            'count' => "0"
        )
    );

    if($result_insert > 0){
        $countInserted = count($arrMsgInsert);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'sucesso' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'mensagem' => "$countInserted novas notificações de cobrança inseridas."
                ),
                'linhas' => $arrMsgInsert
            )
        );
    }

    if($falha > 0){
        $countUpdated = count($arrMsgUpdate);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'atencao' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'subtitulo' => "$countUpdated notificações de cobrança atualizadas."
                ),
                'linhas' => $arrMsgUpdate
            )
        );
    }

    if(isset($arrMsgReturn['sucesso'], $arrMsgReturn['sucesso']['linhas'])){
        $countSucesso = count($arrMsgReturn['sucesso']['linhas']);
        $arrCountNotificacao['notificacao']['sucesso']['count'] = $countSucesso;
    }

    if(isset($arrMsgReturn['atencao'], $arrMsgReturn['atencao']['linhas'])){
        $countAtencao = count($arrMsgReturn['atencao']['linhas']);
        $arrCountNotificacao['notificacao']['atencao']['count'] = $countAtencao;
    }

    $arrMsgReturn['notificacao'] = $arrCountNotificacao;

    return $arrMsgReturn;

}

function fc_formatar_telefone($telefone){
    $tam = strlen(preg_replace("/[^0-9]/", "", $telefone));
    if ($tam == 13) { // COM CÓDIGO DE ÁREA NACIONAL E DO PAIS e 9 dígitos
        return "+".substr($telefone,0,$tam-11)."(".substr($telefone,$tam-11,2).")".substr($telefone,$tam-9,5)."-".substr($telefone,-4);
    }
          
    if ($tam == 12) { // COM CÓDIGO DE ÁREA NACIONAL E DO PAIS
        return "+".substr($telefone,0,$tam-10)."(".substr($telefone,$tam-10,2).")".substr($telefone,$tam-8,4)."-".substr($telefone,-4);
    }

    if ($tam == 11) { // COM CÓDIGO DE ÁREA NACIONAL e 9 dígitos
       return "(".substr($telefone,0,2).")".substr($telefone,2,5)."-".substr($telefone,7,11);
    }

    if ($tam == 10) { // COM CÓDIGO DE ÁREA NACIONAL
       return "(".substr($telefone,0,2).")".substr($telefone,2,4)."-".substr($telefone,6,10);
    }

    if ($tam <= 9) { // SEM CÓDIGO DE ÁREA
      return substr($telefone,0,$tam-4)."-".substr($telefone,-4);
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

function gera_regua_cobranca($id_empresa){

    $arr_logs = [];

    $nmDatabase = dbNameCob($id_empresa);

    echo "Nome Data Base: $nmDatabase<hr>";

    $conexao = new Conexao();
    $conn = $conexao->open($nmDatabase);

    $conexao_contratos = new Conexao();
    $conn_name = $id_empresa == 1 ? 'db_contrato_dsv': 'db_contrato';
    $conn_contratos = $conexao_contratos->open($conn_name);

    $sql_search = "
    SELECT COALESCE(id, 0) AS id_condominio
        FROM db_condominio.tb_condominio
        WHERE id_empresa = $id_empresa
    ";

    $ret = pg_query($conn, $sql_search);
    $id_condominio = pg_fetch_all($ret)[0]['id_condominio'];

    echo "Id condominio: $id_condominio<hr>";

    $resultReguaCobranca = pg_query($conn, "
        SELECT
            id_empresa, 
            lo_ativo_primeira, 
            nu_dias_primeira, 
            tx_assunto_primeira, 
            tx_texto_primeira, 
            tx_momemto_primeira, 
            lo_ativo_segunda, 
            nu_dias_segunda, 
            tx_assunto_segunda, 
            tx_texto_segunda, 
            tx_momemto_segunda, 
            lo_ativo_terceira,
            nu_dias_terceira, 
            tx_assunto_terceira, 
            tx_texto_terceira,
            tx_momemto_terceira, 
            lo_ativo_quarta, 
            nu_dias_quarta, 
            tx_assunto_quarta, 
            tx_texto_quarta,
            tx_momemto_quarta,
            lo_ativo_quinta,
            nu_dias_quinta, 
            tx_assunto_quinta,
            tx_texto_quinta, 
            tx_momemto_quinta,
            lo_ativo_sexta, 
            nu_dias_sexta, 
            tx_assunto_sexta, 
            tx_texto_sexta, 
            tx_momemto_sexta, 
            COALESCE(nu_dias_setima, 0) AS nu_dias_setima, 
            tx_assunto_setima,
            tx_texto_setima, 
            tx_momemto_setima, 
            lo_ativo_setima, 
            lo_sms_ativo_primeira, 
            lo_sms_ativo_segunda, 
            lo_sms_ativo_terceira,
            lo_sms_ativo_quarta, 
            lo_sms_ativo_quinta,
            lo_sms_ativo_sexta, 
            lo_sms_ativo_setima,
            tx_texto_sms_primeira, 
            tx_texto_sms_segunda,
            tx_texto_sms_terceira, 
            tx_texto_sms_quarta,
            tx_texto_sms_quinta,
            tx_texto_sms_sexta,
            tx_texto_sms_setima,
            lo_whatsapp_ativo_primeira,
            lo_whatsapp_ativo_segunda, 
            lo_whatsapp_ativo_terceira,
            lo_whatsapp_ativo_quarta, 
            lo_whatsapp_ativo_quinta, 
            lo_whatsapp_ativo_sexta,
            lo_whatsapp_ativo_setima,
            tx_texto_whatsapp_primeira,
            tx_texto_whatsapp_segunda, 
            tx_texto_whatsapp_terceira, 
            tx_texto_whatsapp_quarta, 
            tx_texto_whatsapp_quinta, 
            tx_texto_whatsapp_sexta,
            tx_texto_whatsapp_setima,
            tx_tipo_anexo_email,
            lo_ativo_not_ass, 
            nu_cadencia_not_ass, 
            horario_not_ass, 
            tx_assunto_email_not_ass, 
            tx_msg_email_not_ass
        FROM db_gol.tb_regua_cobranca
        WHERE id_empresa = $id_empresa
        AND lo_ativo_cobranca = 'S'
    ");

    if($resultReguaCobranca){

        $arr_param_old = array();
        $arr_aux = array();
        $arr_param_new = array();
        $params = array();

        $sql = "SELECT id_empresa  
                    FROM db_gol.tb_empresa
                    ORDER BY id_empresa";


        $result = pg_query($conn, $sql);
        
        
        while($row = pg_fetch_array($result)) {
            
            $arr_id_empresa[] = $row[0];
        }   
        
        foreach($arr_id_empresa as $key){
            //$var_id_empresa = $key[0];
            $var_id_empresa = $key;
            
            $sql2 = "SELECT * FROM db_gol.tb_msysparam WHERE id_empresa = $var_id_empresa";
            
            $result2 = pg_query($conn, $sql2);
            
            //MONTA UM ARRAY COM OS PARAMETROS ANTIGOS
            $arr_param_old = pg_fetch_array($result2, 0, PGSQL_BOTH);
            
            $sql3 = "SELECT a.tx_descricao
                            , a.tx_valor  
                        FROM db_gol.tb_parametro a
                        INNER JOIN db_gol.tb_empresa b
                        ON a.id_empresa = b.id_empresa
                        WHERE b.id_empresa = $var_id_empresa";                          
                                        
            $result3 = pg_query($conn, $sql3);

            while($row2 = pg_fetch_array($result3)) {
                $arr_param_new[$row2[0]] = $row2[1];
            }                           

            $params[$var_id_empresa]['old'] = $arr_param_old;
            $params[$var_id_empresa]['new'] = $arr_param_new;    
        }

        // echo "Params: <pre>";
        // print_r($params);
        // echo "</pre>";

        while($reguaCobranca = pg_fetch_array($resultReguaCobranca)){

            // echo "Regua Cobranca: <pre>";
            // print_r($reguaCobranca);
            // echo "</pre>";

            $resultDadosServidorEmail = pg_query($conn, "
                SELECT  
                    servidor_email,
                    usuario_email,
                    senha_email,
                    tipo_servidor,
                    enviado_de
                FROM db_gol.tb_msysparam
                WHERE id_empresa = $id_empresa
            ");

            if($resultDadosServidorEmail){
                while($dadoServidorEmail = pg_fetch_array($resultDadosServidorEmail)){

                    echo "Dados Servidor Email: <pre>";
                    print_r($dadoServidorEmail);
                    echo "</pre>";

                    $emailHost = $dadoServidorEmail['servidor_email'];
                    $emailUser = $dadoServidorEmail['usuario_email'];
                    $emailPwd = $dadoServidorEmail['senha_email'];
                    $emailFrom = $dadoServidorEmail['enviado_de'];
                    $serverType =  $dadoServidorEmail['tipo_servidor'];

                    $emailTServe = '';
                    switch ($serverType){
                        case 'GU':
                            $emailTServe = 465;
                        break;
                        case 'TL':
                            $emailTServe = 587;
                        break;
                        case '25':
                            $emailTServe = 25;
                        break;
                        case '26':
                            $emailTServe = 26;
                        break;
                    }

                    $arrMessages = array(
                        'modelo' => 5,
                        'msg' => array()
                    );

                    if($reguaCobranca['lo_ativo_primeira'] == 'S'){
                        if($reguaCobranca['lo_ativo_primeira'] == 'S'){
                            #VERIFICA NO SELECT
                            #A - ANTES DO VENCIMENTO
                            #V - NO VENCIMENTO
                            #D - DEPOIS DO VENCIMENTO
                            switch($reguaCobranca['tx_momemto_primeira']){
                                case 'A':
                                    $nu_not = 1;
                                    $dias_venc = $reguaCobranca['nu_dias_primeira'];
                                     $arr_logs[] = m_data_interval_antes_vencimento(
                                        "primeira", 
                                        $id_empresa, 
                                        $dias_venc,
                                        $nu_not,
                                        $emailHost,
                                        $emailUser,
                                        $emailPwd,
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'V':
                                    $nu_not = 1;
                                    $dias_venc = $reguaCobranca['nu_dias_primeira'];
                                     $arr_logs[] = m_data_interval_no_vencimento(
                                        "primeira", 
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost, 
                                        $emailUser, 
                                        $emailPwd, 
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'D':
                                    $nu_not = 1;
                                    $dias_venc = $reguaCobranca['nu_dias_primeira'];
                                     $arr_logs[] = m_data_interval_depois_vencimento(
                                        "primeira", 
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost, 
                                        $emailUser, 
                                        $emailPwd, 
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                            }
                        }
                    }
                
                    if($reguaCobranca['lo_ativo_segunda']){
                        if($reguaCobranca['lo_ativo_segunda']){     
                            #   VERIFICA NO SELECT
                            #A - ANTES DO VENCIMENTO
                            #V - NO VENCIMENTO
                            #D - DEPOIS DO VENCIMENTO
                
                            switch($reguaCobranca['tx_momemto_segunda']){
                                case 'A':
                                    $nu_not = 2;
                                    $dias_venc = $reguaCobranca['nu_dias_segunda'];
                                     $arr_logs[] = m_data_interval_antes_vencimento(
                                        "segunda", 
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not,
                                        $emailHost,
                                        $emailUser, 
                                        $emailPwd,
                                        $emailTServe,
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'V':
                                    $nu_not = 2;
                                    $dias_venc = $reguaCobranca['nu_dias_segunda'];
                                     $arr_logs[] = m_data_interval_no_vencimento(
                                        "segunda", 
                                        $id_empresa,
                                        $dias_venc,
                                        $nu_not,
                                        $emailHost,
                                        $emailUser,
                                        $emailPwd,
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'D':
                                    $nu_not = 2;
                                    $dias_venc = $reguaCobranca['nu_dias_segunda'];
                                     $arr_logs[] = m_data_interval_depois_vencimento(
                                        "segunda", 
                                        $id_empresa,
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost,
                                        $emailUser,
                                        $emailPwd,
                                        $emailTServe,
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                            }
                        }
                    }
                
                    if($reguaCobranca['lo_ativo_terceira'] == 'S'){
                        if($reguaCobranca['lo_ativo_terceira'] == 'S'){
                            #   VERIFICA NO SELECT
                            #A - ANTES DO VENCIMENTO
                            #V - NO VENCIMENTO
                            #D - DEPOIS DO VENCIMENTO
                
                            switch($reguaCobranca['tx_momemto_terceira']){
                                case 'A':
                                    $nu_not = 3;
                                    $dias_venc = $reguaCobranca['nu_dias_terceira'];
                                     $arr_logs[] = m_data_interval_antes_vencimento(
                                        "terceira",
                                        $id_empresa,
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost,
                                        $emailUser, 
                                        $emailPwd,
                                        $emailTServe,
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'V':
                                    $nu_not = 3;
                                    $dias_venc  = $reguaCobranca['nu_dias_terceira'];
                                     $arr_logs[] = m_data_interval_no_vencimento(
                                        "terceira",
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost, 
                                        $emailUser, 
                                        $emailPwd,
                                        $emailTServe,
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'D':
                                    $nu_not = 3;
                                    $dias_venc  = $reguaCobranca['nu_dias_terceira'];
                                     $arr_logs[] = m_data_interval_depois_vencimento(
                                        "terceira",
                                        $id_empresa,
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost, 
                                        $emailUser, 
                                        $emailPwd,
                                        $emailTServe,
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                            }
                        }
                    }
                
                    if($reguaCobranca['lo_ativo_quarta'] == 'S'){
                        if($reguaCobranca['lo_ativo_quarta'] == 'S'){           
                            #   VERIFICA NO SELECT
                            #A - ANTES DO VENCIMENTO
                            #V - NO VENCIMENTO
                            #D - DEPOIS DO VENCIMENTO
                
                            switch($reguaCobranca['tx_momemto_quarta']){
                                case 'A':
                                    $nu_not = 4;
                                    $dias_venc = $reguaCobranca['nu_dias_quarta'];
                                     $arr_logs[] = m_data_interval_antes_vencimento(
                                        "quarta", 
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost, 
                                        $emailUser, 
                                        $emailPwd, 
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'V':
                                    $nu_not = 4;
                                    $dias_venc = $reguaCobranca['nu_dias_quarta'];
                                     $arr_logs[] = m_data_interval_no_vencimento(
                                        "quarta", 
                                        $id_empresa, 
                                        $dias_venc,
                                        $nu_not,
                                        $emailHost,
                                        $emailUser,
                                        $emailPwd,
                                        $emailTServe,
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'D':
                                    $nu_not = 4;
                                    $dias_venc = $reguaCobranca['nu_dias_quarta'];
                                     $arr_logs[] = m_data_interval_depois_vencimento(
                                        "quarta", 
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost, 
                                        $emailUser, 
                                        $emailPwd, 
                                        $emailTServe,
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                            }
                        }
                    }
                
                    if($reguaCobranca['lo_ativo_quinta'] == 'S'){
                        if($reguaCobranca['lo_ativo_quinta'] == 'S'){           
                            #   VERIFICA NO SELECT
                            #A - ANTES DO VENCIMENTO
                            #V - NO VENCIMENTO
                            #D - DEPOIS DO VENCIMENTO       
                
                            switch($reguaCobranca['tx_momemto_quinta']){
                                case 'A':
                                    $nu_not = 5;
                                    $dias_venc = $reguaCobranca['nu_dias_quinta'];
                                     $arr_logs[] = m_data_interval_antes_vencimento(
                                        "quinta",
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost,
                                        $emailUser, 
                                        $emailPwd, 
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'V':
                                    $nu_not = 5;
                                    $dias_venc = $reguaCobranca['nu_dias_quinta'];
                                     $arr_logs[] = m_data_interval_no_vencimento(
                                        "quinta", 
                                        $id_empresa,
                                        $dias_venc, 
                                        $nu_not,
                                        $emailHost, 
                                        $emailUser, 
                                        $emailPwd, 
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'D':
                                    $nu_not = 5;
                                    $dias_venc = $reguaCobranca['nu_dias_quinta'];
                                     $arr_logs[] = m_data_interval_depois_vencimento(
                                        "quinta", 
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost,
                                        $emailUser,
                                        $emailPwd,
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                            }
                        }
                    }
                
                    if($reguaCobranca['lo_ativo_sexta'] == 'S'){
                        if($reguaCobranca['lo_ativo_sexta'] == 'S'){
                            #   VERIFICA NO SELECT
                            #A - ANTES DO VENCIMENTO
                            #V - NO VENCIMENTO
                            #D - DEPOIS DO VENCIMENTO       
                
                            switch($reguaCobranca['tx_momemto_sexta']){
                                case 'A':
                                    $nu_not = 6;
                                    $dias_venc = $reguaCobranca['nu_dias_sexta'];
                                     $arr_logs[] = m_data_interval_antes_vencimento(
                                        "sexta", 
                                        $id_empresa,
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost,
                                        $emailUser, 
                                        $emailPwd, 
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'V':
                                    $nu_not = 6;
                                    $dias_venc = $reguaCobranca['nu_dias_sexta'];
                                     $arr_logs[] = m_data_interval_no_vencimento(
                                        "sexta",
                                        $id_empresa,
                                        $dias_venc,
                                        $nu_not,
                                        $emailHost,
                                        $emailUser,
                                        $emailPwd,
                                        $emailTServe,
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                                case 'D':
                                    $nu_not = 6;
                                    $dias_venc = $reguaCobranca['nu_dias_sexta'];
                                     $arr_logs[] = m_data_interval_depois_vencimento(
                                        "sexta", 
                                        $id_empresa, 
                                        $dias_venc, 
                                        $nu_not, 
                                        $emailHost, 
                                        $emailUser, 
                                        $emailPwd, 
                                        $emailTServe, 
                                        $emailFrom,
                                        $conn,
                                        $params,
                                        $id_condominio,
                                        $conn_contratos
                                    );
                                break;
                            }
                        }
                    }
                
                    if($reguaCobranca['lo_ativo_setima'] == 'S'){
                        if($reguaCobranca['lo_ativo_setima'] == 'S'){
                            $nu_not = 7;
                            $nu_dia_fixo = $reguaCobranca['nu_dias_setima'];
                             $arr_logs[] = m_data_dia_fixo2(
                                "setima", 
                                $id_empresa, 
                                $nu_dia_fixo, 
                                $nu_not, 
                                $emailHost, 
                                $emailUser, 
                                $emailPwd, 
                                $emailTServe, 
                                $emailFrom,
                                $conn,
                                $params,
                                $id_condominio,
                                $conn_contratos
                            );
                        }
                    }
                }
            }
        }
    }

    echo "ARR LOGS: <pre>";
    print_r($arr_logs);
    echo "</pre>";

    return $arr_logs;
}

function m_data_interval_antes_vencimento($nm_not, $var_id_empresa, $nu_dias_venc, $nu_notificacao, $email_host, $email_user, $email_pwd, $email_porta, $email_from, $conn, $params, $id_condominio, $conn_contratos){

    echo "</hr>m_data_interval_antes_vencimento<hr>";
    $falha              = 0;
    $result_insert      = 0;
    $redir              = false;
    $msg                = "";
    $var_dt_enviado     = "";   
    $arrMsgReturn       = array();
    $arrMsgInsert       = array();
    $arrMsgUpdate       = array();
    $tituloNotificacao = '';
    $var_regua_sms_notificacao = 'N';
    $var_regua_whatsapp_notificacao = 'N';

    switch ($nm_not) {
        case 'primeira':
            $msg_header = "1ª NOTIFICAÇÃO";
            $tituloNotificacao = "1ª Notificação";  
            
        break;      
        case 'segunda':
            $msg_header = "2ª NOTIFICAÇÃO";
            $tituloNotificacao = "2ª Notificação";
            
        break;
        case 'terceira':
            $msg_header = "3ª NOTIFICAÇÃO";
            $tituloNotificacao = "3ª Notificação";

        break;
        case 'quarta':
            $msg_header = "4ª NOTIFICAÇÃO";
            $tituloNotificacao = "4ª Notificação";
            
        break;
        case 'quinta':
            $msg_header = "5ª NOTIFICAÇÃO";
            $tituloNotificacao = "5ª Notificação";
            
        break;
        case 'sexta':
            $msg_header = "6ª NOTIFICAÇÃO";
            $tituloNotificacao = "6ª Notificação";
            
            $var_regua_sms_notificacao = 'N';
            $var_regua_whatsapp_notificacao = 'N';
        break;              
    }

     echo "MSG HEADER: $msg_header<hr>";
     echo "TITULO NOTIFICAÇÃO: $tituloNotificacao<hr>";

    $var_join_condominio = "INNER";
    if($id_condominio > 0){
        $var_join_condominio = "LEFT";
    }else{
        $var_join_condominio = "INNER";
    }


    $sql_search = "
        SELECT  
        /*0*/   a.id_empresa AS id_empresa
                , a.id_mov AS id_mov
                , a.id_desd AS id_desd
                , a.tipo AS tipo
                , a.dt_vencimento AS dt_vencimento
        /*5*/   , TRIM(b.email) AS email_cliente
                , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                , c.cnpj AS cnpj_empresa
                , TRIM(c.razao_social) AS razao_social_empresa
                , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
        /*10*/  , c.email AS email_empresa
                , c.telefone AS telefone_empresa
                , c.tx_logo_nfe AS logo_empresa
                , a.id_venda AS id_venda
                , SUM(a.vr_rec_original) AS valor_fianceiro
        /*15*/  , b.razao_social
                , d.telefone1
                , e.dt_venda
                , b.contato
        /*20*/  , e.nr_pedido_talao AS referencia   
                ,CASE WHEN  a.nr_boleto > '900' 
                       THEN f.linha_digitavel
                       ELSE  a.tx_linha_digitavel
                       END as codigo_barras
                , COALESCE(b.lo_envia_whatsapp,'N')::character(1) AS lo_envia_whatsapp
                , a.id_unidade
                , a.dt_emissao
        /*25*/  , d.logradouro
                , d.numero
                , d.complemento
                , d.bairro
                , d.cidade
        /*30*/  , d.uf
                , d.cep
                , COALESCE(b.lo_envia_sms,'N')::character(1) AS lo_envia_sms
        FROM db_gol.tb_rec_pag a
        INNER JOIN db_gol.tb_pessoa b
        ON a.id_empresa = b.id_empresa
        AND a.id_pessoa = b.id_pessoa
        INNER JOIN db_gol.tb_empresa c
        ON a.id_empresa = c.id_empresa
        LEFT JOIN db_gol.tb_pessoa_endereco d
        ON d.tipo_endereco = 1 
        AND d.id_empresa = a.id_empresa
        AND d.id_pessoa_endereco = a.id_pessoa
        $var_join_condominio JOIN db_gol.tb_venda e
        ON a.id_empresa = e.id_empresa
        AND a.id_venda = e.id_venda
        LEFT JOIN db_gol.tb_rec_pag_virtual f
        ON a.id_empresa = f.id_empresa
        AND a.id_mov = f.id_venda
        AND a.id_desd = f.id_desd
        AND f.lo_status = 0
        WHERE a.id_empresa = $var_id_empresa
        AND a.id_condominio = $id_condominio
        AND a.situacao = 'A'
        AND a.tipo = 'RC'
        AND b.lo_cobranca_automatica = 'S'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20, e.nr_pedido_talao,lo_envia_whatsapp
            , f.linha_digitavel
            , d.logradouro
            , d.numero
            , d.complemento
            , d.bairro
            , d.cidade
            , d.uf
            , d.cep
            , lo_envia_sms
        ORDER BY a.dt_vencimento ASC
    ";

    $ret = pg_query($conn, $sql_search);
    $dt_interval_venc_antes = pg_fetch_all($ret);

    // echo "dt_interval_venc_antes: <pre>";
    // print_r($dt_interval_venc_antes);
    // echo "</pre>";

    if(empty($dt_interval_venc_antes)){
        $arrMsgReturn = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Não há recebimentos em aberto para gerar notificações de cobrança.'
        );
        return $arrMsgReturn;
    }   
    #INFORMAÇÕES DOS DADOS DA NOTIFICAÇÃO
    $sql_search = "
        SELECT 
            tx_assunto_".$nm_not.",
            tx_texto_".$nm_not." ,
            tx_texto_sms_".$nm_not." ,
            lo_sms_ativo_".$nm_not.",
            tx_texto_whatsapp_".$nm_not." ,
            lo_whatsapp_ativo_".$nm_not.",
            tx_tipo_anexo_email
        FROM db_gol.tb_regua_cobranca 
        WHERE id_empresa = $var_id_empresa
        --AND id_condominio = $id_condominio
    ";

    $ret = pg_query($conn, $sql_search);
    $ds_dados_notificacao_antes = pg_fetch_all($ret)[0];

    // echo "ds_dados_notificacao_antes: <pre>";
    // print_r($ds_dados_notificacao_antes);
    // echo "</pre>";

    $var_regua_sms_notificacao = 'N';
    $var_regua_whatsapp_notificacao = 'N';


    $var_assunto          = $ds_dados_notificacao_antes['tx_assunto_'.$nm_not];
    $var_texto            = $ds_dados_notificacao_antes['tx_texto_'.$nm_not];
    $var_texto_sms        = $ds_dados_notificacao_antes['tx_texto_sms_'.$nm_not];
    $var_regua_sms_notificacao        = $ds_dados_notificacao_antes['lo_sms_ativo_'.$nm_not] == 'S'? $ds_dados_notificacao_antes['lo_sms_ativo_'.$nm_not] : 'N';
    $var_texto_whatsapp   = $ds_dados_notificacao_antes['tx_texto_whatsapp_'.$nm_not];
    $var_regua_whatsapp_notificacao   = $ds_dados_notificacao_antes['lo_whatsapp_ativo_'.$nm_not] == 'S'? $ds_dados_notificacao_antes['lo_whatsapp_ativo_'.$nm_not] : 'N';
    $var_tipo_anexo_email = $ds_dados_notificacao_antes['tx_tipo_anexo_email'];

    //PARAMETRO PARA OCULTAR OS CAMPOS Enviar SMS E Enviar WHATSAPP
    $var_permissao_sms         = isset($param['utiliza_notificacao_sms']) ? $param['utiliza_notificacao_sms'] : 'N';
    $var_permissao_whatsapp    = isset($param['utiliza_notificacao_whatsapp']) ? $param['utiliza_notificacao_whatsapp'] : 'N';

    /** CARREGA OS DADOS PARA ENVIAR PARA A DB_CONTRATOS */
    $var_whatsapp_account_sid    = isset($param['whatsapp_account_sid']) ? $param['whatsapp_account_sid'] : ' ';
    $var_whatsapp_auth_token     = isset($param['whatsapp_auth_token']) ? $param['whatsapp_auth_token'] : ' ';
    $var_whatsapp_messaging_service_sid   = isset($param['whatsapp_messaging_service_sid']) ? $param['whatsapp_messaging_service_sid'] : ' ';
    $var_whatsapp_sender_number  = isset($param['whatsapp_sender_number']) ? $param['whatsapp_sender_number'] : ' ';

    $var_envia_sms = 'N'; 
    $var_envia_whatsapp = 'N' ; 

    if($var_permissao_sms == 'N'){
        $var_envia_sms = 'N'; 
    }

    if($var_permissao_whatsapp == 'N'){
        $var_envia_whatsapp = 'N' ;
    }

    $var_valida_qtd_bilhetagem_sms = true;

    if($var_valida_qtd_bilhetagem_sms == false){
        $var_envia_sms = 'N';
    }

    $var_valida_qtd_bilhetagem_whatsapp = true;

    if($var_valida_qtd_bilhetagem_whatsapp == false){
        $var_envia_whatsapp = 'N';
    }

    $var_assunto = pg_escape_string($var_assunto);
    $var_texto = pg_escape_string($var_texto);                            
    $var_texto_sms = pg_escape_string($var_texto_sms);

    if(empty($var_assunto)){
        $arrMsgReturn = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo assunto do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }
    if(empty($var_texto)){
        $arrMsgReturn = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo mensagem do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }

    #PERCORRE OS DADOS E INSERE NO BANCO
    $size = count($dt_interval_venc_antes);

    echo "SIZE: $size<hr>";

    for($i = 0; $i < $size ; $i++){

        echo "<pre>";
        print_r($dt_interval_venc_antes[$i]);
        echo "</pre>";

        $var_id_empresa           = $dt_interval_venc_antes[$i]['id_empresa'];
        $var_id_mov               = $dt_interval_venc_antes[$i]['id_mov'];
        $var_id_desd              = $dt_interval_venc_antes[$i]['id_desd'];
        $var_tipo                 = $dt_interval_venc_antes[$i]['tipo'];
        $var_dt_vencimento        = $dt_interval_venc_antes[$i]['dt_vencimento'];
        $var_email_address        = $dt_interval_venc_antes[$i]['email_cliente'];                                                
        $var_nm_fantasia_cliente  = $dt_interval_venc_antes[$i]['nm_fantasia_cliente'];
        $var_cnpj_empresa         = $dt_interval_venc_antes[$i]['cnpj_empresa'];
        $var_razao_social_empresa = $dt_interval_venc_antes[$i]['razao_social_empresa'];
        $var_nm_fantasia_empresa  = $dt_interval_venc_antes[$i]['nm_fantasia_empresa'];
        $var_email_empresa        = $dt_interval_venc_antes[$i]['email_empresa'];
        $var_telefone_empresa     = $dt_interval_venc_antes[$i]['telefone_empresa'];
        $var_logo_empresa         = $dt_interval_venc_antes[$i]['logo_empresa'];
        $var_id_venda             = $dt_interval_venc_antes[$i]['id_venda'];
        $var_valor_fianceiro      = $dt_interval_venc_antes[$i]['valor_fianceiro'];
        $var_razao_social         = $dt_interval_venc_antes[$i]['razao_social'];
        $var_numero_telefone      = $dt_interval_venc_antes[$i]['telefone1'];
        $var_dt_venda             = $dt_interval_venc_antes[$i]['dt_venda'];
        $var_dt_vencimento_venda  = $dt_interval_venc_antes[$i]['dt_vencimento'];
        $var_contato              = $dt_interval_venc_antes[$i]['contato'];
        $var_referencia           = $dt_interval_venc_antes[$i]['referencia'];   
        
        $var_codigo_barras        = $dt_interval_venc_antes[$i]['codigo_barras'];   
        
        if($var_regua_whatsapp_notificacao == 'S'){
            $var_envia_whatsapp  = $dt_interval_venc_antes[$i]['lo_envia_whatsapp'] == 'S'? $dt_interval_venc_antes[$i]['lo_envia_whatsapp'] : 'N';
        }else{
            $var_envia_whatsapp = 'N';
        }
        
        if($var_regua_sms_notificacao == 'S'){
            $var_envia_sms       = $dt_interval_venc_antes[$i]['lo_envia_sms'] == 'S'? $dt_interval_venc_antes[$i]['lo_envia_sms'] : 'N';
        }else{
            $var_envia_sms = 'N';
        }

        // CONDOMINIO
        $var_id_unidade           = $dt_interval_venc_antes[$i]['id_unidade'];
        $var_dt_emissao           = $dt_interval_venc_antes[$i]['dt_emissao'];
        $var_logradouro           = $dt_interval_venc_antes[$i]['logradouro'];
        $var_numero               = $dt_interval_venc_antes[$i]['numero'];
        $var_complemento          = $dt_interval_venc_antes[$i]['complemento'];
        $var_bairro               = $dt_interval_venc_antes[$i]['bairro'];
        $var_cidade               = $dt_interval_venc_antes[$i]['cidade'];
        $var_uf                   = $dt_interval_venc_antes[$i]['uf'];
        $var_cep                  = $dt_interval_venc_antes[$i]['cep'];
        
        $arr_valor_hashtag = array();
        $arr_valor_hashtag['nm_fantasia_cliente'] = $var_nm_fantasia_cliente;
        $arr_valor_hashtag['cnpj_empresa'] = $var_cnpj_empresa;
        $arr_valor_hashtag['razao_social_empresa'] = $var_razao_social_empresa;
        $arr_valor_hashtag['nm_fantasia_empresa'] = $var_nm_fantasia_empresa;
        $arr_valor_hashtag['email_empresa'] = $var_email_empresa;
        $arr_valor_hashtag['telefone_empresa'] = $var_telefone_empresa;
        $arr_valor_hashtag['logo_empresa'] = $var_logo_empresa;
        $arr_valor_hashtag['id_venda'] = $var_id_venda;
        $arr_valor_hashtag['texto'] = $var_texto;
        $arr_valor_hashtag['assunto'] = $var_assunto;
        $arr_valor_hashtag['valor_fianceiro'] = $var_valor_fianceiro;
        $arr_valor_hashtag['razao_social'] = $var_razao_social; 
        $arr_valor_hashtag['data_vencimento'] = $var_dt_vencimento; 
        $arr_valor_hashtag['data_venda'] = $var_dt_venda; 
        $arr_valor_hashtag['data_vencimento_venda'] = $var_dt_vencimento_venda;
        $arr_valor_hashtag['contato'] = $var_contato;
        $arr_valor_hashtag['referencia'] = $var_referencia; 
        $arr_valor_hashtag['email_cliente'] = $var_email_address ;
        $arr_valor_hashtag['id_mov'] = $var_id_mov;
        $arr_valor_hashtag['id_desd'] = $var_id_desd;   
        $arr_valor_hashtag['url_empresa'] = 'www.sempretecnologia.com.br';  
        $arr_valor_hashtag['linha_digitavel'] = $var_codigo_barras;
        
            //CONDOMINIO
        $arr_valor_hashtag['id_unidade']           = $var_id_unidade;
        $arr_valor_hashtag['dt_emissao']           = $var_dt_emissao;
        $arr_valor_hashtag['logradouro']           = $var_logradouro;
        $arr_valor_hashtag['numero']               = $var_numero;
        $arr_valor_hashtag['complemento']          = $var_complemento;
        $arr_valor_hashtag['bairro']               = $var_bairro;
        $arr_valor_hashtag['cidade']               = $var_cidade;
        $arr_valor_hashtag['uf']                   = $var_uf;
        $arr_valor_hashtag['cep']                  = $var_cep;


        if($id_condominio > 0){
            
            if($var_id_unidade  > 0){
                $sql_search = "SELECT 
                                   lo_envia_sms,
                                   lo_envia_whatsapp,
                                   tx_telefone
                            FROM   db_condominio.tb_condominio_unidade
                            WHERE  id_empresa = $var_id_empresa
                                   AND id = $var_id_unidade";
                
                $ret = pg_query($conn, $sql_search);
                $ds_unidade = pg_fetch_all($ret)[0];

                // echo "ds_unidade: <pre>";
                // print_r($ds_unidade);
                // echo "</pre>";
                
                $var_envia_sms = $ds_unidade['lo_envia_sms'] == "S" ? $ds_unidade['lo_envia_sms']: "N"; 
                $var_envia_whatsapp = $ds_unidade['lo_envia_whatsapp'] == "S" ? $ds_unidade['lo_envia_whatsapp']: "N";
                $var_numero_telefone = $ds_unidade['tx_telefone']; 
        
                if(empty($var_numero_telefone) || $var_numero_telefone ==""){
                    $var_envia_sms = 'N'; 
                    $var_envia_whatsapp = 'N' ; 
                }
            
            }else{
                $var_envia_sms = 'N'; 
                $var_envia_whatsapp = 'N' ; 
            }
            //metodo criado para trocar os valores das hashtags do condominio
            $var_hastag_alteradas   = m_altera_hastag_condominio($conn, $var_id_empresa, $id_condominio, $arr_valor_hashtag);

            // echo "var_hastag_alteradas: <pre>";
            // print_r($var_hastag_alteradas);
            // echo "</pre>";

            $var_novo_texto   = $var_hastag_alteradas[0];
            $var_novo_assunto = $var_hastag_alteradas[1];
        
        }else{
            //metodo criado para trocar os valores das hashtags
            $var_novo_texto = m_altera_hastag($var_id_empresa, $arr_valor_hashtag);

            // //metodo criado para trocar os valores das hashtags
            $var_novo_assunto = m_adiciona_hastag_assunto($arr_valor_hashtag);

            // echo "Novo texto: ";
            // echo "$var_novo_texto<hr>";
            // echo "Novo assunto: ";
            // echo "$var_novo_assunto<hr>";
        }
        
        $whatsapp_parametro_hashtag = m_altera_hastag_whatsapp($conn, $var_id_empresa, $id_condominio, $arr_valor_hashtag, $nm_not);

        // echo "whatsapp_parametro_hashtag: <pre>";
        // print_r($whatsapp_parametro_hashtag);
        // echo "</pre>";

        // TROCO APENAS A VARIAL DO TEXT SMS
        $arr_valor_hashtag['texto'] = $var_texto_sms;
        $var_novo_texto_sms = m_altera_hastag($var_id_empresa, $arr_valor_hashtag);

        #verifica o intervalo entre o vencimento e o dia estipulado     
        $sql_search = "SELECT CAST('$var_dt_vencimento'::DATE - interval '$nu_dias_venc day' AS DATE) AS dt_envio";

        echo  $sql_search.'<hr>';

        $ret = pg_query($conn, $sql_search);
        $var_dt_envio = pg_fetch_all($ret)[0]['dt_envio'];

        echo "var_dt_envio: $var_dt_envio<hr>";

        #COMPARA AS DATAS SE FOR ULTRAPASSADO A DATA ESPULADA A DATA DE VENCIMENTO, NÃO IRÁ INSERIR
        $date_venc           = date_create($var_dt_vencimento);
        $date_now            = date_create(date('Y-m-d'));
        $date_env            = date_create(date('Y-m-d'));//($var_dt_envio);

        echo "date_venc: " . $date_venc->format('Y-m-d') . "<hr>";
        echo "date_now: " . $date_now->format('Y-m-d') . "<hr>";
        echo "date_env: " . $date_env->format('Y-m-d') . "<hr>";

        if($date_env >= $date_now){
            echo "Entrou<hr>";
             #verifica se existe dados

            echo "DB Contrato: <pre>";
            print_r($conn_contratos);
            echo "</pre>";

            $sql_search = "       
                SELECT  
                    * 
                FROM db_contratos.tb_notificacao 
                WHERE id_empresa    = $var_id_empresa 
                AND id_mov          = $var_id_mov
                AND id_desd         = $var_id_desd
                AND nu_notificacao  = $nu_notificacao";

            echo $sql_search.'<hr>';

            $ret = pg_query($conn_contratos, $sql_search);
            $ds_check_notificacao = pg_fetch_all($ret)[0];

            echo "ds_check_notificacao: <pre>";
            print_r($ds_check_notificacao);
            echo "</pre>";

            if(empty($ds_check_notificacao)){

                $sql_insert = "
                    INSERT INTO db_contratos.tb_notificacao (
                        id_empresa,
                        id_mov,
                        id_desd,
                        nu_notificacao,
                        tipo,
                        tx_email_host,
                        tx_email_smtp_secure,
                        tx_email_username,
                        tx_email_password,
                        tx_email_porta,
                        tx_email_from,
                        tx_email_address,
                        tx_email_subject,
                        tx_email_body,
                        tx_login,
                        tx_retorno,
                        lo_enviado,
                        dt_vencimento,
                        dt_inc,
                        dt_envio,
                        tx_texto_sms,
                        lo_envia_sms,
                        nu_numero_telefone,
                        tx_texto_whatsapp,
                        lo_envia_whatsapp,
                        tx_tipo_anexo_email,
                        whatsapp_account_sid,
                        whatsapp_auth_token,
                        whatsapp_messaging_service_sid,
                        whatsapp_sender_number,
                        whatsapp_parametro_hashtag
                        
                    ) VALUES (
                        $var_id_empresa,
                        $var_id_mov,
                        $var_id_desd,
                        $nu_notificacao,
                        '$var_tipo',
                        '$email_host',
                        'tls',
                        '$email_user',
                        '$email_pwd',
                        '$email_porta',
                        '$email_from',
                        '$var_email_address',
                        '$var_novo_assunto',
                        '$var_novo_texto',
                        'SQL',
                        '',
                        'N',
                        '$var_dt_vencimento',
                        now(),
                        '$var_dt_envio',
                        '$var_novo_texto_sms',
                        '$var_envia_sms',
                        '$var_numero_telefone',
                        '$var_texto_whatsapp',
                        '$var_envia_whatsapp',
                        '$var_tipo_anexo_email',
                        '$var_whatsapp_account_sid',
                        '$var_whatsapp_auth_token',
                        '$var_whatsapp_messaging_service_sid',
                        '$var_whatsapp_sender_number',
                        '$whatsapp_parametro_hashtag'
                    )";

                   if(pg_query($conn_contratos, $sql_insert)){
                        $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");
                        $result_insert++;
                   }else{
                        echo "Erro ao tentar inserir nota $var_id_mov/$var_id_desd<hr>";
                   }

            }else{
                $var_lo_enviado = $ds_check_notificacao['lo_enviado'];
                if($var_lo_enviado == 'N'){
                    $arr = array(
                        'tipo'                      => $var_tipo,
                        'tx_email_host'             => $email_host,
                        'tx_email_smtp_secure'      => 'tls',
                        'tx_email_username'         => $email_user,
                        'tx_email_password'         => $email_pwd,
                        'tx_email_porta'            => $email_porta,
                        'tx_email_from'             => $email_from,
                        'tx_email_address'          => $var_email_address,
                        'tx_email_subject'          => $var_novo_assunto,
                        'tx_email_body'             => $var_novo_texto,
                        'tx_login'                  => 'SQL',
                        'tx_retorno'                => '',
                        'lo_enviado'                => 'N',
                        'dt_vencimento'             => $var_dt_vencimento,
                        'dt_inc'                    => 'NOW()',
                        'dt_envio'                  => $var_dt_envio,
                        'tx_tipo_anexo_email'       => $var_tipo_anexo_email,
                        'whatsapp_account_sid'      => $var_whatsapp_account_sid,
                        'whatsapp_auth_token'       => $var_whatsapp_auth_token,
                        'whatsapp_messaging_service_sid ' => $var_whatsapp_messaging_service_sid,
                        'whatsapp_sender_number'     => $var_whatsapp_sender_number,
                        'whatsapp_parametro_hashtag' => $whatsapp_parametro_hashtag,
                        'nu_numero_telefone'         => $var_numero_telefone,
                        'tx_texto_whatsapp'          => $var_texto_whatsapp,
                        'lo_envia_whatsapp'          => $var_envia_whatsapp
                    );
                    

                    $tb_valor = "";
                    $tb_coluna = array();

                    foreach($arr as $key => $value){
                        $tb_valor       = $value;
                        $tb_coluna[]    = "$key = '$value'";        
                    }

                    $implode_result = implode(', ', $tb_coluna);

                    $sql_update = "
                        UPDATE db_contratos.tb_notificacao 
                        SET $implode_result 
                        WHERE id_empresa = $var_id_empresa
                        AND id_mov = $var_id_mov        
                        AND id_desd = $var_id_desd
                        AND nu_notificacao = $nu_notificacao
                    ";
                    
                    if(pg_query($conn_contratos, $sql_update)){
                        $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                        $falha++;
                    }else{
                        echo "Erro ao tentar atualizar nota $var_id_mov/$var_id_desd<hr>";
                    }

                }#endif
                // $conexao_contratos->close();
            }#endif
        }#endif
    }#endfor

    $arrCountNotificacao = array(
        'tituloNotificacao' => $tituloNotificacao,
        'sucesso' => array(
            'count' => "0"
        ),
        'atencao' => array(
            'count' => "0"
        )
    );

    if($result_insert > 0){
        $countInserted = count($arrMsgInsert);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'sucesso' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'mensagem' => "$countInserted novas notificações de cobrança inseridas."
                ),
                'linhas' => $arrMsgInsert
            )
        );
    }

    if($falha > 0){
        $countUpdated = count($arrMsgUpdate);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'atencao' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'subtitulo' => "$countUpdated notificações de cobrança atualizadas."
                ),
                'linhas' => $arrMsgUpdate
            )
        );
    }

    if(isset($arrMsgReturn['sucesso'], $arrMsgReturn['sucesso']['linhas'])){
        $countSucesso = count($arrMsgReturn['sucesso']['linhas']);
        $arrCountNotificacao['notificacao']['sucesso']['count'] = $countSucesso;
    }

    if(isset($arrMsgReturn['atencao'], $arrMsgReturn['atencao']['linhas'])){
        $countAtencao = count($arrMsgReturn['atencao']['linhas']);
        $arrCountNotificacao['notificacao']['atencao']['count'] = $countAtencao;
    }

    $arrMsgReturn['notificacao'] = $arrCountNotificacao;

    echo "ARR MSG RETURN 1: <pre>";
    print_r($arrMsgReturn);
    echo "</pre>";

    return $arrMsgReturn;
}

function m_data_interval_no_vencimento($nm_not_2, $var_id_empresa_2, $nu_dias_venc_2, $nu_notificacao_2, $email_host_2, $email_user_2, $email_pwd_2, $email_porta_2, $email_from_2, $conn, $params, $id_condominio, $conn_contratos){

    echo "</hr>m_data_interval_no_vencimento<hr>";

    $falha          = 0;
    $result_insert  = 0;
    $redir          = false;
    $msg            = "";
    $arrMsgReturn   = array();
    $arrMsgInsert   = array();
    $arrMsgUpdate   = array();
    $tituloNotificacao = '';

    switch ($nm_not_2) {
        case 'primeira':
            $msg_header = "1ª NOTIFICAÇÃO";
            $tituloNotificacao = "1ª Notificação";      
        break;      
        case 'segunda':
            $msg_header = "2ª NOTIFICAÇÃO";
            $tituloNotificacao = "2ª Notificação";
        break;
        case 'terceira':
            $msg_header = "3ª NOTIFICAÇÃO";
            $tituloNotificacao = "3ª Notificação";
        break;
        case 'quarta':
            $msg_header = "4ª NOTIFICAÇÃO";
            $tituloNotificacao = "4ª Notificação";
        break;
        case 'quinta':
            $msg_header = "5ª NOTIFICAÇÃO";
            $tituloNotificacao = "5ª Notificação";
        break;
        case 'sexta':
            $msg_header = "6ª NOTIFICAÇÃO";
            $tituloNotificacao = "6ª Notificação";
        break;              
    }

    echo "MSG HEADER: $msg_header<hr>";
    echo "TITULO NOTIFICACAO: $tituloNotificacao<hr>";

    // join para nao consultar venda no condominio
    $var_join_condominio = "INNER";
    if($id_condominio > 0){
        $var_join_condominio = "LEFT";
    }else{
        $var_join_condominio = "INNER";
    }

    $sql_search = "
        SELECT  
            /*0*/   a.id_empresa AS id_empresa
                    , a.id_mov AS id_mov
                    , a.id_desd AS id_desd
                    , a.tipo AS tipo
                    , a.dt_vencimento AS dt_vencimento
            /*5*/   , TRIM(b.email) AS email_cliente
                    , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                    , c.cnpj AS cnpj_empresa
                    , TRIM(c.razao_social) AS razao_social_empresa
                    , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
            /*10*/  , c.email AS email_empresa
                    , c.telefone AS telefone_empresa
                    , c.tx_logo_nfe AS logo_empresa
                    , a.id_venda AS id_venda
                    , SUM(a.vr_rec_original) AS valor_fianceiro
            /*15*/  , b.razao_social
                    , d.telefone1
                    , e.dt_venda
                    , b.contato
            /*20*/  , e.nr_pedido_talao AS referencia
                    ,CASE WHEN  a.nr_boleto > '900' 
                    THEN f.linha_digitavel
                    ELSE  a.tx_linha_digitavel
                    END as codigo_barras
                    , COALESCE(b.lo_envia_whatsapp,'N')::character(1) AS lo_envia_whatsapp
                    , a.id_unidade
                    , a.dt_emissao
            /*25*/  , d.logradouro
                    , d.numero
                    , d.complemento
                    , d.bairro
                    , d.cidade
        /*30*/   , d.uf
                    , d.cep
                    , COALESCE(b.lo_envia_sms,'N')::character(1) AS lo_envia_sms
                FROM db_gol.tb_rec_pag a
                INNER JOIN db_gol.tb_pessoa b
                ON a.id_empresa = b.id_empresa
                AND a.id_pessoa = b.id_pessoa
                INNER JOIN db_gol.tb_empresa c
                ON a.id_empresa = c.id_empresa
                LEFT JOIN db_gol.tb_pessoa_endereco d
                ON d.tipo_endereco = 1 
                AND d.id_empresa = a.id_empresa
                AND d.id_pessoa_endereco = a.id_pessoa
                $var_join_condominio JOIN db_gol.tb_venda e
                ON a.id_empresa = e.id_empresa
                AND a.id_venda = e.id_venda
                LEFT JOIN db_gol.tb_rec_pag_virtual f
                ON a.id_empresa = f.id_empresa
                AND a.id_mov = f.id_venda
                AND a.id_desd = f.id_desd
                AND f.lo_status = 0
                WHERE a.id_empresa = $var_id_empresa_2
                AND a.id_condominio = $id_condominio
                AND a.situacao = 'A'
                AND a.tipo = 'RC'
                AND b.lo_cobranca_automatica = 'S'
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20, e.nr_pedido_talao, lo_envia_whatsapp,f.linha_digitavel,d.logradouro, d.numero
                    , d.complemento
                    , d.bairro
                    , d.cidade
                    , d.uf
                    , d.cep
                    , lo_envia_sms
                ORDER BY a.dt_vencimento ASC
    ";
    
    $ret = pg_query($conn, $sql_search);
    $dt_interval_venc_v = pg_fetch_all($ret); 

    // echo "dt_interval_venc_v: <pre>";
    // print_r($dt_interval_venc_v);
    // echo "</pre>";

    if(empty($dt_interval_venc_v)){
        $arrMsgReturn[$nm_not_2] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Não há recebimentos em aberto para gerar notificações de cobrança.'
        );
        return $arrMsgReturn;
    }

    $var_regua_sms_notificacao = 'N';
    $var_regua_whatsapp_notificacao = 'N';

    #INFORMAÇÕES DOS DADOS DA NOTIFICAÇÃO
    $sql_search = "
        SELECT tx_assunto_".$nm_not_2.",
            tx_texto_".$nm_not_2.",
                tx_texto_sms_".$nm_not_2." ,
                lo_sms_ativo_".$nm_not_2." ,
                tx_texto_whatsapp_".$nm_not_2." ,
                lo_whatsapp_ativo_".$nm_not_2.",
                tx_tipo_anexo_email
        FROM db_gol.tb_regua_cobranca 
        WHERE id_empresa = $var_id_empresa_2
        
    ";

    $ret = pg_query($conn, $sql_search);
    $ds_dados_notificacao_venc = pg_fetch_all($ret)[0];

    echo "ds_dados_notificacao_venc: <pre>";
    print_r($ds_dados_notificacao_venc);
    echo "</pre>";

    $var_assunto     = $ds_dados_notificacao_venc["tx_assunto_".$nm_not_2];
    $var_texto       = $ds_dados_notificacao_venc["tx_texto_".$nm_not_2];
    $var_texto_sms   = $ds_dados_notificacao_venc["tx_texto_sms_".$nm_not_2];
    $var_regua_sms_notificacao   = $ds_dados_notificacao_venc["lo_sms_ativo_".$nm_not_2] == 'S'? $ds_dados_notificacao_venc["lo_sms_ativo_".$nm_not_2] : 'N';
    $var_texto_whatsapp   = $ds_dados_notificacao_venc["tx_texto_whatsapp_".$nm_not_2];
    $var_regua_whatsapp_notificacao   = $ds_dados_notificacao_venc["lo_whatsapp_ativo_".$nm_not_2] == 'S'? $ds_dados_notificacao_venc["lo_whatsapp_ativo_".$nm_not_2] : 'N';
    $var_tipo_anexo_email = $ds_dados_notificacao_venc["tx_tipo_anexo_email"];

    //PARAMETRO PARA OCULTAR OS CAMPOS Enviar SMS E Enviar WHATSAPP
    $var_permissao_sms         = isset($params[$var_id_empresa_2]['new']['utiliza_notificacao_sms']) ? $params[$var_id_empresa_2]['new']['utiliza_notificacao_sms'] : 'N';
    $var_permissao_whatsapp    = isset($params[$var_id_empresa_2]['new']['utiliza_notificacao_whatsapp']) ? $params[$var_id_empresa_2]['new']['utiliza_notificacao_whatsapp'] : 'N';

    /** CARREGA OS DADOS PARA ENVIAR PARA A DB_CONTRATOS */
    $var_whatsapp_account_sid    = isset($params[$var_id_empresa_2]['new']['whatsapp_account_sid']) ? $params[$var_id_empresa_2]['new']['whatsapp_account_sid'] : ' ';
    $var_whatsapp_auth_token     = isset($params[$var_id_empresa_2]['new']['whatsapp_auth_token']) ? $params[$var_id_empresa_2]['new']['whatsapp_auth_token'] : ' ';
    $var_whatsapp_messaging_service_sid   = isset($params[$var_id_empresa_2]['new']['whatsapp_messaging_service_sid']) ? $params[$var_id_empresa_2]['new']['whatsapp_messaging_service_sid'] : ' ';
    $var_whatsapp_sender_number  = isset($params[$var_id_empresa_2]['new']['whatsapp_sender_number']) ? $params[$var_id_empresa_2]['new']['whatsapp_sender_number'] : ' ';
    $var_envia_sms = 'N'; 
    $var_envia_whatsapp = 'N' ;

    // echo "Params: <pre>";
    // print_r($params);
    // echo "</pre>";

    // echo "Param: ".$var_permissao_sms.'<hr>';         
    // echo "Param: ".$var_permissao_whatsapp.'<hr>';    
    // echo "Param: ".$var_whatsapp_account_sid.'<hr>';  
    // echo "Param: ".$var_whatsapp_auth_token.'<hr>';   
    // echo "Param: ".$var_whatsapp_messaging_service_sid.'<hr>';
    // echo "Param: ".$var_whatsapp_sender_number.'<hr>';

    if($var_permissao_sms == 'N'){
        $var_envia_sms = 'N'; 
    }
    if($var_permissao_whatsapp == 'N'){
        $var_envia_whatsapp = 'N' ;
    }

    $var_valida_qtd_bilhetagem_sms = true;

    if($var_valida_qtd_bilhetagem_sms == false){
        $var_envia_sms = 'N';
    }

    $var_valida_qtd_bilhetagem_whatsapp = true;

    if($var_valida_qtd_bilhetagem_whatsapp == false){
        $var_envia_whatsapp = 'N';
    }

    $var_assunto     = pg_escape_string($var_assunto);
    $var_texto       = pg_escape_string($var_texto);
    $var_texto_sms   = pg_escape_string($var_texto_sms);

    if(empty($var_assunto)){
        $arrMsgReturn[$nm_not_2] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo assunto do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }
    if(empty($var_texto)){
        $arrMsgReturn[$nm_not_2] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo mensagem do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }

    #PERCORRE OS DADOS E INSERE NO BANCO
    $size = count($dt_interval_venc_v);


    for($i = 0; $i < $size ; $i++){
        $var_id_empresa           = $dt_interval_venc_v[$i]['id_empresa'];
        $var_id_mov               = $dt_interval_venc_v[$i]['id_mov'];
        $var_id_desd              = $dt_interval_venc_v[$i]['id_desd'];
        $var_tipo                 = $dt_interval_venc_v[$i]['tipo'];
        $var_dt_vencimento        = $dt_interval_venc_v[$i]['dt_vencimento'];
        $var_email_address        = $dt_interval_venc_v[$i]['email_cliente'];
        $var_nm_fantasia_cliente  = $dt_interval_venc_v[$i]['nm_fantasia_cliente'];
        $var_cnpj_empresa         = $dt_interval_venc_v[$i]['cnpj_empresa'];
        $var_razao_social_empresa = $dt_interval_venc_v[$i]['razao_social_empresa'];
        $var_nm_fantasia_empresa  = $dt_interval_venc_v[$i]['nm_fantasia_empresa'];
        $var_email_empresa        = $dt_interval_venc_v[$i]['email_empresa'];
        $var_telefone_empresa     = $dt_interval_venc_v[$i]['telefone_empresa'];
        $var_logo_empresa         = $dt_interval_venc_v[$i]['logo_empresa'];
        $var_id_venda             = $dt_interval_venc_v[$i]['id_venda'];
        $var_valor_fianceiro      = $dt_interval_venc_v[$i]['valor_fianceiro'];
        $var_razao_social         = $dt_interval_venc_v[$i]['razao_social'];
        $var_numero_telefone      = $dt_interval_venc_v[$i]['telefone1'];
        $var_dt_venda             = $dt_interval_venc_v[$i]['dt_venda'];
        $var_dt_vencimento_venda  = $dt_interval_venc_v[$i]['dt_vencimento'];
        $var_contato              = $dt_interval_venc_v[$i]['contato'];
        $var_referencia           = $dt_interval_venc_v[$i]['referencia'];
        $var_codigo_barras        = $dt_interval_venc_v[$i]['codigo_barras'];
        
        if($var_regua_whatsapp_notificacao == 'S'){
            $var_envia_whatsapp       = $dt_interval_venc_v[$i]['lo_envia_whatsapp'] == 'S'? $dt_interval_venc_v[$i]['lo_envia_whatsapp'] : 'N';
        }else{
            $var_envia_whatsapp = 'N';
        }
        
        
        if($var_regua_sms_notificacao == 'S'){
            $var_envia_sms       = $dt_interval_venc_v[$i]['lo_envia_sms'] == 'S'? $dt_interval_venc_v[$i]['lo_envia_sms'] : 'N';
        }else{
            $var_envia_sms = 'N';
        }
        
        $var_id_unidade           = $dt_interval_venc_v[$i]['id_unidade'];
        $var_dt_emissao           = $dt_interval_venc_v[$i]['dt_emissao'];
        $var_logradouro           = $dt_interval_venc_v[$i]['logradouro'];
        $var_numero               = $dt_interval_venc_v[$i]['numero'];
        $var_complemento          = $dt_interval_venc_v[$i]['complemento'];
        $var_bairro               = $dt_interval_venc_v[$i]['bairro'];
        $var_cidade               = $dt_interval_venc_v[$i]['cidade'];
        $var_uf                   = $dt_interval_venc_v[$i]['uf'];
        $var_cep                  = $dt_interval_venc_v[$i]['cep'];

        $arr_valor_hashtag = array();
        $arr_valor_hashtag['nm_fantasia_cliente'] = $var_nm_fantasia_cliente;
        $arr_valor_hashtag['cnpj_empresa'] = $var_cnpj_empresa;
        $arr_valor_hashtag['razao_social_empresa'] = $var_razao_social_empresa;
        $arr_valor_hashtag['nm_fantasia_empresa'] = $var_nm_fantasia_empresa;
        $arr_valor_hashtag['email_empresa'] = $var_email_empresa;
        $arr_valor_hashtag['telefone_empresa'] = $var_telefone_empresa;
        $arr_valor_hashtag['logo_empresa'] = $var_logo_empresa;
        $arr_valor_hashtag['id_venda'] = $var_id_venda;
        $arr_valor_hashtag['texto'] = $var_texto;
        $arr_valor_hashtag['assunto'] = $var_assunto;
        $arr_valor_hashtag['valor_fianceiro'] = $var_valor_fianceiro;
        $arr_valor_hashtag['razao_social'] = $var_razao_social;
        $arr_valor_hashtag['data_vencimento'] = $var_dt_vencimento;
        $arr_valor_hashtag['data_venda'] = $var_dt_venda; 
        $arr_valor_hashtag['data_vencimento_venda'] = $var_dt_vencimento_venda; 
        $arr_valor_hashtag['contato'] = $var_contato;
        $arr_valor_hashtag['referencia'] = $var_referencia;
        $arr_valor_hashtag['email_cliente'] = $var_email_address ;
        $arr_valor_hashtag['id_mov'] = $var_id_mov;
        $arr_valor_hashtag['id_desd'] = $var_id_desd;   
        $arr_valor_hashtag['url_empresa'] = 'www.sempretecnologia.com.br';  
        $arr_valor_hashtag['linha_digitavel'] = $var_codigo_barras;
        $arr_valor_hashtag['id_unidade']           = $var_id_unidade;
        $arr_valor_hashtag['dt_emissao']           = $var_dt_emissao;
        $arr_valor_hashtag['logradouro']           = $var_logradouro;
        $arr_valor_hashtag['numero']               = $var_numero;
        $arr_valor_hashtag['complemento']          = $var_complemento;
        $arr_valor_hashtag['bairro']               = $var_bairro;
        $arr_valor_hashtag['cidade']               = $var_cidade;
        $arr_valor_hashtag['uf']                   = $var_uf;
        $arr_valor_hashtag['cep']                  = $var_cep;

        // echo "ARR VALOR HASHTAG: <pre>";
        // print_r($arr_valor_hashtag);
        // echo "</pre>";

        if($id_condominio > 0){
            
            if($var_id_unidade  > 0){
                $sql_search = "SELECT 
                                lo_envia_sms,
                                lo_envia_whatsapp,
                                tx_telefone
                            FROM   db_condominio.tb_condominio_unidade
                            WHERE  id_empresa = $var_id_empresa_2
                                AND id = $var_id_unidade";

                $ret = pg_query($conn, $sql_search);
                $ds_unidade = pg_fetch_all($ret)[0];

                // echo "UNIDADE: <pre>";
                // print_r($ds_unidade);
                // echo "</pre>";
                
                $var_envia_sms = $ds_unidade['lo_envia_sms'] == "S" ? $ds_unidade['lo_envia_sms']: "N"; 
                $var_envia_whatsapp = $ds_unidade['lo_envia_whatsapp'] == "S" ? $ds_unidade['lo_envia_whatsapp']: "N";
                $var_numero_telefone = $ds_unidade['tx_telefone']; 
        
                if(empty($var_numero_telefone) || $var_numero_telefone ==""){
                    $var_envia_sms = 'N'; 
                    $var_envia_whatsapp = 'N' ; 
                }
            
            }else{
                $var_envia_sms = 'N'; 
                $var_envia_whatsapp = 'N' ; 
            }
            //metodo criado para trocar os valores das hashtags do condominio
            $var_hastag_alteradas   = m_altera_hastag_condominio($conn, $var_id_empresa, $id_condominio, $arr_valor_hashtag);

            // echo "hashtags alteradas: <pre>";
            // print_r($var_hastag_alteradas);
            // echo "</pre>";

            $var_novo_texto   = $var_hastag_alteradas[0];
            $var_novo_assunto = $var_hastag_alteradas[1];
        
        }else{
            //metodo criado para trocar os valores das hashtags
            $var_novo_texto = m_altera_hastag($var_id_empresa, $arr_valor_hashtag);

            echo "Novo texto: $var_novo_texto<hr>";

            //metodo criado para trocar os valores das hashtags
            $var_novo_assunto = m_adiciona_hastag_assunto($arr_valor_hashtag);

            echo "Novo assunto: $var_novo_assunto<hr>";
        }
        
        //troca as hashtags dos templates da twilio
        $whatsapp_parametro_hashtag = m_altera_hastag_whatsapp($conn, $var_id_empresa, $id_condominio, $arr_valor_hashtag, $nm_not_2);

        // TROCO APENAS A VARIAL DO TEXT SMS
        $arr_valor_hashtag['texto'] = $var_texto_sms;
        $var_novo_texto_sms = m_altera_hastag($var_id_empresa, $arr_valor_hashtag);

        #verifica o intervalo entre o vencimento e o dia estipulado     
        $sql_search = "SELECT CAST('$var_dt_vencimento'::DATE - interval '$nu_dias_venc_2 day' AS DATE) as dt";
        
        $ret = pg_query($conn, $sql_search);
  
        $var_dt_envio = pg_fetch_all($ret)[0]['dt']; 

        // echo "Data  envio $var_dt_envio<hr>";

        #COMPARA AS DATAS SE FOR ULTRAPASSADO A DATA ESTIPULADA A DATA DE VENCIMENTO, NÃO IRÁ INSERIR
        $date_venc = date_create($var_dt_vencimento);
        $date_now  = date_create(date('Y-m-d'));
        $date_env  = date_create($var_dt_envio);

        if($date_env >= $date_now){

            $sql_search = "
                SELECT
                    * 
                FROM db_contratos.tb_notificacao 
                WHERE id_empresa    = $var_id_empresa 
                AND id_mov          = $var_id_mov
                AND id_desd         = $var_id_desd
                AND nu_notificacao  = $nu_notificacao_2";

            $ret = pg_query($conn_contratos, $sql_search);
            $ds_check_notificacao = pg_fetch_all($ret);

            if(empty($ds_check_notificacao)){

                $sql_insert = "
                    INSERT INTO db_contratos.tb_notificacao(
                        id_empresa,
                        id_mov,
                        id_desd,
                        nu_notificacao,
                        tipo,
                        tx_email_host,
                        tx_email_smtp_secure,
                        tx_email_username,
                        tx_email_password,
                        tx_email_porta,
                        tx_email_from,
                        tx_email_address,
                        tx_email_subject,
                        tx_email_body,
                        tx_login,
                        tx_retorno,
                        lo_enviado,
                        dt_vencimento,
                        dt_inc,
                        dt_envio,
                        tx_texto_sms,
                        lo_envia_sms,
                        nu_numero_telefone,
                        tx_texto_whatsapp,
                        lo_envia_whatsapp,
                        tx_tipo_anexo_email,
                        whatsapp_account_sid,
                        whatsapp_auth_token,
                        whatsapp_messaging_service_sid,
                        whatsapp_sender_number,
                        whatsapp_parametro_hashtag
                        
                    ) VALUES (
                        $var_id_empresa,
                        $var_id_mov,
                        $var_id_desd,
                        $nu_notificacao_2,
                        '$var_tipo',
                        '$email_host_2',
                        'tls',
                        '$email_user_2',
                        '$email_pwd_2',
                        '$email_porta_2',
                        '$email_from_2',
                        '$var_email_address',
                        '$var_novo_assunto',
                        '$var_novo_texto',
                        'SQL',
                        '',
                        'N',
                        '$var_dt_vencimento',
                        now(),
                        '$var_dt_envio',
                        '$var_novo_texto_sms',
                        '$var_envia_sms',
                        '$var_numero_telefone',
                        '$var_texto_whatsapp',
                        '$var_envia_whatsapp',
                        '$var_tipo_anexo_email',
                        '$var_whatsapp_account_sid',
                        '$var_whatsapp_auth_token',
                        '$var_whatsapp_messaging_service_sid',
                        '$var_whatsapp_sender_number',
                        '$whatsapp_parametro_hashtag'
                    )";
                    
                    if(pg_query($conn_contratos, $sql_insert)){
                        $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");
                        $result_insert++;
                   }else{
                        echo "Erro ao tentar inserir nota $var_id_mov/$var_id_desd<hr>"; 
                   }
                    
                
            }else{
                $var_lo_enviado = $ds_check_notificacao[0]['lo_enviado'];
                if($var_lo_enviado == 'N'){
                    $arr = array(
                        'tipo'                      => $var_tipo,
                        'tx_email_host'             => $email_host_2,
                        'tx_email_smtp_secure'      => 'tls',
                        'tx_email_username'         => $email_user_2,
                        'tx_email_password'         => $email_pwd_2,
                        'tx_email_porta'            => $email_porta_2,
                        'tx_email_from'             => $email_from_2,
                        'tx_email_address'          => $var_email_address,
                        'tx_email_subject'          => $var_novo_assunto,
                        'tx_email_body'             => $var_novo_texto,
                        'tx_login'                  => 'SQL',
                        'tx_retorno'                => '',
                        'lo_enviado'                => 'N',
                        'dt_vencimento'             => $var_dt_vencimento,
                        'dt_inc'                    => 'NOW()',
                        'dt_envio'                  => $var_dt_envio,
                        'tx_tipo_anexo_email'       => $var_tipo_anexo_email,
                        'whatsapp_account_sid'      => $var_whatsapp_account_sid,
                        'whatsapp_auth_token'       => $var_whatsapp_auth_token,
                        'whatsapp_messaging_service_sid ' => $var_whatsapp_messaging_service_sid,
                        'whatsapp_sender_number'     => $var_whatsapp_sender_number,
                        'whatsapp_parametro_hashtag' => $whatsapp_parametro_hashtag,
                        'nu_numero_telefone'         => $var_numero_telefone,
                        'tx_texto_whatsapp'          => $var_texto_whatsapp,
                        'lo_envia_whatsapp'          => $var_envia_whatsapp
                    );

                    $tb_valor = "";
                    $tb_coluna = array();

                    foreach ($arr as $key => $value) {
                        $tb_valor    = $value;
                        $tb_coluna[] = "$key = '$value'";       
                    }

                    $implode_result = implode(', ', $tb_coluna);

                    $sql_update = "
                        UPDATE db_contratos.tb_notificacao 
                        SET $implode_result 
                        WHERE id_empresa = $var_id_empresa
                        AND id_mov = $var_id_mov
                        AND id_desd = $var_id_desd
                        AND nu_notificacao = $nu_notificacao_2
                    ";

                    if(pg_query($conn_contratos, $sql_update)){
                        $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                        $falha++;
                    }else{
                        echo "Erro ao tentar atualizar nota $var_id_mov/$var_id_desd<hr>";
                    }
                }#endif
            }#endif
            // $conexao_contratos->close();
        }#endif
    }#endfor

    $arrCountNotificacao = array(
        'tituloNotificacao' => $tituloNotificacao,
        'sucesso' => array(
            'count' => "0"
        ),
        'atencao' => array(
            'count' => "0"
        )
    );

    if($result_insert > 0){
        $countInserted = count($arrMsgInsert);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'sucesso' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'mensagem' => "$countInserted novas notificações de cobrança inseridas."
                ),
                'linhas' => $arrMsgInsert
            )
        );
    }

    if($falha > 0){
        $countUpdated = count($arrMsgUpdate);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'atencao' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'subtitulo' => "$countUpdated notificações de cobrança atualizadas."
                ),
                'linhas' => $arrMsgUpdate
            )
        );
    }

    if(isset($arrMsgReturn['sucesso'], $arrMsgReturn['sucesso']['linhas'])){
        $countSucesso = count($arrMsgReturn['sucesso']['linhas']);
        $arrCountNotificacao['notificacao']['sucesso']['count'] = $countSucesso;
    }

    if(isset($arrMsgReturn['atencao'], $arrMsgReturn['atencao']['linhas'])){
        $countAtencao = count($arrMsgReturn['atencao']['linhas']);
        $arrCountNotificacao['notificacao']['atencao']['count'] = $countAtencao;
    }

    $arrMsgReturn['notificacao'] = $arrCountNotificacao;

    echo "ARR MSG RETURN 2: <pre>";
    print_r($arrMsgReturn);
    echo "</pre>";

    return $arrMsgReturn;

}

function m_data_interval_depois_vencimento($nm_not_3, $var_id_empresa_3, $nu_dias_venc_3, $nu_notificacao_3, $email_host_3, $email_user_3, $email_pwd_3, $email_porta_3, $email_from_3, $conn, $params, $id_condominio, $conn_contratos){

    echo "<hr>m_data_interval_depois_vencimento<hr>";

    $falha          = 0;
    $result_insert  = 0;
    $redir          = false;
    $msg            = "";
    $arrMsgReturn   = array();
    $arrMsgInsert   = array();
    $arrMsgUpdate   = array();
    $tituloNotificacao = '';

    switch ($nm_not_3) {
        case 'primeira':
            $msg_header = "1ª NOTIFICAÇÃO";
            $tituloNotificacao = "1ª Notificação";      
        break;      
        case 'segunda':
            $msg_header = "2ª NOTIFICAÇÃO";
            $tituloNotificacao = "2ª Notificação";
        break;
        case 'terceira':
            $msg_header = "3ª NOTIFICAÇÃO";
            $tituloNotificacao = "3ª Notificação";
        break;
        case 'quarta':
            $msg_header = "4ª NOTIFICAÇÃO";
            $tituloNotificacao = "4ª Notificação";
        break;
        case 'quinta':
            $msg_header = "5ª NOTIFICAÇÃO";
            $tituloNotificacao = "5ª Notificação";
        break;
        case 'sexta':
            $msg_header = "6ª NOTIFICAÇÃO";
            $tituloNotificacao = "6ª Notificação";
        break;              
    }

    // join para nao consultar venda no condominio
    $var_join_condominio = "INNER";
    if($id_condominio > 0){
        $var_join_condominio = "LEFT";
    }else{
        $var_join_condominio = "INNER";
    }

    $sql_search = "
        SELECT  
        /*0*/   a.id_empresa AS id_empresa
                , a.id_mov AS id_mov
                , a.id_desd AS id_desd
                , a.tipo AS tipo
                , a.dt_vencimento AS dt_vencimento
        /*5*/   , TRIM(b.email) AS email_cliente
                , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                , c.cnpj AS cnpj_empresa
                , TRIM(c.razao_social) AS razao_social_empresa
                , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
        /*10*/  , c.email AS email_empresa
                , c.telefone AS telefone_empresa
                , c.tx_logo_nfe AS logo_empresa
                , a.id_venda AS id_venda
                , SUM(a.vr_rec_original) AS valor_fianceiro
        /*15*/  , b.razao_social
                , d.telefone1
                , e.dt_venda
                , b.contato
        /*20*/  , e.nr_pedido_talao AS referencia
                , CASE WHEN  a.nr_boleto > '900' 
                    THEN f.linha_digitavel
                    ELSE  a.tx_linha_digitavel
                    END AS codigo_barras
                , COALESCE(b.lo_envia_whatsapp,'N')::character(1) AS lo_envia_whatsapp
                , a.id_unidade
                , a.dt_emissao
        /*25*/  , d.logradouro
                , d.numero
                , d.complemento
                , d.bairro
                , d.cidade
    /*30*/   , d.uf
                , d.cep
                , COALESCE(b.lo_envia_sms,'N')::character(1) AS lo_envia_sms
        FROM db_gol.tb_rec_pag a
        INNER JOIN db_gol.tb_pessoa b
        ON a.id_empresa = b.id_empresa
        AND a.id_pessoa = b.id_pessoa
        INNER JOIN db_gol.tb_empresa c
        ON a.id_empresa = c.id_empresa
        LEFT JOIN db_gol.tb_pessoa_endereco d
        ON d.tipo_endereco = 1 
        AND d.id_empresa = a.id_empresa
        AND d.id_pessoa_endereco = a.id_pessoa
        $var_join_condominio JOIN db_gol.tb_venda e
        ON a.id_empresa = e.id_empresa
        AND a.id_venda = e.id_venda
        LEFT JOIN db_gol.tb_rec_pag_virtual f
        ON a.id_empresa = f.id_empresa
        AND a.id_mov = f.id_venda
        AND a.id_desd = f.id_desd
        AND f.lo_status = 0
        WHERE a.id_empresa = $var_id_empresa_3
        AND a.id_condominio = $id_condominio
        AND a.situacao = 'A'
        AND a.tipo = 'RC'
        AND b.lo_cobranca_automatica = 'S'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18,19,20, e.nr_pedido_talao,lo_envia_whatsapp
        , f.linha_digitavel
        , d.logradouro
        , d.numero
        , d.complemento
        , d.bairro
        , d.cidade
        , d.uf
        , d.cep
        , lo_envia_sms
        ORDER BY a.dt_vencimento ASC
    ";

    $ret = pg_query($conn, $sql_search);
    $dt_interval_venc_d = pg_fetch_all($ret);

    // echo "dt_interval_venc_d: <pre>";
    // print_r($dt_interval_venc_d);
    // echo "</pre>";

    if(empty($dt_interval_venc_d)){
        $arrMsgReturn[$nm_not_3] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Não há recebimentos em aberto para gerar notificações de cobrança.'
        );
        return $arrMsgReturn;
    }
    $var_regua_sms_notificacao = 'N';
    $var_regua_whatsapp_notificacao = 'N';

    #INFORMAÇÕES DOS DADOS DA NOTIFICAÇÃO
    $sql_search = "
        SELECT 
            tx_assunto_".$nm_not_3.",
            tx_texto_".$nm_not_3.",
            tx_texto_sms_".$nm_not_3." ,
            lo_sms_ativo_".$nm_not_3.",
            tx_texto_whatsapp_".$nm_not_3." ,
            lo_whatsapp_ativo_".$nm_not_3.",
            tx_tipo_anexo_email
        FROM db_gol.tb_regua_cobranca 
        WHERE id_empresa = $var_id_empresa_3
        --AND id_condominio = $id_condominio
    ";
    
    $ret = pg_query($conn, $sql_search);
    $ds_dados_notificacao_depois = pg_fetch_all($ret)[0];

    // echo "ds_dados_notificacao_depois: <pre>";
    // print_r($ds_dados_notificacao_depois);
    // echo "</pre>";

    $var_assunto     = $ds_dados_notificacao_depois["tx_assunto_".$nm_not_3];
    $var_texto       = $ds_dados_notificacao_depois["tx_texto_".$nm_not_3];
    $var_texto_sms   = $ds_dados_notificacao_depois["tx_texto_sms_".$nm_not_3];
    $var_regua_sms_notificacao   = $ds_dados_notificacao_depois["lo_sms_ativo_".$nm_not_3] == 'S'? $ds_dados_notificacao_depois["lo_sms_ativo_".$nm_not_3] : 'N';
    $var_texto_whatsapp   = $ds_dados_notificacao_depois["tx_texto_whatsapp_".$nm_not_3];
    $var_regua_whatsapp_notificacao   = $ds_dados_notificacao_depois["lo_whatsapp_ativo_".$nm_not_3] == 'S'? $ds_dados_notificacao_depois["lo_whatsapp_ativo_".$nm_not_3] : 'N';
    $var_tipo_anexo_email = $ds_dados_notificacao_depois['tx_tipo_anexo_email'];

    //PARAMETRO PARA OCULTAR OS CAMPOS Enviar SMS E Enviar WHATSAPP
    $var_permissao_sms         = isset($params[$var_id_empresa_3]['new']['utiliza_notificacao_sms']) ? $params[$var_id_empresa_3]['new']['utiliza_notificacao_sms'] : 'N';
    $var_permissao_whatsapp    = isset($params[$var_id_empresa_3]['new']['utiliza_notificacao_whatsapp']) ? $params[$var_id_empresa_3]['new']['utiliza_notificacao_whatsapp'] : 'N';

    /** CARREGA OS DADOS PARA ENVIAR PARA A DB_CONTRATOS */
    $var_whatsapp_account_sid    = isset($params[$var_id_empresa_3]['new']['whatsapp_account_sid']) ? $params[$var_id_empresa_3]['new']['whatsapp_account_sid'] : ' ';
    $var_whatsapp_auth_token     = isset($params[$var_id_empresa_3]['new']['whatsapp_auth_token']) ? $params[$var_id_empresa_3]['new']['whatsapp_auth_token'] : ' ';
    $var_whatsapp_messaging_service_sid   = isset($params[$var_id_empresa_3]['new']['whatsapp_messaging_service_sid']) ? $params[$var_id_empresa_3]['new']['whatsapp_messaging_service_sid'] : ' ';
    $var_whatsapp_sender_number  = isset($params[$var_id_empresa_3]['new']['whatsapp_sender_number']) ? $params[$var_id_empresa_3]['new']['whatsapp_sender_number'] : ' ';
    $var_envia_sms = 'N'; 
    $var_envia_whatsapp = 'N' ; 

    // echo "Param: ".$var_permissao_sms.'<hr>';         
    // echo "Param: ".$var_permissao_whatsapp.'<hr>';    
    // echo "Param: ".$var_whatsapp_account_sid.'<hr>';  
    // echo "Param: ".$var_whatsapp_auth_token.'<hr>';   
    // echo "Param: ".$var_whatsapp_messaging_service_sid.'<hr>';
    // echo "Param: ".$var_whatsapp_sender_number.'<hr>';


    if($var_permissao_sms == 'N'){
        $var_envia_sms = 'N'; 
    }

    if($var_permissao_whatsapp == 'N'){
        $var_envia_whatsapp = 'N' ;
    }

    $var_valida_qtd_bilhetagem_sms = true;

    if($var_valida_qtd_bilhetagem_sms == false){
        $var_envia_sms = 'N';
    }

    $var_valida_qtd_bilhetagem_whatsapp = true;

    if($var_valida_qtd_bilhetagem_whatsapp == false){
        $var_envia_whatsapp = 'N';
    }

    $var_assunto = pg_escape_string($var_assunto);
    $var_texto   = pg_escape_string($var_texto);
    $var_texto_sms   = pg_escape_string($var_texto_sms);

    if(empty($var_assunto)){
        $arrMsgReturn[$nm_not_3] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo assunto do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;

    }
    if(empty($var_texto)){
        $arrMsgReturn[$nm_not_3] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo mensagem do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;

    }

    #PERCORRE OS DADOS E INSERE NO BANCO
    $size = count($dt_interval_venc_d);

    for($i = 0; $i < $size ; $i++){
        $var_id_empresa           = $dt_interval_venc_d[$i]['id_empresa'];
        $var_id_mov               = $dt_interval_venc_d[$i]['id_mov'];
        $var_id_desd              = $dt_interval_venc_d[$i]['id_desd'];
        $var_tipo                 = $dt_interval_venc_d[$i]['tipo'];
        $var_dt_vencimento        = $dt_interval_venc_d[$i]['dt_vencimento'];
        $var_email_address        = $dt_interval_venc_d[$i]['email_cliente'];
        $var_nm_fantasia_cliente  = $dt_interval_venc_d[$i]['nm_fantasia_cliente'];
        $var_cnpj_empresa         = $dt_interval_venc_d[$i]['cnpj_empresa'];
        $var_razao_social_empresa = $dt_interval_venc_d[$i]['razao_social_empresa'];
        $var_nm_fantasia_empresa  = $dt_interval_venc_d[$i]['nm_fantasia_empresa'];
        $var_email_empresa        = $dt_interval_venc_d[$i]['email_empresa'];
        $var_telefone_empresa     = $dt_interval_venc_d[$i]['telefone_empresa'];
        $var_logo_empresa         = $dt_interval_venc_d[$i]['logo_empresa'];
        $var_id_venda             = $dt_interval_venc_d[$i]['id_venda'];
        $var_valor_fianceiro      = $dt_interval_venc_d[$i]['valor_fianceiro'];
        $var_razao_social         = $dt_interval_venc_d[$i]['razao_social'];
        $var_numero_telefone      = $dt_interval_venc_d[$i]['telefone1'];
        $var_dt_venda             = $dt_interval_venc_d[$i]['dt_venda'];
        $var_dt_vencimento_venda  = $dt_interval_venc_d[$i]['dt_vencimento'];
        $var_contato              = $dt_interval_venc_d[$i]['contato'];
        $var_referencia           = $dt_interval_venc_d[$i]['referencia'];
        $var_codigo_barras        = $dt_interval_venc_d[$i]['codigo_barras'];
        
        if($var_regua_whatsapp_notificacao == 'S'){
            $var_envia_whatsapp   = $dt_interval_venc_d[$i]['lo_envia_whatsapp'] == 'S'? $dt_interval_venc_d[$i]['lo_envia_whatsapp'] : 'N';
        }else{
            $var_envia_whatsapp = 'N';
        }
        
        if($var_regua_sms_notificacao == 'S'){
            $var_envia_sms       = $dt_interval_venc_d[$i]['lo_envia_sms'] == 'S'? $dt_interval_venc_d[$i]['lo_envia_sms'] : 'N';
        }else{
            $var_envia_sms = 'N';
        }
        
        // CONDOMINIO
        $var_id_unidade           = $dt_interval_venc_d[$i]['id_unidade'];
        $var_dt_emissao           = $dt_interval_venc_d[$i]['dt_emissao'];
        $var_logradouro           = $dt_interval_venc_d[$i]['logradouro'];
        $var_numero               = $dt_interval_venc_d[$i]['numero'];
        $var_complemento          = $dt_interval_venc_d[$i]['complemento'];
        $var_bairro               = $dt_interval_venc_d[$i]['bairro'];
        $var_cidade               = $dt_interval_venc_d[$i]['cidade'];
        $var_uf                   = $dt_interval_venc_d[$i]['uf'];
        $var_cep                  = $dt_interval_venc_d[$i]['cep'];
        
        $arr_valor_hashtag = array();
        $arr_valor_hashtag['nm_fantasia_cliente'] = $var_nm_fantasia_cliente;                                       
        $arr_valor_hashtag['cnpj_empresa'] = $var_cnpj_empresa;
        $arr_valor_hashtag['razao_social_empresa'] = $var_razao_social_empresa;
        $arr_valor_hashtag['nm_fantasia_empresa'] = $var_nm_fantasia_empresa;
        $arr_valor_hashtag['email_empresa'] = $var_email_empresa;
        $arr_valor_hashtag['telefone_empresa'] = $var_telefone_empresa;
        $arr_valor_hashtag['logo_empresa'] = $var_logo_empresa;
        $arr_valor_hashtag['id_venda'] = $var_id_venda;
        $arr_valor_hashtag['texto'] = $var_texto;
        $arr_valor_hashtag['assunto'] = $var_assunto;
        $arr_valor_hashtag['valor_fianceiro'] = $var_valor_fianceiro;
        $arr_valor_hashtag['razao_social'] = $var_razao_social;
        $arr_valor_hashtag['data_vencimento'] = $var_dt_vencimento;
        $arr_valor_hashtag['data_venda'] = $var_dt_venda; 
        $arr_valor_hashtag['data_vencimento_venda'] = $var_dt_vencimento_venda;
        $arr_valor_hashtag['contato'] = $var_contato;   
        $arr_valor_hashtag['referencia'] = $var_referencia;
        $arr_valor_hashtag['email_cliente'] = $var_email_address ;
        $arr_valor_hashtag['id_mov'] = $var_id_mov;
        $arr_valor_hashtag['id_desd'] = $var_id_desd;   
        $arr_valor_hashtag['url_empresa'] = 'www.sempretecnologia.com.br';  
        $arr_valor_hashtag['linha_digitavel'] = $var_codigo_barras;
        //CONDOMINIO
        $arr_valor_hashtag['id_unidade']           = $var_id_unidade;
        $arr_valor_hashtag['dt_emissao']           = $var_dt_emissao;
        $arr_valor_hashtag['logradouro']           = $var_logradouro;
        $arr_valor_hashtag['numero']               = $var_numero;
        $arr_valor_hashtag['complemento']          = $var_complemento;
        $arr_valor_hashtag['bairro']               = $var_bairro;
        $arr_valor_hashtag['cidade']               = $var_cidade;
        $arr_valor_hashtag['uf']                   = $var_uf;
        $arr_valor_hashtag['cep']                  = $var_cep;

        $whatsapp_parametro_hashtag = m_altera_hastag_whatsapp($conn, $var_id_empresa, $id_condominio, $arr_valor_hashtag, $nm_not_3);

        if($id_condominio > 0){
            
            if($var_id_unidade  > 0){
                $sql_search = "SELECT 
                                lo_envia_sms,
                                lo_envia_whatsapp,
                                tx_telefone
                            FROM   db_condominio.tb_condominio_unidade
                            WHERE  id_empresa = $var_id_empresa_3
                                AND id = $var_id_unidade";
                
                $ret = pg_query($conn, $sql_search);
                $ds_unidade = pg_fetch_all($ret)[0];
                
                $var_envia_sms = $ds_unidade['lo_envia_sms'] == "S" ? $ds_unidade['lo_envia_sms']: "N"; 
                $var_envia_whatsapp = $ds_unidade['lo_envia_whatsapp'] == "S" ? $ds_unidade['lo_envia_whatsapp']: "N";
                $var_numero_telefone = $ds_unidade['tx_telefone']; 
        
                if(empty($var_numero_telefone) || $var_numero_telefone ==""){
                    $var_envia_sms = 'N'; 
                    $var_envia_whatsapp = 'N' ; 
                }
            
            }else{
                $var_envia_sms = 'N'; 
                $var_envia_whatsapp = 'N' ; 
            }
            
            //metodo criado para trocar os valores das hashtags do condominio
            $var_hastag_alteradas   = m_altera_hastag_condominio($conn, $var_id_empresa, $id_condominio, $arr_valor_hashtag);   
            $var_novo_texto   = $var_hastag_alteradas[0];
            $var_novo_assunto = $var_hastag_alteradas[1];
        
        }else{
            //metodo criado para trocar os valores das hashtags
            $var_novo_texto = m_altera_hastag($var_id_empresa, $arr_valor_hashtag);

            //metodo criado para trocar os valores das hashtags
            $var_novo_assunto = m_adiciona_hastag_assunto($arr_valor_hashtag);
        }
        
        // TROCO APENAS A VARIAL DO TEXT SMS
        $arr_valor_hashtag['texto'] = $var_texto_sms;
        $var_novo_texto_sms = m_altera_hastag($var_id_empresa, $arr_valor_hashtag);

        #verifica o intervalo entre o vencimento e o dia estipulado     
        $sql_search = "SELECT CAST('$var_dt_vencimento'::DATE + interval '$nu_dias_venc_3 day' AS DATE) AS dt_vencimento";
        
        $ret = pg_query($conn, $sql_search);
        $var_dt_envio       = pg_fetch_all($ret)[0]['dt_vencimento']; 

        #COMPARA AS DATAS SE FOR ULTRAPASSADO A DATA ESPULADA A DATA DE VENCIMENTO, NÃO IRÁ INSERIR
        $date_venc           = date_create($var_dt_vencimento);
        $date_now            = date_create(date('Y-m-d'));
        $date_env            = date_create($var_dt_envio);

        // echo "Date venc: ".$date_venc->format('d/m/Y').'<hr>';
        // echo "Date now: ".$date_now->format('d/m/Y').'<hr>';
        // echo "Date env: ".$date_env->format('d/m/Y').'<hr>';

        if($date_env >= $date_now){

            $sql_search = "   
                SELECT
                    * 
                FROM db_contratos.tb_notificacao 
                WHERE id_empresa    = $var_id_empresa 
                AND id_mov          = $var_id_mov
                AND id_desd         = $var_id_desd
                AND nu_notificacao  = $nu_notificacao_3";

            $ret = pg_query($conn_contratos, $sql_search);
            $ds_check_notificacao = pg_fetch_all($ret);

            echo "ds_check_notificacao 3: <pre>";
            print_r($ds_check_notificacao);
            echo "</pre>";

            if(empty($ds_check_notificacao)){

                $sql_insert = "
                    INSERT INTO db_contratos.tb_notificacao(
                        id_empresa,
                        id_mov,
                        id_desd,
                        nu_notificacao,
                        tipo,
                        tx_email_host,
                        tx_email_smtp_secure,
                        tx_email_username,
                        tx_email_password,
                        tx_email_porta,
                        tx_email_from,
                        tx_email_address,
                        tx_email_subject,
                        tx_email_body,
                        tx_login,
                        tx_retorno,
                        lo_enviado,
                        dt_vencimento,
                        dt_inc,
                        dt_envio,
                        tx_texto_sms,
                        lo_envia_sms,
                        nu_numero_telefone,
                        tx_texto_whatsapp,
                        lo_envia_whatsapp,
                        tx_tipo_anexo_email,
                        whatsapp_account_sid,
                        whatsapp_auth_token,
                        whatsapp_messaging_service_sid,
                        whatsapp_sender_number,
                        whatsapp_parametro_hashtag

                    ) VALUES (
                        $var_id_empresa,
                        $var_id_mov,
                        $var_id_desd,
                        $nu_notificacao_3,
                        '$var_tipo',
                        '$email_host_3',
                        'tls',
                        '$email_user_3',
                        '$email_pwd_3',
                        '$email_porta_3',
                        '$email_from_3',
                        '$var_email_address',
                        '$var_novo_assunto',
                        '$var_novo_texto',
                        'SQL',
                        '',
                        'N',
                        '$var_dt_vencimento',
                        now(),
                        '$var_dt_envio',
                        '$var_novo_texto_sms',
                        '$var_envia_sms',
                        '$var_numero_telefone',
                        '$var_texto_whatsapp',
                        '$var_envia_whatsapp',
                        '$var_tipo_anexo_email',
                        '$var_whatsapp_account_sid',
                        '$var_whatsapp_auth_token',
                        '$var_whatsapp_messaging_service_sid',
                        '$var_whatsapp_sender_number',
                        '$whatsapp_parametro_hashtag'
                    )";

                    echo "Nota 3".$sql_insert.'<hr>';

                    if(pg_query($conn_contratos, $sql_insert)){
                            $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");
                            $result_insert++;
                        }else{
                            echo "Erro ao tentar inserir nota $var_id_mov/$var_id_desd<hr>"; 
                        }

                $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");                             
                $result_insert++;
            }else{
                $var_lo_enviado = $ds_check_notificacao[0]['lo_enviado'];
                if($var_lo_enviado == 'N'){
                    $arr = array(
                        'tipo'                      => $var_tipo,
                        'tx_email_host'             => $email_host_3,
                        'tx_email_smtp_secure'      => 'tls',
                        'tx_email_username'         => $email_user_3,
                        'tx_email_password'         => $email_pwd_3,
                        'tx_email_porta'            => $email_porta_3,
                        'tx_email_from'             => $email_from_3,
                        'tx_email_address'          => $var_email_address,
                        'tx_email_subject'          => $var_novo_assunto,
                        'tx_email_body'             => $var_novo_texto,
                        'tx_login'                  => 'SQL',
                        'tx_retorno'                => '',
                        'lo_enviado'                => 'N',
                        'dt_vencimento'             => $var_dt_vencimento,
                        'dt_inc'                    => 'NOW()',
                        'dt_envio'                  => $var_dt_envio,
                        'tx_tipo_anexo_email'       => $var_tipo_anexo_email,
                        'whatsapp_account_sid'      => $var_whatsapp_account_sid,
                        'whatsapp_auth_token'       => $var_whatsapp_auth_token,
                        'whatsapp_messaging_service_sid ' => $var_whatsapp_messaging_service_sid,
                        'whatsapp_sender_number'     => $var_whatsapp_sender_number,
                        'whatsapp_parametro_hashtag' => $whatsapp_parametro_hashtag,
                        'nu_numero_telefone'         => $var_numero_telefone,
                        'tx_texto_whatsapp'          => $var_texto_whatsapp,
                        'lo_envia_whatsapp'          => $var_envia_whatsapp
                    );

                    $tb_valor = "";
                    $tb_coluna = array();

                    foreach ($arr as $key => $value) {
                        $tb_valor       = $value;
                        $tb_coluna[]    = "$key = '$value'";        
                    }

                    $implode_result = implode(', ', $tb_coluna);

                    $sql_update = "
                        UPDATE db_contratos.tb_notificacao 
                        SET $implode_result
                        WHERE id_empresa = $var_id_empresa
                        AND id_mov = $var_id_mov        
                        AND id_desd = $var_id_desd
                        AND nu_notificacao = $nu_notificacao_3
                    ";

                    if(pg_query($conn_contratos, $sql_update)){
                        $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                        $falha++;
                    }else{
                        echo "Erro ao tentar atualizar nota $var_id_mov/$var_id_desd<hr>";
                    }

                    $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                    $falha++;
                }#endif
            }#endelse
            // $conexao_contratos->close();
        }else{
            $msg .= '</br> Não há notificação a ser enviada para este título: '.$var_id_mov.'/'.$var_id_desd;
            echo $msg.'<hr>';
            $falha++;
        }
    }#endfor

    $arrCountNotificacao = array(
        'tituloNotificacao' => $tituloNotificacao,
        'sucesso' => array(
            'count' => "0"
        ),
        'atencao' => array(
            'count' => "0"
        )
    );

    if($result_insert > 0){
        $countInserted = count($arrMsgInsert);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'sucesso' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'mensagem' => "$countInserted novas notificações de cobrança inseridas."
                ),
                'linhas' => $arrMsgInsert
            )
        );
    }

    if($falha > 0){
        $countUpdated = count($arrMsgUpdate);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'atencao' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'subtitulo' => "$countUpdated notificações de cobrança atualizadas."
                ),
                'linhas' => $arrMsgUpdate
            )
        );
    }

    if(isset($arrMsgReturn['sucesso'], $arrMsgReturn['sucesso']['linhas'])){
        $countSucesso = count($arrMsgReturn['sucesso']['linhas']);
        $arrCountNotificacao['notificacao']['sucesso']['count'] = $countSucesso;
    }

    if(isset($arrMsgReturn['atencao'], $arrMsgReturn['atencao']['linhas'])){
        $countAtencao = count($arrMsgReturn['atencao']['linhas']);
        $arrCountNotificacao['notificacao']['atencao']['count'] = $countAtencao;
    }

    $arrMsgReturn['notificacao'] = $arrCountNotificacao;

    echo "ARR MSG RETURN 3: <pre>";
    print_r($arrMsgReturn);
    echo "</pre>";

    return $arrMsgReturn;
}

function m_data_dia_fixo2($nm_not_4, $var_id_empresa_4, $nu_dia_fixo, $nu_notificacao_4, $email_host_4, $email_user_4, $email_pwd_4, $email_porta_4, $email_from_4, $conn, $params, $id_condominio, $conn_contratos){

    echo "<hr>m_data_dia_fixo2<hr>";

    $falha          = 0;
    $result_insert  = 0;
    $redir          = false;
    $msg            = "";
    $arrMsgReturn   = array();
    $arrMsgInsert   = array();
    $arrMsgUpdate   = array();

    $msg_header     = "7ª NOTIFICAÇÃO";
    $tituloNotificacao = '7ª Notificação';

    if($nu_dia_fixo > 30){
        $nu_dia_fixo = 30;
    } 

    $nu_dia_fixo = str_pad($nu_dia_fixo, 2, "0", STR_PAD_LEFT); 

    $var_join_condominio = "INNER";
    if($id_condominio > 0){
        $var_join_condominio = "LEFT";
    }else{
        $var_join_condominio = "INNER";
    }

    if($var_id_empresa_4 == 8858){
        $sql_search = "
            SELECT  
                    a.id_empresa AS id_empresa
                    , a.id_mov AS id_mov
                    , a.id_desd AS id_desd
                    , a.tipo AS tipo
                    , a.dt_vencimento AS dt_vencimento
            /*5*/   , TRIM(b.email) AS email_cliente
                    , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                    , c.cnpj AS cnpj_empresa
                    , TRIM(c.razao_social) AS razao_social_empresa
                    , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
            /*10*/  , c.email AS email_empresa
                    , c.telefone AS telefone_empresa
                    , c.tx_logo_nfe AS logo_empresa
                    , a.id_venda AS id_venda
                    , a.dt_emissao AS dt_emissao    
            /*15*/  , CASE WHEN a.dt_emissao::date  < ((EXTRACT('YEAR' FROM now()::date))
                                                        ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                                        ||'-'|| '01')::Date
                        THEN ((EXTRACT('YEAR' FROM now()::date))
                                ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                ||'-'|| '$nu_dia_fixo')::Date
                        ELSE ((EXTRACT('YEAR' FROM a.dt_emissao))
                                ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                THEN 
                                                    CASE WHEN '$nu_dia_fixo' > '28'
                                                    THEN '28'
                                                    ELSE '$nu_dia_fixo'                                         
                                                END
                                        ELSE '$nu_dia_fixo'
                                        END
                            ))::date 
                        END as dt_envio
                    , SUM(a.vr_rec_original) AS valor_fianceiro
                    , b.razao_social
                    , e.dt_venda
            /*20*/  , f.telefone1
                    , b.contato
                    , e.nr_pedido_talao AS referencia
                    , CASE WHEN  a.nr_boleto > '900' 
                    THEN g.linha_digitavel
                    ELSE  a.tx_linha_digitavel
                    END as codigo_barras
                    , COALESCE(b.lo_envia_whatsapp,'N')::character(1) AS lo_envia_whatsapp
            /*25*/  , a.id_unidade
                    , a.dt_emissao
                    , f.logradouro
                    , f.numero
                    , f.complemento
            /*30*/  , f.bairro
                    , f.cidade
                    , f.uf
                    , f.cep
                    , COALESCE(b.lo_envia_sms,'N')::character(1) AS lo_envia_sms
            FROM db_gol.tb_rec_pag a
            INNER JOIN db_gol.tb_pessoa b
            ON a.id_empresa = b.id_empresa
            AND a.id_pessoa = b.id_pessoa
            INNER JOIN db_gol.tb_empresa c
            ON a.id_empresa = c.id_empresa                              
            INNER JOIN db_gol.tb_formapg d
            ON a.id_empresa = d.id_empresa
            AND a.id_forma = d.id_forma
            INNER JOIN db_gol.tb_venda e
            ON a.id_empresa = e.id_empresa
            AND a.id_venda = e.id_venda
            LEFT JOIN db_gol.tb_pessoa_endereco f
            ON f.tipo_endereco = 1 
            AND f.id_empresa = a.id_empresa
            AND f.id_pessoa_endereco = a.id_pessoa
            LEFT JOIN db_gol.tb_rec_pag_virtual g
            ON a.id_empresa = g.id_empresa
            AND a.id_mov = g.id_venda
            AND a.id_desd = g.id_desd
            AND g.lo_status = 0
            WHERE a.id_empresa = $var_id_empresa_4
            AND a.id_condominio = 0
            AND a.situacao = 'A'
            AND a.tipo = 'RC'
            AND a.dt_emissao >= (NOW()::date - interval '55 days')
            AND a.dt_vencimento >= ((EXTRACT('YEAR' FROM a.dt_emissao))
                                    ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                    ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                            THEN 
                                                CASE WHEN '$nu_dia_fixo' > '28'
                                                THEN '28'
                                                ELSE '$nu_dia_fixo'                                         
                                                END
                                            ELSE '$nu_dia_fixo'
                                            END
                                ))::date
            AND b.lo_cobranca_automatica = 'S'
            AND d.lo_tipo_boleto = 'S'
            /*AND ((EXTRACT('YEAR' FROM a.dt_emissao))
                ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                            THEN 
                                                CASE WHEN '$nu_dia_fixo' > '28'
                                                THEN '28'
                                                ELSE '$nu_dia_fixo'                                         
                                                END
                                            ELSE '$nu_dia_fixo'
                                            END
                ))::date >= NOW()::date*/
            AND a.id_mov IN(SELECT
                                vd.id_venda
                            FROM db_gol.tb_venda vd
                            WHERE vd.id_empresa = a.id_empresa
                            AND vd.tx_origem_venda = 'MS'                                               
                            AND vd.status IN(4, 5, 8)
                            AND vd.dt_venda >= (NOW()::date - interval '55 days'))  
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22, e.nr_pedido_talao,lo_envia_whatsapp,
            g.linha_digitavel
            , f.logradouro
            , f.numero
            , f.complemento
            , f.bairro
            , f.cidade
            , f.uf
            , f.cep
            , lo_envia_sms
            ORDER BY a.dt_vencimento ASC
        ";

        echo "SQL: ".$sql_search.'<hr>';

        $ret = pg_query($conn, $sql_search);
        $ds_dia_fixo = pg_fetch_all($ret);

        echo "ds_dia_fixo: <pre>";
        print_r($ds_dia_fixo);
        echo "</pre>";
    }else{
        if($id_condominio > 0){
            
            $sql_search = "
                        SELECT  
                        a.id_empresa AS id_empresa
                        , a.id_mov AS id_mov
                        , a.id_desd AS id_desd
                        , a.tipo AS tipo
                        , a.dt_vencimento AS dt_vencimento
                /*5*/   , TRIM(b.email) AS email_cliente
                        , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                        , c.cnpj AS cnpj_empresa
                        , TRIM(c.razao_social) AS razao_social_empresa
                        , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
                /*10*/  , c.email AS email_empresa
                        , c.telefone AS telefone_empresa
                        , c.tx_logo_nfe AS logo_empresa
                        , a.id_venda AS id_venda
                        , a.dt_emissao AS dt_emissao    
                /*15*/  , CASE WHEN a.dt_emissao::date  < ((EXTRACT('YEAR' FROM now()::date))
                                                            ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                                            ||'-'|| '01')::Date
                            THEN ((EXTRACT('YEAR' FROM now()::date))
                                    ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                    ||'-'|| '$nu_dia_fixo')::Date
                            ELSE ((EXTRACT('YEAR' FROM a.dt_emissao))
                                    ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                    ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                    THEN 
                                                        CASE WHEN '$nu_dia_fixo' > '28'
                                                        THEN '28'
                                                        ELSE '$nu_dia_fixo'                                         
                                                    END
                                            ELSE '$nu_dia_fixo'
                                            END
                                ))::date 
                            END as dt_envio
                        , SUM(a.vr_rec_original) AS valor_fianceiro
                        , b.razao_social
                        , e.dt_venda
                /*20*/  , f.telefone1
                        , b.contato
                        , e.nr_pedido_talao AS referencia
                        , CASE WHEN  a.nr_boleto > '900' 
                        THEN g.linha_digitavel
                        ELSE  a.tx_linha_digitavel
                        END as codigo_barras
                        , COALESCE(b.lo_envia_whatsapp,'N')::character(1) AS lo_envia_whatsapp
                /*25*/  , a.id_unidade
                        , a.dt_emissao
                        , f.logradouro
                        , f.numero
                        , f.complemento
                /*30*/  , f.bairro
                        , f.cidade
                        , f.uf
                        , f.cep
                        , COALESCE(b.lo_envia_sms,'N')::character(1) AS lo_envia_sms
                FROM db_gol.tb_rec_pag a
                INNER JOIN db_gol.tb_pessoa b
                ON a.id_empresa = b.id_empresa
                AND a.id_pessoa = b.id_pessoa
                INNER JOIN db_gol.tb_empresa c
                ON a.id_empresa = c.id_empresa                              
                INNER JOIN db_gol.tb_formapg d
                ON a.id_empresa = d.id_empresa
                AND a.id_forma = d.id_forma
                LEFT JOIN db_gol.tb_venda e
                ON a.id_empresa = e.id_empresa
                AND a.id_venda = e.id_venda
                LEFT JOIN db_gol.tb_pessoa_endereco f
                ON f.tipo_endereco = 1 
                AND f.id_empresa = a.id_empresa
                AND f.id_pessoa_endereco = a.id_pessoa
                LEFT JOIN db_gol.tb_rec_pag_virtual g
                ON a.id_empresa = g.id_empresa
                AND a.id_mov = g.id_venda
                AND a.id_desd = g.id_desd
                AND g.lo_status = 0
                WHERE a.id_empresa = $var_id_empresa_4
                AND a.id_condominio = 1
                AND a.situacao = 'A'
                AND a.tipo = 'RC'
                AND a.dt_emissao >= (NOW()::date - interval '25 days')
                AND a.dt_vencimento >= ((EXTRACT('YEAR' FROM a.dt_emissao))
                                        ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                        ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                THEN 
                                                    CASE WHEN '$nu_dia_fixo' > '28'
                                                    THEN '28'
                                                    ELSE '$nu_dia_fixo'                                         
                                                    END
                                                ELSE '$nu_dia_fixo'
                                                END
                                    ))::date
                AND b.lo_cobranca_automatica = 'S'
                AND d.lo_tipo_boleto = 'S'
                /*AND ((EXTRACT('YEAR' FROM a.dt_emissao))
                    ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                    ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                THEN 
                                                    CASE WHEN '$nu_dia_fixo' > '28'
                                                    THEN '28'
                                                    ELSE '$nu_dia_fixo'                                         
                                                    END
                                                ELSE '$nu_dia_fixo'
                                                END
                    ))::date >= NOW()::date*/
                /*AND a.id_mov IN(SELECT
                                    vd.id_venda
                                FROM db_gol.tb_venda vd
                                WHERE vd.id_empresa = a.id_empresa
                                AND vd.tx_origem_venda = 'MS'                                               
                                AND vd.status IN(4, 5, 8)
                                AND vd.dt_venda >= (NOW()::date - interval '25 days'))*/    
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22, e.nr_pedido_talao,lo_envia_whatsapp
                ,g.linha_digitavel
                , f.logradouro
                , f.numero
                , f.complemento
                , f.bairro
                , f.cidade
                , f.uf
                , f.cep
                , lo_envia_sms
                ORDER BY a.dt_vencimento ASC
            ";

            $ret = pg_query($conn, $sql_search);
            $ds_dia_fixo = pg_fetch_all($ret);

            echo "ds_dia_fixo: <pre>";
            print_r($ds_dia_fixo);
            echo "</pre>";
        
}else{
        
            $sql_search = "
                SELECT  
                        a.id_empresa AS id_empresa
                        , a.id_mov AS id_mov
                        , a.id_desd AS id_desd
                        , a.tipo AS tipo
                        , a.dt_vencimento AS dt_vencimento
                /*5*/   , TRIM(b.email) AS email_cliente
                        , TRIM(b.nm_fantasia) AS nm_fantasia_cliente
                        , c.cnpj AS cnpj_empresa
                        , TRIM(c.razao_social) AS razao_social_empresa
                        , TRIM(c.nm_fantasia) AS nm_fantasia_empresa
                /*10*/  , c.email AS email_empresa
                        , c.telefone AS telefone_empresa
                        , c.tx_logo_nfe AS logo_empresa
                        , a.id_venda AS id_venda
                        , a.dt_emissao AS dt_emissao    
                /*15*/  , CASE WHEN a.dt_emissao::date  < ((EXTRACT('YEAR' FROM now()::date))
                                                            ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                                            ||'-'|| '01')::Date
                            THEN ((EXTRACT('YEAR' FROM now()::date))
                                    ||'-'||(EXTRACT('MONTH' FROM now()::date))
                                    ||'-'|| '$nu_dia_fixo')::Date
                            ELSE ((EXTRACT('YEAR' FROM a.dt_emissao))
                                    ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                    ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                    THEN 
                                                        CASE WHEN '$nu_dia_fixo' > '28'
                                                        THEN '28'
                                                        ELSE '$nu_dia_fixo'                                         
                                                    END
                                            ELSE '$nu_dia_fixo'
                                            END
                                ))::date 
                            END as dt_envio
                        , SUM(a.vr_rec_original) AS valor_fianceiro
                        , b.razao_social
                        , e.dt_venda
                /*20*/  , f.telefone1
                        , b.contato
                        , e.nr_pedido_talao AS referencia
                        , CASE WHEN  a.nr_boleto > '900' 
                        THEN g.linha_digitavel
                        ELSE  a.tx_linha_digitavel
                        END as codigo_barras
                        , COALESCE(b.lo_envia_whatsapp,'N')::character(1) AS lo_envia_whatsapp
                /*25*/  , a.id_unidade
                        , a.dt_emissao
                        , f.logradouro
                        , f.numero
                        , f.complemento
                /*30*/  , f.bairro
                        , f.cidade
                        , f.uf
                        , f.cep
                        , COALESCE(b.lo_envia_sms,'N')::character(1) AS lo_envia_sms
                FROM db_gol.tb_rec_pag a
                INNER JOIN db_gol.tb_pessoa b
                ON a.id_empresa = b.id_empresa
                AND a.id_pessoa = b.id_pessoa
                INNER JOIN db_gol.tb_empresa c
                ON a.id_empresa = c.id_empresa                              
                INNER JOIN db_gol.tb_formapg d
                ON a.id_empresa = d.id_empresa
                AND a.id_forma = d.id_forma
                INNER JOIN db_gol.tb_venda e
                ON a.id_empresa = e.id_empresa
                AND a.id_venda = e.id_venda
                LEFT JOIN db_gol.tb_pessoa_endereco f
                ON f.tipo_endereco = 1 
                AND f.id_empresa = a.id_empresa
                AND f.id_pessoa_endereco = a.id_pessoa
                LEFT JOIN db_gol.tb_rec_pag_virtual g
                ON a.id_empresa = g.id_empresa
                AND a.id_mov = g.id_venda
                AND a.id_desd = g.id_desd
                AND g.lo_status = 0
                WHERE a.id_empresa = $var_id_empresa_4
                AND a.id_condominio = 0
                AND a.situacao = 'A'
                AND a.tipo = 'RC'
                AND a.dt_emissao >= (NOW()::date - interval '25 days')
                AND a.dt_vencimento >= ((EXTRACT('YEAR' FROM a.dt_emissao))
                                        ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                                        ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                THEN 
                                                    CASE WHEN '$nu_dia_fixo' > '28'
                                                    THEN '28'
                                                    ELSE '$nu_dia_fixo'                                         
                                                    END
                                                ELSE '$nu_dia_fixo'
                                                END
                                    ))::date
                AND b.lo_cobranca_automatica = 'S'
                AND d.lo_tipo_boleto = 'S'
                /*AND ((EXTRACT('YEAR' FROM a.dt_emissao))
                    ||'-'||(EXTRACT('MONTH' FROM a.dt_emissao))
                    ||'-'||(CASE WHEN EXTRACT('MONTH' FROM a.dt_emissao) = '02'
                                                THEN 
                                                    CASE WHEN '$nu_dia_fixo' > '28'
                                                    THEN '28'
                                                    ELSE '$nu_dia_fixo'                                         
                                                    END
                                                ELSE '$nu_dia_fixo'
                                                END
                    ))::date >= NOW()::date*/
                AND a.id_mov IN(SELECT
                                    vd.id_venda
                                FROM db_gol.tb_venda vd
                                WHERE vd.id_empresa = a.id_empresa
                                AND vd.tx_origem_venda = 'MS'                                               
                                AND vd.status IN(4, 5, 8)
                                AND vd.dt_venda >= (NOW()::date - interval '25 days'))  
                GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,18,19,20,21,22, e.nr_pedido_talao,lo_envia_whatsapp
                ,g.linha_digitavel
                , f.logradouro
                , f.numero
                , f.complemento
                , f.bairro
                , f.cidade
                , f.uf
                , f.cep
                , lo_envia_sms
                ORDER BY a.dt_vencimento ASC
            ";


            $ret = pg_query($conn, $sql_search);
            $ds_dia_fixo = pg_fetch_all($ret);

            echo "ds_dia_fixo: <pre>";
            print_r($ds_dia_fixo);
            echo "</pre>";
        }
   }        

    if(empty($ds_dia_fixo)){
        $arrMsgReturn[$nm_not_4] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Não há recebimentos em aberto para gerar notificações de cobrança.'
        );
        return $arrMsgReturn;
    }

    $var_regua_sms_notificacao = 'N';
    $var_regua_whatsapp_notificacao = 'N';

    #INFORMAÇÕES DOS DADOS DA NOTIFICAÇÃO
    $sql_search = "
        SELECT 
            tx_assunto_".$nm_not_4.",
            tx_texto_".$nm_not_4." ,
            tx_texto_sms_".$nm_not_4." ,
            lo_sms_ativo_".$nm_not_4." ,
            tx_texto_whatsapp_".$nm_not_4." ,
            lo_whatsapp_ativo_".$nm_not_4.",
            tx_tipo_anexo_email 
        FROM db_gol.tb_regua_cobranca 
        WHERE id_empresa = $var_id_empresa_4
        --AND id_condominio = 0 
    ";  
    
    $ret = pg_query($conn, $sql_search);
    $ds_dados_notificacao_dia_fixo = pg_fetch_all($ret)[0];

    $var_assunto          = $ds_dados_notificacao_dia_fixo["tx_assunto_".$nm_not_4];
    $var_texto            = $ds_dados_notificacao_dia_fixo["tx_texto_".$nm_not_4];
    $var_texto_sms        = $ds_dados_notificacao_dia_fixo["tx_texto_sms_".$nm_not_4];
    $var_regua_sms_notificacao        = $ds_dados_notificacao_dia_fixo["lo_sms_ativo_".$nm_not_4] == 'S'? $ds_dados_notificacao_dia_fixo["lo_sms_ativo_".$nm_not_4] : 'N';
    $var_texto_whatsapp   = $ds_dados_notificacao_dia_fixo["tx_texto_whatsapp_".$nm_not_4];
    $var_regua_whatsapp_notificacao   = $ds_dados_notificacao_dia_fixo["lo_whatsapp_ativo_".$nm_not_4] == 'S'? $ds_dados_notificacao_dia_fixo["lo_whatsapp_ativo_".$nm_not_4] : 'N';
    $var_tipo_anexo_email = $ds_dados_notificacao_dia_fixo["tx_tipo_anexo_email"];

    //PARAMETRO PARA OCULTAR OS CAMPOS Enviar SMS E Enviar WHATSAPP
    $var_permissao_sms         = isset($params[$var_id_empresa_4]['new']['utiliza_notificacao_sms']) ? $params[$var_id_empresa_4]['new']['utiliza_notificacao_sms'] : 'N';
    $var_permissao_whatsapp    = isset($params[$var_id_empresa_4]['new']['utiliza_notificacao_whatsapp']) ? $params[$var_id_empresa_4]['new']['utiliza_notificacao_whatsapp'] : 'N';

    /** CARREGA OS DADOS PARA ENVIAR PARA A DB_CONTRATOS */
    $var_whatsapp_account_sid    = isset($params[$var_id_empresa_4]['new']['whatsapp_account_sid']) ? $params[$var_id_empresa_4]['new']['whatsapp_account_sid'] : ' ';
    $var_whatsapp_auth_token     = isset($params[$var_id_empresa_4]['new']['whatsapp_auth_token']) ? $params[$var_id_empresa_4]['new']['whatsapp_auth_token'] : ' ';
    $var_whatsapp_messaging_service_sid   = isset($params[$var_id_empresa_4]['new']['whatsapp_messaging_service_sid']) ? $params[$var_id_empresa_4]['new']['whatsapp_messaging_service_sid'] : ' ';
    $var_whatsapp_sender_number  = isset($params[$var_id_empresa_4]['new']['whatsapp_sender_number']) ? $params[$var_id_empresa_4]['new']['whatsapp_sender_number'] : ' ';
    $var_envia_sms = 'N'; 
    $var_envia_whatsapp = 'N' ; 

    // echo "Param: ".$var_permissao_sms.'<hr>';         
    // echo "Param: ".$var_permissao_whatsapp.'<hr>';    
    // echo "Param: ".$var_whatsapp_account_sid.'<hr>';  
    // echo "Param: ".$var_whatsapp_auth_token.'<hr>';   
    // echo "Param: ".$var_whatsapp_messaging_service_sid.'<hr>';
    // echo "Param: ".$var_whatsapp_sender_number.'<hr>';

    if($var_permissao_sms == 'N'){
        $var_envia_sms = 'N'; 
    }

    if($var_permissao_whatsapp == 'N'){
        $var_envia_whatsapp = 'N' ;
    }

    $var_valida_qtd_bilhetagem_sms = true;

    if($var_valida_qtd_bilhetagem_sms == false){
        $var_envia_sms = 'N';
    }

    $var_valida_qtd_bilhetagem_whatsapp = true;

    if($var_valida_qtd_bilhetagem_whatsapp == false){
        $var_envia_whatsapp = 'N';
    }

    $var_assunto    = pg_escape_string($var_assunto);
    $var_texto      = pg_escape_string($var_texto);
    $var_texto_sms  = pg_escape_string($var_texto_sms);

    if(empty($var_assunto)){
        $arrMsgReturn[$nm_not_4] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo assunto do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }
    if(empty($var_texto)){
        $arrMsgReturn[$nm_not_4] = array(
            'modelo' => 2,
            'sucesso' => false,
            'mensagem' => 'Por favor, preencha o campo mensagem do e-mail para continuar. Para isso, clique no botão "Mensagem" e preencha o assunto e o corpo da mensagem.'
        );
        return $arrMsgReturn;
    }

    #PERCORRE OS DADOS E INSERE NO BANCO
    $size = count($ds_dia_fixo);
    for($i = 0; $i < $size ; $i++){

        $var_id_empresa           = $ds_dia_fixo[$i]['id_empresa'];
        $var_id_mov               = $ds_dia_fixo[$i]['id_mov'];
        $var_id_desd              = $ds_dia_fixo[$i]['id_desd'];
        $var_tipo                 = $ds_dia_fixo[$i]['tipo'];
        $var_dt_vencimento        = $ds_dia_fixo[$i]['dt_vencimento'];
        $email_cliente            = $ds_dia_fixo[$i]['email_cliente'];
        $var_nm_fantasia_cliente  = $ds_dia_fixo[$i]['nm_fantasia_cliente'];
        $var_cnpj_empresa         = $ds_dia_fixo[$i]['cnpj_empresa'];
        $var_razao_social_empresa = $ds_dia_fixo[$i]['razao_social_empresa'];
        $var_nm_fantasia_empresa  = $ds_dia_fixo[$i]['nm_fantasia_empresa'];
        $var_email_empresa        = $ds_dia_fixo[$i]['email_empresa'];
        $var_telefone_empresa     = $ds_dia_fixo[$i]['telefone_empresa'];
        $var_logo_empresa         = $ds_dia_fixo[$i]['logo_empresa'];
        $var_id_venda             = $ds_dia_fixo[$i]['id_venda'];
        $var_dt_envio             = $ds_dia_fixo[$i]['dt_envio'];
        $var_valor_fianceiro      = $ds_dia_fixo[$i]['valor_fianceiro'];
        $var_razao_social         = $ds_dia_fixo[$i]['razao_social'];
        $var_dt_venda             = $ds_dia_fixo[$i]['dt_venda'];
        $var_contato              = $ds_dia_fixo[$i]['contato'];
        $var_dt_vencimento_venda  = $ds_dia_fixo[$i]['dt_vencimento'];
        $var_numero_telefone      = $ds_dia_fixo[$i]['telefone1'];
        $var_referencia           = $ds_dia_fixo[$i]['referencia'];
        $var_codigo_barras        = $ds_dia_fixo[$i]['codigo_barras'];
        
        if($var_regua_whatsapp_notificacao == 'S'){
            $var_envia_whatsapp = $ds_dia_fixo[$i]['lo_envia_whatsapp'] == 'S' ? $ds_dia_fixo[$i]['lo_envia_whatsapp'] : 'N';
        }else{
            $var_envia_whatsapp = 'N';
        }
        
        if($var_regua_sms_notificacao == 'S'){
            $var_envia_sms       = $ds_dia_fixo[$i]['lo_envia_sms'] == 'S'? $ds_dia_fixo[$i]['lo_envia_sms'] : 'N';
        }else{
            $var_envia_sms = 'N';
        }
        
        // CONDOMINIO
        $var_id_unidade           = $ds_dia_fixo[$i]['id_unidade'];
        $var_dt_emissao           = $ds_dia_fixo[$i]['dt_emissao'];
        $var_logradouro           = $ds_dia_fixo[$i]['logradouro'];
        $var_numero               = $ds_dia_fixo[$i]['numero'];
        $var_complemento          = $ds_dia_fixo[$i]['complemento'];
        $var_bairro               = $ds_dia_fixo[$i]['bairro'];
        $var_cidade               = $ds_dia_fixo[$i]['cidade'];
        $var_uf                   = $ds_dia_fixo[$i]['uf'];
        $var_cep                  = $ds_dia_fixo[$i]['cep'];

        $arr_valor_hashtag = array();
        $arr_valor_hashtag['nm_fantasia_cliente'] = $var_nm_fantasia_cliente;                                       
        $arr_valor_hashtag['cnpj_empresa'] = $var_cnpj_empresa;
        $arr_valor_hashtag['razao_social_empresa'] = $var_razao_social_empresa;
        $arr_valor_hashtag['nm_fantasia_empresa'] = $var_nm_fantasia_empresa;
        $arr_valor_hashtag['email_empresa'] = $var_email_empresa;
        $arr_valor_hashtag['telefone_empresa'] = $var_telefone_empresa;
        $arr_valor_hashtag['logo_empresa'] = $var_logo_empresa;
        $arr_valor_hashtag['id_venda'] = $var_id_venda;
        $arr_valor_hashtag['texto'] = $var_texto;
        $arr_valor_hashtag['assunto'] = $var_assunto;
        $arr_valor_hashtag['valor_fianceiro'] = $var_valor_fianceiro;
        $arr_valor_hashtag['razao_social'] = $var_razao_social;
        $arr_valor_hashtag['data_vencimento'] = $var_dt_vencimento;
        $arr_valor_hashtag['data_venda'] = $var_dt_venda; 
        $arr_valor_hashtag['data_vencimento_venda'] = $var_dt_vencimento_venda;
        $arr_valor_hashtag['contato'] = $var_contato;
        $arr_valor_hashtag['referencia'] = $var_referencia;
        $arr_valor_hashtag['email_cliente'] = $email_cliente ;
        
        //Douglas
        $arr_valor_hashtag['id_mov'] = $var_id_mov;
        $arr_valor_hashtag['id_desd'] = $var_id_desd;   
        $arr_valor_hashtag['url_empresa'] = 'www.sempretecnologia.com.br';  
        $arr_valor_hashtag['linha_digitavel'] = $var_codigo_barras;
        
        //CONDOMINIO
        $arr_valor_hashtag['id_unidade']           = $var_id_unidade;
        $arr_valor_hashtag['dt_emissao']           = $var_dt_emissao;
        $arr_valor_hashtag['logradouro']           = $var_logradouro;
        $arr_valor_hashtag['numero']               = $var_numero;
        $arr_valor_hashtag['complemento']          = $var_complemento;
        $arr_valor_hashtag['bairro']               = $var_bairro;
        $arr_valor_hashtag['cidade']               = $var_cidade;
        $arr_valor_hashtag['uf']                   = $var_uf;
        $arr_valor_hashtag['cep']                  = $var_cep;

        if($id_condominio > 0){
            
            if($var_id_unidade  > 0){

                $sql_search = "SELECT 
                                lo_envia_sms,
                                lo_envia_whatsapp,
                                tx_telefone
                            FROM   db_condominio.tb_condominio_unidade
                            WHERE  id_empresa = $var_id_empresa_4
                                AND id = $var_id_unidade";

                $ret = pg_query($conn, $sql_search);
                $ds_unidade = pg_fetch_all($ret)[0];

                echo "Lasts unidades: <pre>";
                print_r($ds_unidade);
                echo "</pre>";
                
                $var_envia_sms = $ds_unidade['lo_envia_sms'] == "S" ? $ds_unidade['lo_envia_sms'] : "N"; 
                $var_envia_whatsapp = $ds_unidade['lo_envia_whatsapp'] == "S" ? $ds_unidade['lo_envia_whatsapp'] : "N";
                $var_numero_telefone = $ds_unidade['tx_telefone'];
        
                if(empty($var_numero_telefone) || $var_numero_telefone ==""){
                    $var_envia_sms = 'N'; 
                    $var_envia_whatsapp = 'N' ; 
                }
            
            }else{
                $var_envia_sms = 'N'; 
                $var_envia_whatsapp = 'N' ; 
            }

            $var_hastag_alteradas   = m_altera_hastag_condominio($conn, $var_id_empresa, $id_condominio, $arr_valor_hashtag);   
            
            $var_novo_texto   = $var_hastag_alteradas[0];
            $var_novo_assunto = $var_hastag_alteradas[1];
        
        }else{
            //metodo criado para trocar os valores das hashtags
            $var_novo_texto = m_altera_hastag($var_id_empresa, $arr_valor_hashtag);

            //metodo criado para trocar os valores das hashtags
            $var_novo_assunto = m_adiciona_hastag_assunto($arr_valor_hashtag);
        }
        
        $whatsapp_parametro_hashtag = m_altera_hastag_whatsapp($conn, $var_id_empresa, $id_condominio, $arr_valor_hashtag, $nm_not_4);
        // TROCO APENAS A VARIAL DO TEXT SMS
        $arr_valor_hashtag['texto'] = $var_texto_sms;
        $var_novo_texto_sms = m_altera_hastag($var_id_empresa, $arr_valor_hashtag);

        $sql_search = "   
            SELECT 
                * 
            FROM db_contratos.tb_notificacao 
            WHERE id_empresa    = $var_id_empresa 
            AND id_mov          = $var_id_mov
            AND id_desd         = $var_id_desd
            AND nu_notificacao  = $nu_notificacao_4"
        ;

        $ret = pg_query($conn_contratos, $sql_search);
        $ds_check_notificacao = pg_fetch_all($ret);

        if(empty($ds_check_notificacao)){
            $sql_insert = "
                INSERT INTO db_contratos.tb_notificacao(
                    id_empresa,
                    id_mov,
                    id_desd,
                    nu_notificacao,
                    tipo,
                    tx_email_host,
                    tx_email_smtp_secure,
                    tx_email_username,
                    tx_email_password,
                    tx_email_porta,
                    tx_email_from,
                    tx_email_address,
                    tx_email_subject,
                    tx_email_body,
                    tx_login,
                    tx_retorno,
                    lo_enviado,
                    dt_vencimento,
                    dt_inc,
                    dt_envio,
                    tx_texto_sms,
                    lo_envia_sms,
                    nu_numero_telefone,
                    tx_texto_whatsapp,
                    lo_envia_whatsapp,
                    tx_tipo_anexo_email,
                    whatsapp_account_sid,
                    whatsapp_auth_token,
                    whatsapp_messaging_service_sid,
                    whatsapp_sender_number,
                    whatsapp_parametro_hashtag
                        
                ) VALUES (
                    $var_id_empresa,
                    $var_id_mov,
                    $var_id_desd,
                    $nu_notificacao_4,
                    '$var_tipo',
                    '$email_host_4',
                    'tls',
                    '$email_user_4',
                    '$email_pwd_4',
                    '$email_porta_4',
                    '$email_from_4',
                    '$email_cliente',
                    '$var_novo_assunto',
                    '$var_novo_texto',
                    'SQL',
                    '',
                    'N',
                    '$var_dt_vencimento',
                    now(),
                    '$var_dt_envio',
                    '$var_novo_texto_sms',
                    '$var_envia_sms',
                    '$var_numero_telefone',
                    '$var_texto_whatsapp',
                    '$var_envia_whatsapp',
                    '$var_tipo_anexo_email',
                    '$var_whatsapp_account_sid',
                    '$var_whatsapp_auth_token',
                    '$var_whatsapp_messaging_service_sid',
                    '$var_whatsapp_sender_number',
                    '$whatsapp_parametro_hashtag'
                    )";

           if(pg_query($conn_contratos, $sql_insert)){
                $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");
                $result_insert++;
            }else{
                echo "Erro ao tentar inserir nota $var_id_mov/$var_id_desd<hr>"; 
            }

            $arrMsgInsert[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota inserida: $var_id_mov/$var_id_desd");
            $result_insert++;
        }else{
            $var_lo_enviado = $ds_check_notificacao[0]['lo_enviado'];
            if($var_lo_enviado == 'N'){
                $arr = array(
                    'tipo'                      => $var_tipo,
                    'tx_email_host'             => $email_host_4,
                    'tx_email_smtp_secure'      => 'tls',
                    'tx_email_username'         => $email_user_4,
                    'tx_email_password'         => $email_pwd_4,
                    'tx_email_porta'            => $email_porta_4,
                    'tx_email_from'             => $email_from_4,
                    'tx_email_address'          => $email_cliente,
                    'tx_email_subject'          => $var_novo_assunto,
                    'tx_email_body'             => $var_novo_texto,
                    'tx_login'                  => 'SQL',
                    'tx_retorno'                => '',
                    'lo_enviado'                => 'N',
                    'dt_vencimento'             => $var_dt_vencimento,
                    'dt_inc'                    => 'NOW()',
                    'dt_envio'                  => $var_dt_envio,
                    'tx_tipo_anexo_email'       => $var_tipo_anexo_email,
                    'whatsapp_account_sid'      => $var_whatsapp_account_sid,
                    'whatsapp_auth_token'       => $var_whatsapp_auth_token,
                    'whatsapp_messaging_service_sid ' => $var_whatsapp_messaging_service_sid,
                    'whatsapp_sender_number'     => $var_whatsapp_sender_number,
                    'whatsapp_parametro_hashtag' => $whatsapp_parametro_hashtag,
                    'nu_numero_telefone'         => $var_numero_telefone,
                    'tx_texto_whatsapp'          => $var_texto_whatsapp,
                    'lo_envia_whatsapp'          => $var_envia_whatsapp
                );

                $tb_valor = "";
                $tb_coluna = array();

                foreach ($arr as $key => $value) {
                    $tb_valor       = $value;
                    $tb_coluna[]    = "$key = '$value'";        
                }

                $implode_result = implode(', ', $tb_coluna);

                $sql_update = "
                    UPDATE db_contratos.tb_notificacao 
                        SET $implode_result
                    WHERE id_empresa = $var_id_empresa
                    AND id_mov = $var_id_mov        
                    AND id_desd = $var_id_desd
                    AND nu_notificacao = $nu_notificacao_4
                ";

                if(pg_query($conn_contratos, $sql_update)){
                    $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                    $falha++;
                }else{
                    echo "Erro ao tentar atualizar nota $var_id_mov/$var_id_desd<hr>";
                }

                $arrMsgUpdate[] = array('codigo' => "$var_id_mov/$var_id_desd", 'mensagem' => "Nota atualizada: $var_id_mov/$var_id_desd");
                $falha++;
            }#endif
            //$conexao_contratos->close();
        }#endelse
    }#endfor

    $arrCountNotificacao = array(
        'tituloNotificacao' => $tituloNotificacao,
        'sucesso' => array(
            'count' => "0"
        ),
        'atencao' => array(
            'count' => "0"
        )
    );

    if($result_insert > 0){
        $countInserted = count($arrMsgInsert);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'sucesso' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'mensagem' => "$countInserted novas notificações de cobrança inseridas."
                ),
                'linhas' => $arrMsgInsert
            )
        );
    }

    if($falha > 0){
        $countUpdated = count($arrMsgUpdate);
        $arrMsgReturn = array(
            'notificacao' => array(),
            'atencao' => array(
                'cabecalho' => array(
                    'titulo' => $msg_header,
                    'subtitulo' => "$countUpdated notificações de cobrança atualizadas."
                ),
                'linhas' => $arrMsgUpdate
            )
        );
    }

    if(isset($arrMsgReturn['sucesso'], $arrMsgReturn['sucesso']['linhas'])){
        $countSucesso = count($arrMsgReturn['sucesso']['linhas']);
        $arrCountNotificacao['notificacao']['sucesso']['count'] = $countSucesso;
    }

    if(isset($arrMsgReturn['atencao'], $arrMsgReturn['atencao']['linhas'])){
        $countAtencao = count($arrMsgReturn['atencao']['linhas']);
        $arrCountNotificacao['notificacao']['atencao']['count'] = $countAtencao;
    }

    $arrMsgReturn['notificacao'] = $arrCountNotificacao;

    echo "ARR MSG RETURN 4: <pre>";
    print_r($arrMsgReturn);
    echo "</pre>";

    return $arrMsgReturn;
}

function m_altera_hastag_condominio($conn, $id_empresa, $id_condominio, $arr_valor_hashtag){
    /*DEFINE A CONSTANTE CAMINHO_INSTALACAO COM O ENDEREÇO DE DESENVOLVIMENTO OU PRODUÇÃO*/


    $antes            = array();
    $depois           = array();
    $var_logo         = '';
    $texto            = '';
    $var_tx_descricao = '';
    $var_vr_total     = 0;
    $html             = '';
    $var_tx_nome      = '';

    $var_nm_fantasia_cliente  = $arr_valor_hashtag['nm_fantasia_cliente'];
    $var_cnpj_empresa         = $arr_valor_hashtag['cnpj_empresa'];
    $var_razao_social_empresa = $arr_valor_hashtag['razao_social_empresa'];
    $var_nm_fantasia_empresa  = $arr_valor_hashtag['nm_fantasia_empresa'];
    $var_email_empresa        = $arr_valor_hashtag['email_empresa'];
    $var_telefone_empresa     = $arr_valor_hashtag['telefone_empresa'];
    $var_logo_empresa         = $arr_valor_hashtag['logo_empresa'];
    $var_id_venda             = $arr_valor_hashtag['id_venda'];
    $var_texto                = $arr_valor_hashtag['texto'];
    $var_assunto              = $arr_valor_hashtag['assunto'];
    $var_valor_fianceiro      = number_format($arr_valor_hashtag['valor_fianceiro'],2,',','.');
    $var_razao_social         = $arr_valor_hashtag['razao_social'];
    $var_dt_vencimento        = $arr_valor_hashtag['data_vencimento'];
    $varIdMov                 = $arr_valor_hashtag['id_mov'];
    $varIdDesd                = $arr_valor_hashtag['id_desd'];
    $varDescUnidade           = $arr_valor_hashtag['id_unidade'];
    $varLinhaDigitavel        = $arr_valor_hashtag['linha_digitavel'];
    $varEmailCliente          = $arr_valor_hashtag['email_cliente'];
    $varDataEmissao           = $arr_valor_hashtag['dt_emissao'];
    $varLogradouro            = $arr_valor_hashtag['logradouro'];
    $varNumero                = $arr_valor_hashtag['numero'];
    $varComplemento           = $arr_valor_hashtag['complemento'];
    $varBairro                = $arr_valor_hashtag['bairro'];
    $varCidade                = $arr_valor_hashtag['cidade'];
    $varUf                    = $arr_valor_hashtag['uf'];
    $varCep                   = $arr_valor_hashtag['cep'];


    if(strlen($varCep) == 8){
        $cepMasked = fc_mask($varCep, "#####-###");
    }else{
        if($varCep == ''){
            $cepMasked = '-';
        }else{
            $cepMasked = $varCep;
        }
    }

    $enderecoCompleto = '';
            
    if($varLogradouro != ''){
        $enderecoCompleto .= "$varLogradouro, ";
    }

    if($varNumero != ''){
        $enderecoCompleto .= "$varNumero - ";
    }

    if($varComplemento != ''){
        $enderecoCompleto .= "$varComplemento - ";
    }

    if($varBairro != ''){
        $enderecoCompleto .= "$varBairro, ";
    }

    if($varCidade != ''){
        $enderecoCompleto .= "$varCidade";
    }

    if($varUf != ''){
        $enderecoCompleto .= " - $varUf";
    }

    if($cepMasked != '' && $cepMasked != '-'){
        $enderecoCompleto .= ", $cepMasked";
    }   

    $var_id_empresa_aux = str_pad($id_empresa, 6, '0', STR_PAD_LEFT);

    $var_caminho_logo = CAMINHO_INSTALACAO."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if( isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] == 'on' ) {
        $var_endereco = "https://";
    }else{
        $var_endereco = "http://";
    }

    $var_endereco_logo = $var_endereco.ENDERECO_SISTEMA."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if ((TRIM($var_logo_empresa) != '') && (is_file($var_caminho_logo))){
        $var_logo = "<img src=$var_endereco_logo width=300px height=120px/>";
    }

    $sql_search = "SELECT tx_descricao,
                        vr_total
                        FROM db_gol.tb_rec_pag_itens_cobranca
                        WHERE  id_condominio = $id_condominio
                        AND id_empresa = $id_empresa
                        AND id_mov = $varIdMov
                        AND id_desd = $varIdDesd";

    $ret = pg_query($conn, $sql_search);
    $ds_rec_pag_itens_cobranca = pg_fetch_all($ret);

    // echo "ds_rec_pag_itens_cobranca: <pre>";
    // print_r($ds_rec_pag_itens_cobranca);
    // echo "</pre>";
        
    $sql_search = "SELECT tx_nome 
                        FROM db_condominio.tb_condominio_unidade 
                        WHERE id_empresa = $id_empresa
                        AND id = $varDescUnidade";

    $ret = pg_query($conn, $sql_search);
    $ds_desc_unidade = pg_fetch_all($ret);

    // echo "ds_desc_unidade: <pre>";
    // print_r($ds_desc_unidade);
    // echo "</pre>";

    if(!empty($ds_rec_pag_itens_cobranca)){
        foreach($ds_rec_pag_itens_cobranca as $itensCobranca){
            $var_tx_descricao = $itensCobranca['tx_descricao'];
            $var_vr_total     = $itensCobranca['vr_total'];
            $var_vr_total_aux = str_replace(".", ",","$var_vr_total");
        
            $html .="<ul>
                         <li>$var_tx_descricao (R$ $var_vr_total_aux)</li>
                     </ul>";        
        }
    }

    if(!empty($ds_desc_unidade)){

        $var_tx_nome = $ds_desc_unidade[0]['tx_nome'];
        
    }


    $varDataArrecadacao = m_formata_dt_arrecadacao($varDataEmissao);

    /*$var_logo = "<img src=$var_caminho_logo width=300px height=120px/>";*/

    $antes =  [ "#CNPJ_EMPRESA"
              , "#RAZAO_SOCIAL_EMPRESA"
              , "#NOME_FANTASIA_EMPRESA"
              , "#EMAIL_EMPRESA"
              , "#TELEFONE_EMPRESA"
              , "#LOGO_EMPRESA"
              , "#NOME_FANTASIA_CLIENTE"
              , "#NUMERO_VENDA"
              , "#FINANCEIRO_VALOR"
              , "#RAZAO_SOCIAL_CLIENTE"
              , "#DATA_VENCIMENTO"
              , "#REFERENCIA_VENCIMENTO"
              , "#COMPOSICAO_ARRECADACAO"
              , "#UNIDADE"
              , "#LINHA_DIGITAVEL"
              , "#EMAIL_CLIENTE"
              , "#DATA_EMISSAO"
              , "#ENDERECO_COMPLETO_CLIENTE"
              , "#LOGRADOURO"
              , "#NUMERO"
              , "#COMPLEMENTO"
              , "#BAIRRO"
              , "#CIDADE"
              , "#UF"
              , "#CEP"
              , "#MES_REF_ARRECADACAO"];

    $depois = [$var_cnpj_empresa
              , $var_razao_social_empresa
              , $var_nm_fantasia_empresa
              , $var_email_empresa
              , m_formatar_telefone($var_telefone_empresa)
              , $var_logo 
              , $var_nm_fantasia_cliente
              , $var_id_venda
              , $var_valor_fianceiro
              , $var_razao_social
              , date('d/m/Y', strtotime($var_dt_vencimento))
              , date('m/Y', strtotime($var_dt_vencimento))
              , $html
              , $var_tx_nome
              , $varLinhaDigitavel
              , $varEmailCliente
              , date('d/m/Y', strtotime($varDataEmissao))
              , $enderecoCompleto
              , $varLogradouro
              , $varNumero
              , $varComplemento
              , $varBairro
              , $varCidade
              , $varUf
              , $varCep
              , $varDataArrecadacao];

                    


    $var_novo_texto   = str_replace($antes, $depois, $var_texto);
    $var_novo_assunto = str_replace($antes, $depois, $var_assunto);

    return array($var_novo_texto, $var_novo_assunto);

}

function m_altera_hastag($id_empresa, $arr_valor_hashtag){
    /*DEFINE A CONSTANTE CAMINHO_INSTALACAO COM O ENDEREÇO DE DESENVOLVIMENTO OU PRODUÇÃO*/


    $antes = array();
    $depois = array();
    $var_logo = '';

    $var_nm_fantasia_cliente = $arr_valor_hashtag['nm_fantasia_cliente'];
    $var_cnpj_empresa = $arr_valor_hashtag['cnpj_empresa'];
    $var_razao_social_empresa = $arr_valor_hashtag['razao_social_empresa'];
    $var_nm_fantasia_empresa = $arr_valor_hashtag['nm_fantasia_empresa'];
    $var_email_empresa = $arr_valor_hashtag['email_empresa'];
    $var_telefone_empresa = $arr_valor_hashtag['telefone_empresa'];
    $var_logo_empresa = $arr_valor_hashtag['logo_empresa'];
    $var_id_venda = $arr_valor_hashtag['id_venda'];
    $var_texto = $arr_valor_hashtag['texto'];
    $var_valor_fianceiro = number_format($arr_valor_hashtag['valor_fianceiro'],2,',','.');
    $var_razao_social = $arr_valor_hashtag['razao_social'];
    $var_dt_vencimento = $arr_valor_hashtag['data_vencimento'];
    $var_dt_venda = $arr_valor_hashtag['data_venda'];
    $var_dt_vencimento_venda = $arr_valor_hashtag['data_vencimento_venda'];
    $var_contato = $arr_valor_hashtag['contato'];
    $var_referencia = $arr_valor_hashtag['referencia'];

    $var_id_empresa_aux = str_pad($id_empresa, 6, '0', STR_PAD_LEFT);

    $var_caminho_logo = CAMINHO_INSTALACAO."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if( isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] == 'on' ) {
        $var_endereco = "https://";
    }else{
        $var_endereco = "http://";
    }

    $var_endereco_logo = $var_endereco.ENDERECO_SISTEMA."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if ((TRIM($var_logo_empresa) != '') && (is_file($var_caminho_logo))){
        $var_logo = "<img src=$var_endereco_logo width=300px height=120px/>";
    }

    /*$var_logo = "<img src=$var_caminho_logo width=300px height=120px/>";*/

    $antes =  [ "#CNPJ_EMPRESA"
              , "#RAZAO_SOCIAL_EMPRESA"
              , "#NOME_FANTASIA_EMPRESA"
              , "#EMAIL_EMPRESA"
              , "#TELEFONE_EMPRESA"
              , "#LOGO_EMPRESA"
              , "#NOME_FANTASIA_CLIENTE"
              , "#NUMERO_VENDA"
              , "#FINANCEIRO_VALOR"
              , "#RAZAO_SOCIAL_CLIENTE"
              , "#DATA_VENCIMENTO_COMPLETA"
              , "#DATA_VENCIMENTO_VENDA"
              , "#DATA_VENCIMENTO"
              , "#DATA_VENDA"
              , "#CONTATO_CLIENTE"
              , "#REFERENCIA"];

    $depois = [$var_cnpj_empresa
              , $var_razao_social_empresa
              , $var_nm_fantasia_empresa
              , $var_email_empresa
              , m_formatar_telefone($var_telefone_empresa)
              , $var_logo 
              , $var_nm_fantasia_cliente
              , $var_id_venda
              , $var_valor_fianceiro
              , $var_razao_social
              , date('d/m/Y', strtotime($var_dt_vencimento))
              , date('m/Y', strtotime($var_dt_vencimento_venda))
              , date('d/m/Y', strtotime($var_dt_vencimento))
              , date('m/Y', strtotime($var_dt_venda))
              , $var_contato
              , $var_referencia];
              
              

    $var_novo_texto = str_replace($antes, $depois, $var_texto);


    return $var_novo_texto;

}

function m_adiciona_hastag_assunto($arr_valor_hashtag){
    /*DEFINE A CONSTANTE CAMINHO_INSTALACAO COM O ENDEREÇO DE DESENVOLVIMENTO OU PRODUÇÃO*/


    $antes = array();
    $depois = array();
    $var_logo = '';

    $var_nm_fantasia_cliente = $arr_valor_hashtag['nm_fantasia_cliente'];
    $var_cnpj_empresa = $arr_valor_hashtag['cnpj_empresa'];
    $var_razao_social_empresa = $arr_valor_hashtag['razao_social_empresa'];
    $var_nm_fantasia_empresa = $arr_valor_hashtag['nm_fantasia_empresa'];
    $var_email_empresa = $arr_valor_hashtag['email_empresa'];
    $var_telefone_empresa = $arr_valor_hashtag['telefone_empresa'];
    $var_logo_empresa = $arr_valor_hashtag['logo_empresa'];
    $var_id_venda = $arr_valor_hashtag['id_venda'];
    $var_assunto = $arr_valor_hashtag['assunto'];
    $var_valor_fianceiro = number_format($arr_valor_hashtag['valor_fianceiro'],2,',','.');
    $var_razao_social = $arr_valor_hashtag['razao_social'];
    $var_dt_vencimento = $arr_valor_hashtag['data_vencimento'];
    $var_dt_venda = $arr_valor_hashtag['data_venda'];
    $var_dt_vencimento_venda = $arr_valor_hashtag['data_vencimento_venda'];
    $var_contato = $arr_valor_hashtag['contato'];
    $var_referencia = $arr_valor_hashtag['referencia'];

    $var_id_empresa_aux = str_pad($id_empresa, 6, '0', STR_PAD_LEFT);

    $var_caminho_logo = CAMINHO_INSTALACAO."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if( isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] == 'on' ) {
        $var_endereco = "https://";
    }else{
        $var_endereco = "http://";
    }

    $var_endereco_logo = $var_endereco.ENDERECO_SISTEMA."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if ((TRIM($var_logo_empresa) != '') && (is_file($var_caminho_logo))){
        $var_logo = "<img src=$var_endereco_logo width=300px height=120px/>";
    }

    /*$var_logo = "<img src=$var_caminho_logo width=300px height=120px/>";*/

    $antes =  [ "#CNPJ_EMPRESA"
            , "#RAZAO_SOCIAL_EMPRESA"
            , "#NOME_FANTASIA_EMPRESA"
            , "#EMAIL_EMPRESA"
            , "#TELEFONE_EMPRESA"
            , "#LOGO_EMPRESA"
            , "#NOME_FANTASIA_CLIENTE"
            , "#NUMERO_VENDA"
            , "#FINANCEIRO_VALOR"
            , "#RAZAO_SOCIAL_CLIENTE"
            , "#DATA_VENCIMENTO_COMPLETA"
            , "#DATA_VENCIMENTO"
            , "#DATA_VENDA"
            , "#DATA_VENCIMENTO_VENDA"
            , "#CONTATO_CLIENTE"
            , "#REFERENCIA"];

    $depois = [$var_cnpj_empresa
            , $var_razao_social_empresa
            , $var_nm_fantasia_empresa
            , $var_email_empresa
            , m_formatar_telefone($var_telefone_empresa)
            , $var_logo 
            , $var_nm_fantasia_cliente
            , $var_id_venda
            , $var_valor_fianceiro
            , $var_razao_social
            , date('d/m/Y', strtotime($var_dt_vencimento))
            , date('d/m/Y', strtotime($var_dt_vencimento))
            , date('m/Y', strtotime($var_dt_venda))
            , date('m/Y', strtotime($var_dt_vencimento_venda))
            , $var_contato
            , $var_referencia];
            
            

    $var_novo_assunto = str_replace($antes, $depois, $var_assunto);

    return $var_novo_assunto;

}

function m_altera_hastag_whatsapp($conn, $id_empresa, $id_condominio, $arr_valor_hashtag, $nm_not){
    /*DEFINE A CONSTANTE CAMINHO_INSTALACAO COM O ENDEREÇO DE DESENVOLVIMENTO OU PRODUÇÃO*/


    $antes = array();
    $depois = array();
    $var_logo = '';
    $var_nm_fantasia_cliente = $arr_valor_hashtag['nm_fantasia_cliente'];
    $var_cnpj_empresa = $arr_valor_hashtag['cnpj_empresa'];
    $var_razao_social_empresa = $arr_valor_hashtag['razao_social_empresa'];
    $var_nm_fantasia_empresa = $arr_valor_hashtag['nm_fantasia_empresa'];
    $var_email_empresa = $arr_valor_hashtag['email_empresa'];
    $var_telefone_empresa = $arr_valor_hashtag['telefone_empresa'];
    $var_logo_empresa = $arr_valor_hashtag['logo_empresa'];
    $var_id_venda = $arr_valor_hashtag['id_venda'];
    $var_texto = $arr_valor_hashtag['texto'];
    $var_valor_fianceiro = number_format($arr_valor_hashtag['valor_fianceiro'],2,',','.');
    $var_razao_social = $arr_valor_hashtag['razao_social'];
    $var_dt_vencimento = $arr_valor_hashtag['data_vencimento'];
    $var_dt_venda = $arr_valor_hashtag['data_venda'];
    $var_dt_vencimento_venda = $arr_valor_hashtag['data_vencimento_venda'];
    $var_contato = $arr_valor_hashtag['contato'];
    $var_referencia = $arr_valor_hashtag['referencia'];
    $var_id_mov = $arr_valor_hashtag['id_mov'];
    $var_id_desd = $arr_valor_hashtag['id_desd'];
    $var_codigo_barras = $arr_valor_hashtag['linha_digitavel'];
    $var_url_empresa = $arr_valor_hashtag['url_empresa'];

    $var_id_empresa_aux = str_pad($id_empresa, 6, '0', STR_PAD_LEFT);

    $var_caminho_logo = CAMINHO_INSTALACAO."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if( isset($_SERVER['HTTPS']) && $_SERVER['HTTPS'] == 'on' ) {
        $var_endereco = "https://";
        $var_url_sistema =  $var_endereco.$_SERVER['HTTP_HOST'];
    }else{
        $var_endereco = "http://";
        $var_url_sistema =  $var_endereco.$_SERVER['HTTP_HOST'];
    }

    $var_endereco_logo = $var_endereco.ENDERECO_SISTEMA."/_lib/file/img/".$var_id_empresa_aux."/".$var_logo_empresa;

    if ((TRIM($var_logo_empresa) != '') && (is_file($var_caminho_logo))){
        $var_logo = "<img src=$var_endereco_logo width=300px height=120px/>";
    }

    $dados_parametro = base64_encode(json_encode([
        "url" => $var_url_sistema,
        "id_empresa" => $id_empresa,
        "id_mov" => $var_id_mov,
        "id_desd" => $var_id_desd,  
        "id_condominio" => $id_condominio,

    ]));

    $sql_search = "SELECT 
                        COALESCE(nu_dias_".$nm_not.",0) as dias
                        , tx_texto_whatsapp_".$nm_not." as tx_texto_whatsapp
                    FROM db_gol.tb_regua_cobranca 
                    WHERE id_empresa = $id_empresa";

    $ret = pg_query($conn, $sql_search);
    $ds_dias = pg_fetch_all($ret)[0];

    // echo "<pre> dias: ";
    // print_r($ds_dias);
    // echo "</pre>"

    $var_dias = $ds_dias['dias'];
    $var_id_template_whatsapp = $ds_dias['tx_texto_whatsapp'];

    $array_hastag = [
                    "cnpj_empresa"=> $var_cnpj_empresa,
                    "razao_social_empresa"=> $var_razao_social_empresa,
                    "nome_fantasia_empresa"=> $var_nm_fantasia_empresa,
                    "email_empresa"=> $var_email_empresa,
                    "telefone_empresa" => m_formatar_telefone($var_telefone_empresa),
                    "logo_empresa"=> $var_logo,
                    "nome_fantasia_cliente"=> $var_nm_fantasia_cliente,
                    "numero_venda"=> $var_id_venda,
                    "financeiro_valor"=> $var_valor_fianceiro,
                    "razao_social_cliente"=> $var_razao_social,
                    "data_vencimento_completa"=> date('d/m/Y', strtotime($var_dt_vencimento)),
                    "data_vencimento_venda"=> date('m/Y', strtotime($var_dt_vencimento_venda)),
                    "data_vencimento"=> date('m/Y', strtotime($var_dt_vencimento)),
                    "data_venda"=> date('m/Y', strtotime($var_dt_venda)),
                    "contato_cliente"=>  $var_contato,                          
                    "dias"=>  "".$ds_dias['dias']."",
                    "referencia"    =>   $var_referencia,                                               
                    "linha_digitavel"=>"$var_codigo_barras",
                ];

    $sql_search = "SELECT 
                    tx_descricao 
                FROM db_gol.tb_template_whatsapp 
                WHERE id_empresa = $id_empresa 
                AND id_template_whatsapp = '$var_id_template_whatsapp'";

    $ret = pg_query($conn, $sql_search);
    $ds_template = pg_fetch_all($ret);

    $var_texto_whatsapp  = "";
    if($ds_template && $ds_template[0] > 0){
        $var_texto_whatsapp = $ds_template[0]['tx_descricao'];
    }

    $dados_extraidos = m_extrair_dados_hashtag_whatsapp($var_texto_whatsapp);
    $array_hash_tratado = m_filtrar_array_por_chaves_whatsapp($dados_extraidos, $array_hastag);

    $array_hash_tratado['parametro_boleto'] = $dados_parametro;

    $var_ret_json = json_encode($array_hash_tratado);

    return $var_ret_json;

}

function m_formata_dt_arrecadacao($varDataEmissao){

    $numero_mes = date("n", strtotime($varDataEmissao));

    $mes_formatado = "";

    switch ($numero_mes) {
        case 1:
            $mes_formatado = 'JANEIRO';
            break;
        case 2:
            $mes_formatado = 'FEVEREIRO';
            break;
        case 3:
            $mes_formatado = 'MARÇO';
            break;
        case 4:
            $mes_formatado = 'ABRIL';
            break;
        case 5:
            $mes_formatado = 'MAIO';
            break;
        case 6:
            $mes_formatado = 'JUNHO';
            break;
        case 7:
            $mes_formatado = 'JULHO';
            break;
        case 8:
            $mes_formatado = 'AGOSTO';
            break;
        case 9:
            $mes_formatado = 'SETEMBRO';
            break;
        case 10:
            $mes_formatado = 'OUTUBRO';
            break;
        case 11:
            $mes_formatado = 'NOVEMBRO';
            break;
        case 12:
            $mes_formatado = 'DEZEMBRO';
            break;
        default:
            break;
    }


    $varDataArrecadacao = strtoupper($mes_formatado) . '/' . date("Y", strtotime($varDataEmissao));
    return $varDataArrecadacao;

}

function m_formatar_telefone($telefone){

    $tam = strlen(preg_replace("/[^0-9]/", "", $telefone));
    if ($tam == 13) { // COM CÓDIGO DE ÁREA NACIONAL E DO PAIS e 9 dígitos
        return "+".substr($telefone,0,$tam-11)."(".substr($telefone,$tam-11,2).")".substr($telefone,$tam-9,5)."-".substr($telefone,-4);
    }
        
    if ($tam == 12) { // COM CÓDIGO DE ÁREA NACIONAL E DO PAIS
        return "+".substr($telefone,0,$tam-10)."(".substr($telefone,$tam-10,2).")".substr($telefone,$tam-8,4)."-".substr($telefone,-4);
    }

    if ($tam == 11) { // COM CÓDIGO DE ÁREA NACIONAL e 9 dígitos
        return "(".substr($telefone,0,2).")".substr($telefone,2,5)."-".substr($telefone,7,11);
    }

    if ($tam == 10) { // COM CÓDIGO DE ÁREA NACIONAL
        return "(".substr($telefone,0,2).")".substr($telefone,2,4)."-".substr($telefone,6,10);
    }

    if ($tam <= 9) { // SEM CÓDIGO DE ÁREA
        return substr($telefone,0,$tam-4)."-".substr($telefone,-4);
    }

}

function m_extrair_dados_hashtag_whatsapp($var_texto){
      // Usamos preg_match_all para encontrar todas as ocorrências entre {{ e }}
      preg_match_all('/\{\{(.*?)\}\}/', $var_texto, $matches);
    
      // O primeiro elemento do $matches é a string completa, então pegamos o segundo elemento
      return $matches[1];
}

function m_filtrar_array_por_chaves_whatsapp($dados_extraidos, $array_hastag){
      // Criamos um novo array associativo com as chaves que existem em ambos arrays
      $resultado = [];
    
      foreach ($dados_extraidos as $item) {
          if (array_key_exists($item, $array_hastag)) {
              $resultado[$item] = $array_hastag[$item];  // Adiciona ao array associativo
          }
      }
      
      return $resultado;
}

function fc_mask($val, $mask){
    $maskared = '';
    $k = 0;

    for($i = 0; $i<=strlen($mask)-1; $i++){
      if($mask[$i] == '#'){
          if(isset($val[$k]))
          $maskared .= $val[$k++];
      } else {
          if(isset($mask[$i]))
              $maskared .= $mask[$i];
      }
    }

    return $maskared;
}



?>conn_contrato_dsv