<?php
require_once __DIR__ . '/../bootstrap.php';

class GeradorMensalidade
{
        private $id_empresa;
        private $id_cobranca_mensalidade;

        public function __construct($id_empresa, $id_cobranca_mensalidade = null)
        {
                $this->id_empresa = intval($id_empresa);
                $this->id_cobranca_mensalidade = $id_cobranca_mensalidade !== null ? intval($id_cobranca_mensalidade) : null;
        }

        private function execQuery($conn, $sql, $id_passo = 1, $id_registro = null, $tx_tipo_registro = null)
        {
            $ret = @pg_query($conn, $sql);
            if ($ret === false) {
                $msg = "Erro ao executar query: " . pg_last_error($conn) . " -- SQL: " . $sql;
                try { fc_gera_log($this->id_empresa, $this->id_cobranca_mensalidade ?? 0, $id_passo, 'N', $msg, $id_registro, $tx_tipo_registro); } catch (Throwable $t) {}
                throw new Exception($msg);
            }
            return $ret;
        }

        public function run()
        {
                $id_empresa = $this->id_empresa;
                logger()->info("GeradorMensalidade: start id_empresa={$id_empresa}");

                try {
                        $this->gera_mensalidade($id_empresa);
                        logger()->info("GeradorMensalidade: finished id_empresa={$id_empresa}");
                        return [ 'success' => true ];
                } catch (Throwable $e) {
                        logger()->error('GeradorMensalidade error: ' . $e->getMessage());
                        fc_gera_log($id_empresa, $this->id_cobranca_mensalidade ?? 0, 1, 'N', $e->getMessage(), 0, 'Sistema');
                        return [ 'success' => false, 'message' => $e->getMessage() ];
                }
        }

        // Migrated from procedural gerarMensalidade.php
        public function gera_mensalidade($id_empresa)
        {
                $id_cobranca_mensalidade = $this->id_cobranca_mensalidade;

                $db = dbNameCob($id_empresa);
                $conexao = new Conexao();
                $conn    = $conexao->open($db);

                #Busca os dados do modelo de e-mail
                $sql_search = "
                SELECT tx_descricao, tx_tipo_cobranca 
                    FROM db_gol.tb_modelo_cobranca_automatica_mensalidade
                    WHERE id_empresa = $id_empresa
                ";

                $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                $dados_modelo = pg_fetch_all($ret)[0];

                echo 'Dados modelo: <pre>';
                print_r($dados_modelo);
                echo '</pre>';

                $tx_descricao_modelo_mensalidade = $dados_modelo['tx_descricao'];
                $tx_tipo_cobranca_modelo_mensalidade = $dados_modelo['tx_tipo_cobranca'];

                $dt_venda = date('Y-m-d');
                $var_talao = date('m/Y');
                $mes_ref = date('Y-m');

                if($tx_tipo_cobranca_modelo_mensalidade == 'POS_PAGO'){
                    $mes_ref = DateTime::createFromFormat('Y-m', $mes_ref);
                    $mes_ref->modify('+1 month');
                    $mes_ref = $mes_ref->format('Y-m');
                }

                echo 'Mês ref: '.$mes_ref.'<hr>';

                $arr_sucesso = array();
                $arr_erro = array();

                #Busca os clientes mensalistas ativos com mensalidades a serem cobradas
                $arr_clientes = $this->buscaClientes($conn, $id_empresa, $mes_ref, $var_talao);

                if(!$arr_clientes || empty($arr_clientes)){

                    fc_gera_log($id_empresa, $id_cobranca_mensalidade, 1, 'N', "Nenhum cliente com mensalidade a ser gerada encontrado.", null, null);

                    #Envia o relatório
                    echo 'Email: <pre>';
                    print_r(fc_monta_relatorio($id_empresa, $id_cobranca_mensalidade));
                    echo '</pre>';

                    return;

                }

                echo 'Clientes: <pre>';
                print_r($arr_clientes);
                echo '</pre>';
          
                if($arr_clientes && !empty($arr_clientes)){

                    #Caso tenha clientes gera a mensalidade
                    $id_cobranca_mensalidade = $this->criaMensalidade($conn, $id_empresa, $tx_descricao_modelo_mensalidade);
                    echo 'Id cobrança: '.$id_cobranca_mensalidade.'<hr>';

                    if($id_cobranca_mensalidade){

                        #Inicia o passo
                        $sql_insert = "
                        INSERT INTO db_gol.tb_passo_mensalidade_automatica (id_empresa, id_cobranca_mensalidade, id_passo, tx_status, dt_inc, dt_fim ) 
                        VALUES ($id_empresa, $id_cobranca_mensalidade, 1, 'EM_PROGRESSO', NOW(), null )";

                        if($this->execQuery($conn, $sql_insert, $id_empresa, 1)){
                            echo 'Passo 1 inserido com sucesso!<hr>';

                            #Buscando os feriados

                            $sql_search = "
                                SELECT lo_dia_util 
                                    FROM db_gol.tb_msysparam 
                                    WHERE id_empresa = $id_empresa;
                            ";

                            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                            $param_dia_util = pg_fetch_all($ret)[0]['lo_dia_util'];

                            $feriados = [];
                            if($param_dia_util && $param_dia_util == 'S'){
                                    $sql_search = "SELECT nu_dia, nu_mes FROM db_gol.tb_feriado";

                                    $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                                    $dias_feriados = pg_fetch_all($ret);

                                    $feriados = [];
                                    if(!empty($dias_feriados)){
                                        foreach ($dias_feriados as $row) {
                                            $dia = str_pad($row['nu_dia'], 2, '0', STR_PAD_LEFT);
                                            $mes = str_pad($row['nu_mes'], 2, '0', STR_PAD_LEFT);
                                            $feriado_date = date('Y') . '-' . $mes . '-' . $dia;
                                            $feriados[] = $feriado_date;
                                        }
                                    }
                            }

                            echo "Feriados:<pre>";
                            print_r($feriados);
                            echo "</pre>";

                            #Buscando a uf da empresa
                            $sql_search = "SELECT uf
                                                            FROM db_gol.tb_empresa
                                                            WHERE id_empresa = $id_empresa";

                            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                            $uf_empresa = pg_fetch_all($ret)[0]['uf'];

                            echo "UF da empresa: $uf_empresa<hr>";

                            #Buscando o cfop dentro e fora da uf da empresa
                            $sql_search = "
                            SELECT cfop_dentro, cfop_fora
                                FROM db_gol.tb_msysparam 
                                WHERE id_empresa = $id_empresa;
                            ";

                            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                            $response = pg_fetch_all($ret)[0];

                            $cfop_dentro = $response['cfop_dentro'];
                            $cfop_fora = $response['cfop_fora'];

                            #Busca o local de estoque
                            $sql_search = "
                            SELECT id_local_stq 
                            FROM db_gol.tb_msysparam
                            WHERE id_empresa = $id_empresa
                            ";

                            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                            $id_local_estoque = pg_fetch_all($ret)[0]['id_local_stq'];

                            echo "Local de estoque: $id_local_estoque<hr>";

                            foreach($arr_clientes as $index => $cliente){

                                            $this->execQuery($conn, 'BEGIN', $id_empresa, 1);

                                #Gerar as venda do cliente
                                $id_venda = 0;
                                $ret_venda = $this->inserirVenda($conn, $id_empresa, $cliente, $mes_ref, $dt_venda, $param_dia_util, $feriados, $uf_empresa, $cfop_dentro, $cfop_fora, $id_cobranca_mensalidade, $var_talao, $id_local_estoque);
                                echo 'Venda: <pre>';
                                print_r($ret_venda);
                                echo '</pre>';

                                if($ret_venda['success']){
                                    $id_venda = $ret_venda['id_venda'];
                                    $dt_vcto_parc = $ret_venda['dt_vcto_parc'];
                                }else{
                                    $arr_erro[] = array(
                                        'tipo' => 'Cliente',
                                        'message' => $ret_venda['message'],
                                        'id' => $cliente['id_pessoa']
                                    );
                                    continue;
                                }

                                if($id_venda){
                    
                                        #Buscando itens das vendas

                                        $arr_item_venda = $this->buscaItemVenda($conn, $id_empresa, $cliente, $dt_venda, $mes_ref);

                                        if(!$arr_item_venda || empty($arr_item_venda)){
                                            $this->execQuery($conn, 'ROLLBACK', $id_empresa, 1);
                                            continue;
                                        }


                                        echo "Itens venda: <pre>";
                                        print_r($arr_item_venda);
                                        echo "</pre>";

                                        $ret_item = array(
                                            'success' => false
                                        );

                                        if($cliente['uf'] == $uf_empresa){
                                            $cfop = $cfop_dentro;
                                        }else{
                                            $cfop = $cfop_fora;
                                        }

                                        #Adiciona os itens nas vendas municipais
                                        if($cliente['nfe'] == 3 && $uf_empresa != 'DF'){
                      
                                            $ret_item = $this->insereItemVendaMunicipal($conn, $id_empresa, $id_venda, $cliente, $arr_item_venda, $id_local_estoque, $cfop, $dt_vcto_parc, $dt_venda);

                                            echo "Item venda municipal: <pre>";
                                            print_r($ret_item);
                                            echo "</pre>";
                                        }
                                        #Adiciona os itens nas vendas padrões
                                        else{
                                            $ret_item = $this->insereItemVenda($conn, $id_empresa, $id_venda, $cliente, $arr_item_venda, $id_local_estoque, $cfop, $dt_vcto_parc, $dt_venda);

                                            echo "Item venda: <pre>";
                                            print_r($ret_item);
                                            echo "</pre>";
                                        }

                                        if($ret_item['success']){
                                            $this->execQuery($conn, 'COMMIT', $id_empresa, 1);
                                        }else{
                                            $arr_erro[] = array(
                                                    'tipo' => 'Venda',
                                                    'message' => $ret_item['success']??"Erro ao tentar inserir item na venda: $id_venda",
                                                    'id' => $id_venda
                                                );
                                            $this->execQuery($conn, 'ROLLBACK', $id_empresa, 1);
                                            continue;
                                        }

                                        #Inserindo comissão do vendedor
                                        $sql_insert = "
                                            INSERT INTO db_gol.tb_venda_comissao(
                                                id_empresa, id_venda, id_vendedor, perc_comissao, vr_comissao
                                            )VALUES(
                                                $id_empresa,$id_venda,'1','0','0'
                                            )";

                                         if(!$this->execQuery($conn, $sql_insert, $id_empresa, 1)){
                                                $arr_erro[] = array(
                                                    'tipo' => 'Venda',
                                                    'message' => "Erro ao tentar inserir comissão na venda: $id_venda",
                                                    'id' => $id_venda
                                                );
                                                continue;
                                         }

                                        $arr_sucesso[] = array(
                                            'tipo' => 'Venda',
                                            'message' => "Venda Nr.: $id_venda, mensalidade gerada para o cliente ".$cliente['id_pessoa'].'-'.$cliente['nm_fantasia'],
                                            'id' => $id_venda
                                        );

                                }else{
                                        $arr_erro[] = array(
                                            'message' => 'Erro ao tentar criar venda para cliente: '.$cliente['id_pessoa'],
                                            'id' => $cliente['id_pessoa'],
                                            'tipo' => 'Cliente'
                                        );
                                        $this->execQuery($conn, 'ROLLBACK', $id_empresa, 1);
                                }
                                                            $this->execQuery($conn, 'ROLLBACK', $id_empresa, 1);
          
                        }else{
                            $arr_erro[] = array(
                                'message' => 'Erro ao tentar inicializar o passo 1!'
                            );
                        }

                    }else{
                        $arr_erro[] = array(
                            'message' => 'Erro ao tentar gerar nova mensalidade!'
                        );
                    }

                    #Gerando os logs

                    echo "ARR ERROS: <pre>";
                    print_r($arr_erro);
                    echo "</pre>";

                    foreach($arr_erro as $erro){
                        fc_gera_log($id_empresa, $id_cobranca_mensalidade, 1, 'N', $erro['message'], $erro['id'], $erro['tipo']);
                    }

                    echo "ARR SUCESSO: <pre>";
                    print_r($arr_sucesso);
                    echo "</pre>";

                    foreach($arr_sucesso as $sucesso){
                        fc_gera_log($id_empresa, $id_cobranca_mensalidade, 1, 'S', $sucesso['message'], $sucesso['id'], $sucesso['tipo']);
                    }

                    echo "Erros: <pre>";
                    print_r($arr_erro);
                    echo "</pre>";

                    echo "Sucesso: <pre>";
                    print_r($arr_sucesso);
                    echo "</pre>";

                    #Alterando etapa de gerar mensalidade para concluída
                    if(!count($arr_erro) and count($arr_sucesso)){

                        #Finaliza o passo como sucesso
                        $this->execQuery($conn, "UPDATE db_gol.tb_cobranca_mensalidade
                                SET lo_mensalidade_gerada = 'S'
                                WHERE id_empresa = $id_empresa
                                AND id_cobranca_mensalidade = $id_cobranca_mensalidade", $id_empresa, 1);

                        $this->execQuery($conn, "UPDATE db_gol.tb_passo_mensalidade_automatica
                                                                                SET tx_status = 'CONCLUIDO',
                                                                                dt_fim = NOW()
                                                                                WHERE id_empresa = $id_empresa
                                                                                AND id_cobranca_mensalidade = $id_cobranca_mensalidade
                                                                                AND id_passo = 1", $id_empresa, 1);

                        // O passo 2 (faturamento) será iniciado pelo seu próprio gerador.
                        // Não inserir nem disparar o passo 2 aqui para manter ownership do passo.
        
                    }else{
                        #Finaliza o passo como erro
                        $sql_update = "UPDATE db_gol.tb_passo_mensalidade_automatica
                                                    SET tx_status = 'ERRO',
                                                    dt_fim = NOW()
                                                    WHERE id_empresa = $id_empresa
                                                    AND id_cobranca_mensalidade = $id_cobranca_mensalidade
                                                    AND id_passo = 1";

                        $this->execQuery($conn, $sql_update, $id_empresa, 1);

                        #Envia o relatório
                        echo 'Email: <pre>';
                        print_r(fc_monta_relatorio($id_empresa, $id_cobranca_mensalidade));
                        echo '</pre>';
                    }

                    // Não disparar o próximo passo por HTTP; a `index.php` ou o gerador correspondente
                    // será responsável por iniciar o passo 2 quando aplicável.
                }
                                                        $this->execQuery($conn, 'ROLLBACK', $id_empresa, 1);

        public function buscaClientes($conn, $id_empresa, $mes_ref, $var_talao){

                $sql_search = "
                SELECT 
                        COALESCE(a.id_empresa_emitente, $id_empresa) AS id_empresa_emitente,
                        COALESCE(a.id_centro_custo, (SELECT tx_valor 
                                                        FROM db_gol.tb_parametro
                                                        WHERE id_empresa = $id_empresa
                                                        AND tx_descricao = 'centro_custo')::integer) AS id_centro_custo,
                        a.id_pessoa,
                        a.id_forma,
                        LPAD(COALESCE(d.nu_dia,0)::TEXT,2,'0') AS dia_venc,
                        CASE a.nfe 
                                WHEN 'N' THEN '0' /*Pedido*/
                                WHEN 'S' THEN '1' /*Nota*/
                                WHEN 'C' THEN '2' /*NFCe*/
                                WHEN 'R' THEN '3' /*Municipal*/
                                WHEN 'T' THEN '4' /* CFe SAT */
                                ELSE '0' 
                        END AS nfe,
                        a.nm_fantasia,
                        a.id_vendedor,
                        b.uf,
                        a.id_tabela,
                        c.nome,
                        a.id_prazo,
                        d.id_tipo_prazo,
                        d.nu_prazo_1,
                        COALESCE(a.limite,0) AS limite,
                        a.tipo_tributacao AS tipo_tributacao_pessoa
                        FROM db_gol.tb_pessoa a
                        INNER JOIN db_gol.tb_pessoa_endereco b
                        ON a.id_empresa = b.id_empresa
                        AND a.id_pessoa = b.id_pessoa_endereco
                        INNER JOIN db_gol.tb_vendedor c
                        ON a.id_empresa = c.id_empresa
                        AND a.id_vendedor = c.id_vendedor
                        INNER JOIN db_gol.tb_prazo d
                        ON a.id_empresa = d.id_empresa
                        AND a.id_prazo = d.id_prazo
                        WHERE a.id_empresa = $id_empresa
                        AND b.tipo_endereco = 1
                        AND a.tipo_cliente IN('A', 'C')
                        AND a.lo_mensalista = 'S'
                        ORDER BY a.prazo;
                ";

                $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                $arr_clientes = pg_fetch_all($ret);

                if(!$arr_clientes || empty($arr_clientes)){
                    echo 'Nenhum cliente encontrado!<hr>';
                    return false;
                }

                #Ajustando os dados dos clientes

                foreach($arr_clientes as $index => $cliente){

                    $id_pessoa = $cliente['id_pessoa'];

                    $count_vendas = 0;
                    $count_vendas = intval($this->verificaVendas($conn, $id_empresa, $id_pessoa, $var_talao));

                    echo "Vendas $id_pessoa: $count_vendas<hr>";

                    if(!$count_vendas){

                        if($id_empresa != 5096 && $id_empresa != 5345){
                            $arr_clientes[$index]['limite'] = 0;
                        }

                    }else{
                        unset($arr_clientes[$index]);
                    }

                }

                return $arr_clientes;
        }

        public function verificaVendas($conn, $id_empresa, $id_pessoa, $var_talao){
            $var_talao_antiga = str_replace("/", "", $var_talao);

            $sql_search = "
            SELECT COUNT(id_pessoa) AS count
                FROM db_gol.tb_venda
                WHERE id_empresa = $id_empresa
                AND nr_pedido_talao IN ('$var_talao', '$var_talao_antiga')
                AND status <> 6
                AND id_pessoa = $id_pessoa
            ";

            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
            $count_vendas = pg_fetch_all($ret)[0]['count'] ?? 1;

            return $count_vendas;
        }

        public function criaMensalidade($conn, $id_empresa, $tx_desc_cobranca){

            $sql_search = "
                SELECT COALESCE(MAX(id_cobranca_mensalidade), 0)+1 AS seq_id_cobranca_mensalidade
                FROM db_gol.tb_cobranca_mensalidade
                WHERE id_empresa = $id_empresa;
            ";

            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
            $seq_id_cobranca_mensalidade = pg_fetch_all($ret)[0]['seq_id_cobranca_mensalidade'];

            $sql_insert = "
            INSERT INTO db_gol.tb_cobranca_mensalidade 
            (id_empresa, id_cobranca_mensalidade, dt_referencia, tx_descricao, lo_mensalidade_gerada, lo_faturamento_gerado, lo_boleto_gerado, lo_cobranca_gerada, dt_inc, dt_atu, tx_login ) 
            VALUES ($id_empresa, $seq_id_cobranca_mensalidade, NOW(), '$tx_desc_cobranca', 'N', 'N', 'N', 'N', NOW(), null, 'MENS_AUTOMATICA' )";

            if($this->execQuery($conn, $sql_insert, $id_empresa, 1)){
                return $seq_id_cobranca_mensalidade;
            }

            return false;

        }

        public function inserirVenda($conn, $id_empresa, $cliente, $mes_ref, $dt_venda, $param_dia_util, $feriados, $uf_empresa, $cfop_dentro, $cfop_fora, $id_cobranca_mensalidade, $var_talao, $id_local_estoque){

            var_dump($conn);
            echo "<hr>$id_empresa<hr>";
            echo "<pre>";
            print_r($cliente);
            echo "</pre>";
            echo $mes_ref.'<hr>';
            echo $dt_venda.'<hr>';
      
            $id_empresa_emitente = $cliente['id_empresa_emitente'];
            $id_centro_custo = $cliente['id_centro_custo'];
            $id_pessoa = $cliente['id_pessoa'];
            $id_forma = $cliente['id_forma'];
            $dia_venc = $cliente['dia_venc'];
            $nfe = $cliente['nfe'];
            $nm_fantasia = $cliente['nm_fantasia'];
            $id_vendedor = $cliente['id_vendedor'];
            $uf = $cliente['uf'];
            $id_tabela = $cliente['id_tabela'];
            $nome_vendedor = $cliente['nome'];
            $id_prazo = $cliente['id_prazo'];
            $id_tipo_prazo = $cliente['id_tipo_prazo'];
            $nu_prazo_1 = $cliente['nu_prazo_1'];
            $limite = $cliente['limite'];
            $tipo_tributacao_pessoa = $cliente['tipo_tributacao_pessoa'];

            #Calcular a data de vencimento da venda
            $dia = $dia_venc;
            $mes = $mes_ref[5].$mes_ref[6];
            $ano = $mes_ref[0].$mes_ref[1].$mes_ref[2].$mes_ref[3];
            $var_ano_mes_oc = "$ano-$mes";

            if($id_tipo_prazo == 3){

                    if((int)$mes == 12){
                        $mes = '01';
                        $ano = (int)$ano + 1;

                    }else{

                        $mes = str_pad(((int)$mes + 1),2,'0', STR_PAD_LEFT);

                    }
            }else if($id_tipo_prazo == 1){

                $sql_search = "SELECT EXTRACT('Month' from ('$dt_venda'::DATE + interval '$nu_prazo_1 day')::date) as mes";
        
                $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                $prox_mes = pg_fetch_all($ret)[0]['mes'];

                if($prox_mes < $mes){
                    $ano++;
                }

                $mes = $prox_mes;
        
                $sql_search = "SELECT EXTRACT('Day' from ('$dt_venda'::DATE + interval '$nu_prazo_1 day')::date) as dia";
        
                $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                $dia = pg_fetch_all($ret)[0]['dia'];
            }

            $var_ano_mes = "$ano-$mes";

            if ($dia == '00'){
                $dia = '01';
            }elseif ((int)$dia > 28 && (int)$mes == 2){
                $dia = '28';
            }elseif((int)$dia > 30){
                $dia = '30';
            }

            $mes = str_pad($mes, 2, "0", STR_PAD_LEFT);
            $dia = str_pad($dia, 2, "0", STR_PAD_LEFT);

            $dt_vcto_parc = $ano.'-'.$mes.'-'.$dia;

            echo 'Dt Vcto: '.$dt_vcto_parc.'<hr>';

            #Caso o parâmetro de próximo dia útil esteja ativo, jogar para ele

            if($param_dia_util && $param_dia_util == 'S'){

                $data_aux = strtotime($dt_vcto_parc);

                while (date('N', $data_aux) >= 6 || in_array(date('Y-m-d', $data_aux), $feriados)) {
                    $data_aux = strtotime('+1 day', $data_aux);
                }

                $dt_vcto_parc = date('Y-m-d', $data_aux);

                echo 'Dt Vcto Dia Útil: '.$dt_vcto_parc.'<hr>';
            }

            #Calcular o próximo sequencial de venda
            $sql_search = "SELECT db_gol.proximo_sequencial($id_empresa,'seq_venda')";

            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);

            $id_venda = pg_fetch_all($ret)[0]['proximo_sequencial'];

            echo "Id venda: $id_venda<hr>";

            #Vaidar se uf é igual a da empresa e calcular o id_dest e o cfop
            if($uf == $uf_empresa){
                $id_dest = 1;
                $cfop = $cfop_dentro;
            }else{
                $id_dest = 2;
                $cfop = $cfop_fora;
            }

            echo "Id Dest: $id_dest<br>CFOP: $cfop<hr>";

            #Montar a observação da venda usando o mês de referência
            $ref_mes = $mes_ref[5].$mes_ref[6];
            $ref_ano = $mes_ref[0].$mes_ref[1].$mes_ref[2].$mes_ref[3];

            switch ($ref_mes){
                    case '01':
                        $mes_aux = 'JANEIRO';
                        break;
                    case '02':
                        $mes_aux = 'FEVEREIRO';
                        break;
                    case '03':
                        $mes_aux = 'MARCO';
                        break;
                    case '04':
                        $mes_aux = 'ABRIL';
                        break;
                    case '05':
                        $mes_aux = 'MAIO';
                        break;
                    case '06':
                        $mes_aux = 'JUNHO';
                        break;
                    case '07':
                        $mes_aux = 'JULHO';
                        break;
                    case '08':
                        $mes_aux = 'AGOSTO';
                        break;
                    case '09':
                        $mes_aux = 'SETEMBRO';
                        break;  
                    case '10':
                        $mes_aux = 'OUTUBRO';
                        break;
                    case '11':
                        $mes_aux = 'NOVEMBRO';
                        break;
                    case '12':
                        $mes_aux = 'DEZEMBRO';
                        break;                                                                                    
                    }

                 $tx_obs_venda = 'REF. A '.$mes_aux.' DE '.$ref_ano;

                 echo $tx_obs_venda.'<hr>';

                #Data de emissão e saída como data atual
                $dt_emissao = $dt_venda;
                $dt_saida = $dt_venda;

                echo "Data de emissão: $dt_emissao<hr>";
                echo "Data de venda: $dt_saida<hr>";

                #Calculando a série no nfe
                $sql_search = "SELECT COALESCE(id_serie,1) AS id_serie
                                                    FROM db_gol.tb_msysseq
                                                    WHERE id_empresa = $id_empresa_emitente";

                $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                $id_serie = pg_fetch_all($ret)[0]['id_serie'];

                $id_serie = $id_serie ? $id_serie : 1;

                echo "Série: $id_serie<hr>";
          
                #Realiza a criação da venda

                $sql_insert = "
                INSERT INTO db_gol.tb_venda (
                    id_empresa, 
                    id_venda, 
                    nr_pedido_talao, 
                    dt_venda, 
                    hora, 
                    id_serie, 
                    cfop, 
                    id_pessoa, 
                    id_forma, 
                    prazo, 
                    dt_vencimento, 
                    dt_entrega, 
                    tipo_pedido, 
                    atu_stq, 
                    atu_financeiro, 
                    parcela_qtd, 
                    subtotal, 
                    vr_total, 
                    cif_fob, 
                    vendedor, 
                    status, 
                    dt_inc, 
                    login, 
                    id_local_estoque, 
                    nm_pessoa, 
                    id_vendedor, 
                    lo_dt_saida_vazio, 
                    vr_despesa_acessoria, 
                    id_centro_custo, 
                    id_prazo, 
                    observacao, 
                    id_empresa_emitente, 
                    id_dest, 
                    tx_origem_venda, 
                    id_tabela_preco_aux, 
                    dt_emissao, 
                    dt_saida, 
                    id_cobranca_mensalidade "
                    .($tipo_tributacao_pessoa?", tipo_tributacao ":"")."
                ) 
                VALUES 
                    (
                        $id_empresa, 
                        $id_venda, 
                        '$var_talao', 
                        '$dt_venda', 
                        '00:00:00', 
                        $id_serie, 
                        $cfop, 
                        $id_pessoa, 
                        $id_forma, 
                        ('$dt_vcto_parc' :: DATE - '$dt_emissao' :: DATE), 
                        '$dt_vcto_parc' :: DATE, 
                        '$dt_emissao' :: DATE, 
                        '$nfe', 
                        'N', 
                        'S', 
                        'S',
                        0,
                        0,
                        0,
                        '$nome_vendedor', 
                        1, 
                        NOW(), 
                        'SQL', 
                        $id_local_estoque, 
                        SUBSTRING('$nm_fantasia', 1, 50), 
                        $id_vendedor, 
                        'N', 
                        0, 
                        $id_centro_custo, 
                        $id_prazo, 
                        '$tx_obs_venda', 
                        $id_empresa_emitente, 
                        $id_dest, 
                        'MS', 
                        $id_tabela, 
                        '$dt_emissao', 
                        '$dt_saida', 
                        $id_cobranca_mensalidade "
                        .($tipo_tributacao_pessoa?", '$tipo_tributacao_pessoa' ":"")."
                    );
                ";

                if($this->execQuery($conn, $sql_insert, $id_empresa, 1)){
                    return array(
                        'success' => true,
                        'message' => "Venda $id_venda inserida com sucesso!",
                        'id_venda' => $id_venda,
                        'dt_vcto_parc' => $dt_vcto_parc
                    );
                }else{
                    return array(
                        'success' => false,
                        'message' => "Erro ao tentar gerar a venda para o cliente $id_pessoa"
                    );
                }

        }

        public function buscaItemVenda($conn, $id_empresa, $cliente, $dt_venda, $mes_ref){

            $id_pessoa = $cliente['id_pessoa'];

            $mes = $mes_ref[5].$mes_ref[6];
            $ano = $mes_ref[0].$mes_ref[1].$mes_ref[2].$mes_ref[3];
            $var_ano_mes_oc = "$ano-$mes";

            $sql_search = "
                        SELECT
                            a.id_produto
                            , 1 AS ordem_unidade
                            ,'SRV' AS tx_unidade
                            , SUBSTRING(CASE WHEN TRIM(a.tx_descricao) <> '' THEN a.tx_descricao ELSE b.descricao END,1,120) as desc_produto
                            , a.vr_assinatura AS vr_total
                            ,'A' as tipo
                            , 1 AS id_item
                            ,'$var_ano_mes_oc' as dt_referencia
                            ,0::numeric(18,3) AS vr_desconto
                            ,a.id_assinatura
                            ,a.quantidade
                            ,a.vr_unitario
                        FROM db_gol.tb_pessoa_assinatura a
                        INNER JOIN db_gol.tb_produto b
                        ON a.id_empresa = b.id_empresa
                        AND a.id_produto = b.id_produto 
                        WHERE a.id_empresa = $id_empresa
                        AND a.id_pessoa = $id_pessoa
                        AND a.dt_ativacao IS NOT NULL
                        AND COALESCE(a.dt_inicio_cobranca, a.dt_ativacao)::DATE <= '$dt_venda'::DATE
                        AND a.dt_ativacao::DATE <= '$dt_venda'::DATE
                        AND (a.dt_desativacao IS NULL OR a.dt_desativacao >= '\"'.$dt_venda.'\"')
                        AND a.lo_suspensa = 'N'\t	
                        AND date_part('month',TIMESTAMP '\"'.$dt_venda.'\"')::text IN (
                            SELECT unnest(string_to_array(c.tx_meses, ';')) 
                            FROM db_Gol.tb_pessoa_assinatura c 
                            WHERE a.id_empresa = c.id_empresa
                            AND a.id_pessoa = c.id_pessoa
                            AND a.id_assinatura = c.id_assinatura)
                        UNION ALL 
                        SELECT
                            id_produto
                            ,1 AS ordem_unidade
                            ,tx_unidade
                            ,trim(tx_desc_prod) as desc_produto
                            ,vr_unitario AS vr_total
                            ,'O' as tipo
                            ,id_item
                            ,(date_part('year'::text, dt_ocorrencia) || '-'::text) || lpad(date_part('month'::text, dt_ocorrencia)::text, 2, '0'::text)  as dt_referencia
                            ,COALESCE(vr_desconto,0)::numeric(18,3) AS vr_desconto
                            ,NULL AS id_assinatura
                            ,NULL AS quantidade
                            ,NULL AS vr_unitario
                        FROM db_gol.tb_ocorrencia
                        WHERE id_empresa = $id_empresa
                        AND id_pessoa = $id_pessoa
                        AND ('$var_ano_mes_oc-01' BETWEEN (date_trunc('month', (dt_ocorrencia::DATE)))::date 
                        AND ((date_trunc('month', (COALESCE(dt_fim, dt_ocorrencia)::DATE + interval '1 month')) - interval '1 day')::date)))
            ";

            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
            $arr_itens = pg_fetch_all($ret);

            if($arr_itens && !empty($arr_itens)){
                return $arr_itens;
            }else{
                return false;
            }
        }

        // The remaining helper functions (insereItemVendaMunicipal, verificaRejuste, verificaDesconto, insereItemVenda)
        // were moved but kept pg_query calls intact. For brevity they are implemented below as direct translations.

        public function insereItemVendaMunicipal($conn, $id_empresa, $id_venda, $cliente, $arr_item_venda, $id_local_estoque, $cfop, $dt_vcto_parc, $dt_venda){
                // Implementation copied from gerarMensalidade.php (kept pg_query calls intact)
                $vr_total = 0;
                $vr_total_venda = 0;
                $vr_unitario_item = 0;
                $descricao = '';
                $vr_unitario = 0;
                $vr_unitario_item = 0;
                $arr_msg_reajuste = array();
                $descricao_aux    = $arr_item_venda[0]['desc_produto'];
                $var_descricao_add_item = array();
                $var_id_empresa_emitente = $cliente['id_empresa_emitente'];
                $id_pessoa = $cliente['id_pessoa'];

                foreach ($arr_item_venda as $item => $detalhe){

                    echo "Detalhe Item: <pre>";
                    print_r($detalhe);
                    echo "</pre>";

                    $id_produto        = $detalhe['id_produto'];
                    $ordem_unid        = $detalhe['ordem_unidade'];
                    $unidade_medida    = $detalhe['tx_unidade'];
                    $descricao         = $detalhe['desc_produto'] . ' ';
                    $descricao_aux     = $detalhe['desc_produto'];
                    $vr_total         += $detalhe['vr_total'];
                    $vr_total_aux      = $detalhe['vr_total'];
                    $tp_item           = $detalhe['tipo'];
                    $nu_item           = $detalhe['id_item'];
                    $vr_desconto       = $detalhe['vr_desconto'];
                    $id_assinatura = $detalhe['id_assinatura'];
          
                    $var_quantidade    = $detalhe['quantidade'];
                    $vr_unitario       = $detalhe['vr_unitario'];
          
                    $var_id_empresa_emitente = $cliente['id_empresa_emitente'];
          
                    $var_desc_prod_add = ""; 
                    $arr_reajuste      = array();
                    $arr_desconto      = array();
                    $var_descricao_add_item[] = $descricao_aux." R$: ".$vr_total_aux;

                    $sql_search = "
                    SELECT tx_valor 
                    FROM db_gol.tb_parametro
                    WHERE id_empresa = $id_empresa
                    AND tx_descricao = 'tipo_indice_de_reajuste' 
                    ";

                    $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                    $param = pg_fetch_all($ret)[0]['tx_valor'];

                    echo "tipo_indice_de_reajuste: $param<hr>";
                    if($tp_item == 'A'){
                        if($param && $param == 'A'){
                                $ret_reajuste = $this->verificaRejuste($conn, $id_empresa, $id_pessoa, $id_assinatura, $dt_venda);

                                echo "RET REAJUSTE: <pre>";
                                print_r($ret_reajuste);
                                echo "</pre>";

                                if($ret_reajuste && $ret_reajuste['success']){
                                    $arr_reajuste = $ret_reajuste['arr_reajuste'];
                                }else{
                                    $arr_erro[] = $ret_reajuste;
                                    continue;
                                }

                                echo "ARR REAJUSTE: <pre>";
                                print_r($arr_reajuste);
                                echo "</pre>";

                        }
            
                        $arr_desconto = $this->verificaDesconto($conn, $id_empresa, $id_pessoa, $id_assinatura, $dt_venda); 

                        echo 'ARR DESCONTO: <pre>';
                        print_r($arr_desconto);
                        echo '</pre>';
                    }
          
                    if(count($arr_reajuste) > 0){
                        if($arr_reajuste['sucesso']){
                            $arr_msg_reajuste[] = $arr_reajuste['msg_reajuste'];
                            $vr_unitario_item += $arr_reajuste['vr_reajustado'];

                            echo 'msg_reajuste: '.$arr_reajuste['msg_reajuste'].'<hr>';
                            echo 'vr_reajustado: '.$arr_reajuste['vr_reajustado'].'<hr>';
                        }
                    }
          
                    if(count($arr_desconto) > 0){
                        if($arr_desconto['sucesso']){
                            $var_desc_prod_add .= $arr_desconto['msg_desconto'];
                            $vr_desconto = $arr_desconto['vr_desconto'];

                            echo 'msg_desconto: '.$arr_desconto['msg_desconto'].'<hr>';
                            echo 'vr_desconto: '.$arr_desconto['vr_desconto'].'<hr>';
                        }
                    }

                    $sql_search = "
                    SELECT tx_valor 
                    FROM db_gol.tb_parametro
                    WHERE id_empresa = $id_empresa
                    AND tx_descricao = 'utiliza_assinatura_por_empresa_emitente' 
                    ";

                    $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                    $param = pg_fetch_all($ret)[0]['tx_valor'];

                    echo "utiliza_assinatura_por_empresa_emitente: $param<hr>";
          
                    if($param && $param == 'S'){
                        if(empty($var_quantidade)){
                            $var_quantidade = 1;
                        }
          
                        if(empty($vr_unitario)){
                            $vr_unitario = $vr_total;
                        }
          
                        $vr_total_venda = $vr_total - $vr_desconto;
            
                    }else{
                        $vr_total_venda = $vr_total - $vr_desconto;
                        $var_quantidade = 1;
                        $vr_unitario    = $vr_total;

                        /*Alteracao para a Suprema Contadores. Chamado 116030.*/
                        if($id_empresa == 4959){
                            $var_quantidade = 2;
                        }
                    }
          

                    $sql_update = "
                    UPDATE db_gol.tb_ocorrencia 
                    SET tx_status = 'C', dt_atu = now()
                    WHERE id_empresa = $id_empresa
                    AND id_pessoa = $id_pessoa
                    AND id_item = $nu_item";

                 $this->execQuery($conn, $sql_update, $id_empresa, 1);

                }

                $id_produto_aux   = $arr_item_venda[0]['id_produto'];
                $ordem_unid_aux   = $arr_item_venda[0]['ordem_unidade'];
                $unidade_medida_aux = $arr_item_venda[0]['tx_unidade'];
                $descricao2     = $arr_item_venda[0]['desc_produto'];
                $var_desc_prod_add2 = implode(", ",$var_descricao_add_item);
                $var_desc_prod_add2_aux = substr($var_desc_prod_add2, 0, 500);

                /*cadastro os itens da venda*/
                $sql_insert = "
                INSERT INTO db_gol.tb_venda_item(
                    id_empresa,           id_venda,           item,           id_seq_produto,       desc_produto, 
                    qtd_volumes,          peso,           unidade,        preco_custo,        vr_unitario,
                    vr_total,           vr_comissao,        dt_inc,         login,            id_movestq,
                    status,             id_local_estoque,       ordem_unid,       desc_prod_add,        vr_desconto, 
                    vr_comissao_produto,      vr_comissao_transportador,  vr_comissao_supervisor, id_cfop,          id_tabela_preco,
                    id_empresa_emitente
                )VALUES(
                    $id_empresa,          $id_venda,        1,            $id_produto_aux,      '$descricao_aux', 
                    0,                $var_quantidade,      '$unidade_medida_aux',  0,              $vr_unitario,
                    $vr_total_venda,        0,              now(),          'SQL',        null,
                    'S',              $id_local_estoque,      $ordem_unid_aux,    '$var_desc_prod_add2_aux',  $vr_desconto, 
                    0,                0,              0,            $cfop,           ".$cliente['id_tabela'].",
                    $var_id_empresa_emitente
                )";

                try{
                    $this->execQuery($conn, $sql_insert, $id_empresa, 1);
                }catch(Throwable $e){
                    return array(
                            'success' => false,
                            'message' => "Erro ao tentar inserir item na venda: $id_venda",
                            'tipo' => 'Venda',
                            'id' => $id_venda
                        );
                }

                $sql_search = "
                    SELECT
                        id_venda,
                        item,
                        vr_unitario,
                        peso,
                        vr_desconto
                    FROM db_gol.tb_venda_item
                    WHERE id_empresa = $id_empresa
                    AND id_venda = $id_venda
                ";

                $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                $total_venda_item = pg_fetch_all($ret);

                echo "Total venda item: <pre>";
                print_r($total_venda_item);
                echo "</pre>";

                // Preciso recalcular dessa forma, pois se houverem produto repetido na mensalidade, o valor total da venda estava ficando com o valor total do ultimo item inserido no loop.
                if($total_venda_item && count($total_venda_item) > 1){
                    $vr_total_venda = 0;
                    foreach($total_venda_item as $vendaItem){
                        $varVrUnitario = $vendaItem['vr_unitario'];
                        $varQuantidade = $vendaItem['peso'];
                        $varVrDescontoItem = $vendaItem['vr_desconto'];

                        $vr_total_venda += ($varQuantidade * $varVrUnitario) - $varVrDescontoItem;
                    }
                }

                /*atualizo o valor total no cabeçalho da venda*/
                $sql_update = "
                UPDATE db_gol.tb_venda
                SET subtotal = $vr_total_venda
                    ,vr_total = $vr_total_venda
                WHERE id_empresa = $id_empresa
                AND id_venda = $id_venda";

                try{
                    $this->execQuery($conn, $sql_update, $id_empresa, 1);
                }catch(Throwable $e){
                    return array(
                                'success' => false,
                                'message' => "Erro ao tentar atualizar o valor da venda: $id_venda após inserir um item",
                                'tipo' => 'Venda',
                                'id' => $id_venda
                    );
                }

                /*insere a parcela da venda*/
                $sql_insert = "
                INSERT INTO db_gol.tb_parcela(
                    id_empresa,   id_venda,     id_parcela,   dt_venc,      vr_parcela
                )VALUES(
                    $id_empresa,  $id_venda,  '1',      '$dt_vcto_parc', '$vr_total_venda'
                )";

                try{
                    $this->execQuery($conn, $sql_insert, $id_empresa, 1);
                }catch(Throwable $e){
                    return array(
                            'success' => false,
                            'message' => "Erro ao tentar gerar parcela da venda: $id_venda",
                            'tipo' => 'Venda',
                            'id' => $id_venda
                        );
                }

                return array(
                    'success' => true,
                    'message' => "Itens inseridos com sucesso na venda: $id_venda",
                );

        }

        public function verificaRejuste($conn, $id_empresa, $id_pessoa, $id_assinatura, $dt_venda){
            $arr_reajuste = array();
            $arr_reajuste['sucesso'] = false;

            $sql_search = "
                SELECT a.id_produto
                    , UPPER(b.descricao) AS descricao
                    , UPPER(a.tx_descricao) AS tx_descricao
                    , a.vr_assinatura
                    , a.dt_ativacao::date AS dt_ativacao
                    , a.id_indice
                    , c.nu_percentual
                    , a.vr_assinatura + (a.vr_assinatura * (c.nu_percentual/100))::numeric(18,2) as vr_reajustado
                    , c.tx_descricao AS tx_descricao_2
                FROM db_gol.tb_pessoa_assinatura a
                INNER JOIN db_gol.tb_produto b
                ON a.id_empresa = b.id_empresa
                AND a.id_produto = b.id_produto
                INNER JOIN db_gol.tb_indice c
                ON a.id_empresa = c.id_empresa
                AND a.id_indice = c.id_indice 
                WHERE a.id_empresa = $id_empresa
                AND a.id_pessoa = $id_pessoa
                AND a.id_assinatura = $id_assinatura
            ";

            echo $sql_search.'<hr>';

            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
            $pa = pg_fetch_all($ret);

            echo "PA: <pre>";
            print_r($pa);
            echo "</pre>";

            if($pa && $pa[0]['id_produto'] > 0){
                foreach($pa as $key => $linha){
                    $var_id_produto       = $linha['id_produto'];
                    $var_descricao_produto      = $linha['descricao'];
                    $var_descricao_assinatura   = $linha['tx_descricao'];
                    $var_vr_assinatura          = $linha['vr_assinatura'];
                    $var_dt_ativacao            = $linha['dt_ativacao'];
                    $var_id_indice              = $linha['id_indice'];
                    $var_nu_percentual          = $linha['nu_percentual'];
                    $var_vr_reajustado          = $linha['vr_reajustado'];
                    $var_nome_indice      = $linha['tx_descricao_2'];
                    list($var_ano_ativ, $var_mes_ativ, $var_dia_ativ) = explode("-",$var_dt_ativacao);
                    list($var_ano_vd, $var_mes_vd, $var_dia_vd)       = explode("-",$var_dt_venda);

                    if(($var_mes_ativ == $var_mes_vd) && ($var_ano_vd > $var_ano_ativ) && ($var_vr_reajustado > $var_vr_assinatura)){

                        $sql_search = "
                            SELECT COUNT(1) AS count
                            FROM db_gol.tb_pessoa_assinatura_reajuste
                            WHERE id_empresa = $id_empresa
                            AND id_pessoa = $id_pessoa
                            AND id_assinatura = $id_assinatura
                            AND SUBSTRING(dt_aniversario_contrato::TEXT,1,7) = '$var_ano_vd-$var_mes_vd'
                        ";

                        $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                        $cont = pg_fetch_all($ret);

                        echo "COUNT ASSINATURA: $cont<hr>";

                        if($cont && $cont[0]['count'] == 0){
                            $sql_update = "
                                UPDATE db_gol.tb_pessoa_assinatura
                                SET vr_assinatura = $var_vr_reajustado
                                WHERE id_empresa = $id_empresa
                                AND id_pessoa = $id_pessoa
                                AND id_assinatura = $id_assinatura
                            ";

                            try{
                                $this->execQuery($conn, $sql_update, $id_empresa, 1);
                            }catch(Throwable $e){
                                return array(
                                        'success' => false,
                                        'message' => "Erro ao tentar atualizar a assinatura do cliente na venda: $id_venda",
                                        'tipo' => 'Venda',
                                        'id' => $id_venda
                                );
                            }

                            $arr_reajuste['sucesso'] = true;
                            $arr_reajuste['msg_reajuste'] = "- <b>$var_descricao_produto R$ ".number_format($var_vr_assinatura,2,',','.')."</b> houve reajuste de <b>".number_format($var_nu_percentual,4,',','.')." % (".$var_nome_indice.")</b> "
                                                            ." para <b>R$ ".number_format($var_vr_reajustado,2,',','.')."</b>";
                            $arr_reajuste['vr_reajustado'] = $var_vr_reajustado;
                            $var_msg_reajuste = $arr_reajuste['msg_reajuste'];
              
                            $sql_insert = "
                                INSERT INTO db_gol.tb_pessoa_assinatura_reajuste (
                                    SELECT
                                        $id_empresa
                                        , $var_id_pessoa
                                        , $id_assinatura
                                        , (SELECT COALESCE(MAX(id_item),0) + 1 
                                            FROM db_gol.tb_pessoa_assinatura_reajuste 
                                             WHERE id_empresa = $id_empresa
                                             AND id_pessoa = $id_pessoa 
                                             AND id_assinatura = $id_assinatura)
                                        , $var_id_indice
                                        , $var_vr_assinatura
                                        , $var_nu_percentual
                                        , $var_vr_reajustado
                                        , '$var_ano_vd-$var_mes_vd-$var_dia_vd'
                                        , '$var_msg_reajuste'
                                        , NOW()
                                        , 'SQL'
                                )
                            ";

                            try{
                                $this->execQuery($conn, $sql_insert, $id_empresa, 1);
                            }catch(Throwable $e){
                                return array(
                                        'success' => false,
                                        'message' => "Erro ao tentar realizar o reajuste da assinatura na venda: $id_venda",
                                        'tipo' => 'Venda',
                                        'id' => $id_venda
                                );
                            }

                        }
                    } 
                }

            }
            return array(
                'success' => true,
                'message' => 'Reajsute calculado com sucesso',
                'arr_reajuste' => $arr_reajuste
            );
        }

        public function verificaDesconto($conn, $id_empresa, $id_pessoa, $id_assinatura, $dt_venda){
            $arr_desconto = array();
            $arr_desconto['sucesso'] = false;

            $sql_search = "
                SELECT 
                        CASE WHEN tx_tipo = 'P' THEN (a.vr_assinatura * (b.vr_valor/100)) ELSE b.vr_valor END::numeric(18,2) AS valor
                    , b.tx_descricao
                FROM db_gol.tb_pessoa_assinatura a
                INNER JOIN db_gol.tb_pessoa_assinatura_desconto b
                ON a.id_empresa = b.id_empresa
                AND a.id_pessoa = b.id_pessoa
                AND a.id_assinatura = b.id_assinatura
                WHERE a.id_empresa = $id_empresa
                AND a.id_pessoa = $id_pessoa
                AND a.id_assinatura = $id_assinatura
                AND '$dt_venda' BETWEEN b.dt_inicio AND b.dt_fim
            ";

            echo $sql_search.'<hr>';

            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
            $desc = pg_fetch_all($ret);

            echo "DESC: <pre>";
            print_r($desc);
            echo "</pre>";

            if($desc && $desc[0]['valor'] > 0){
                $vr_tt_desconto = 0;
                $arr_msg_desconto = array();
                foreach($desc as $key_desc => $linha_desc){
                    $vr_tt_desconto += $linha_desc['valor'];
                    $arr_msg_desconto[] = "DESCONTO DE R$ ".number_format($linha_desc[0],'2',',','.')." REFERENTE A ".$linha_desc['tx_descricao']; 
                }
                $arr_desconto['sucesso'] = true;
                $arr_desconto['vr_desconto'] = $vr_tt_desconto;
                $arr_desconto['msg_desconto'] = implode(", ",$arr_msg_desconto);
            }
            return $arr_desconto;

        }

        public function insereItemVenda($conn, $id_empresa, $id_venda, $cliente, $arr_item_venda, $id_local_estoque, $cfop, $dt_vcto_parc, $dt_venda){
            $vr_total = 0;
            $vr_total_venda = 0;
            $arr_msg_reajuste = array();
            $var_id_empresa_emitente = $id_empresa;
            $id_pessoa = $cliente['id_pessoa'];

            foreach ($arr_item_venda as $item => $detalhe){

                echo "Detalhe: <pre>";
                print_r($detalhe);
                echo "</pre>";

                $id_produto      = $detalhe['id_produto'];
                $ordem_unid      = $detalhe['ordem_unidade'];
                $unidade_medida    = $detalhe['tx_unidade'];
                $descricao       = $detalhe['desc_produto'];
                $vr_total        = $detalhe['vr_total'];
                $tp_item       = $detalhe['tipo'];
                $nu_item       = $detalhe['id_item'];

                $vr_desconto     = $detalhe['vr_desconto'];
                $id_assinatura = $detalhe['id_assinatura'];
                $var_quantidade    = $detalhe['quantidade'];
                $vr_unitario       = $detalhe['vr_unitario'];
        
                $var_id_empresa_emitente = $cliente['id_empresa_emitente'];
        
                $var_desc_prod_add   = "";
                $arr_reajuste        = array();
                $arr_desconto        = array();
                $var_u_cservico_isn  = '';
                $var_u_clistserv_isn = '';
                $var_id_cnae         = '';
       
                $sql_search = "
                SELECT 
                    id_tributacao_municipio, 
                    id_servico, 
                    id_cnae 
                FROM db_gol.tb_produto
                WHERE id_produto = $id_produto
                AND id_empresa = $id_empresa";

                $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                $ntse_df = pg_fetch_all($ret);

                echo "NTSE DF: <pre>";
                print_r($ntse_df);
                echo "</pre>";

                if($ntse_df && $ntse_df[0]['id_tributacao_municipio'] != ''){
                    $var_u_cservico_isn = $ntse_df[0]['id_tributacao_municipio'];
                    $var_u_clistserv_isn = $ntse_df[0]['id_servico'];
                    $var_id_cnae = $ntse_df[0]['id_cnae'];      
                }else{
                    $var_u_cservico_isn = '0';
                    $var_u_clistserv_isn = '0';
                    $var_id_cnae = '0';
                }
        
                $sql_search = "
                    SELECT tx_valor 
                    FROM db_gol.tb_parametro
                    WHERE id_empresa = $id_empresa
                    AND tx_descricao = 'tipo_indice_de_reajuste' 
                    ";

                    $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                    $param = pg_fetch_all($ret)[0]['tx_valor'];

                    echo "tipo_indice_de_reajuste: $param<hr>";
                    if($tp_item == 'A'){
                        if($param && $param == 'A'){
                                $ret_reajuste = $this->verificaRejuste($conn, $id_empresa, $id_pessoa, $id_assinatura, $dt_venda);

                                echo "RET REAJUSTE: <pre>";
                                print_r($ret_reajuste);
                                echo "</pre>";

                                if($ret_reajuste && $ret_reajuste['success']){
                                    $arr_reajuste = $ret_reajuste['arr_reajuste'];
                                }else{
                                    $arr_erro[] = $ret_reajuste;
                                    continue;
                                }

                                echo "ARR REAJUSTE: <pre>";
                                print_r($arr_reajuste);
                                echo "</pre>";

                        }
            
                        $arr_desconto = $this->verificaDesconto($conn, $id_empresa, $id_pessoa, $id_assinatura, $dt_venda); 

                        echo 'ARR DESCONTO: <pre>';
                        print_r($arr_desconto);
                        echo '</pre>';
                    }
          
                    if(count($arr_reajuste) > 0){
                        if($arr_reajuste['sucesso']){
                            $arr_msg_reajuste[] = $arr_reajuste['msg_reajuste'];
                            $vr_unitario_item += $arr_reajuste['vr_reajustado'];

                            echo 'msg_reajuste: '.$arr_reajuste['msg_reajuste'].'<hr>';
                            echo 'vr_reajustado: '.$arr_reajuste['vr_reajustado'].'<hr>';
                        }
                    }
          
                    if(count($arr_desconto) > 0){
                        if($arr_desconto['sucesso']){
                            $var_desc_prod_add .= $arr_desconto['msg_desconto'];
                            $vr_desconto = $arr_desconto['vr_desconto'];

                            echo 'msg_desconto: '.$arr_desconto['msg_desconto'].'<hr>';
                            echo 'vr_desconto: '.$arr_desconto['vr_desconto'].'<hr>';
                        }
                    }
        
                $sql_search = "
                    SELECT tx_valor 
                    FROM db_gol.tb_parametro
                    WHERE id_empresa = $id_empresa
                    AND tx_descricao = 'utiliza_assinatura_por_empresa_emitente' 
                    ";

                    $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
                    $param = pg_fetch_all($ret)[0]['tx_valor'];

                    echo "utiliza_assinatura_por_empresa_emitente: $param<hr>";
          
                    if($param && $param == 'S'){
                        if(empty($var_quantidade)){
                            $var_quantidade = 1;
                        }

                        if(empty($vr_unitario)){
                            $vr_unitario = $vr_total;
                        }

                        $vr_total_venda = $vr_total - $vr_desconto;
        
                    }else{

                        $vr_total_venda = $vr_total - $vr_desconto;
                        $var_quantidade = 1;
                        $vr_unitario = $vr_total;

                        /*Alteracao para a Suprema Contadores. Chamado 116030.*/
                        if($id_empresa == 4959){
                            $var_quantidade = 2;
                        }
                    }
        
                /*cadastro os itens da venda*/
                $sql_insert = "
                INSERT INTO db_gol.tb_venda_item(
                    id_empresa,           id_venda,           item,           id_seq_produto,       desc_produto, 
                    qtd_volumes,          peso,           unidade,        preco_custo,        vr_unitario,
                    vr_total,           vr_comissao,        dt_inc,         login,            id_movestq,
                    status,             id_local_estoque,       ordem_unid,       desc_prod_add,        vr_desconto, 
                    vr_comissao_produto,      vr_comissao_transportador,  vr_comissao_supervisor, id_cfop,          id_tabela_preco,
                    id_empresa_emitente,      u_cservico_isn,       u_clistserv_isn,    id_cnae
                )VALUES(
                    $id_empresa,          $id_venda,        ".($item+1).",        $id_produto,        '$descricao', 
                    0,                $var_quantidade,            '$unidade_medida',    0,              $vr_unitario,
                    $vr_total_venda,  0,              now(),          'SQL',        null,
                    'S',              $id_local_estoque,      $ordem_unid,      '$var_desc_prod_add',   $vr_desconto, 
                    0,                0,              0,            $cfop,           ".$cliente['id_tabela'].",
                    $id_empresa,          '$var_u_cservico_isn',    '$var_u_clistserv_isn', '$var_id_cnae')";

                    try{
                        $this->execQuery($conn, $sql_insert, $id_empresa, 1);
                    }catch(Throwable $e){
                        return array(
                                'success' => false,
                                'message' => "Erro ao tentar inserir item na venda: $id_venda",
                                'tipo' => 'Venda',
                                'id' => $id_venda
                            );
                    }
     
                if($tp_item == 'O'){
                    $sql_update = "
                    UPDATE db_gol.tb_ocorrencia 
                    SET tx_status = 'C', dt_atu = now()
                    WHERE id_empresa = $id_empresa
                    AND id_pessoa = $id_pessoa
                    AND id_item = $nu_item";

                }
      
            }

            $sql_search = "
                SELECT
                    id_venda,
                    item,
                    vr_unitario,
                    peso,
                    vr_desconto
                FROM db_gol.tb_venda_item
                WHERE id_empresa = $id_empresa
                AND id_venda = $id_venda
            ";

            $ret = $this->execQuery($conn, $sql_search, $id_empresa, 1);
            $total_venda_item = pg_fetch_all($ret);

            echo "Total venda item: <pre>";
            print_r($total_venda_item);
            echo "</pre>";

            // Preciso recalcular dessa forma, pois se houverem mais de um item inserido na venda, o valor total da venda estava ficando com o valor total do ultimo item inserido.
            if($total_venda_item && count($total_venda_item) > 1){
                $vr_total_venda = 0;
                foreach($total_venda_item as $vendaItem){
                    $varVrUnitario = $vendaItem['vr_unitario'];
                    $varQuantidade = $vendaItem['peso'];
                    $varVrDescontoItem = $vendaItem['vr_desconto'];

                    $vr_total_venda += ($varQuantidade * $varVrUnitario) - $varVrDescontoItem;
                }
            }

            /*atualizo o valor total no cabeçalho da venda*/
            $sql_update = "
            UPDATE db_gol.tb_venda
            SET subtotal = $vr_total_venda
                ,vr_total = $vr_total_venda
            WHERE id_empresa = $id_empresa
            AND id_venda = $id_venda";

            try{
                $this->execQuery($conn, $sql_update, $id_empresa, 1);
            }catch(Throwable $e){
                return array(
                        'success' => false,
                        'message' => "Erro ao tentar atualizar valor da venda: $id_venda após inserir um item",
                        'tipo' => 'Venda',
                        'id' => $id_venda
                    );
            }

            /*insere a parcela da venda*/
            $sql_insert = "
            INSERT INTO db_gol.tb_parcela(
                id_empresa,   id_venda,     id_parcela,   dt_venc,    vr_parcela
            )VALUES(
                $id_empresa,  $id_venda,  '1',      '$dt_vcto_parc',  '$vr_total_venda'
            )";

            try{
                $this->execQuery($conn, $sql_insert, $id_empresa, 1);
            }catch(Throwable $e){
                return array(
                        'success' => false,
                        'message' => "Erro ao tentar gerar parcela na venda: $id_venda",
                        'tipo' => 'Venda',
                        'id' => $id_venda
                    );
            }

            return array(
                    'success' => true,
                    'message' => "Itens inseridos com sucesso na venda: $id_venda",
                );

        }

}

?>
