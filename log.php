<?php

if (!class_exists('Conexao')) {
	$var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
	require_once($var_caminho_con);
}

//Função para realizar a inserção dos logs
function fc_gera_log($id_empresa, $id_cobranca_mensalidade, $id_passo, $lo_sucesso, $tx_descricao, $id_registro=false, $tx_tipo_registro=false)
{

	//echo 'Descrição: '.$tx_descricao.'<hr>';

	$conexao = new Conexao();
	$conn = $conexao->open(dbNameCob($id_empresa));

	$sql_insert = "
		INSERT INTO db_gol.tb_log_mensalidade_automatica(id_empresa, id_cobranca_mensalidade, id_log, id_passo, lo_sucesso, tx_descricao".($id_registro?', id_registro':'').($tx_tipo_registro?', tx_tipo_registro':'').")
		VALUES($id_empresa, $id_cobranca_mensalidade, (
			SELECT COALESCE(MAX(id_log), 0)+ 1 
			FROM db_gol.tb_log_mensalidade_automatica 
			), $id_passo, '$lo_sucesso', '$tx_descricao'".($id_registro?", '".$id_registro."'":"").($tx_tipo_registro?", '".$tx_tipo_registro."'":"")."); 
	";

	//echo "Insert: $sql_insert<hr>";

	if (!pg_query($conn, $sql_insert)) {
		return [
			'success' => false,
			'message' => 'Erro ao tentar inserir o log!'
		];
	}

	return [
		'success' => true,
		'message' => 'Log inserido com sucesso!'
	];
}

//Função para buscar todos os logs de determinada cobrança divididos por passos
function fc_busca_log($id_empresa, $id_cobranca_mensalidade)
{

	$conexao = new Conexao();
	$conn = $conexao->open(dbNameCob($id_empresa));

	$sql_search = "
		SELECT id_log, id_passo, lo_sucesso, tx_descricao, id_registro, tx_tipo_registro
		FROM db_gol.tb_log_mensalidade_automatica
		WHERE id_empresa = $id_empresa
		AND id_cobranca_mensalidade = $id_cobranca_mensalidade
		ORDER BY id_passo, id_log 
	";

	$ret = pg_query($conn, $sql_search);

	if (!$ret) {
		return [
			'success' => false,
			'message' => 'Erro ao tentar buscar os logs!'
		];
	}

	$logs = pg_fetch_all($ret);
	$passos_logs = [];

	foreach ($logs as $log) {
		$passos_logs[(intval($log['id_passo']) - 1)][$log['id_log']] = $log;
	}
	return [
		'success' => true,
		'message' => 'logs encontrados com sucesso!',
		'data' => $passos_logs
	];
}
