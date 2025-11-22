<?php

include_once('../defineVariavelAmbiente.php');

if (!class_exists('Conexao')) {
  $var_caminho_con = CAMINHO_INSTALACAO . '/_lib/nfephp/config/conexao.php';
  require_once($var_caminho_con);
}

function configuraServidor()
{

  $arrayServidor = array(
    'https://gestor01.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor05.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor06.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor07.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor08.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor10.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor11.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor12.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor13.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor14.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor15.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor16.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor17.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor18.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor19.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor20.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor21.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor23.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor24.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor25.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php',

    'https://gestor26.sempretecnologia.com.br/includes/geraMensalidadeAutomatica/index.php'
  );


  if($_SERVER['SERVER_NAME'] == 'srvdsv3.axmsolucoes.com.br'){
    $arrayServidor = array('https://srvdsv3.axmsolucoes.com.br/scriptcase915/app/GOL/includes/geraMensalidadeAutomatica/index.php');
  }

  echo 'Servidores: <pre>';
  print_r($arrayServidor);
  echo '</pre>';

  $resultados = curlMultiplasURLS($arrayServidor);

  echo '<pre>';
  print_r($resultados);
  echo '</pre>';

}

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


configuraServidor();
