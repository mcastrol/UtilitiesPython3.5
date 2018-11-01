<?php
/**
 * Created by IntelliJ IDEA.
 * User: marcela
 * Date: 1/11/18
 * Time: 11:57
 */




/*
 * Generic functions
 */



function getDbConnParam($host,$port,$database,$user,$password){


    $conn_string = "host=".$host." dbname=".$database." port=".$port." user=".$user." password=".$password;
    print($conn_string);
    $conn = pg_pconnect($conn_string);

    $stat = pg_connection_status($conn);
    if ($stat === PGSQL_CONNECTION_OK) {
        echo 'Estado de la conexión ok';
    } else {
        echo 'No se ha podido conectar';
    }

    print("connection ".$conn."\n");
    if ($conn == false){
        print ("No connection to db");
        exit(0);
    }

    print ("conection to db ok\n");

    print "connection to ".$host." user: ".$user." ok\n";
    return $conn;
}


function executeDBSentence($conn, $query)
{
    print "\nExecuting sentence ".$query."\n\n";


    $r = pg_exec_exec($conn, $query);
    if (!$r)
    {
        print("pg_ERROR: ".pg_errormessage($conn)."\n");
        trigger_error(htmlentities(odbc_error($conn), ENT_QUOTES), E_USER_ERROR);
    }
    print "$r"."\n";
    print(pg_num_rows($r)." rows \n");
    return($r);
}


function executeDBQuery($conn, $query)
{
    print "\nExecuting DB query ".$query."\n\n";
    $res = pg_query($conn, $query);

    if (!$res)
    {
        print("pg_ERROR: ".pg_errormessage($conn)."\n");
        trigger_error(htmlentities(pg_errormessage($conn), ENT_QUOTES), E_USER_ERROR);
    }

    $toReturn=pg_fetch_assoc($res);

    return($toReturn);
}

function executeDBQueryKey($conn, $query,$keydef)
{
    print "\nExecuting sentence ".$query."\n\n";
    $res = pg_exec($conn, $query);
    if (!$res)
    {
        print("pg_ERROR: ".pg_errormessage($conn)."\n");
        trigger_error(htmlentities(pg_errormessage($conn), ENT_QUOTES), E_USER_ERROR);
    }
    $toReturn = array();

    $k = 0;
    $j = 0;
    //print_r($keydef);
    $tamkey=sizeof($keydef);
    $ar = [];


    $hayfilas=true;
    while($hayfilas)  {

        print("fetching row ");

        $arrayKey = "";
        $haycol=false;
        for ($j = 0; $j < pg_num_fields($res); $j++)
        {

            $field_name = pg_field_name($res, $j);
            $val=pg_result($res, $field_name);
            print("fetching fields ".$field_name." ".$val."\n");
            if($j==0) {
                //print("$j=$j tamkey: ".$tamkey."\n");
                $arrayKey = $val;
                $haycol=true;

            }
            elseif($j<$tamkey) {
                //print("$j=$j tamkey: ".$tamkey."\n");
                $arrayKey = $arrayKey. "|" .$val;
            }
            else
            {
                //print("$j=$j tamkey: ".$tamkey."\n");
                $ar[strtoupper($field_name)] = $val;
                //print($field_name.":" .$ar[strtoupper($field_name)]."\n");
            }
        }

        if($haycol) $toReturn[$arrayKey] = $ar;
        print_r($toReturn);
        print("$k=$k\n");
        $k++;
        $fila=pg_fetch_row($res);
        print_r($fila);
        if(!$fila) $hayfilas=false;

    }

    print "$res"."\n";
    print($k." rows \n");
    print_r($toReturn);
    return($toReturn);
}



$config_ini = parse_ini_file("config.ini", true);

$Server=$config_ini['Postgress']['Server'];
$Database=$config_ini['Postgress']['Database'];
$Port=$config_ini['Postgress']['Port'];
$user=$config_ini['Postgress']['user'];
$password=$config_ini['Postgress']['password'];


$rd=getDbConnParam($Server,$Port,$Database,$user,$password);

if($rd) {
     $arrayResult= executeDBQuery($rd, "select * from shop_portfolio_shoes where shop_id=18763603 and to_char(created_at,'YYYYMMDD')='20181101'");
     print_r($arrayResult);

    $keydef=array('shop_id','created_at');
    $query="SELECT SHOP_ID, TO_CHAR(CREATED_AT,'YYYYMMDD') AS CREATED_AT, COUNT(*)
        from shop_portfolio_shoes where to_char(created_at,'YYYYMMDD')='20181101' 
        GROUP BY SHOP_ID, TO_CHAR(CREATED_AT,'YYYYMMDD')";
    $arrayImps=executeDBQueryKey($rd, $query,$keydef);

}
else print("No connection\n");




