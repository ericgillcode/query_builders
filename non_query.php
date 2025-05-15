<?php 
/*
    Returns a basic UPDATE or DELETE statement
*/
function basic_non_query ( $fields_to_update, $where, $action, $table ) {
    $actions = [
        'U' => 'UPDATE',
        'D' => 'DELETE',
    ];

    $action = strtoupper($action);
    $query_items = [];
    if ($actions[$action] == 'UPDATE') {
        $query_items[] = $actions[$action] . ' ' . $table . ' SET ';
        $tmp = [];
        foreach ($fields_to_update as $field => $value) {
            $tmp[] = "$field = '$value'";
        }
        $query_items[] = implode(', ', $tmp);
        $tmp = NULL;
    } else {
        $query_items[] = $actions[$action] . ' FROM ' . $table .' ';
    }
    $tmp = [];
    foreach ($where as $field => $value) {
        $tmp[] = "(TRIM(UPPER($field)) = '$value')";
    }
    $query_items[] = ' WHERE ( ' . implode(' AND ', $tmp) . ' )';
    $tmp = NULL;
    
    return implode('', $query_items);
    $query_items = NULL;

}

?>
