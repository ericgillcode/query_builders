<?php 
/* 
    Class used to automate a DB2 query via lists
    Can be used with either the ODBC Functional Library or PDO

    10-12-24: 
    - Added order by Function
    - Enclosed WHERE in a SQL List for future additional criteria

    10-13-24:
    - Updated create_table_index to:
        - use the array keys stored with set_fields_and_lists
        - Skip after adding to the table index if the value is not in the select list

    10-14-24: Updated select_fields to also check the left joins table for the previous table

    [!!!!]__Major Revision__[!!!!]
    10-20-24: Re-worked create_where_clause to include negative checks, comparison operators, and wildcards

    10-22-24: Updated Table Index Identifiers to use 'T' with the count of tableIndex list + 1


*/
class rmsquery  {
    function set_fields_and_lists ( $list ) {
        // Must be passed as FIELD => TABLE ie RMSACCTNUM => RMSPMASTER
        $this->fields_and_lists = $list;

    }

    function set_selections ($list) {
        //can be a standard PHP array, list of records to select / display
        $this->fields = $list;

    }

    function create_table_index () {
        $rmsFields = $this->fields_and_lists;
        $selections = $this->fields;
        //Creates and labels tables ie RMSPMASTER A or RMSPEXTDTL B

        $tableIndex = [];
        $fields = [];
        foreach ($selections as $f) {
            $table = $rmsFields[$f];
            $identifier = NULL;
            if (array_key_exists($table, $tableIndex) === FALSE) {
                $identifier = 'T' . count($tableIndex) + 1;
                $tableIndex[$table] = $identifier;

            } else {
                $identifier = $tableIndex[$table];
            }

            if (!in_array($f, $selections)) {
                continue;
            }
        
            $fields[] = $identifier.'.'.$f;
        }
        $this->table_index = $tableIndex;
        $this->rms_fields_list = $fields;

    }

    function left_joins_table ($list) {
        $this->left_joins_table = $list;
    }

    function select_fields () {
        $joins = $this->left_joins_table ;
        $tableIndex = $this->table_index ;
        $tableSelections = [];
        foreach ($tableIndex as $table => $identifier) {
            if (array_search($table, array_keys($tableIndex)) === 0 ) {
                $tableSelections[] = sprintf("FROM RMSPHBOBJ.%s %s", $table, $identifier);
                continue;
            }
            $prevTable = array_keys($tableIndex)[0];//Always joins to RMSPMASTER
            
            $prevTableIdentifier = $tableIndex[$prevTable];
            if (array_key_exists($table, $joins) === TRUE || array_key_exists($prevTable, $joins) === TRUE ) {
                $string = 'LEFT';
            } else {
                $string = 'INNER';
            }
        
            $string .= sprintf( ' JOIN RMSPHBOBJ.%s %s ON %s.RMSFILENUM = ', $table, $identifier, $prevTableIdentifier );
        
            if (array_key_exists($table, $joins) === TRUE) {
                $joinValues = $joins[$table];
        
                $string .= sprintf("%s.%s AND %s.%s = '%s' ", $identifier, $joinValues[0], $identifier, $joinValues[1], $joinValues[2] );
            } else {
                $string .= "$identifier.RMSFILENUM ";
            }
            $tableSelections[] = $string;
        
        }

        $this->select_statement = 'SELECT ' . implode(', ', $this->rms_fields_list  ) . ' ' . implode(' ', $tableSelections);
    }

    function create_where_clause ($where_values_map) {
        /*
            Passed lists must be like:
            $where_values_map =
                [
                    [
                    'FIELD' => 'RMSOFFCRCD',
                    'VALUES' => ['CAV', 'PHR'] ,
                    'RANGE' => False,
                    'EQUALS' => True,
                    'WILDCARD' => True ,
                    'SPLIT' => 'OR',
                    ],
                    
                ] ;
        */
        $rmsFields = $this->fields_and_lists;
        $tableIndex = $this->table_index ;

        $where = [];
        foreach ($where_values_map as $key => $list) {
            $field = $list['FIELD'];
            $table = $rmsFields[$field];
            $identifier =  $tableIndex[$table];
            $shelf = [];
            $tmp = [];
        
            /* Date Range Block */
            if ($list['RANGE'] === TRUE ) {
                $shelf[] = sprintf("(%s.%s BETWEEN '%s' AND '%s')", $identifier, $field, $list['VALUES'][0], $list['VALUES'][1], );
            }
        
            /* Comparison Query Block */
            if (array_key_exists('OPERATOR', $list) === TRUE ) {
                foreach ($list['VALUES'] as $value) {
                    $tmp[] = sprintf(" (%s.%s %s %s) ", $identifier, $field, $list['OPERATOR'], $value );
                }
                $shelf[] = '( ' . implode($list['SPLIT'], $tmp) . ' )';
            }
        
            /*Wildcard and Non-Wildcard Query Block */
            if (array_key_exists('WILDCARD', $list) === TRUE ) {
                if ($list['WILDCARD'] === TRUE) {
                    if ($list['EQUALS'] === TRUE) {
                        $operator = 'LIKE';
                    } else {
                        $operator = 'NOT LIKE';
                    }
                    foreach ($list['VALUES'] as $value) {
                        $tmp[] = sprintf(" %s.%s  %s '%s' ", $identifier, $field, $operator, $value);
                    }
            
                } else {
        
                    if ($list['EQUALS'] === TRUE) {
                        $operator = 'IN';
                    } else {
                        $operator = 'NOT IN';
                    }
                    $comparison_shelf = [];
                    foreach ($list['VALUES'] as $value ) {
                        $comparison_shelf[] = "'$value'";
                    }
                    $tmp[] = sprintf("(%s.%s %s (%s)) ", $identifier, $field, $operator, implode(', ', $comparison_shelf));
                    $comparison_shelf = null;
        
                }
        
        
                $shelf[] = '( ' . implode($list['SPLIT'] ,$tmp) . ' )' ;
                $operator = NULL;
        
        
            }
        
            $tmp = NULL;
            foreach ($shelf as $s) {
                $where[] = $s;
            }
        
        
        
        }
        
        $sql =  sprintf(" WHERE ( %s ) ", implode(' AND ', $where) );
        $this->where_clauses = $sql ;

    }

    function create_order_by_list ($list) {
        $rmsFields = $this->fields_and_lists;
        $tableIndex = $this->table_index ;

        $sql = ' ORDER BY ';
        foreach ($list as $order_field => $sort_flag) {
            $sql .= sprintf( "%s.%s ", $tableIndex[  $rmsFields[$order_field] ], $order_field );
            if (ctype_alpha($sort_flag)) {
                $sql .=  $sort_flag . ' ' ;
            } 
            if ($order_field !== array_key_last($list) ) {
                $sql .= ', ';
            }
        
        }
        $this->order_by_list = $sql ;
        
    }

    function create_query () {
        $order_by = $this->order_by_list ;
        $where_clauses = $this->where_clauses;
        $select = $this->select_statement ;

        return $select .' '. $where_clauses . ' ' . $order_by;
    }


}


?>
