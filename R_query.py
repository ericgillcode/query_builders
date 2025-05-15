""" 
    11-27-24: Python version of PHP based Query Builder
"""

class r_query() :

    def set_fields_and_tables(self, array) :
        ''' 
            Pass a dict of field => Table values of ALL fields for query, even if they
            are not in the SELECT criteria
        '''
        self.fields_and_tables = array
    
    def set_joins_tables(self, array):
        ''' 
            Pass a dict of tables that require specific joins via RMSFILENUM and record types
        '''
        self.joins = array
    
    def set_selected_fields(self, array):
        #List of fields for SELECT criteria
        self.selections = array 
    
    def create_table_index_selections_and_fields(self):
        # Creates a master index through a DICT of all tables and fields to be used and their table identifiers
        rms_tables = self.fields_and_tables
        table_index = {}
        fields = {}
        selected_fields = []
        selections = self.selections

        for field in rms_tables:
            table = rms_tables[field]
            identifier = None 
            if table not in table_index.values():
                identifier = 'T' + str(len(table_index)+1)
                table_index['%s' %identifier] = table

            else: 
                identifier = [i for i in table_index if table_index[i] == table][0]
            fields[field] = identifier
            if field in selections:
                selected_fields.append('%s.%s' % (identifier, field))

        self.table_index = table_index
        self.fields = fields 
        self.selected_fields = selected_fields

    def create_where_clause (self, where_list):
        # Assemble where clause
        fields = self.fields 

        where_clauses = {}
        WHERE = []
        for array in where_list:
            #print(array)
            where_clauses[len(where_clauses) +1] = array
            #break
        where_list = None
        for key, array in where_clauses.items():
            #break
            tmp = []
            shelf = []
            rms_field = array['FIELD']
            identifier = fields[ rms_field ]
            query_field = identifier + '.' + rms_field

            if array['RANGE'] == True:
                WHERE.append( "(%s BETWEEN '%s' AND '%s')" % (query_field, array['VALUES'][0], array['VALUES'][1]) )
                continue
                    
            if 'OPERATOR' in array.keys():
                for value in array['VALUES']:
                    tmp.append( '(%s %s %s)' % (query_field, array['OPERATOR'], value ) )
                shelf.append( '(%s)' % array['SPLIT'].join(tmp) )
            
            if 'WILDCARD' in array.keys():
                if array['WILDCARD'] == True: 
                    if array['EQUALS'] == True: 
                        operator = 'LIKE'
                    else: 
                        operator = 'NOT LIKE'
                    for value in array['VALUES']:
                        tmp.append( " %s %s '%s' " %(query_field, operator, value) )
                else: 
                    if array['EQUALS'] == True:
                        operator = 'IN'
                    else: 
                        operator = 'NOT IN'
                    comparison_shelf = []
                    for value in array['VALUES']:
                        comparison_shelf.append("'%s'" %value)
                    tmp.append( "%s %s (%s)" % (query_field, operator, ', '.join(comparison_shelf)) )
                    comparison_shelf = None
            shelf = ' (%s) ' % (array['SPLIT'].join(tmp))
            operator = None 
            tmp = None 
            WHERE.append(shelf)
            shelf = None
        where_clauses = None
        #print(WHERE)
        self.where = 'WHERE ( %s )' % ' AND '.join(WHERE)
        WHERE = None
    
    def create_order_by_list (self, array) :
        # Create Order list
        fields = self.fields 
        shelf = []
        for field, sort in array.items():
            identifier = fields[field]
            shelf.append('%s.%s %s' % (identifier, field, sort) )
        self.order_by = ' ORDER BY ' + ', '.join(shelf)
        shelf = None
    
    def create_query(self) :
        # Finish and create the query. Query is returned as text string
        table_index = self.table_index
        fields = self.fields 
        order_by = self.order_by
        selected_fields = self.selected_fields
        joins = self.joins
        where = self.where

        master_identifier = list(table_index)[0] #Always join to the master table 
        master = table_index[master_identifier] 
        sql = 'SELECT ' + ', '.join(selected_fields)

        for tableID in table_index: 
            #continue
            table = table_index[tableID]
            #identifier = table_index[table]

            if table == master:
                sql += ' FROM RMSPHBOBJ.%s %s ' % (table, tableID)
                continue 
            #print(table)
            
            if table in list(joins): 
                string = 'LEFT'
            else: 
                string = 'INNER'
            
            string += ' JOIN RMSPHBOBJ.%s %s ON %s.RMSFILENUM = ' % (table, tableID, master_identifier)

            if table in list(joins):
                string += "%s.%s AND %s.%s = '%s' " % (tableID, joins[table][0], tableID, joins[table][1], joins[table][2]   )
            else:
                string += "%s.RMSFILENUM " % (tableID)
            sql += string
        
        for statement in [where, order_by]:
            sql += statement
        return sql
"""
EXAMPLE: 

pq = r_query()
rms_tables = {
    'RMSACCTNUM' : 'RMSPMASTER',
    'RMSACCNTST' : 'RMSPMASTER',
    'RMSRECVRCD' : 'RMSPMASTER',
    'RMSOFFCRCD' : 'RMSPPRDBAL',
    'RMSLOANCD' : 'RMSPMASTER',
    'RMSALPHA1' : 'RMSPEXTDTL',
}
pq.set_fields_and_tables(rms_tables)

joins = {
    'RMSPLEGAL' : [ 'RMSFILENUM', 'RECORDTYPE', 'A' ],
    'RMSPEXTDTL' : [ 'RMSFILENUM', 'RECORDTYPE', 'RF' ],

}

selections = ['RMSACCTNUM', 'RMSACCNTST', 'RMSOFFCRCD']

pq.set_joins_tables(joins)
pq.set_selected_fields(selections)
pq.create_table_index_selections_and_fields()

where = [
    {
        'FIELD' : 'RMSALPHA1',
        'VALUES' : ['JUD'],
        'WILDCARD' : False,
        'RANGE' : False,
        'EQUALS' : True, 
        'SPLIT' : 'OR',
    },

    {
        'FIELD' : 'RMSACCNTST',
        'VALUES' : ['1B%', '1P%'],
        'WILDCARD' : True,
        'RANGE' : False,
        'EQUALS' : True, 
        'SPLIT' : 'OR',
    },

    {
        'FIELD' : 'RMSACCNTST',
        'VALUES' : ['DCE', 'DCF'],
        'WILDCARD' : False,
        'RANGE' : False,
        'EQUALS' : True, 
        'SPLIT' : 'AND',
    },
]

pq.create_where_clause(where)

order_by = {
    'RMSOFFCRCD' : 'DESC',
    'RMSACCNTST' : '',
}

pq.create_order_by_list(order_by)
pq.create_query()
"""
