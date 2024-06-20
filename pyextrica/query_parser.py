from sql_metadata import Parser
import re

__all__ = ["QueryParser"]

class QueryParser:
    
    #Check if query is for select tables
    def parse_query(self, query, platform):
        if query.strip().lower().startswith("select"):
            query = ' '.join(query.split())
            copatibility_query = ['select 1', 'select 2', 'select 3']
            if query.lower() in copatibility_query:
                return True
            else:
                return False
        else:
            return False

        
    def remove_schema_from_query(self, _query, platform):
        parsed_query = Parser(_query)
        if platform == "data_products" and parsed_query.tables:
            for i, table in enumerate(parsed_query.tables):
                parts = table.split('.')
                if len(parts) == 2:
                    part_1 = parts[0]
                    part_2 = parts[1]
                    pattern_1 = re.compile(rf'"{re.escape(part_1)}"\."{re.escape(part_2)}"')
                    pattern_2 = re.compile(rf'"{re.escape(part_1)}"\.{re.escape(part_2)}')
                    pattern_3 = re.compile(rf'{re.escape(part_1)}\."{re.escape(part_2)}"')
                    pattern_4 = re.compile(rf'{re.escape(part_1)}\.{re.escape(part_2)}')

                    _query = pattern_1.sub(part_2, _query)
                    _query = pattern_2.sub(part_2, _query)
                    _query = pattern_3.sub(part_2, _query)
                    _query = pattern_4.sub(part_2, _query)
                    parsed_query.tables[i] = part_2
            return _query
        else:
            return _query
        
    def concat_parts(self, part_1, part_2):
        if part_1.startswith('"') and part_2.startswith('"'):
            return f'{part_1}.{part_2}'
        elif part_1.startswith('"') and not part_2.startswith('"'):
            return f'{part_1}{part_2}'
        elif not part_1.startswith('"') and part_2.startswith('"'):
            return f'{part_1}.{part_2}'
        else:
            return f'{part_1}.{part_2}'

    def parse_where_clause(self, parsed_query):
        key_values = {}
        for token in parsed_query.tokens:
            
            if token.last_keyword == "WHERE" and token.value == "=":
                value = token.next_token.value
                if value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                key_values[token.previous_token.value] = value
        
        return key_values
