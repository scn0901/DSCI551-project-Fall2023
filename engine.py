import json
from typing import List, Callable, Dict, Union
import os
import shutil
from database import Database



class Engine:
    def __init__(self) -> None:
        self.current_database = None



#######################   database start   #########################


    def create_database(self, database_name: str) -> None:
        '''
        Create a new database.

        Args:
            database_name: str, the name of the database to be created.
        
        Returns:
            None.
        '''
        os.mkdir(os.path.join('databases', database_name))
        with open(os.path.join('databases', database_name, 'metadata.jsonl'), 'w') as f:
            line = json.dumps({}) + '\n'
            f.write(line)


    def drop_database(self, database_name: str) -> None:
        '''
        Drop an existing database.

        Args:
            database_name: str, the name of the database to be dropped.
        
        Returns:
            None.
        '''
        shutil.rmtree(os.path.join('databases', database_name))


    def show_database_names(self) -> None:
        '''
        Show available database names.

        Args:
            None.
        
        Returns:
            None.
        '''
        database_names = os.listdir('databases')
        for database_name in database_names:
            print(database_name)


    def use_database(self, database_name: str) -> None:
        '''
        Specify the database to use.

        Args:
            database_name: str, the name of the database to use.
        
        Returns:
            None.
        '''
        self.current_database = Database(database_name)


#######################   database end   #########################



#######################   interaction start   #########################


    def run(self) -> None:
        if not os.path.exists('databases'):
            os.mkdir('databases')
        if not os.path.exists('tmp'):
            os.mkdir('tmp')
        while True:
            response = input('>>>Chenning_DBMS: Please enter which function about database do you want to use? Options include: "1" for "create database", "2" for "drop database", "3" for "show database names", "4" for "use database". Enter "exit" to finish process.\nYour input: ')
            if response == 'exit':
                shutil.rmtree('tmp')
                return
            self.parser_database(response)


    def parser_database(self, response: str) -> None:
        if response == '1':
            response = input('>>>Chenning_DBMS: Please enter the name of the database you want to create. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            database_name = response
            self.create_database(database_name)

        elif response == '2':
            response = input('>>>Chenning_DBMS: Please enter the name of the database you want to drop. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            database_name = response
            self.drop_database(database_name)

        elif response == '3':
            self.show_database_names()

        elif response == '4':
            response = input('>>>Chenning_DBMS: Please enter the name of the database you want to use. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            database_name = response
            self.current_database = Database(database_name)
            response = input('>>>Chenning_DBMS: Please enter which function about table do you want to use? Options include: "1" for "create table", "2" for "drop table", "3" for "show table names", "4" for "use table". Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            self.parser_table(response)

    
    def parser_table(self, response: str) -> None:
        if response == '1':
            response = input('>>>Chenning_DBMS: Please enter the name of the table you want to create. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            table_name = response
            response = input('>>>Chenning_DBMS: Please enter the field-data_type pairs of the table you want to create. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            field_data_type_pairs = eval(response)
            self.current_database.create_table(table_name, field_data_type_pairs)

        elif response == '2':
            response = input('>>>Chenning_DBMS: Please enter the name of the table you want to drop. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            table_name = response
            self.current_database.drop_table(table_name)

        elif response == '3':
            self.current_database.show_table_names()

        elif response == '4':
            response = input('>>>Chenning_DBMS: Please enter which function about record do you want to use? Options include: "1" for "insert record", "2" for "update record", "3" for "delete record", "4" for "query record". Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            self.parser_record(response)


    def parser_record(self, response: str) -> None:
        if response == '1':
            response = input('>>>Chenning_DBMS: Please enter the name of the table you want to insert record. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            table_name = response
            response = input('>>>Chenning_DBMS: Please enter the record want to insert. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            record = eval(response)
            record = self.convert_data_type(table_name, record)
            self.current_database.insert_record(table_name, record)        

        elif response == '2':
            response = input('>>>Chenning_DBMS: Please enter the name of the table you want to update record. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            table_name = response
            response = input('>>>Chenning_DBMS: Please enter the field-value pairs want to update. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            field_value_pairs = eval(response)
            field_value_pairs = self.convert_data_type(table_name, field_value_pairs)
            response = self.parser_conditions('>>>Chenning_DBMS: Please enter conditions one at a time to filter records that you want to update. Enter "stop" to stop adding conditions. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            conditions = response
            self.current_database.update_record(table_name, field_value_pairs, conditions)

        elif response == '3':
            response = input('>>>Chenning_DBMS: Please enter the name of the table you want to delete record. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            table_name = response
            response = self.parser_conditions('>>>Chenning_DBMS: Please enter conditions one at a time to filter records that you want to delete. Enter "stop" to stop adding conditions. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            conditions = response
            self.current_database.delete_record(table_name, conditions)

        elif response == '4':
            self.parser_query()


    def parser_query(self) -> None:
        # read_table
        response = input('>>>Chenning_DBMS: Please enter the name of the table you want to query record. Enter "exit" to return to main menu.\nYour input: ')
        if response == 'exit':
            return
        table_name = response
        current_table = self.current_database.read_table(os.path.join('databases', self.current_database.database_name, table_name+'.jsonl'))
        
        # cross_product
        response = input('>>>Chenning_DBMS: Do you want to do cross product? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.\nYour input: ')
        if response == 'exit':
            return
        elif response == 'y':
            while True:
                response = input('>>>Chenning_DBMS: Please enter the name of the table you want to do cross product with. Enter "stop" to stop doing cross product. Enter "exit" to return to main menu.\nYour input: ')
                if response == 'exit':
                    return
                elif response == 'stop':
                    break
                else:
                    table_name = response
                    new_table = self.current_database.read_table(os.path.join('databases', self.current_database.database_name, table_name+'.jsonl'))
                    current_table = self.current_database.cross_product(current_table, new_table)
        elif response == 'n':
            pass
        
        # theta_inner_join
        response = input('>>>Chenning_DBMS: Do you want to do theta inner join? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.\nYour input: ')
        if response == 'exit':
            return
        elif response == 'y':
            while True:
                response = input('>>>Chenning_DBMS: Please enter the name of the table you want to do theta inner join with. Enter "stop" to stop doing theta inner join. Enter "exit" to return to main menu.\nYour input: ')
                if response == 'exit':
                    return
                elif response == 'stop':
                    break
                else:
                    table_name = response
                    new_table = self.current_database.read_table(os.path.join('databases', self.current_database.database_name, table_name+'.jsonl'))
                    response = self.parser_conditions('>>>Chenning_DBMS: Please enter conditions one at a time to decide how to do theta inner join. Enter "stop" to stop adding conditions. Enter "exit" to return to main menu.\nYour input: ')
                    if response == 'exit':
                        return
                    conditions = response
                    current_table = self.current_database.theta_inner_join(current_table, new_table, conditions)
        elif response == 'n':
            pass
        
        # select
        response = input('>>>Chenning_DBMS: Do you want to do selection? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.\nYour input: ')
        if response == 'exit':
            return
        elif response == 'y':
            response = self.parser_conditions('>>>Chenning_DBMS: Please enter conditions one at a time to select records that you want. Enter "stop" to stop adding conditions. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            conditions = response
            current_table = self.current_database.select(current_table, conditions)
        elif response == 'n':
            pass
        
        # group_by_and_aggregate
        response = input('>>>Chenning_DBMS: Do you want to do grouping by and aggregation? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.\nYour input: ')
        if response == 'exit':
            return
        elif response == 'y':
            response = input('>>>Chenning_DBMS: Please enter all fields that you want to group by, using white space to separate fields. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            group_by_fields = response.split()
            response = input('>>>Chenning_DBMS: Please enter all fields that you want to aggregate, using white space to separate fields. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            aggregate_fields = response.split()
            aggregate_field_aggregate_function_pairs = {}
            for aggregate_field in aggregate_fields:
                response = input(f'>>>Chenning_DBMS: Please enter the lambda function that you want to use to aggregate "{aggregate_field}". Enter "exit" to return to main menu.\nYour input: ')
                if response == 'exit':
                    return
                aggregate_function = eval(response)
                aggregate_field_aggregate_function_pairs[aggregate_field] = aggregate_function
            current_table = self.current_database.group_by_and_aggregate(current_table, group_by_fields, aggregate_field_aggregate_function_pairs)
        elif response == 'n':
            pass

        # project
        response = input('>>>Chenning_DBMS: Do you want to do projection? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.\nYour input: ')
        if response == 'exit':
            return
        elif response == 'y':
            response = input('>>>Chenning_DBMS: Please enter all fields that you want to remain, using white space to separate fields. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            fields = response.split()
            current_table = self.current_database.project(current_table, fields)
        elif response == 'n':
            pass

        # sort_merge
        response = input('>>>Chenning_DBMS: Do you want to do sorting using sort merge algorithm? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.\nYour input: ')
        if response == 'exit':
            return
        elif response == 'y':
            response = input('>>>Chenning_DBMS: Please enter the field that you want to sort by. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            sort_field = response
            response = input('>>>Chenning_DBMS: Please enter whether you want to sort in ascending order or not. Enter "a" for ascending order. Enter "d" for descending order. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            elif response == 'a':
                response = input('>>>Chenning_DBMS: Please enter the chunk size when using sort merge algorithm. Enter "exit" to return to main menu.\nYour input: ')
                if response == 'exit':
                    return
                chunk_size = int(response)
                current_table = self.current_database.sort_merge(current_table, sort_field, True, chunk_size)
            elif response == 'd':
                response = input('>>>Chenning_DBMS: Please enter the chunk size when using sort merge algorithm. Enter "exit" to return to main menu.\nYour input: ')
                if response == 'exit':
                    return
                chunk_size = int(response)
                current_table = self.current_database.sort_merge(current_table, sort_field, False, chunk_size)
        elif response == 'n':
            pass

        # show
        response = input('>>>Chenning_DBMS: Do you want to show the query result? Enter "y" for yes. Enter "n" for no. Enter "exit" to return to main menu.\nYour input: ')
        if response == 'exit':
            return
        elif response == 'y':
            response = input('>>>Chenning_DBMS: Please enter the number of records that you want to show. Enter "all" to show all records. Enter "exit" to return to main menu.\nYour input: ')
            if response == 'exit':
                return
            elif response == 'all':
                self.current_database.show(current_table)
            else:
                head_n = int(response)
                self.current_database.show(current_table, head_n)
        elif response == 'n':
            pass


#######################   interaction end   #########################



#######################   tool start   #########################


    def convert_data_type(self, table_name: str, field_value_pairs: Dict[str, Union[int, float, bool, str]]) -> Dict[str, Union[int, float, bool, str]]:
        '''
        Convert data type according to table's schema.

        Args:
            table_name: str, the name of the table that the field-value pairs belongs to.
            field_value_pairs: Dict[str, Union[int, float, bool, str]], the field-value pairs that need to be converted.
        
        Returns:
            field_value_pairs_convert_data_type: Dict[str, Union[int, float, bool, str]], the field-value pairs that have been converted.
        '''
        table_name_field_data_type_pairs_pairs = self.current_database.table_name_field_data_type_pairs_pairs
        field_data_type_pairs = table_name_field_data_type_pairs_pairs[table_name]
        field_value_pairs_convert_data_type = {}
        for field, value in field_value_pairs.items():
            data_type = field_data_type_pairs[field]
            if data_type == 'int':
                value_convert_data_type = int(value)
            elif data_type == 'float':
                value_convert_data_type = float(value)
            elif data_type == 'bool':
                value_convert_data_type = bool(value)
            elif data_type == 'str':
                value_convert_data_type = str(value)
            field_value_pairs_convert_data_type[field] = value_convert_data_type
        return field_value_pairs_convert_data_type


    def parser_condition(self, response: str) -> Callable:
        '''
        Parse a condition and convert it into a lambda function.

        Args:
            response: str, the condition string.
        
        Returns:
            condition: Callable, the converted lambda function.
        '''
        if '==' in response:
            left, right = response.split('==')
            left = left.strip()
            right = right.strip()
            try:
                left = eval(left)
                return lambda x: left == x[right]
            except:
                try:
                    right = eval(right)
                    return lambda x: x[left] == right
                except:
                    return lambda x: x[left] == x[right]
        elif '!=' in response:
            left, right = response.split('!=')
            left = left.strip()
            right = right.strip()
            try:
                left = eval(left)
                return lambda x: left != x[right]
            except:
                try:
                    right = eval(right)
                    return lambda x: x[left] != right
                except:
                    return lambda x: x[left] != x[right]
        elif '>=' in response:
            left, right = response.split('>=')
            left = left.strip()
            right = right.strip()
            try:
                left = eval(left)
                return lambda x: left >= x[right]
            except:
                try:
                    right = eval(right)
                    return lambda x: x[left] >= right
                except:
                    return lambda x: x[left] >= x[right]
        elif '<=' in response:
            left, right = response.split('<=')
            left = left.strip()
            right = right.strip()
            try:
                left = eval(left)
                return lambda x: left <= x[right]
            except:
                try:
                    right = eval(right)
                    return lambda x: x[left] <= right
                except:
                    return lambda x: x[left] <= x[right]
        elif '>' in response:
            left, right = response.split('>')
            left = left.strip()
            right = right.strip()
            try:
                left = eval(left)
                return lambda x: left > x[right]
            except:
                try:
                    right = eval(right)
                    return lambda x: x[left] > right
                except:
                    return lambda x: x[left] > x[right]
        elif '<' in response:
            left, right = response.split('<')
            left = left.strip()
            right = right.strip()
            try:
                left = eval(left)
                return lambda x: left < x[right]
            except:
                try:
                    right = eval(right)
                    return lambda x: x[left] < right
                except:
                    return lambda x: x[left] < x[right]


    def parser_conditions(self, hint: str) -> Union[List[Callable], str]:
        '''
        Parse conditions and convert them into lambda functions.

        Args:
            hint: str, the hint when asking user to enter condition.
        
        Returns:
            conditions / "exit": Union[List[Callable], str], the converted lambda functions / "exit" signal.
        '''
        conditions = []
        while True:
            response = input(hint)
            if response == 'exit':
                return 'exit'
            elif response == 'stop':
                return conditions
            else:
                condition = self.parser_condition(response)
                conditions.append(condition)


#######################   tool end   #########################




