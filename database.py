import json
from typing import Generator, List, Callable, Dict, Union, Optional
from functools import reduce
import os
import tempfile



class Database:
    def __init__(self, database_name: str) -> None:
        '''
        Load the metadata of the database.

        Args:
            database_name: str, the name of the database to be used.
        
        Returns:
            None.
        '''
        self.database_name = database_name
        with open(os.path.join('databases', self.database_name, 'metadata.jsonl'), 'r') as f:
            self.table_name_field_data_type_pairs_pairs = json.loads(next(f).rstrip('\n'))



#######################   tool start   #########################


    def read_table(self, table_path: str) -> Generator:
        '''
        Read the table file and return the table as a generator.

        Args:
            table_path: str, where the table stores.
        
        Returns:
            table_out: Generator, which generate records from the table file.
        '''
        with open(table_path, 'r') as f:
            for line in f:
                record = json.loads(line.rstrip('\n'))
                yield record


    def write_table(self, table: Generator, table_path: str) -> None:
        '''
        Write a table Generator to the file.

        Args:
            table: Generator.
            table_path: str, where the table stores.
        
        Returns:
            None
        '''
        with open(table_path, 'w') as f:
            for record in table:
                line = json.dumps(record) + '\n'
                f.write(line)


    def match_record(self, record: Dict[str, Union[int, float, bool, str]], conditions: List[Callable]) -> bool:
        '''
        Judge whether a record matches all the conditions.

        Args:
            record: Dict[str, Union[int, float, bool, str]], the record to be judged.
            conditions: List[Callable], the conditions to judge the record.
        
        Returns:
            match_result: bool, whether a record matches all the conditions.
        '''
        match_result = all([condition(record) for condition in conditions])
        return match_result


#######################   tool end   #########################



#######################   table start   #########################


    def create_table(self, table_name: str, field_data_type_pairs: Dict[str, str]) -> None:
        '''
        Create a new table and update database's metadata.

        Args:
            table_name: str, the name of the new table to be created.
            field_data_type_pairs: Dict[str, str], the fields of the table and their corresponding data types.
        
        Returns:
            None.
        '''
        table_path = os.path.join('databases', self.database_name, table_name+'.jsonl')
        with open(table_path, 'w') as f:
            pass
        self.table_name_field_data_type_pairs_pairs[table_name] = field_data_type_pairs
        with open(os.path.join('databases', self.database_name, 'metadata.jsonl'), 'w') as f:
            f.write(json.dumps(self.table_name_field_data_type_pairs_pairs)+'\n')


    def drop_table(self, table_name: str) -> None:
        '''
        Drop an existing table and update database's metadata.

        Args:
            table_name: str, the name of the existing table to be dropped.
        
        Returns:
            None.
        '''
        table_path = os.path.join('databases', self.database_name, table_name+'.jsonl')
        os.remove(table_path)
        del self.table_name_field_data_type_pairs_pairs[table_name]
        with open(os.path.join('databases', self.database_name, 'metadata.jsonl'), 'w') as f:
            f.write(json.dumps(self.table_name_field_data_type_pairs_pairs)+'\n')


    def show_table_names(self) -> None:
        '''
        Show available table names in current database.

        Args:
            None.
        
        Returns:
            None.
        '''
        for table_name in self.table_name_field_data_type_pairs_pairs.keys():
            print(table_name)


#######################   table end   #########################



#######################   data modification start   #########################


    def insert_record(self, table_name: str, record: Dict[str, Union[int, float, bool, str]]) -> None:
        '''
        Insert a record into a table.

        Args:
            table_name: str, the table to be inserted into.
            record: Dict[str, Union[int, float, bool, str]], the record to insert.
        
        Returns:
            None.
        '''
        table_path = os.path.join('databases', self.database_name, table_name+'.jsonl')
        with open(table_path, 'a') as f:
            line = json.dumps(record) + '\n'
            f.write(line)


    def update_record(self, table_name: str, field_value_pairs: Dict[str, Union[int, float, bool, str]], conditions: List[Callable]) -> None:
        '''
        Update records matching all the conditions in a table.

        Args:
            table_name: str, the table to be updated records.
            field_value_pairs: Dict[str, Union[int, float, bool, str]], the fields that needs to update and their corresponding new value.
            conditions: List[Callable], conditions to judge whether a record should be updated.
        
        Returns:
            None.
        '''
        table_path = os.path.join('databases', self.database_name, table_name+'.jsonl')
        table = self.read_table(table_path)
        _, tmp_file_path = tempfile.mkstemp(prefix='update_record_', suffix='.jsonl', dir='tmp/', text=True)
        with open(tmp_file_path, 'w') as f:
            for record in table:
                if self.match_record(record, conditions):
                    for field, value in field_value_pairs.items():
                        record[field] = value
                line = json.dumps(record) + '\n'
                f.write(line)
        os.rename(tmp_file_path, table_path)
                

    def delete_record(self, table_name: str, conditions: List[Callable]) -> None:
        '''
        Delete records matching all the conditions in a table.

        Args:
            table_name: str, the table to be deleted records.
            conditions: List[Callable], conditions to judge whether a record should be deleted.
        
        Returns:
            None.
        '''
        table_path = os.path.join('databases', self.database_name, table_name+'.jsonl')
        table = self.read_table(table_path)
        _, tmp_file_path = tempfile.mkstemp(prefix='delete_record_', suffix='.jsonl', dir='tmp/', text=True)
        with open(tmp_file_path, 'w') as f:
            for record in table:
                if not self.match_record(record, conditions):
                    line = json.dumps(record) + '\n'
                    f.write(line)
        os.rename(tmp_file_path, table_path)


#######################   data modification end   #########################



#######################   data query start   #########################


    def show(self, table: Generator, head_n: Optional[int] = None) -> None:
        '''
        print the entire table or head n records of the table.

        Args:
            table: Generator.
            head_n: Optional[int] = None, the number of records at the head of the table to be printed.
        
        Returns:
            None
        '''
        count = 0
        if head_n:
            for _ in range(head_n):
                try:
                    record = next(table)
                    print(json.dumps(record))
                    count += 1
                except StopIteration:
                    break
        else:
            for record in table:
                print(json.dumps(record))
                count += 1
        print(f'{count} record(s)')


    def select(self, table: Generator, conditions: List[Callable]) -> Generator:
        '''
        Read a table as a Generator, select records which match all of the conditions.

        Args:
            table: Generator.
            conditions: List[Callable], a list of functions which take a record as input and judge where the record match the condition or not.
        
        Returns:
            table_out: Generator, which generate records that match all the conditions.
        '''
        for record in table:
            if self.match_record(record, conditions):
                yield record


    def project(self, table: Generator, fields: List[str]) -> Generator:
        '''
        Read a table as a Generator, project the given fields of the record.

        Args:
            table: Generator.
            fields: List[str], there are fields that we need in the list.
        
        Returns:
            table_out: Generator, which contains records that are projected.
        '''
        for record in table:
            record_project = {field: record[field] for field in fields}
            yield record_project


    def cross_product(self, table_left: Generator, table_right: Generator) -> Generator:
        '''
        Read two tables as Generators, do cross product between two tables.

        Args:
            table_left: Generator.
            table_right: Generator.

        Returns:
            table_out: Generator, each record is a possible combination of records from two input tables.
        '''
        _, tmp_file_path = tempfile.mkstemp(prefix='cross_product_', suffix='.jsonl', dir='tmp/', text=True)
        self.write_table(table_right, tmp_file_path)
        for record_left in table_left:
            table_right = self.read_table(tmp_file_path)
            for record_right in table_right:
                record_cross_product = {**record_left, **record_right}
                yield record_cross_product
        os.remove(tmp_file_path)
        

    def theta_inner_join(self, table_left: Generator, table_right: Generator, conditions: List[Callable]) -> Generator:
        '''
        Read two tables as Generators, do theta inner join between two tables.

        Args:
            table_left: Generator.
            table_right: Generator.
            conditions: List[Callable], only join two records when they meet all of the conditions.

        Returns:
            table_out: Generator, each record is a joined record which meets all of the conditions.
        '''
        _, tmp_file_path = tempfile.mkstemp(prefix='theta_inner_join_', suffix='.jsonl', dir='tmp/', text=True)
        self.write_table(table_right, tmp_file_path)
        for record_left in table_left:
            table_right = self.read_table(tmp_file_path)
            for record_right in table_right:
                record_theta_inner_join = {**record_left, **record_right}
                if self.match_record(record_theta_inner_join, conditions): 
                    yield record_theta_inner_join
        os.remove(tmp_file_path)


    def aggregate(self, table: Generator, field: str, function: Callable) -> Union[int, float, bool, str]:
        '''
        Read a table as a Generator, do the aggregation function on the field.

        Args:
            table: Generator.
            field: str, the field to be aggregated.
            function: Callable, the aggregate function.
        
        Returns:
            aggregate_result: Union[int, float, bool, str], the result of aggregation, usually a numeric value.
        '''
        aggregate_result = reduce(function, (record[field] for record in table))
        return aggregate_result


    def group_by_and_aggregate(self, table: Generator, group_by_fields: List[str], aggregate_field_aggregate_function_pairs: Dict[str, Callable]) -> Generator:
        '''
        Read a table as a Generator, group it by a list of fields used for group, then aggregate fields using corresponding functions.

        Args:
        table: Generator
        group_by_fields: List[str], the fields used for group
        aggregate_field_aggregate_function_pairs: Dict[str, Callable], the fields needed to be aggregated and their corresponding functions.

        Returns:
            table_out: Generator, each record is a result of group by and aggregate.

        '''
        group_str_tmp_file_path_pairs = {}
        for record in table:
            group = {}
            for group_by_field in group_by_fields:
                group[group_by_field] = record[group_by_field]
            group_str = json.dumps(group)
            try:
                tmp_file_path = group_str_tmp_file_path_pairs[group_str]
            except KeyError:
                _, tmp_file_path = tempfile.mkstemp(prefix=f'group_by_and_aggregate_{group_str}_', suffix='.jsonl', dir='tmp', text=True)
                group_str_tmp_file_path_pairs[group_str] = tmp_file_path
            with open(tmp_file_path, 'a') as f:
                line = json.dumps(record) + '\n'
                f.write(line)
        
        for group_str, tmp_file_path in group_str_tmp_file_path_pairs.items():
            group = json.loads(group_str)
            aggregate_field_aggregate_result_pairs = {}
            for aggregate_field, aggregate_function in aggregate_field_aggregate_function_pairs.items():
                table_group = self.read_table(tmp_file_path)
                aggregate_result = self.aggregate(table_group, aggregate_field, aggregate_function)
                aggregate_field_aggregate_result_pairs[aggregate_field] = aggregate_result
            record_group_by_and_aggregate = {**group, **aggregate_field_aggregate_result_pairs}
            yield record_group_by_and_aggregate
            os.remove(tmp_file_path)


    def sort(self, table: Generator, sort_field: str, ascending: bool, chunk_size: int) -> List[str]:
        '''
        Split table into small tables, sort them separately and store them to temp files.

        Args:
            table: Generator, the large table to be sorted.
            sort_field: str, the key of sorting.
            ascending: bool, determine whether sorting in ascending or descending order.
            chunk_size: int, the size of each small table.
        
        Returns:
            tmp_file_paths: List[str], the path where temp files of sorted small tables store.
        '''
        count = 0
        current_run = []
        tmp_file_paths = []
        for record in table:
            count += 1
            current_run.append(record)
            if count % chunk_size == 0:
                if ascending:
                    current_run = sorted(current_run, key=lambda x: x[sort_field])
                else:
                    current_run = sorted(current_run, key=lambda x: x[sort_field], reverse=True)
                _, tmp_file_path = tempfile.mkstemp(prefix='sort_', suffix='.jsonl', dir='tmp', text=True)
                self.write_table(current_run, tmp_file_path)
                current_run = []
                tmp_file_paths.append(tmp_file_path)
        if current_run:
            if ascending:
                current_run = sorted(current_run, key=lambda x: x[sort_field])
            else:
                current_run = sorted(current_run, key=lambda x: x[sort_field], reverse=True)
            _, tmp_file_path = tempfile.mkstemp(prefix='sort_', suffix='.jsonl', dir='tmp', text=True)
            self.write_table(current_run, tmp_file_path)
            current_run = []
            tmp_file_paths.append(tmp_file_path)
        return tmp_file_paths


    def merge(self, tmp_file_path_1: str, tmp_file_path_2: str, sort_field: str, ascending: bool) -> str:
        '''
        Merge 2 small tables into one large table in order and store it.

        Args:
            tmp_file_path_1: str, the path where small table 1 stores.
            tmp_file_path_2: str, the path where small table 2 stores.
            sort_field: str, the key of sorting.
            ascending: bool, determine whether sorting in ascending or descending order.
        
        Returns:
            tmp_file_path_merge: the path where merged table stores.
        '''
        run_1 = self.read_table(tmp_file_path_1)
        run_2 = self.read_table(tmp_file_path_2)
        _, tmp_file_path_merge = tempfile.mkstemp(prefix='merge_', suffix='.jsonl', dir='tmp', text=True)
        with open(tmp_file_path_merge, 'w') as f:
            record_1 = next(run_1)
            record_2 = next(run_2)
            while True:
                if ascending:
                    if record_1[sort_field] <= record_2[sort_field]:
                        line = json.dumps(record_1) + '\n'
                        f.write(line)
                        try:
                            record_1 = next(run_1)
                        except:
                            line = json.dumps(record_2) + '\n'
                            f.write(line)
                            for record_2 in run_2:
                                line = json.dumps(record_2) + '\n'
                                f.write(line)
                            break
                    else:
                        line = json.dumps(record_2) + '\n'
                        f.write(line)
                        try:
                            record_2 = next(run_2)
                        except:
                            line = json.dumps(record_1) + '\n'
                            f.write(line)
                            for record_1 in run_1:
                                line = json.dumps(record_1) + '\n'
                                f.write(line)
                            break
                else:
                    if record_1[sort_field] >= record_2[sort_field]:
                        line = json.dumps(record_1) + '\n'
                        f.write(line)
                        try:
                            record_1 = next(run_1)
                        except:
                            line = json.dumps(record_2) + '\n'
                            f.write(line)
                            for record_2 in run_2:
                                line = json.dumps(record_2) + '\n'
                                f.write(line)
                            break
                    else:
                        line = json.dumps(record_2) + '\n'
                        f.write(line)
                        try:
                            record_2 = next(run_2)
                        except:
                            line = json.dumps(record_1) + '\n'
                            f.write(line)
                            for record_1 in run_1:
                                line = json.dumps(record_1) + '\n'
                                f.write(line)
                            break
        os.remove(tmp_file_path_1)
        os.remove(tmp_file_path_2)
        return tmp_file_path_merge


    def sort_merge(self, table: Generator, sort_field: str, ascending: bool, chunk_size: int) -> Generator:
        '''
        Sort a large table using merge-sort.

        Args:
            table: Generator, the large table to be sorted.
            sort_field: str, the key of sorting.
            ascending: bool, determine whether sorting in ascending or descending order.
            chunk_size: int, the size of each small table.
        
        Returns:
            table_out: Generator, the sorted large table.
        '''
        tmp_file_paths = self.sort(table, sort_field, ascending, chunk_size)
        current_tmp_file_path = tmp_file_paths[0]
        for tmp_file_path in tmp_file_paths[1:]:
            current_tmp_file_path = self.merge(current_tmp_file_path, tmp_file_path, sort_field, ascending)
        table_sort_merge = self.read_table(current_tmp_file_path)
        for record_sort_merge in table_sort_merge:
            yield record_sort_merge
        os.remove(current_tmp_file_path)


#######################   data query end   #########################




