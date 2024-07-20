# test_snowflake.py
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import snowflake.connector
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import src.etl_pipeline


class TestSnowflake(unittest.TestCase):
    
    @patch('src.etl_pipeline.snowflake.connector.connect')
    def test_get_snowflake_connection(self, mock_connect):
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        config = {
            'user': 'CARRIE',
            'password': 'yutianI%1',
            'account': 'uafwvdf-lb96787',
            'warehouse': 'COMPUTE_WH',
            'database': 'your_database',
            'schema': 'your_schema'
        }
        conn = src.etl_pipeline.get_snowflake_connection(config)
        mock_connect.assert_called_once_with(
            user=config['user'],
            password=config['password'],
            account=config['account'],
            warehouse=config['warehouse'],
            database=config['database'],
            schema=config['schema']
        )
        self.assertEqual(conn, mock_conn)
    
    @patch('src.etl_pipeline.get_snowflake_connection')
    def test_extract_data(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn
        
        mock_cursor.fetchall.return_value = [(1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)]
        mock_cursor.description = [('ID',), ('NAME',), ('AGE',)]
        
        config = {
            'user': 'CARRIE',
            'password': 'yutianI%1',
            'account': 'uafwvdf-lb96787',
            'warehouse': 'COMPUTE_WH',
            'database': 'your_database',
            'schema': 'your_schema'
        }
        query = "SELECT * FROM my_table"
        df = src.etl_pipeline.extract_data(query, config)
        
        expected_df = pd.DataFrame({
            'ID': [1, 2, 3],
            'NAME': ['Alice', 'Bob', 'Charlie'],
            'AGE': [30, 25, 35]
        })
        pd.testing.assert_frame_equal(df, expected_df)
    
    def test_transform_data(self):
        df = pd.DataFrame({
            'ID': [1, 2, 3],
            'NAME': ['Alice', 'Bob', 'Charlie'],
            'AGE': [30, 25, 35]
        })
        transformed_df = src.etl_pipeline.transform_data(df)
        
        expected_df = pd.DataFrame({
            'ID': [1, 2, 3],
            'NAME': ['Alice', 'Bob', 'Charlie'],
            'AGE': [30, 25, 35],
            'age_plus_ten': [40, 35, 45]
        })
        pd.testing.assert_frame_equal(transformed_df, expected_df)

if __name__ == '__main__':
    unittest.main()
