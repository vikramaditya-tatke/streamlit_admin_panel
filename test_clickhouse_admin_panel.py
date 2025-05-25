import unittest
from unittest.mock import patch, MagicMock, call
import polars as pl
from clickhouse_admin_panel import (
    parse_readable_size,
    format_bytes_for_display,
    ClickHouseDataExtraction,
    transform,
    create_clickhouse_client,
    settings # Imported to be patched
)

class TestHelperFunctions(unittest.TestCase):
    def test_parse_readable_size(self):
        self.assertEqual(parse_readable_size("10.0 KiB"), 10.0 * 1024)
        self.assertEqual(parse_readable_size("5 MiB"), 5.0 * 1024**2)
        self.assertEqual(parse_readable_size("2.5 GiB"), 2.5 * 1024**3)
        self.assertEqual(parse_readable_size("1.0 TiB"), 1.0 * 1024**4)
        # The actual parser uses 'pb' and 'eb' in its units dict.
        self.assertEqual(parse_readable_size("0.5 PB"), 0.5 * 1024**5)
        self.assertEqual(parse_readable_size("0.2 EB"), 0.2 * 1024**6)
        self.assertEqual(parse_readable_size("100 bytes"), 100.0)
        self.assertEqual(parse_readable_size("0"), 0.0)
        self.assertEqual(parse_readable_size("0 bytes"), 0.0)
        self.assertEqual(parse_readable_size("abc"), 0.0) # Invalid string
        self.assertEqual(parse_readable_size("10 kib"), 10.0 * 1024) # Case insensitivity
        self.assertEqual(parse_readable_size("  10.0 KiB  "), 10.0 * 1024) # Whitespace
        self.assertEqual(parse_readable_size(None), 0.0) # Non-string input
        self.assertEqual(parse_readable_size(123), 0.0)  # Non-string input (integer)
        self.assertEqual(parse_readable_size("10.5"), 10.5) # Number as string without unit

    def test_format_bytes_for_display(self):
        self.assertEqual(format_bytes_for_display(1024), "1.0 KiB")
        self.assertEqual(format_bytes_for_display(5 * 1024**2), "5.0 MiB")
        self.assertEqual(format_bytes_for_display(100), "100.0 B")
        self.assertEqual(format_bytes_for_display(0), "0.0 B")
        self.assertEqual(format_bytes_for_display(2.5 * 1024**3), "2.5 GiB")
        self.assertEqual(format_bytes_for_display(1023), "1023.0 B")
        self.assertEqual(format_bytes_for_display(1.5 * 1024**4), "1.5 TiB") # Test TiB
        self.assertEqual(format_bytes_for_display(None), "N/A")


@patch('clickhouse_admin_panel.create_clickhouse_client')
class TestClickHouseDataExtraction(unittest.TestCase):

    def setUp(self, mock_create_client: MagicMock):
        self.mock_client = MagicMock()
        mock_create_client.return_value = self.mock_client
        self.extractor = ClickHouseDataExtraction()

    def test_make_queries(self, mock_create_client: MagicMock):
        # mock_create_client is passed by class decorator but not always used in method body
        base_logical_query, physical_query = self.extractor.make_queries()
        self.assertIsInstance(base_logical_query, str)
        self.assertIsInstance(physical_query, str)
        self.assertIn("SELECT name AS Table, total_rows AS `Row_Count`", base_logical_query)
        self.assertIn("FROM system.tables", base_logical_query)
        self.assertIn("FROM system.parts", physical_query)
        self.assertIn("formatReadableSize(sum(data_compressed_bytes))", physical_query)

    def test_extract_data(self, mock_create_client: MagicMock):
        base_logical_query_str = "dummy_base_logical_query"
        physical_query_str = "dummy_physical_query"

        # Expected data from the queries
        mock_physical_results_data = [('table1', '1.0 KiB', '2.0 KiB', 0.5, 50.0),
                                      ('table2', '100 bytes', '150 bytes', 2/3, 100/3)]
        mock_base_logical_results_data = [('table1', 1000), ('table2', 2000)]
        mock_event_count_t1_data = [(500,)]
        mock_event_count_t2_data = [(2000,)]

        # Mock query responses
        mock_physical_response = MagicMock()
        mock_physical_response.result_set = mock_physical_results_data
        
        mock_base_logical_response = MagicMock()
        mock_base_logical_response.result_set = mock_base_logical_results_data
        
        mock_event_t1_response = MagicMock()
        mock_event_t1_response.result_set = mock_event_count_t1_data
        
        mock_event_t2_response = MagicMock()
        mock_event_t2_response.result_set = mock_event_count_t2_data

        self.mock_client.query.side_effect = [
            mock_physical_response,         # Corresponds to self.client.query(physical_query)
            mock_base_logical_response,     # Corresponds to self.client.query(base_logical_query)
            mock_event_t1_response,         # Corresponds to event_count_query for table1
            mock_event_t2_response          # Corresponds to event_count_query for table2
        ]

        logical_results, physical_result_set = self.extractor.extract(base_logical_query_str, physical_query_str)

        expected_logical_results = [
            ('table1', 500, 1000, 1000 / 500.0, (1.0 - (1000 / 500.0)) * 100.0),
            ('table2', 2000, 2000, 2000 / 2000.0, (1.0 - (2000 / 2000.0)) * 100.0)
        ]
        self.assertEqual(logical_results, expected_logical_results)
        self.assertEqual(physical_result_set, mock_physical_results_data)

        # Verify calls to client.query
        # Table names in event count queries are quoted: `table_name.replace('`', '``')`
        expected_query_calls = [
            call(physical_query_str),
            call(base_logical_query_str),
            call("SELECT SUM(`Event Count`) AS `Event_Count_Value` FROM default.`table1`"),
            call("SELECT SUM(`Event Count`) AS `Event_Count_Value` FROM default.`table2`")
        ]
        self.mock_client.query.assert_has_calls(expected_query_calls)

    def test_extract_data_event_count_failure(self, mock_create_client: MagicMock):
        base_logical_query_str = "dummy_base_logical_query"
        physical_query_str = "dummy_physical_query"

        mock_physical_results_data = [('table1', '1.0 KiB', '2.0 KiB', 0.5, 50.0),
                                      ('table2', '0 bytes', '0 bytes', 0.0, 0.0)] # table2 has 0 for sizes
        mock_base_logical_results_data = [('table1', 1000), ('table2', 2000)]
        mock_event_count_t2_data = [(None,)] # Event count for table2 returns (None,)

        mock_physical_response = MagicMock()
        mock_physical_response.result_set = mock_physical_results_data
        
        mock_base_logical_response = MagicMock()
        mock_base_logical_response.result_set = mock_base_logical_results_data
        
        mock_event_t2_response = MagicMock()
        mock_event_t2_response.result_set = mock_event_count_t2_data

        self.mock_client.query.side_effect = [
            mock_physical_response,
            mock_base_logical_response,
            Exception("Simulated query failure for Event Count table1"), # For table1
            mock_event_t2_response # For table2
        ]

        logical_results, physical_result_set = self.extractor.extract(base_logical_query_str, physical_query_str)

        expected_logical_results = [
            ('table1', 0, 1000, 0.0, 0.0), # Event count failure, defaults to 0
            ('table2', 0, 2000, 0.0, 0.0)  # Event count is None, defaults to 0
        ]
        self.assertEqual(logical_results, expected_logical_results)
        self.assertEqual(physical_result_set, mock_physical_results_data)


class TestTransform(unittest.TestCase):
    def test_transform_data(self):
        logical_data = [('table1', 500, 1000, 2.0, -100.0)]
        # Physical data: Table, Compressed Size (str), Uncompressed Size (str), Ratio, Physical Compression
        physical_data = [('table1', "1.0 KiB", "2.0 KiB", 0.5, 50.0)]

        df1, df2 = transform(logical_data, physical_data)

        self.assertIsInstance(df1, pl.DataFrame)
        self.assertIsInstance(df2, pl.DataFrame)

        expected_df1_schema = {
            "Table": pl.Utf8, "Event_Count": pl.Int64, "Row_Count": pl.Int64,
            "Ratio": pl.Float64, "Logical_Compression": pl.Float64
        }
        # After transform, "Compressed Size" and "Uncompressed Size" become Float64
        expected_df2_schema = {
            "Table": pl.Utf8, "Compressed Size": pl.Float64, "Uncompressed Size": pl.Float64,
            "Ratio": pl.Float64, "Physical Compression": pl.Float64
        }

        self.assertEqual(df1.schema, expected_df1_schema)
        self.assertEqual(df2.schema, expected_df2_schema)

        # Check df1 values
        self.assertEqual(df1["Table"][0], "table1")
        self.assertEqual(df1["Event_Count"][0], 500)
        self.assertEqual(df1["Logical_Compression"][0], -100.0)
        
        # Check df2 values (especially parsed sizes)
        self.assertEqual(df2["Table"][0], "table1")
        self.assertEqual(df2["Compressed Size"][0], 1024.0)
        self.assertEqual(df2["Uncompressed Size"][0], 2048.0)
        self.assertEqual(df2["Physical Compression"][0], 50.0)
        self.assertEqual(df2["Ratio"][0], 0.5)


# Patch settings for TestCreateClickhouseClient
# Order: innermost decorator first for args, so mock_get_client, then mock_settings
@patch('clickhouse_admin_panel.settings')
@patch('clickhouse_admin_panel.clickhouse_connect.get_client')
class TestCreateClickhouseClient(unittest.TestCase):

    def test_create_client_success(self, mock_get_client: MagicMock, mock_settings_module: MagicMock):
        # Configure mock settings object that is imported in clickhouse_admin_panel
        mock_settings_module.clickhouse_base_url = "test_host"
        mock_settings_module.clickhouse_user = "test_user"
        mock_settings_module.clickhouse_password = "test_password"
        mock_settings_module.clickhouse_port = 1234
        mock_settings_module.clickhouse_compression_protocol = "lz4"
        # settings in create_clickhouse_client is fixed as {"insert_deduplicate": False}

        mock_client_instance = MagicMock()
        mock_get_client.return_value = mock_client_instance

        client = create_clickhouse_client()

        mock_get_client.assert_called_once_with(
            host="test_host",
            user="test_user",
            password="test_password",
            port=1234,
            secure=True, # This is hardcoded in create_clickhouse_client
            compress="lz4",
            settings={"insert_deduplicate": False} # This is hardcoded
        )
        self.assertEqual(client, mock_client_instance)

    def test_create_client_failure(self, mock_get_client: MagicMock, mock_settings_module: MagicMock):
        # Configure mock settings (values don't matter as much as the exception)
        mock_settings_module.clickhouse_base_url = "test_host"
        # ... set other attributes if their absence would cause error before get_client call

        mock_get_client.side_effect = Exception("Connection Error")

        with self.assertRaises(Exception) as context:
            create_clickhouse_client()
        self.assertTrue("Connection Error" in str(context.exception))

if __name__ == '__main__':
    # This allows running the tests from the command line:
    # python -m unittest test_clickhouse_admin_panel.py
    unittest.main()
```
