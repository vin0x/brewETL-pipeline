import unittest
import json
from unittest.mock import patch, mock_open
from scripts.extract import extract_data

class TestExtractData(unittest.TestCase):

    @patch('scripts.extract.requests.get')  # mock API
    @patch('builtins.open', new_callable=mock_open)  # mock file handling
    def test_extract_data_success(self, mock_open_file, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = [{'id': 1, 'name': 'Test Brewery'}]

        # testing extract.py function extract_data
        extract_data()

        # creating mock test to receive a .json from API call, try to read and write using python
        mock_open_file.assert_called_once_with('/opt/airflow/data/bronze/breweries_raw.json', 'w')
        written_data = ''.join(call.args[0] for call in mock_open_file().write.call_args_list)
        written_json = json.loads(written_data)

        self.assertEqual(written_json[0]['id'], 1)
        self.assertEqual(written_json[0]['name'], 'Test Brewery')

if __name__ == '__main__':
    unittest.main()
