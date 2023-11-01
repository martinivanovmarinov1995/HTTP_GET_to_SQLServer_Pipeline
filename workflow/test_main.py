import pytest
from unittest.mock import patch, Mock
from workflow import main as job


@pytest.fixture(scope="function")
def conn():
    with patch.object(job, 'CONN') as conn_mock:
        yield conn_mock

def test_main(conn):
    dataframe = Mock()
    with patch.object(job, "process_dataframe") as process_dataframe_mock, \
         patch.object(job, "create_table") as create_table_mock, \
         patch.object(job, "insert_data") as insert_data_mock:

        job.create_table(conn)
        job.process_dataframe(counter=1)
        job.insert_data(conn, dataframe)

    assert process_dataframe_mock.call_count == 1
    assert create_table_mock.call_count == 1
    assert insert_data_mock.call_count == 1
