from elasticsearch import Elasticsearch
from collections import namedtuple
from ..release_tool.cli import main
import click
from click.testing import CliRunner


import pytest

# # ======================================================================
# # Test Settings

# Index = namedtuple('Index', 'case, gene, annotations, projects')

# TEST_DIR = os.path.dirname(os.path.realpath(__file__))
# BIN_DIR = os.path.join(os.path.dirname(TEST_DIR), 'bin')

# ES_HOST = 'localhost'
# ES_PORT = 9200
# ES_CONF = 'test.conf'
# ES_TIMEOUT = 60


# # ======================================================================
# # Fixtures

# @pytest.fixture
# def environment(monkeypatch):
#     """Monkeypatch the script environment"""

#     monkeypatch.setenv('ELASTICSEARCH_HOST', 'localhost')
#     monkeypatch.setenv('ES_USER', '')
#     monkeypatch.setenv('ES_PASSWORD', '')



# ======================================================================
# Elasticsearch test index


@pytest.yield_fixture(scope='module')
def test_index():
    """Generate an index as a fixture for re-use between tests"""
    print 'Generate an index as a fixture for re-use between tests'

    runner = CliRunner()
    result = runner.invoke(main, ['--debug', 'build'])

    return result.exit_code

    # es_driver = Elasticsearch(ES_HOST, port=ES_PORT)
    # index = 'test_index__'
    # doc_type = 'test'
    # docs = [{
    #     'id': 'test-doc-1',
    #     'value': 1,
    # }, {
    #     'id': 'test-doc-2',
    #     'value': 2,
    # }]

    # es_driver.indices.create(index=index, ignore=400)
    # for doc in docs:
    #     es_driver.create(
    #         index=index,
    #         id=doc['id'],
    #         doc_type=doc_type,
    #         body=doc,
    #         ignore=409,
    #     )

    # while True:
    #     count = es_driver.count(index=index, doc_type=doc_type)['count']
    #     if count == len(docs):
    #         break
    #     time.sleep(0.1)

    # yield es_driver, index, doc_type, docs
    # es_driver.indices.delete(index=index, ignore=400)

 
