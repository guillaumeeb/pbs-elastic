# -*- coding: utf-8 -*-
############################################################################
#
# Written by Guillaume Eynard, CNES, 11/2017
#
############################################################################
import pandas as pd
from pandas.compat import StringIO
import subprocess
import json
import logging
from elasticsearch import Elasticsearch, helpers
from argparse import ArgumentParser

QSTAT_ATTRIBUTES = ['JobId', 'Username', 'Queue', 'Jobname', 'SessID', 'NbChunk', 'NbCPUsTot', 'Memory', 'ReqTime',
                    'Status', 'ElapTime']
QSTAT_AGG = {'JobId': 'count', 'NbChunk': 'sum', 'NbCPUsTot': 'sum', 'MemoryGB': 'sum'}

#Default Elastic types to avoid problems
MAPPING_ES = {
    'settings': {
        'index': {
            'analysis': {
                'analyzer': {
                    'default': {
                        'tokenizer': 'standard',
                        'filter': ['standard', 'asciifolding']
                    }
                }
            }
        },
        'number_of_shards': 1
    },
    "mappings": {
        "_default_": {
            "dynamic_templates": [
                {
                    "strings_as_keyword": {
                        "mapping": {
                            "ignore_above": 1024,
                            "type": "keyword"
                        },
                        "match_mapping_type": "string"
                    }
                }
            ],
            "_all": {
                "enabled": False
            },
            "properties": {
                "@timestamp": {
                    "type": "date",
                    "doc_values": True
                },
                "rav_tmpdir_ingb": {
                    "type": "float"
                },
                "ras_tmpdir_ingb": {
                    "type": "float"
                },
                "rav_ncpus": {
                    "type": "float"
                },
                "pcpus": {
                    "type": "float"
                },
                "jobs": {
                    "type": "integer"
                },
                "rav_nmic": {
                    "type": "float"
                }
            }
        }
    }
}

TODAY = pd.to_datetime('today')
TODAY_STR = TODAY.strftime('%Y-%m-%d')

def create_parser():
    """ Read CLI """
    parser = ArgumentParser()

    parser.add_argument("-e", "--es-host", dest="es_host",
                        required=False, default="localhost",
                        help="ELK server for pushing data")

    parser.add_argument("-p", "--pbs-bin", dest="pbs_bin_path",
                        required=False, default="/opt/pbs/default/bin",
                        help="Pbs commands path")

    parser.add_argument("-i", "--index-prefix", dest="index_prefix",
                        required=False, default="hpc.metrics",
                        help="Elasticsearch indice prefix")

    #Active ou désactive l'indexation elastic search ou le scan ilm (pour rejeu et tests)
    parser.set_defaults(index_es=True)
    parser.add_argument('--index-es', dest='index_es', action='store_true')
    parser.add_argument('--no-index-es', dest='index_es', action='store_false')
    options = parser.parse_args()

    return options


def create_stat_index(es_conn, index_name):
    """ create elasticsearch index

    :param es_conn: elasticsearch connection to use
    :type es_conn: elasticsearch.Elasticsearch
    :param index_name: name of the index where to write documents.
    :type index_name: str
    :param mapping: elasticsearch document mapping
    :type mapping: dict
    """
    logging.info("Creation index: %s", index_name)
    es_conn.indices.create(index=index_name, ignore=400, body=MAPPING_ES)


def index_into_es(df, es_conn, index_name, index_date):
    """
    Index data from a Dataframe into Elasticsearch
    :param index:
    :param df:
    :return:
    """
    # Use json to map with unicode string, not to_dict !!
    json_str = df.to_json(orient="records", force_ascii=False)
    tmpdict = json.loads(json_str)
    if len(tmpdict) > 0:
        logging.debug(tmpdict[0])
    else:
        logging.info('Nothing to index ...')
        return

    # Load each record into json format before bulk
    # Create formatted docs for ES
    BULK_BODY = {"_index": index_name, "_type": "document", "_source": dict()}
    documents = list()
    for raw_document in tmpdict:
        document = BULK_BODY.copy()
        document["_source"] = raw_document
        # On ajoute le type de stat
        document["_source"]["timestamp"] = index_date
        documents.append(document)

    chunk_size = 500
    print("There is %s documents to post. Should be %s chunks" %
          (len(documents), len(documents) / chunk_size))
    # helpers.parallel_bulk(es_conn, documents, thread_count=8, chunk_size=chunk_size)
    for success, info in helpers.parallel_bulk(es_conn, documents, thread_count=4, chunk_size=chunk_size):
        if not success:
            print('A document failed:', info)


def get_qstat_as_df(pbs_bin_path="/opt/pbs/default/bin"):
    qstatstr = subprocess.check_output('%s/qstat -watG' % pbs_bin_path, shell=True)
    qstat_df = pd.read_csv(StringIO(qstatstr.decode()), delim_whitespace=True, skiprows=5,
                           names=QSTAT_ATTRIBUTES)
    # Handle case when using phi and memory is like "--"
    qstat_df['MemoryGB'] = qstat_df.Memory.apply(lambda s: int(s[:-2]) if s.endswith("gb") else 0)
    qstat_df = qstat_df.drop(['Memory'], axis=1)
    return qstat_df


def format_string_kb_in_int_gb(s):
    return 0 if pd.isnull(s) else int(s[:-2]) / (1024 * 1024)


def get_pbsnodes_as_df(pbs_bin_path="/opt/pbs/default/bin"):
    pbsnodeout = subprocess.check_output('%s/pbsnodes -a -F json' % pbs_bin_path, shell=True)
    pbsnodedict = json.loads(pbsnodeout)
    pbsnodes_df = pd.read_json(json.dumps(pbsnodedict["nodes"]), orient='index')
    pbsnodes_df2 = pd.concat([pbsnodes_df.drop(['resources_available'], axis=1),
                              pbsnodes_df['resources_available'].apply(lambda x: pd.Series(x).add_prefix("rav_"))],
                             axis=1)
    pbsnodes_df2 = pd.concat([pbsnodes_df2.drop(['resources_assigned'], axis=1),
                              pbsnodes_df2['resources_assigned'].apply(lambda x: pd.Series(x).add_prefix("ras_"))],
                             axis=1)
    # Modifier la mémoire
    pbsnodes_df2["rav_mem_ingb"] = pbsnodes_df2.rav_mem.apply(format_string_kb_in_int_gb)
    pbsnodes_df2["ras_mem_ingb"] = pbsnodes_df2.ras_mem.apply(format_string_kb_in_int_gb)
    pbsnodes_df2["rav_tmpdir_ingb"] = pbsnodes_df2.rav_tmpdir.apply(format_string_kb_in_int_gb)
    if pbsnodes_df2.columns.contains("ras_tmpdir"):
        pbsnodes_df2["ras_tmpdir_ingb"] = pbsnodes_df2.ras_tmpdir.apply(format_string_kb_in_int_gb)
        pbsnodes_df2 = pbsnodes_df2.drop(['ras_tmpdir'], axis=1)
    else:
        pbsnodes_df2["ras_tmpdir_ingb"] = 0

    pbsnodes_df2 = pbsnodes_df2.drop(['rav_mem', 'ras_mem', 'rav_tmpdir', 'rav_vmem'], axis=1, errors='ignore')
    pbsnodes_df2 = pbsnodes_df2.drop(['comment'], axis=1, errors='ignore')
    pbsnodes_df2 = pbsnodes_df2.fillna(0)
    pbsnodes_df2["jobs_nb"] = pbsnodes_df2.jobs.apply(lambda l: 0 if isinstance(l, int) else len(l))
    pbsnodes_df2["used_ncpus_percent"] = pbsnodes_df2.ras_ncpus.divide(pbsnodes_df2.rav_ncpus)
    pbsnodes_df2["used_mem_percent"] = pbsnodes_df2.ras_mem_ingb.divide(pbsnodes_df2.rav_mem_ingb)
    pbsnodes_df2["used_tmp_percent"] = pbsnodes_df2.ras_tmpdir_ingb.divide(pbsnodes_df2.rav_tmpdir_ingb)
    pbsnodes_df2 = pbsnodes_df2.drop(['jobs'], axis=1)
    return pbsnodes_df2


if __name__ == "__main__":
    # Read CLI
    options = create_parser()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    es_conn = Elasticsearch(host=options.es_host, port=9200)
    index_date = pd.datetime.now().replace(second=0, microsecond=0)
    logging.info("Starting qstat indexation")
    qstat_df = get_qstat_as_df(pbs_bin_path=options.pbs_bin_path)
    if options.index_es:
        # One index per day, qstat index can be huge depending on the number of queued jobs
        index_name = options.index_prefix + ".qstat.raw-" + TODAY_STR
        create_stat_index(es_conn, index_name)
        index_into_es(qstat_df, es_conn, index_name, index_date)
    else:
        print(qstat_df.head())

    # Index pbsnodes.
    logging.info("Starting pbsnodes indexation")
    pbsnodes_df = get_pbsnodes_as_df(pbs_bin_path=options.pbs_bin_path)
    if options.index_es:
        # One index per day
        index_name = options.index_prefix + ".pbsnodes.raw-" + TODAY_STR
        create_stat_index(es_conn, index_name)
        index_into_es(pbsnodes_df, es_conn, index_name, index_date)
    else:
        print(pbsnodes_df.head())
