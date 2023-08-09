import pytest
import logging
import os

from dtest import Tester, create_ks, create_cf
from ccmlib.scylla_cluster import ScyllaCluster
from cassandra import ConsistencyLevel, Timeout, Unavailable

from tools.data import (create_c1c2_table, insert_c1c2, insert_columns, query_c1c2, rows_to_list)
from tools.misc import new_node, generate_ssl_stores


logger = logging.getLogger(__name__)

class TestScyllaClusterStart(Tester):

    def test_simple_add_node_1(self):
        cluster = self.cluster

        # create a cluster with a single node and start the nodes
        cluster.populate(1).start(wait_for_binary_proto=True)

        print (cluster.nodelist()[0])
    
        node1 = cluster.nodelist()[0]
        # connect to the node
        session = self.patient_cql_connection(node1, protocol_version=4)
    
        # create keyspace (RF=2), column family, insert data
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, keys=range(1000), consistency=ConsistencyLevel.ONE)
    
        # # create a new node, start it and wait for it to connect to the cluster
        # # when a new node is added to the cluster data is streamed to it based on RF.
        # # In our case all the data will be streamed
        # node2 = new_node(cluster)
        # node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        # session = self.patient_exclusive_cql_connection(node2, 'ks', protocol_version=4)
    
        # # insert an additional 1000 rows with CL=2
        # insert_c1c2(session, keys=range(1000, 2000), consistency=ConsistencyLevel.TWO)
    
        # # shutdown node1, check all data exists on node2, start node1
        # node1.stop()
        # result = session.execute("SELECT * FROM cf LIMIT 2001")

        # self.assertEqual(len(result), 2000, len(result))

        # node1.start(wait_for_binary_proto=True, wait_other_notice=True)
    
        # # shut down node2, check all data exists on node1
        # node2.stop()
        # session = self.patient_cql_connection(node1,'ks')
        # result = session.execute("SELECT * FROM cf LIMIT 2001")
        # self.assertEqual(len(result), 2000, len(result))
    
