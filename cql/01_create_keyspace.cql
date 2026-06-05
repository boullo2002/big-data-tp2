CREATE KEYSPACE IF NOT EXISTS cloud_analytics
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
};
