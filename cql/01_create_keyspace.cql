CREATE KEYSPACE IF NOT EXISTS cloud_analytics
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
