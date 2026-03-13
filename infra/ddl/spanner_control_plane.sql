CREATE TABLE MigrationControlPlane (
  namespace STRING(255) NOT NULL,
  record_key STRING(1024) NOT NULL,
  payload_json STRING(MAX) NOT NULL,
  status STRING(32),
  owner_id STRING(255),
  lease_expires_at TIMESTAMP,
  updated_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (namespace, record_key);
