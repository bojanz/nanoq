-- +goose Up
-- SQL in this section is executed when the migration is applied.

CREATE TABLE `tasks` (
  `id` char(26) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `fingerprint` char(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `type` varchar(50) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `payload` json NOT NULL,
  `retries` tinyint(3) unsigned NOT NULL DEFAULT 0,
  `max_retries` tinyint(3) unsigned NOT NULL DEFAULT 10,
  `created_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `scheduled_at` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`id`),
  UNIQUE KEY `fingerprint` (`fingerprint`),
  KEY `created_at` (`created_at`),
  KEY `scheduled_at` (`scheduled_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- +goose Down
-- SQL in this section is executed when the migration is rolled back.

DROP TABLE `tasks`;
