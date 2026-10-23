CREATE TABLE __DATABASE__.events_alpha (`Event Count` UInt64, payload String) ENGINE = MergeTree ORDER BY tuple()
INSERT INTO __DATABASE__.events_alpha VALUES (10, 'a')
INSERT INTO __DATABASE__.events_alpha VALUES (20, 'b')
OPTIMIZE TABLE __DATABASE__.events_alpha
CREATE TABLE __DATABASE__.events_beta (`Event Count` UInt64, payload String) ENGINE = MergeTree ORDER BY tuple()
INSERT INTO __DATABASE__.events_beta SELECT 2, repeat('x', 200) FROM numbers(100)
CREATE TABLE __DATABASE__.empty_events (`Event Count` UInt64) ENGINE = MergeTree ORDER BY tuple()
CREATE TABLE __DATABASE__.zero_events (`Event Count` UInt64) ENGINE = MergeTree ORDER BY tuple()
INSERT INTO __DATABASE__.zero_events VALUES (0)
CREATE TABLE __DATABASE__.big_events (`Event Count` UInt64) ENGINE = MergeTree ORDER BY tuple()
INSERT INTO __DATABASE__.big_events VALUES (9223372036854775809)
CREATE TABLE __DATABASE__.`odd 'quote;\`name` (`Event Count` UInt64) ENGINE = MergeTree ORDER BY tuple()
INSERT INTO __DATABASE__.`odd 'quote;\`name` VALUES (4)
CREATE TABLE __DATABASE__.without_event_count (id UInt64) ENGINE = MergeTree ORDER BY tuple()
INSERT INTO __DATABASE__.without_event_count VALUES (1)
CREATE VIEW __DATABASE__.ordinary_view AS SELECT * FROM __DATABASE__.events_alpha
CREATE MATERIALIZED VIEW __DATABASE__.materialized_without_suffix TO __DATABASE__.events_beta AS SELECT * FROM __DATABASE__.events_alpha
CREATE TABLE __DATABASE__.excludedMV (`Event Count` UInt64) ENGINE = MergeTree ORDER BY tuple()
CREATE TABLE __DATABASE__.memory_events (`Event Count` UInt64) ENGINE = Memory
CREATE TABLE __OTHER_DATABASE__.events_alpha (`Event Count` UInt64, payload String) ENGINE = MergeTree ORDER BY tuple()
INSERT INTO __OTHER_DATABASE__.events_alpha SELECT 100, repeat('other database', 1000) FROM numbers(500)
