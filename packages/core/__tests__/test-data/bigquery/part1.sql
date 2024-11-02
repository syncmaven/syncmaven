create table if not exists syncmaven_test.syncmaven_test_table
(
    id           integer,
    time         timestamp,
    source       string,
    campaign_id  integer,
    cost         integer,
    clicks       integer,
    impressions  integer,
    utm_source   string,
    utm_campaign string
);
truncate table syncmaven_test.syncmaven_test_table;

INSERT INTO syncmaven_test.syncmaven_test_table (id, time, source, campaign_id, cost, clicks, impressions, utm_source, utm_campaign)
VALUES (1, '2024-05-15 13:39:05.172616Z', 'google', 123, 10, 1, 100, null, null);
INSERT INTO syncmaven_test.syncmaven_test_table (id, time, source, campaign_id, cost, clicks, impressions, utm_source, utm_campaign)
VALUES (2, '2024-05-15 13:39:05.172616Z', 'facebook', 14, 5, 0, 42, 'fb', 'mrk');
INSERT INTO syncmaven_test.syncmaven_test_table (id, time, source, campaign_id, cost, clicks, impressions, utm_source, utm_campaign)
VALUES (3, '2024-05-16 13:39:05.172616Z', 'twitter', 1, 151, 21, 1251, 'tw', null);
INSERT INTO syncmaven_test.syncmaven_test_table (id, time, source, campaign_id, cost, clicks, impressions, utm_source, utm_campaign)
VALUES (4, '2024-05-17 13:39:05.172616Z', 'twitter', 1, 159, 29, 1259, 'tw', null);
INSERT INTO syncmaven_test.syncmaven_test_table (id, time, source, campaign_id, cost, clicks, impressions, utm_source, utm_campaign)
VALUES (5, '2024-05-17 13:39:05.172616Z', 'twitter', 1, 156, 26, 1256, 'tw', null);

