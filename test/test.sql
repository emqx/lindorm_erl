-- INSERT INTO
--     table_name '(' tag_name (',' tag_name) * ',' time ',' field_name (',' field_name) * ')'
-- VALUES
--     (
--         '(' tag_value (',' tag_value) * ',' time_value ',' field_value (',' tag_value) * ')'
--     ) +
INSERT INTO
    demo_sensor (device_id, region, time, humidity, temperature)
VALUES
    (
        't_device_id1',
        't_region1',
        1638239992707,
        1111.111,
        11.11
    ),
(
        't_device_id2',
        't_region2',
        1638239992707,
        22222.222000000002,
        22.22
    )
