ALTER TABLE outbox
    DROP CONSTRAINT outbox_event_id_key;

ALTER TABLE outbox
    DROP COLUMN event_id;
