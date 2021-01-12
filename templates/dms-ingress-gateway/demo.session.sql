INSERT INTO "PERSON" ("ID", "FNAME", "LNAME")
VALUES (
    '10',
    'John10',
    'G10'
  );

UPDATE "PERSON"
SET "FNAME" = 'John.4'
WHERE "ID" = '4';

DELETE FROM "PERSON"
WHERE "ID" = '3';
