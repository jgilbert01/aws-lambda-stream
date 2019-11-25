import 'mocha';
import { expect } from 'chai';

import { fromDynamodb } from '../../../src/from/dynamodb';

import { toDynamodbRecords } from '../../../src/utils/dynamodb';

describe('from/dynamodb.js', () => {
  it('should parse INSERT record', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          hk: '1',
          sk: 'thing',
        },
        newImage: {
          hk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'n1',
        },
      },
    ]);

    fromDynamodb(events)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            eventID: '0',
            eventName: 'INSERT',
            eventSource: 'aws:dynamodb',
            awsRegion: 'us-west-2',
            dynamodb: {
              ApproximateCreationDateTime: 1572832690,
              Keys: {
                hk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: {
                hk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
                discriminator: {
                  S: 'thing',
                },
                name: {
                  S: 'n1',
                },
              },
              OldImage: undefined,
              SequenceNumber: '0',
              StreamViewType: 'NEW_AND_OLD_IMAGES',
            },
          },
          event: {
            id: '0',
            type: 'thing-created',
            partitionKey: '1',
            timestamp: 1572832690000,
            tags: {
              region: 'us-west-2',
            },
            raw: {
              new: {
                hk: '1',
                sk: 'thing',
                discriminator: 'thing',
                name: 'n1',
              },
              old: undefined,
            },
          },
        });
      })
      .done(done);
  });

  it('should parse MODIFY record', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          hk: '1',
          sk: 'thing',
        },
        newImage: {
          hk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'n1',
        },
        oldImage: {
          hk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'N1',
        },
      },
    ]);

    fromDynamodb(events)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            eventID: '0',
            eventName: 'MODIFY',
            eventSource: 'aws:dynamodb',
            awsRegion: 'us-west-2',
            dynamodb: {
              ApproximateCreationDateTime: 1572832690,
              Keys: {
                hk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: {
                hk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
                discriminator: {
                  S: 'thing',
                },
                name: {
                  S: 'n1',
                },
              },
              OldImage: {
                hk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
                discriminator: {
                  S: 'thing',
                },
                name: {
                  S: 'N1',
                },
              },
              SequenceNumber: '0',
              StreamViewType: 'NEW_AND_OLD_IMAGES',
            },
          },
          event: {
            id: '0',
            type: 'thing-updated',
            partitionKey: '1',
            timestamp: 1572832690000,
            tags: {
              region: 'us-west-2',
            },
            raw: {
              new: {
                hk: '1',
                sk: 'thing',
                discriminator: 'thing',
                name: 'n1',
              },
              old: {
                hk: '1',
                sk: 'thing',
                discriminator: 'thing',
                name: 'N1',
              },
            },
          },
        });
      })
      .done(done);
  });

  it('should parse REMOVE record with no discriminator field', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          hk: '1',
          sk: 'thing',
        },
        oldImage: {
          hk: '1',
          sk: 'thing',
          name: 'N1',
        },
      },
    ]);

    fromDynamodb(events)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            eventID: '0',
            eventName: 'REMOVE',
            eventSource: 'aws:dynamodb',
            awsRegion: 'us-west-2',
            dynamodb: {
              ApproximateCreationDateTime: 1572832690,
              Keys: {
                hk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: undefined,
              OldImage: {
                hk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
                name: {
                  S: 'N1',
                },
              },
              SequenceNumber: '0',
              StreamViewType: 'NEW_AND_OLD_IMAGES',
            },
          },
          event: {
            id: '0',
            type: 'thing-deleted',
            partitionKey: '1',
            timestamp: 1572832690000,
            tags: {
              region: 'us-west-2',
            },
            raw: {
              new: undefined,
              old: {
                hk: '1',
                sk: 'thing',
                name: 'N1',
              },
            },
          },
        });
      })
      .done(done);
  });
});
