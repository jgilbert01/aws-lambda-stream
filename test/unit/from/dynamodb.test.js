import 'mocha';
import { expect } from 'chai';

import { fromDynamodb, toDynamodbRecords } from '../../../src/from/dynamodb';

import { ttl } from '../../../src/utils';

describe('from/dynamodb.js', () => {
  it('should parse INSERT record', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        newImage: {
          pk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'n1',
          // insert in the current region will not have the aws:rep:updateregion field
        },
      },
      // dynamodb stream emits an extra update event as it adorns the 'aws:rep' global table metadata
      // so this extra event should be skipped
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        newImage: {
          'pk': '1',
          'sk': 'thing',
          'discriminator': 'thing',
          'name': 'n1',
          'aws:rep:updateregion': 'us-west-2',
        },
        oldImage: {
          pk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'n1',
          // as mentioned above there was no aws:rep:updateregion field on the insert event
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
                pk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: {
                pk: {
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
                pk: '1',
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
          pk: '1',
          sk: 'thing',
        },
        newImage: {
          'pk': '1',
          'sk': 'thing',
          'discriminator': 'thing',
          'name': 'n1',
          // the current region
          'aws:rep:updateregion': 'us-west-2',
        },
        oldImage: {
          'pk': '1',
          'sk': 'thing',
          'discriminator': 'thing',
          'name': 'N1',
          // previously updated in another region
          'aws:rep:updateregion': 'us-east-1',
        },
      },
      // replicated records emit events as well
      // this replica event should be skipped
      {
        timestamp: 1572832990,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        newImage: {
          'pk': '1',
          'sk': 'thing',
          'discriminator': 'thing',
          'name': 'n1',
          // not the current region
          'aws:rep:updateregion': 'us-east-1',
        },
        oldImage: {
          'pk': '1',
          'sk': 'thing',
          'discriminator': 'thing',
          'name': 'N1',
          // previously updated in current region
          'aws:rep:updateregion': 'us-west-2',
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
                pk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: {
                'pk': {
                  S: '1',
                },
                'sk': {
                  S: 'thing',
                },
                'discriminator': {
                  S: 'thing',
                },
                'name': {
                  S: 'n1',
                },
                'aws:rep:updateregion': {
                  S: 'us-west-2',
                },
              },
              OldImage: {
                'pk': {
                  S: '1',
                },
                'sk': {
                  S: 'thing',
                },
                'discriminator': {
                  S: 'thing',
                },
                'name': {
                  S: 'N1',
                },
                'aws:rep:updateregion': {
                  S: 'us-east-1',
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
                'pk': '1',
                'sk': 'thing',
                'discriminator': 'thing',
                'name': 'n1',
                'aws:rep:updateregion': 'us-west-2',
              },
              old: {
                'pk': '1',
                'sk': 'thing',
                'discriminator': 'thing',
                'name': 'N1',
                'aws:rep:updateregion': 'us-east-1',
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
          pk: '1',
          sk: 'thing',
        },
        oldImage: {
          'pk': '1',
          'sk': 'thing',
          'name': 'N1',
          // deleted in current region
          'aws:rep:updateregion': 'us-west-2',
        },
      },
      // replicated records emit events as well
      // this replica event should be skipped
      {
        timestamp: 1572832990,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        oldImage: {
          'pk': '1',
          'sk': 'thing',
          'discriminator': 'thing',
          'name': 'N1',
          // deleted in another region
          'aws:rep:updateregion': 'us-east-1',
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
                pk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: undefined,
              OldImage: {
                'pk': {
                  S: '1',
                },
                'sk': {
                  S: 'thing',
                },
                'name': {
                  S: 'N1',
                },
                'aws:rep:updateregion': {
                  S: 'us-west-2',
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
                'pk': '1',
                'sk': 'thing',
                'name': 'N1',
                'aws:rep:updateregion': 'us-west-2',
              },
            },
          },
        });
      })
      .done(done);
  });

  it('should parse soft delete record', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        newImage: {
          pk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'n1',
          // the soft delete info
          deleted: true,
          ttl: ttl(1572832690000, 2),
        },
        oldImage: {
          pk: '1',
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
                pk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: {
                pk: {
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
                deleted: {
                  BOOL: true,
                },
                ttl: {
                  N: '1573005490',
                },
              },
              OldImage: {
                pk: {
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
            type: 'thing-deleted',
            partitionKey: '1',
            timestamp: 1572832690000,
            tags: {
              region: 'us-west-2',
            },
            raw: {
              new: {
                pk: '1',
                sk: 'thing',
                discriminator: 'thing',
                name: 'n1',
                deleted: true,
                ttl: 1573005490,
              },
              old: {
                pk: '1',
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

  it('should parse soft undelete record', (done) => {
    const events = toDynamodbRecords([
      {
        timestamp: 1572832690,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        newImage: {
          pk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'n1',
          // clear the soft delete info
          deleted: null,
          ttl: null,
        },
        oldImage: {
          pk: '1',
          sk: 'thing',
          discriminator: 'thing',
          name: 'N1',
          deleted: true,
          ttl: 1573005490,
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
                pk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: {
                pk: {
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
                deleted: {
                  NULL: true,
                },
                ttl: {
                  NULL: true,
                },
              },
              OldImage: {
                pk: {
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
                deleted: {
                  BOOL: true,
                },
                ttl: {
                  N: '1573005490',
                },
              },
              SequenceNumber: '0',
              StreamViewType: 'NEW_AND_OLD_IMAGES',
            },
          },
          event: {
            id: '0',
            type: 'thing-undeleted',
            partitionKey: '1',
            timestamp: 1572832690000,
            tags: {
              region: 'us-west-2',
            },
            raw: {
              new: {
                pk: '1',
                sk: 'thing',
                discriminator: 'thing',
                name: 'n1',
                deleted: null,
                ttl: null,
              },
              old: {
                pk: '1',
                sk: 'thing',
                discriminator: 'thing',
                name: 'N1',
                deleted: true,
                ttl: 1573005490,
              },
            },
          },
        });
      })
      .done(done);
  });

  it('should parse REMOVE record for expired soft delete', (done) => {
    // by default we will assume it is ok, even beneficial,
    // to publish an additional deleted event after the ttl expiration
    // otherwise just filter them in your pipeline
    const events = toDynamodbRecords([
      {
        timestamp: 1573005490,
        keys: {
          pk: '1',
          sk: 'thing',
        },
        oldImage: {
          pk: '1',
          sk: 'thing',
          name: 'N1',
          // the soft delete info
          deleted: true,
          ttl: 1573005490,
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
              ApproximateCreationDateTime: 1573005490,
              Keys: {
                pk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
              },
              NewImage: undefined,
              OldImage: {
                pk: {
                  S: '1',
                },
                sk: {
                  S: 'thing',
                },
                name: {
                  S: 'N1',
                },
                deleted: {
                  BOOL: true,
                },
                ttl: {
                  N: '1573005490',
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
            timestamp: 1573005490000,
            tags: {
              region: 'us-west-2',
            },
            raw: {
              new: undefined,
              old: {
                pk: '1',
                sk: 'thing',
                name: 'N1',
                deleted: true,
                ttl: 1573005490,
              },
            },
          },
        });
      })
      .done(done);
  });
});
