import 'mocha';
import { expect } from 'chai';

import { fromS3, toS3Records } from '../../../src/from/s3';

describe('from/s3s.js', () => {
  it('should parse records', (done) => {
    const event = toS3Records([
      {
        bucket: {
          name: 'b1',
        },
        object: {
          key: 'k1',
        },
      },
    ]);

    fromS3(event)
      .collect()
      .tap((collected) => {
        // console.log(JSON.stringify(collected, null, 2));

        expect(collected.length).to.equal(1);
        expect(collected[0]).to.deep.equal({
          record: {
            eventSource: 'aws:s3',
            awsRegion: 'us-west-2',
            responseElements: {
              'x-amz-request-id': '0000000000000000',
            },
            s3: {
              bucket: {
                name: 'b1',
              },
              object: {
                key: 'k1',
              },
            },
          },
        });
      })
      .done(done);
  });
});
