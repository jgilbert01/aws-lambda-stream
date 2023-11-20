import 'mocha';
import { expect } from 'chai';
import sinon from 'sinon';

import { MOCK_GEN_DK_RESPONSE } from 'aws-kms-ee';

import {
  zip, unzip, compress, decompress,
} from '../../../src/utils';

describe('utils/compression.js', () => {
  afterEach(sinon.restore);

  it('should zip and unzip', () => {
    const event = JSON.stringify(eventObject);
    const compressed = zip(event);
    const decompressed = unzip(compressed);

    // console.log('event.length: ', event.length); // 3317
    // console.log('compressed.length: ', compressed.length); // 756
    // console.log('ratio: ', 100 - (compressed.length / event.length) * 100); // 77%
    // console.log('compressed: ', compressed);
    // console.log('decompressed: ', decompressed);

    expect(event.length).to.equal(3317);
    expect(compressed.length).to.equal(756);
    expect(decompressed).to.deep.equal(event);
  });

  it('should compress and decompress', () => {
    const event = JSON.stringify(eventObject);
    const compressed = JSON.stringify(eventObject, compress({ compressionThreshold: 512 }));
    const decompressed = JSON.parse(compressed, decompress);

    // console.log('event.length: ', event.length); // 3317
    // console.log('compressed.length: ', compressed.length); // 1097
    // console.log('ratio: ', 100 - (compressed.length / event.length) * 100); // 67%
    // console.log('compressed: ', JSON.stringify(JSON.parse(compressed), null, 2));
    // console.log('decompressed: ', JSON.stringify(decompressed, null, 2));

    expect(event.length).to.equal(3317);
    expect(compressed.length).to.equal(1097);
    expect(decompressed).to.deep.equal(eventObject);
  });


  it('should NOT compress and decompress', () => {
    const event = JSON.stringify(eventObject);
    const compressed = JSON.stringify(eventObject, compress({ compressionThreshold: 1024 * 10 }));
    const decompressed = JSON.parse(compressed, decompress);

    // console.log('compressed: ', JSON.stringify(JSON.parse(compressed), null, 2));
    // console.log('decompressed: ', JSON.stringify(decompressed, null, 2));

    expect(event.length).to.equal(3317);
    expect(compressed.length).to.equal(3317);
    expect(decompressed).to.deep.equal(eventObject);
  });

  it('should compress with ignored keys', () => {
    const sampleEvent = {
      a: {
        b: {
          a: {
            c: 0,
          },
          d: 1,
        },
      },
    };

    const compressed = JSON.stringify(
      sampleEvent,
      compress({
        compressionThreshold: 1,
        compressionIgnore: ['a', 'b'],
      }),
    );
    const decompressed = JSON.parse(compressed, decompress);

    expect(JSON.parse(compressed)).to.deep.equal({
      a: {
        b: {
          a: { c: 'COMPRESSEDH4sIAAAAAAAAAzMAACHf2/QBAAAA' },
          d: 'COMPRESSEDH4sIAAAAAAAAAzMEALfv3IMBAAAA',
        },
      },
    });
    expect(decompressed).to.deep.equal(sampleEvent);
  });
});

const eventObject = {
  id: 'c92c473a-a968-11ed-afa1-0242ac120002', // v1
  type: 'thing-created',
  partitionKey: '7627955f-643f-4c85-98b3-efc94f5fbd67', // v4
  timestamp: 1574032690000,
  tags: {
    region: 'us-west-2',
  },
  raw: {
    new: {
      pk: '7627955f-643f-4c85-98b3-efc94f5fbd67', // same as partitionKey
      sk: 'thing',
      discriminator: 'thing',
      name: 'bjE=',
      description: 'ZDE=',
      status: 's1',
      stuff: {
        // beefing up the size of the event
        'us-west-1': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64').replace('A', 'B'),
        'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
        'us-west-3': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64').replace('A', 'C'),
        'us-west-4': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
        'us-west-5': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64').replace('A', 'D'),
        'us-west-6': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
        'us-west-7': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64').replace('A', 'E'),
        'us-west-8': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
        'us-west-9': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64').replace('A', 'F'),
        'us-west-10': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
      },
    },
  },
  eem: {
    dataKeys: {
      'us-west-2': MOCK_GEN_DK_RESPONSE.CiphertextBlob.toString('base64'),
    },
    masterKeyAlias: 'alias/aws-kms-ee',
    fields: [
      'name',
      'description',
    ],
  },
};
