const Writable = require('stream').Writable;
const net = require('net');
const Orchestrator = require('./orchestrator');

module.exports = class Producer extends Writable {
  constructor(topic, options = {}) {
    super({
      objectMode: true,
      decodeStrings: true
    });
    this.orc = new Orchestrator(options.clientId, options.host, options.port);
    this.topic = topic;
    this.responseBuffer = Buffer.alloc(1024);
    this.requestBuffer = Buffer.alloc(16384);
  }

  buildTopicMessage(message) {
    return [
      {
        topic: this.topic,
        data: [
          {
            partition: 0,
            recordSet: [
              {
                magicByte: 1,
                attributes: 0,
                timestamp: new Date().getTime(),
                key: null,
                value: message
              }
            ]
          }
        ]
      }
    ];
  }

  _write(chunk, encoding, cb) {
    if(!Buffer.isBuffer(chunk) && typeof chunk === 'object') {
      try {
        chunk = Buffer.from(JSON.stringify(chunk));
      } catch(err) {
        return cb(err);
      }
    }
    this.orc.initStream(this, (err) => {
      if(err) return cb(err);
      let request = this.orc.request(this.socket, 'Produce', {
        acks: 1,
        timeout: 1000,
        topics: this.buildTopicMessage(chunk)
      }, this.requestBuffer, 0, cb);
    });
  }
}
