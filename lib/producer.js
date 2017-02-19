const Writable = require('stream').Writable;
const net = require('net');
const Protocol = require('kafka-wire-protocol').Protocol;
const Orchestrator = require('./orchestrator');

module.exports = class Producer extends Writable {
  constructor(topic, options = {}) {
    super({
      objectMode: true,
      decodeStrings: true
    });
    this.orc = new Orchestrator(options.clientId, options.host, options.port);
    this.topic = topic;
    this.kp = new Protocol({
      clientId: options.clientId
    });
    this.responseBuffer = Buffer.alloc(1024);
    this.requestBuffer = Buffer.alloc(16384);
  }

  init(cb) {
    if(!this.socket || this.socket.destroyed) {
      this.orc.initTopics([this.topic], (err, topics) => {
        if(err) return cb(err);
        // TODO: Support for multiple partitions
        ({ host: this.host, port: this.port } = topics[this.topic][0]);
        Orchestrator.openSocket(this.host, this.port, (socket) => {
          this.socket = socket;

          socket.on('error', (err) => {
            setImmediate(() => {
              this.emit('error', err);
            });
          });

          this.on('close', () => this.socket.end());
          this.on('finish', () => this.socket.end());

          socket.on('data', this.kp.response);
          cb();
        });
      });
    } else {
      cb();
    }
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
    this.init((err) => {
      if(err) return cb(err);
      let request = this.kp.request('Produce', {
        acks: 1,
        timeout: 1000,
        topics: this.buildTopicMessage(chunk)
      }, this.requestBuffer, 0, cb);
      Orchestrator.write(this.socket, request);
    });
  }
}
