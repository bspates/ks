const Readable = require('stream').Readable;
const net = require('net');
const Orchestrator = require('./orchestrator');

module.exports = class Consumer extends Readable {
  constructor(topic, options) {
    super({
      objectMode: true
    });
    this.orc = new Orchestrator(options.clientId, options.host, options.port);
    this.topic = topic;
    this.responseBuffer = Buffer.alloc(16384);
    this.requestBuffer = Buffer.alloc(1024);
    this.index = options.index || 127;
  }

  init(cb) {
    if(!this.socket || this.socket.destroyed) {
      this.orc.topicSocket(this.topic, (err, socket) => {
        if(err) return cb(err);
        this.socket = socket;
        socket.on('error', (err) => {
          setImmediate(() => {
            this.emit('error', err);
          });
        });
        this.on('close', () => this.socket.end());
        this.on('finish', () => this.socket.end());
        cb();
      });
    } else {
      cb();
    }
  }

  buildRequest() {
    return {
      replicaId: -1,
      maxWaitTime: 1000,
      minBytes: 1024,
      topics: [
        {
          topic: this.topic,
          partitions: [
            {
              partition: 0,
              fetchOffset: this.index,
              maxBytes: 16384
            }
          ]
        }
      ]
    };
  }

  fetch() {
    this.init((err) => {
      if(err) return cb(err);
      this.orc.request(
        this.socket,
        'Fetch',
        this.buildRequest(),
        this.requestBuffer,
        0,
        (err, result) => {
        if(err) return this.emit('error', err);
        for(let response of result.responses) {
          // TODO: Handle multiple partitions
          let partResp = response.partitionResponses[0];
          if(partResp.error.code !== 0) {
            return this.emit('error', response.partitionResponses[0].error);
          }

          if(partResp.recordSet && partResp.recordSet.length && partResp.recordSet.length > 0) {
            this.index += partResp.recordSet.length;
            this.push(partResp.recordSet.map( rec => rec.value ));
          } else {
            setTimeout(() => {
              this.fetch();
            }, 1000);
          }
        }
      });
    });
  }

  _read(size) {
    this.fetch();
  }
}
