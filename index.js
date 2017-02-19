var Producer = require('./lib/producer');
var Consumer = require('./lib/consumer');

// var p = new Producer('woot', {
//   clientId: 'wooty',
//   host: 'broker0',
//   port: 9092
// });

var c = new Consumer('woot', {
  clientId: 'way',
  host: 'broker0',
  port: 9092
});

c.on('data', (chunk) => {
  console.log(JSON.stringify(chunk, null, 2));
});

c.on('error', (err) => {
  throw err
});

// p.write('one', (err, res) => {
//   console.log('one done');
//   if(err) throw err;
//   if(res) console.log(res);
// });
//
// p.write('two', (err, res) => {
//   console.log('two done');
//   if(err) throw err;
//   if(res) console.log(res);
// });
//
// p.write('three', (err, res) => {
//   console.log('three done');
//   if(err) throw err;
//   if(res) console.log(res);
// });
//
// p.end({
//   welp: true
// }, (err, res) => {
//   console.log('four done');
//   if(err) throw err;
//   if(res) console.log(res);
// });
