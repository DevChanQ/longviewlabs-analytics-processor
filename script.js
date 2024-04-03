const fs = require("fs");
const path = require('path');
const csv = require('csv-parser');

const column_blacklist = ['hashtag clicks', 'detail expands', 'permalink clicks', 'email tweet', 'dial phone'];

const argv = process.argv;
if (argv.length !== 3) process.exit(1);

const file = argv.slice(-1).pop();
const absPath = path.resolve(file);
const results = [];

fs.createReadStream(absPath)
  .pipe(csv())
  .on('data', (data) => results.push(data))
  .on('end', () => { processAnalytics(results) });

const processAnalytics = (records) => {
  console.log(records.map(processRecord));
};

const formatDate = (date) => {
  let year = date.getFullYear();
  let month = (1 + date.getMonth()).toString().padStart(2, '0');
  let day = date.getDate().toString().padStart(2, '0');

  return month + '/' + day + '/' + year;
}

const processRecord = record => {
  const r = { ...record };

  // format time to MM/DD/YYYY
  r['time'] = formatDate(new Date(r['time']))

  // remove specified columns & promoted tweet related column
  for (let key in r) {
    if (column_blacklist.includes(key) || key.includes("promoted")) {
      delete r[key];
    }
  }

  return r;
}