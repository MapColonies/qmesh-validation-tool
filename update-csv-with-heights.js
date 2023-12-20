const fs = require('fs');

// const FILE_NAME = 'dtm_aza_ai_2022_wg84geo_05';
const FILE_NAME = 'vricon_dtm_wgs84Geo_orthometric_z';
const DEM_HEIGHTS_TOKEN = 'eyJhbGciOiJSUzI1NiIsImtpZCI6Im1hcC1jb2xvbmllcy1pbnQifQ.eyJkIjpbInJhc3RlciIsInJhc3RlcldtcyIsInJhc3RlckV4cG9ydCIsImRlbSIsInZlY3RvciIsIjNkIl0sImlhdCI6MTY3NDYzMjM0Niwic3ViIjoibWFwY29sb25pZXMtYXBwIiwiaXNzIjoibWFwY29sb25pZXMtdG9rZW4tY2xpIn0.D1u28gFlxf_Z1bzIiRHZonUgrdWwhZy8DtmQj15cIzaABRUrGV2n_OJlgWTuNfrao0SbUZb_s0_qUUW6Gz_zO3ET2bVx5xQjBu0CaIWdmUPDjEYr6tw-eZx8EjFFIyq3rs-Fo0daVY9cX1B2aGW_GeJir1oMnJUURhABYRoh60azzl_utee9UdhDpnr_QElNtzJZIKogngsxCWp7tI7wkTuNCBaQM7aLEcymk0ktxlWEAt1E0nGt1R-bx-HnPeeQyZlxx4UQ1nuYTijpz7N8poaCCExOFeafj9T7megv2BzTrKWgfM1eai8srSgNa3I5wKuW0EyYnGZxdbJe8aseZg';
// const DEM_HEIGHTS_URL = `https://dem-int-heights-poc-production-nginx-route-integration.apps.j1lk3njp.eastus.aroapp.io/api/heights/v1/points?token=${DEM_HEIGHTS_TOKEN}`;
const DEM_HEIGHTS_URL = `http://localhost:9000/points?token=${DEM_HEIGHTS_TOKEN}`;
const FILE_EXTENSION = '.csv';
// const CSV_DELIMITER = '\r\n';  // <-For Windows
const CSV_DELIMITER = '\n';  // <-For Linux
const CSV_COLUMN_SEPARATOR = ',';
const DATA_DIR = 'data';
const OUTPUT_DIR = 'output';
const OUTPUT_FILE_NAME = `${OUTPUT_DIR}/${FILE_NAME}_UPDATED${FILE_EXTENSION}`;
const COLUMN_HEADER_NAME_FOR_HEIGHT_DATA = 'HEIGHT';
const COLUMN_HEADER_NAME_FOR_DIFFERENCE = 'DIFFERENCE (ABS)';
const COLUMN_HEADER_NAME_FOR_MAX_DIFFERENCE = 'MAX DIFFERENCE (ABS)';
const LATITUDE_COLUMN_IN_CSV = 'Y';
const LONGITUDE_COLUMN_IN_CSV = 'X';
const REFERENCE_COLUMN_IN_CSV = 'Z'
const NUMBER_OF_TRIES = 10;
const BATCH_SIZE = 50;

function wait(delay) {
  return new Promise(resolve => setTimeout(resolve, delay));
}

function fetchRetry(url, delay, tries = NUMBER_OF_TRIES, fetchOptions = {}, totalTimeStart = performance.now()) {

  function onError(err) {
    let triesLeft = tries - 1;
    if (!triesLeft) {
      throw err;
    }
    return wait(delay).then(() => fetchRetry(url, delay, triesLeft, fetchOptions, totalTimeStart));
  }

  return fetch(url, fetchOptions)
    .then(data => {
      // let totalTimeEnd = performance.now();
      // process.stdout.write(`\u001B[33mTotal Fetch with ${NUMBER_OF_TRIES - tries} retries took: ${totalTimeEnd - totalTimeStart}\u001B[0m\n\r`);
      return data.json();
    })
    .catch(e => {
      return onError(e);
    });
}

async function queryDemHeights(positions) {
  try {
    const res = await fetchRetry(DEM_HEIGHTS_URL, 3000, NUMBER_OF_TRIES, {
      method: 'POST',
      body: JSON.stringify({ positions }),
      headers: { 'Content-Type': 'application/json' }
    });
    return res.data;
  } catch(e) {
    fs.writeFileSync(`${OUTPUT_DIR}/ERRORED_BATCH.json`, JSON.stringify(positions));
    console.error('Fetch ERROR ->', e);
  }
}

function makeBatches(dataArr, countPerBatch) {
  const data = [...dataArr];
  const outputArr = [];
  while (data.length > 0) {
    outputArr.push(data.splice(0, countPerBatch));
  }
  return outputArr;
}

function parseCsvToObject(fileName) {
  const csvString = fs.readFileSync(fileName, 'UTF-8');
  const rows = csvString.split(CSV_DELIMITER);
  rows[0] += `,${COLUMN_HEADER_NAME_FOR_HEIGHT_DATA}`;
  const headers = rows[0].split(CSV_COLUMN_SEPARATOR);
  const data = rows.map(row => row.split(CSV_COLUMN_SEPARATOR));
  const dataWithoutHeaders = data.slice(1);
  const output = dataWithoutHeaders.map(row => {
    const obj = {};
    headers.forEach((header, i) => {
      obj[header] = row[i];
    });
    return obj;
  });
  return output;
}

function sortBy(objArr, param1, param2) {
  return objArr.sort((a, b) => {
    if (+a[param1] < +b[param1]) return -1;
    if (+a[param1] > +b[param1]) return 1;
    if (+b[param2] < +a[param2]) return -1;
    if (+b[param2] > +a[param2]) return 1;
    return 0;
  });
}

async function queryBatches(batches) {
  let maxDiff = 0;
  
  for (let i = 0; i < batches.length; i++) {
    const batch = sortBy(batches[i], LATITUDE_COLUMN_IN_CSV, LONGITUDE_COLUMN_IN_CSV);
    const positions = batch.map(data => ({ latitude: +data[LATITUDE_COLUMN_IN_CSV], longitude: +data[LONGITUDE_COLUMN_IN_CSV] }));

    try {
      const heights = await queryDemHeights(positions);
      process.stdout.write(`\u001B[33mIn Progress: ${Math.floor(((i+1) / batches.length) * 100)}%\u001B[0m\r`);
      const sortedHeightsData = sortBy(heights, 'latitude', 'longitude');
      for (let j = 0; j < batch.length; j++) {
        batch[j][COLUMN_HEADER_NAME_FOR_HEIGHT_DATA] = sortedHeightsData?.[j]?.height;
        if (!!REFERENCE_COLUMN_IN_CSV) {
          const difference = Math.abs(sortedHeightsData?.[j]?.height - batch[j][REFERENCE_COLUMN_IN_CSV]);
          batch[j][COLUMN_HEADER_NAME_FOR_DIFFERENCE] = difference;
          if (difference) {
            maxDiff = Math.max(maxDiff, difference);
          }
        }
      }
    } catch (e) {
      console.error(e);
    }
  }
  
  if (!!REFERENCE_COLUMN_IN_CSV) {
    batches[0][0][COLUMN_HEADER_NAME_FOR_MAX_DIFFERENCE] = maxDiff;
  }
  
  return batches;
}

function parseObjArrToCSVString(objArr) {
  let finalCsvString = '';
  const headers = Object.keys(objArr[0]);
  const headerCsvData = headers.join(CSV_COLUMN_SEPARATOR);
  finalCsvString += headerCsvData + CSV_DELIMITER;
  
  objArr.forEach(rowData => {
    const values = Object.values(rowData);
    const rowCsvData = values.join(CSV_COLUMN_SEPARATOR);
    finalCsvString += rowCsvData + CSV_DELIMITER;
  });
  
  return finalCsvString;
}

const batches = makeBatches(parseCsvToObject(`${DATA_DIR}/${FILE_NAME}${FILE_EXTENSION}`), BATCH_SIZE);
queryBatches(batches).then(data => {
  fs.writeFileSync(OUTPUT_FILE_NAME, parseObjArrToCSVString(data.flat(1)));
  process.stdout.write(`\u001B[32m\nDone!\n${OUTPUT_FILE_NAME}\u001B[0m\r\n`);
});
