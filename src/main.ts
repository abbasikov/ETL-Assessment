import * as fs from 'fs';
import * as zlib from 'zlib';
import * as url from 'url';
import * as path from 'path';

export function transformJSON(input, output) {
  return new Promise<void>((resolve, reject) => {
    fs.readFile(input, (err, data) => {
      if (err) {
        reject(err);
        return;
      }
      zlib.gunzip(data, (err, dezipped) => {
        if (err) {
          reject(err);
          return;
        }
        const jsonData = JSON.parse(dezipped.toString());
        const { ts, u, e } = jsonData;

        const urlData = url.parse(u);
        const queryData = new URLSearchParams(urlData.search);
        const queryObject = Object.fromEntries(queryData);

        const outputData = {
          timestamp: ts,
          url_object: {
            domain: urlData.hostname,
            path: urlData.pathname,
            query_object: queryObject,
            hash: urlData.hash,
          },
          ec: e,
        };

        const outputJSON = JSON.stringify(outputData);
        fs.writeFile(output, outputJSON, (err) => {
          if (err) {
            reject(err);
            return;
          }
          console.log(`saved to ${output}`);
          resolve();
        });
      });
    });
  });
}

async function readFile() {
  try {
    const filePath = path.resolve('input');
    const files = await fs.promises.readdir(filePath);
    const filteredFiles = files.filter((file) => path.extname(file) === '.gz');
    console.log(`Total files present in this folder: ${filteredFiles.length}`);

    const outputDir = path.resolve('output');

    await fs.promises.mkdir(outputDir, { recursive: true });

    const promises = filteredFiles.map((e) => {
      const input = path.resolve('input', e);
      const output = path.resolve(outputDir, `${e}.output.json`);
      return transformJSON(input, output);
    });

    await Promise.all(promises);
    console.log('All files have been transformed');
  } catch (e) {
    console.log(e);
  }
}
readFile();
