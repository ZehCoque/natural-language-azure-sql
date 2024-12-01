import sql from 'mssql';
import fs from 'fs';
import csv from 'csv-parser';
import path from 'path';
import "dotenv/config"
import { SQL_CONFIG } from './sql-config';

function parseDate(dateString: string): string {
  const parts = dateString.split('/');
  if (parts.length === 3) {
    const day = parts[0].padStart(2, '0');
    const month = parts[1].padStart(2, '0');
    const year = parts[2];
    return `${year}-${month}-${day}`;
  }
  console.warn(`Could not parse date: ${dateString}`);
  throw Error();
}

export async function seed() {
  try {
    const connection = await sql.connect(SQL_CONFIG);

    const createTable = await sql.query(`
      IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='unicorns' and xtype='U')
      CREATE TABLE unicorns (
        id INT IDENTITY(1,1) PRIMARY KEY,
        company VARCHAR(255) NOT NULL UNIQUE,
        valuation DECIMAL(10, 2) NOT NULL,
        date_joined DATE,
        country VARCHAR(255) NOT NULL,
        city VARCHAR(255) NOT NULL,
        industry VARCHAR(255) NOT NULL,
        select_investors TEXT NOT NULL
      )
    `);

    console.log(`Created "unicorns" table`);

    const results: any[] = [];
    const csvFilePath = path.join(process.cwd(), 'unicorns.csv');

    await new Promise((resolve, reject) => {
      fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', (data) => results.push(data))
        .on('end', resolve)
        .on('error', reject);
    });

    for (const row of results) {
      const formattedDate = parseDate(row['Date Joined']);
      
      await sql.query`
        IF NOT EXISTS (SELECT 1 FROM unicorns WHERE company = ${row.Company})
        INSERT INTO unicorns (company, valuation, date_joined, country, city, industry, select_investors)
        VALUES (
          ${row.Company},
          ${parseFloat(row['Valuation ($B)'].replace('$', '').replace(',', ''))},
          ${formattedDate},
          ${row.Country},
          ${row.City},
          ${row.Industry},
          ${row['Select Investors']}
        )
      `;
    }

    console.log(`Seeded ${results.length} unicorns`);
    
    await connection.close();

    return {
      createTable,
      unicorns: results,
    };
  } catch (e: any) {
    console.error(e);
  }
}


seed().catch(console.error);