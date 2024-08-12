const axios = require('axios');
const { DATABASE_SCHEMA, DATABASE_URL, SHOW_PG_MONITOR } = require('./config');
const massive = require('massive');
const monitor = require('pg-monitor');

// Call start
(async () => {
    console.log('main.js: before start');

    const db = await massive({
        connectionString: DATABASE_URL,
        ssl: { rejectUnauthorized: false },
    }, {
        // Massive Configuration
        scripts: process.cwd() + '/migration',
        allowedSchemas: [DATABASE_SCHEMA],
        whitelist: [`${DATABASE_SCHEMA}.%`],
        excludeFunctions: true,
    }, {
        // Driver Configuration
        noWarnings: true,
        error: function (err, client) {
            console.log(err);
            // process.emit('uncaughtException', err);
            // throw err;
        }
    });

    if (!monitor.isAttached() && SHOW_PG_MONITOR === 'true') {
        monitor.attach(db.driverConfig);
    }

    const execFileSql = async (schema, type) => {
        return new Promise(async resolve => {
            const objects = db['user'][type];

            if (objects) {
                for (const [key, func] of Object.entries(objects)) {
                    console.log(`executing ${schema} ${type} ${key}...`);
                    await func({
                        schema: DATABASE_SCHEMA,
                    });
                }
            }

            resolve();
        });
    };

    //public
    const migrationUp = async () => {
        return new Promise(async resolve => {
            await execFileSql(DATABASE_SCHEMA, 'schema');

            //cria as estruturas necessarias no db (schema)
            await execFileSql(DATABASE_SCHEMA, 'table');
            await execFileSql(DATABASE_SCHEMA, 'view');

            console.log(`reload schemas ...`)
            await db.reload();

            resolve();
        });
    };

    try {
        await migrationUp();

        const { data: { data, source } } = await axios.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population');

        const formattedData = JSON.stringify(data);

        //exemplo de insert
        const result1 = await db[DATABASE_SCHEMA].api_data.insert({
            api_name: source[0].name,
            doc_id: source[0].annotations.table_id,
            doc_name: source[0].annotations.dataset_name,
            doc_record: formattedData,
        })

        console.log('result1 >>>', result1);

        //exemplo select
        const result2 = await db[DATABASE_SCHEMA].api_data.find({
            is_active: true
        });
        console.log('result2 >>>', result2);

        // cálculo da somatória total de população em memória
        const records = result2[0].doc_record

        const yearsOfInterest = [2020, 2019, 2018];
        const filteredData = records.filter(item => yearsOfInterest.includes(Number(item.Year)));

        const sumPopulationInMemory = filteredData.reduce((prev, curr) => {
            return prev + curr.Population;
        }, 0);

        // calculo da somatória total de população usando select inline
        const [sumPopulationUsingSelect] = await db.query(`
            SELECT SUM((records->>'Population')::integer) AS total_population
            FROM (
                SELECT jsonb_array_elements(doc_record) AS records
                FROM ${DATABASE_SCHEMA}.api_data
            )
            WHERE records->>'Year' IN ('2020', '2019', '2018');

        `);

        // calculo da somatória total de população usando view
        const [sumPopulationUsingView] = await db[DATABASE_SCHEMA].vw_population_summary.find();

        console.log('Somatória total de população feita em memória:', { total_population: sumPopulationInMemory });
        console.log('Somatória total de população feita usando select inline::', sumPopulationUsingSelect);
        console.log('Somatória total de população feita usando view::', sumPopulationUsingView);

    } catch (e) {
        console.log(e.message)
    } finally {
        console.log('finally');
    }
    console.log('main.js: after start');
})();